# Architecture — kafka-model-enricher

How the system is put together, and why each mechanism exists. Requirements it
serves are in `docs/BUSINESS_REQUIREMENTS.md`; the build order is in
`docs/WORKPLAN.md`.

---

## 1. Shape of the pipeline

```
                        ┌─────────────────── pause() / resume() ───────────────────┐
                        │                                                          │
                        ▼                                                          │
  events.raw ──▶ poll loop ──▶ admission (bounded in-flight window) ──▶ dispatcher │
                        │            permits: records + bytes              │       │
                        │                                                  ▼       │
                        │                                         normalise + redact
                        │                                                  │
                        │                                    ┌─────────────┴──────┐
                        │                                    │  result cache      │
                        │                                    │  (content hash)    │
                        │                                    └──hit──┬──miss──────┘
                        │                                            │            │
                        │                                       batcher (N/call)  │
                        │                                            │            │
                        │                                  concurrency limiter    │
                        │                                    (adaptive + bucket)  │
                        │                                            │            │
                        │                                    ModelClient ──▶ Ollama
                        │                                            │      └▶ Fake
                        │                                            ▼            │
                        │                                     schema validate ◀───┘
                        │                                       ok │ fail
                        │                                          │  └▶ error classifier
                        │                                          │       retryable ─▶ backoff
                        │                                          │                    └▶ events.retry
                        │                                          │       terminal  ─▶ events.dlq
                        │                                          ▼                      (+ envelope)
                        │                                    producer ─▶ events.enriched
                        │                                          │
                        └──────── OffsetTracker ◀──────────────────┘
                             contiguous watermark commit
```

Every arrow that leaves the diagram is one of the **three doors** of NFR-1:
`events.enriched`, `events.dlq`, or still in flight. `events.retry` is not a door —
records there re-enter through the poll loop.

## 2. Topics

| Topic | Purpose | Key |
|---|---|---|
| `events.raw` | Input. Domain events as JSON. | Event/entity id |
| `events.enriched` | Output. Original payload plus an `enrichment` object and provenance. | Same as input — partitioning is preserved |
| `events.retry` | Records whose retry backoff is long enough that holding an in-flight permit would be wasteful. Consumed with a delay. | Same as input |
| `events.dlq` | Terminal failures, with a replay envelope. | Same as input |

Preserving the key on the output topic (FR-4) matters: a downstream join or
compacted table keyed the same way keeps working, and enriched records land on the
same partition number as their source.

## 3. Modules

Maven multi-module. The dependency rule is one-directional and enforced by the
build, not by convention.

| Module | Contains | Depends on |
|---|---|---|
| `enricher-core` | `OffsetTracker`, in-flight window, concurrency limiter, retry policy, error classifier, batcher, cache, and the `ModelClient` / `RecordSink` ports | Nothing but the JDK and a JSON library |
| `enricher-model` | `OllamaModelClient`, `FakeModelServer`, prompt templates, response schema | `enricher-core` |
| `enricher-kafka` | Poll loop, pause/resume, `ConsumerRebalanceListener`, producer, DLQ writer, serdes | `enricher-core` |
| `enricher-app` | Spring Boot wiring, configuration, Actuator/Micrometer, health | all of the above |
| `enricher-replay` | Dead-letter inspection and replay CLI | `enricher-core`, `enricher-kafka` |
| `loadgen` | Open-loop generator and latency measurement | `enricher-kafka` |

### 3.1 Why `enricher-core` has no Kafka and no Spring

**Invariant 3 in `CLAUDE.md`.** The offset watermark and the backpressure window
are the two pieces of this system where a subtle bug loses data without raising an
error. They need property tests that run thousands of randomised interleavings in
milliseconds. That is only possible if the logic is plain Java objects with no
broker, no container, and no clock it does not own.

The moment `OffsetTracker` takes a `TopicPartition`, its test needs the Kafka
client on the classpath; the moment it takes a `@Component`, its test needs a
context. Both are small steps that end with the correctness core only being
testable through an integration test that takes 40 seconds and cannot enumerate
interleavings. Same discipline as `engine-core` in `limit-order-book` and
`tsdb-core` in `tsdb`.

`enricher-core` defines its own `PartitionRef(String topic, int partition)` and
`SourceRecord` types. `enricher-kafka` adapts.

## 4. The three mechanisms that carry the design

### 4.1 `OffsetTracker` — commit the contiguous watermark

Model calls complete out of order: offset 105 may finish before 101. Committing
105 at that moment tells Kafka that 101–104 are done. If the process dies, they
are never reprocessed. **That is silent data loss, and it is the single most
likely way this project could quietly be wrong.**

Per partition, the tracker holds:

- `committed` — the watermark; every offset below it is durably done.
- `inFlight` — offsets admitted but not yet completed.
- `completed` — offsets finished above the watermark, waiting for their
  predecessors.

On completion of offset *n*, *n* moves into `completed`; the watermark then
advances across the longest contiguous run of completed offsets, which are dropped
from the map. Only the watermark is ever committed.

The properties, checked with jqwik in Phase 2:

1. The watermark is monotonically non-decreasing.
2. The watermark never exceeds the lowest in-flight offset.
3. After every record admitted has completed, the watermark equals the highest
   offset admitted, plus one.
4. On partition revocation, everything for that partition is discarded atomically
   and never contributes to a later commit.

Memory is bounded because `completed` can only hold offsets above the watermark,
and admission caps the number of records above the watermark.

### 4.2 Backpressure is `pause()` / `resume()`

The application must keep calling `poll()` — that is what sends heartbeats under
the modern consumer and what keeps group membership alive. But it must not be
*given* records it has no capacity for.

`KafkaConsumer.pause(partitions)` is the primitive that separates those two
things. Paused partitions return no records while `poll()` continues to run
normally. So the loop becomes:

```
loop:
  if window.saturated() and not paused:  consumer.pause(assignment)
  if window.hasHeadroom() and paused:    consumer.resume(assignment)
  records = consumer.poll(shortTimeout)     # always called, always returns fast
  for r in records: window.admit(r)         # never blocks — capacity checked above
  commit(offsetTracker.watermarks())
```

**Never block inside the loop** (invariant 2). Sleeping until capacity frees up,
or calling `future.get()` on a model call, is the direct cause of failure path B
in the requirements: `max.poll.interval.ms` is exceeded, the member is evicted,
its partitions are reassigned, the in-flight work is reprocessed by someone else,
and the resulting extra load evicts the next member too.

The consumer's own `max.poll.records` is a *secondary* control here, not the
mechanism. It bounds one batch; it does not bound accumulated in-flight work.

### 4.3 Bounded in-flight window

A two-dimensional semaphore: permits in **records** and in **bytes**. Record
count alone is not enough, because a hundred 400 KB events is a very different
heap footprint from a hundred 400-byte events, and the pathological-payload case
from requirement 2.5 is exactly the one that matters.

Nothing is admitted without permits; permits are released on completion — which
means on reaching a door, not on the model returning. This makes heap independent
of consumer lag (NFR-2), which is the property that lets the application sit
calmly behind a hundred million record backlog.

## 5. Handling model non-determinism

A response passes through three gates before it is trusted:

1. **Parse.** The response must be JSON. Ollama's `format` parameter constrains
   generation, but it is a constraint, not a guarantee; truncation still happens.
2. **Validate.** The parsed object is checked against the declared JSON schema for
   the enrichment. Missing required field, wrong type, out-of-enum category — all
   are failures.
3. **Attribute.** The record is stamped with `model.name`, `model.digest` and
   `prompt.version`.

A failure at gate 1 or 2 goes to the error classifier like any other failure. The
classifier's judgement is deliberately conservative: an unparseable response from
a healthy endpoint is treated as **retryable a small number of times** (sampling
variance often fixes it), then terminal. A schema violation that recurs across
attempts is terminal, because it usually means the prompt and the schema have
drifted apart, and burning retries on it is exactly the cost sink from
requirement 2.3.

### 5.1 Provenance is not optional

Every enriched record carries:

```json
"_meta": {
  "model": "llama3.2:1b",
  "model_digest": "sha256:…",
  "prompt_version": "support-v3",
  "schema_version": 2,
  "enriched_at": "2026-09-03T20:00:00Z",
  "dedupe_key": "events.raw/3/104857",
  "cache_hit": false,
  "attempts": 1
}
```

Without this, a downstream anomaly six months later is unattributable and the only
remedy is re-enriching everything. With it, "which records were produced by the
prompt version we have since found to be wrong" is a query.

## 6. Dead-letter envelope and replay

The envelope must be sufficient to replay **without the original stream**
(invariant 5), because by the time anyone looks, retention may have expired.

```
key:     original key bytes, unchanged
value:   original value bytes, unchanged
headers: dlq.source.topic, dlq.source.partition, dlq.source.offset,
         dlq.source.timestamp, dlq.original.headers (packed),
         dlq.error.class, dlq.error.message, dlq.error.stage,
         dlq.attempts, dlq.first.failed.at, dlq.final.failed.at,
         dlq.model, dlq.prompt.version, dlq.schema.version,
         dlq.app.version
```

The original bytes are preserved verbatim rather than a re-serialised object,
because a deserialisation failure (FR-13) has no object to serialise, and because
re-serialisation can mask the very corruption being investigated.

`enricher-replay` reads the topic and offers: filter by error class, time range or
source partition; `--dry-run` that reports what would be republished; a rate limit
so a replay of two million records cannot starve live traffic (requirement 2.6);
and a quarantine list for records that have already failed replay.

## 7. Concurrency control

Two independent controls, because they answer different questions:

- **Token bucket** — "am I allowed to send this now?" Models a hard provider
  quota. Configured, not learned.
- **Adaptive limiter** (AIMD, with a gradient variant to compare) — "how many
  should I have in flight?" Learned from observed latency and error rate.
  Additive increase while latency is stable; multiplicative decrease on a 429, a
  timeout, or a latency inflection.

The local Ollama backend has no quota but does have a hard concurrency ceiling and
a latency curve that degrades sharply past it, so the adaptive limiter is
exercised for real rather than only against the fake.

## 8. Batching

Two axes, both optional and independently configurable:

- **Micro-batch size** — N events per model call, filled by size or by a linger
  deadline, whichever comes first. Amortises system-prompt tokens (requirement
  2.3).
- **Adaptive batch size** — grows while per-event latency improves, shrinks when
  the batch starts to hurt tail latency or hit token limits.

**Per-record failure attribution is mandatory** (FR-7). Each event in a batch
carries a stable index; the response must return results tagged with those
indices. A response that is missing an index fails only that record. This is the
difference between a batch of ten losing one record and a batch of ten losing ten,
and it is the most common way batching is implemented wrongly.

## 9. Infrastructure

Docker Compose, one file per concern, combined via profiles. No Bitnami images —
the free catalogue was restricted in 2025; `apache/*` and Docker Official images
only. Every service declares explicit `cpus` and `mem_limit`, so that measurements
taken on one day compare to measurements taken on another.

| Profile | Services |
|---|---|
| `core` | Kafka (KRaft, single broker), topic bootstrap |
| `ollama` | `ollama/ollama` plus a one-shot model pull |
| `obs` | Prometheus, Grafana, OpenTelemetry collector |
| `ui` | Topic and dead-letter browser |
| `app` | The enricher itself |

## 10. Measurement discipline

Carried over from `kafka-pulsar-bench`, because the same class of error applies:

- **Latency is measured from the intended send time**, not the actual one.
  Measuring from the actual send time is coordinated omission, and it hides
  precisely the stalls this project exists to survive.
- **Percentiles are never averaged.** HdrHistograms are merged, then percentiles
  are computed from the merged histogram. A mean of p99s is not a p99.
- A run in which the generator could not sustain its target rate is **flagged
  invalid** and excluded, not quietly reported.

## 11. Deliberate omissions

- **No schema registry / Avro** at the start. JSON keeps the failure modes visible
  and the barrier to running it low. Revisit if the enrichment schema starts
  changing often.
- **No distributed cache** at the start. An in-process cache measures the hit rate
  and proves the value; Redis is a Phase 5 stretch once the number justifies it.
- **No Kafka Streams.** The whole subject of the project is steering backpressure
  and offset commit manually. The DSL's threading model would take that away.
