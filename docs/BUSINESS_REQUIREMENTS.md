# Business requirements — kafka-model-enricher

Status: baseline, written at Phase 0 (2026-09-03). Changes to sections 3, 5 or 7
require an ADR.

---

## 1. The problem in one paragraph

An organisation already has an event stream — support tickets, transactions,
product reviews, insurance claims, chat transcripts, application logs. It now
wants fields on those events that only a language model can produce: a category,
a sentiment, an urgency, extracted entities, redacted PII, a summary. The obvious
implementation is a Kafka consumer that calls the model inline and produces the
result. That implementation works on a laptop with a hundred events and fails in
production, in ways whose symptoms point at Kafka rather than at the design.

## 2. Why this is a serious problem and not a toy

### 2.1 The rate mismatch is three to six orders of magnitude

Every other stage in a Kafka pipeline is a pure function measured in microseconds
and costing nothing. This one stage is:

| Property | Normal stream stage | Model call |
|---|---|---|
| Latency | 1–100 µs | 200 ms – 30 s |
| Throughput ceiling | ~ broker-bound | 1–100 req/s, hard quota |
| Cost per invocation | 0 | metered, real money |
| Determinism | total | none |
| Failure modes | serialisation | timeout, 429, 5xx, refusal, malformed output, truncation |

A partition delivering 5,000 events/s into a stage that serves 5 req/s is not a
tuning problem. There is no value of `max.poll.records`, no thread pool size, and
no consumer setting that makes the naive design survive it. The architecture has
to change.

### 2.2 The failure mode is catastrophic, and it lies about its cause

The naive consumer fails along one of two paths, and both are worse than a crash:

**Path A — unbounded async.** Someone notices the calls are slow and makes them
asynchronous, collecting futures. Nothing bounds the collection. Heap grows with
lag. The process OOMs hours later, under load, having committed offsets for work
that never completed. **Data is lost silently.**

**Path B — blocking poll loop.** The call is left synchronous. The loop spends
longer than `max.poll.interval.ms` between polls. The group coordinator concludes
the member is dead and revokes its partitions. The partitions move to another
instance, which starts from the last committed offset and **reprocesses
everything already in flight** — doubling load on the model, which slows down,
which causes the next member to be evicted. This is a **rebalance storm**. The
consumer group thrashes, lag climbs, the model bill climbs, and every log line
points at Kafka.

Neither path produces an error that names the real cause. That is what makes this
worth building carefully once, as a reference, rather than rediscovering per team.

### 2.3 Cost is a design constraint, not an afterthought

At metered pricing, the difference between a careless and a careful pipeline is
routinely 5–10×, from four compounding sources:

- **No batching.** Per-event calls re-pay the system-prompt tokens on every single
  event. Ten events in one call amortise that overhead once.
- **No cache.** Real streams are full of duplicates and near-duplicates — retried
  webhooks, replayed partitions, boilerplate text. Re-enriching identical content
  is pure waste.
- **Blind retries.** Retrying a *terminal* error (malformed input, oversize
  payload) can never succeed and is charged every time.
- **Retry storms.** An unjittered exponential backoff across many instances
  synchronises into thundering herds against an endpoint that is already
  rate-limiting, converting a brief degradation into a sustained billing event.

A pipeline whose cost is not observable per-event cannot be governed. FinOps is a
stakeholder here, not a bystander.

### 2.4 Non-determinism breaks stream-processing assumptions

Classical stream processing assumes the transform is a pure, total function. This
one is neither:

- It returns JSON that does not parse, or that parses but violates the schema.
- It refuses, or returns prose where a field was expected.
- It truncates at the token limit mid-structure.
- It changes behaviour when the model version changes — **including when the
  provider changes it underneath you.**
- It is vulnerable to prompt injection from the event payload itself, which is
  attacker-controlled data in most of these domains.

Consequences that must be designed in, not bolted on: response schema validation
as a mandatory step; a dead-letter path as a normal outcome rather than an
exception; and a **model and prompt version stamped on every output record**, so
that a field can always be attributed to the thing that produced it. An
enrichment you cannot attribute is an enrichment you cannot reproduce, audit, or
safely re-derive.

### 2.5 Head-of-line blocking on a partition

Kafka guarantees order within a partition, and the naive design honours that by
never moving past a record it cannot process. One pathological record — 400 KB of
text, an input the endpoint always rejects, a payload that reliably triggers a
refusal — then stalls that partition **permanently**. Lag on one partition grows
without bound while the other partitions look healthy. This is the classic
incident shape, and the fix (a dead-letter queue with attempt limits) has to
exist before the incident, not after.

### 2.6 Backfill and re-enrichment are a permanent requirement

Prompts improve. Models get replaced. Schemas gain fields. Every one of those
events creates a need to re-enrich history — potentially millions of records —
**without starving live traffic** and without a second copy of the application.
A pipeline with no rate-controlled replay path forces teams into one-off scripts
that bypass every safeguard the real pipeline has.

### 2.7 Compliance and data boundary

Sending event text to a model means moving customer data across a trust boundary.
Most regulated organisations require: knowing exactly what was sent, redacting PII
before it is sent, retaining an audit trail, and often keeping inference on
infrastructure they control. This project runs inference locally by default
(Ollama) precisely so the boundary question has a real answer rather than a
disclaimer.

## 3. Stakeholders and what each needs

| Stakeholder | Needs | Fails when |
|---|---|---|
| Platform engineer | A stage that survives the rate mismatch without babysitting | Rebalance storms, OOMs, mystery lag |
| Data consumer | Enriched fields, on a stated freshness SLO, with a schema they can rely on | Missing records, silently changed fields, unattributable values |
| SRE | Bounded memory, actionable alerts, graceful degradation | The app dies instead of slowing down |
| FinOps | Per-event cost visibility and a ceiling on spend | An unbounded retry loop against a metered endpoint |
| Compliance | Knowing what left the boundary, PII controls, audit trail | Raw customer text in logs or in a third party |

## 4. Functional requirements

| ID | Requirement |
|---|---|
| FR-1 | Consume from a configurable source topic and consumer group, with configurable key/value serdes. |
| FR-2 | Enrich each record by invoking a model behind a pluggable `ModelClient` port. At least two implementations: a local Ollama backend and a deterministic fake. |
| FR-3 | Validate every model response against a declared JSON schema before it is trusted. An unparseable or non-conforming response is a failure, not a value. |
| FR-4 | Produce enriched records to an output topic, preserving the original key so partitioning and downstream joins are unaffected. |
| FR-5 | Route records that cannot be enriched to a dead-letter topic with an envelope sufficient to replay them without access to the original stream. |
| FR-6 | Provide a replay tool over the dead-letter topic supporting dry-run, filtering by error class or time, and a rate limit. |
| FR-7 | Support micro-batching of several events into one model call, with **per-record failure attribution** — one bad record in a batch must not fail the other nine. |
| FR-8 | Cache enrichment results keyed by a hash of the normalised input, with an observable hit rate. |
| FR-9 | Respect provider rate limits and adapt in-flight concurrency to observed latency and error rate. |
| FR-10 | Stamp model identity, model version and prompt version on every enriched record and every dead-letter envelope. |
| FR-11 | Expose metrics, distributed traces and health endpoints covering the full consume → model → produce path. |
| FR-12 | Make every operational parameter configurable without a rebuild. |
| FR-13 | Handle poison bytes: a record that fails deserialisation goes to the dead-letter topic and never stops the loop. |
| FR-14 | Redact configured PII patterns from event text *before* it is sent to the model, and never log raw event text at default levels. |

## 5. Non-functional requirements

| ID | Requirement | How it is verified |
|---|---|---|
| NFR-1 | **Zero silent loss.** Every consumed record leaves by exactly one of three doors: the enriched topic, the dead-letter topic, or still in flight. There is no fourth door. | Chaos tests with a reconciling counter (Phase 2, 9) |
| NFR-2 | **Bounded memory**, independent of consumer lag and of model latency. | 30-minute saturation test with heap ceiling (Phase 3) |
| NFR-3 | **Graceful degradation.** When the model is slower than the stream, lag grows linearly and the application stays in its consumer group. It does not evict, storm, or die. | Phase 3 exit criterion: zero rebalances under 1000× oversubscription |
| NFR-4 | **Restart and rebalance safety.** No offset regression, no lost in-flight work, no double-produce beyond the at-least-once contract. | Property tests on `OffsetTracker` + forced-rebalance chaos test (Phase 2) |
| NFR-5 | **Throughput and latency envelope is measured and published**, never claimed. Percentiles come from merged HdrHistograms; percentiles are never averaged. | `docs/RESULTS.md`, updated per phase |
| NFR-6 | **No secrets and no raw PII in logs** at any level enabled in production. | Review + a test asserting redaction |
| NFR-7 | Cold start to first enriched record under 30 s on the reference machine. | Phase 4 |
| NFR-8 | The whole system runs on one developer laptop with no API keys and no GPU. | `bash scripts/up.sh` on the reference machine |

## 6. Delivery semantics — the honest position

**The contract is at-least-once, and downstream consumers must deduplicate on the
provided key.**

Exactly-once end-to-end is *not* offered, and the reason is structural rather than
incidental. Kafka's transactional EOS covers read → process → write when the
processing is internal to Kafka. Here the processing step is an HTTP call to an
external, non-transactional system. If the process dies after the model call and
before the transaction commits, the call has already happened — it was billed, and
any side effect it had is not rolled back. Additionally, EOS assumes the
process-and-produce step completes in offset order, which is precisely what the
asynchronous, out-of-order design gives up in order to survive section 2.1.

What is provided instead:

- The idempotent producer is enabled, so broker-side retries do not duplicate.
- Every enriched record carries a stable `dedupe_key` derived from the source
  topic, partition and offset, so a downstream sink can make the pipeline
  effectively-once at its own boundary.
- The result cache means a duplicate delivery usually does not re-pay the model
  cost.

This is written down because claiming exactly-once here is the most common
overstatement in this category of system, and a consumer that believes it will
build without deduplication and be wrong exactly once, under load, during an
incident.

## 7. Explicit non-goals

- Training, fine-tuning or evaluating models.
- A general-purpose workflow or DAG engine. This is one stage, done properly.
- Exactly-once end-to-end semantics (section 6).
- Multi-cloud or managed-service deployment abstractions.
- A user interface. Operations are CLI and dashboards.
- Supporting every model provider. The port exists; two implementations ship.

## 8. Success criteria

The project has succeeded when all of the following hold and are backed by a
number in `docs/RESULTS.md`:

1. With the model deliberately oversubscribed 1000:1, the application runs for
   30 minutes with zero rebalances, bounded heap, and linearly growing lag.
2. A reconciliation over a chaos run accounts for 100% of consumed records across
   the three doors, with forced rebalances and a broker restart during the run.
3. A batch containing one poison record dead-letters exactly that record.
4. A 60-second rate-limit storm degrades throughput and fully recovers, with no
   loss and no synchronised retry spike.
5. Killing the model mid-stream sends N records to the dead-letter topic, and the
   replay tool recovers exactly N.
6. One dashboard answers: is it healthy, what is it costing, where is the
   bottleneck.
7. A reader who has never seen the repository can run it end-to-end from the
   README on a laptop, with no API key and no GPU.
