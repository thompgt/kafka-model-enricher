# Workplan — kafka-model-enricher

Phase order is binding. A phase is not done until its exit criterion has a
**number** logged in `docs/RESULTS.md`. Do not add a dependency before the phase
that uses it.

Legend: ☐ not started · ◐ in progress · ☑ done

---

## ☑ Phase 0 — Scaffold, repo, push

Get a clean, buildable, documented skeleton on GitHub with CI green.

- [x] Repository created public, Maven wrapper, LICENSE, `.gitattributes` (LF)
- [x] `docs/BUSINESS_REQUIREMENTS.md`
- [x] `docs/ARCHITECTURE.md`
- [x] `docs/WORKPLAN.md`
- [ ] `CLAUDE.md` with the non-negotiable invariants
- [ ] Maven multi-module skeleton, Java 21 release level on JDK 24
- [ ] `docs/adr/0001-at-least-once-not-exactly-once.md`
- [ ] `docs/adr/0002-vanilla-consumer-not-kafka-streams.md`
- [ ] `docs/adr/0003-local-ollama-as-default-backend.md`
- [ ] Compose `core` + `ollama` profiles, `scripts/`, Makefile wrappers
- [ ] GitHub Actions CI: build and test on Linux
- [ ] `README.md`

**Exit:** a clean clone runs `./mvnw verify` green with a **non-zero test count**;
`bash scripts/up.sh core` yields a reachable broker with topics created;
`bash scripts/up.sh ollama` answers on `/api/tags` with the pinned model; CI green.

> Surefire's default `<includes>` skip `*Properties` classes, and specifying
> `<includes>` *replaces* the defaults rather than extending them. A build can go
> green having run zero tests. Assert the count, do not trust the colour.

---

## ☐ Phase 1 — Walking skeleton (synchronous, deliberately naive)

consume → fake model → produce, with manual offset commit and a dead-letter path
for deserialisation failures only.

This phase is **intentionally the naive design** from requirement 2.2. It is built
so that later phases have a measured baseline to beat, and so the failure it
exhibits in Phase 3 is demonstrated rather than asserted.

- [ ] `SourceRecord` / `PartitionRef` / `ModelClient` / `RecordSink` ports in core
- [ ] `FakeModelServer` with fixed latency
- [ ] Synchronous poll → call → produce loop, commit after each batch
- [ ] Poison-byte handling (FR-13)
- [ ] Testcontainers end-to-end test

**Exit:** 1,000 events through the pipeline with zero loss, verified by
reconciliation. Baseline throughput recorded.

---

## ☐ Phase 2 — Async core and offset correctness ← highest risk

The correctness heart. Nothing after this is trustworthy if this is wrong.

- [ ] `InFlightWindow`: record and byte permits
- [ ] `OffsetTracker`: contiguous watermark, out-of-order completion, revoke
- [ ] Virtual-thread dispatcher; completion advances the watermark
- [ ] `ConsumerRebalanceListener` that discards revoked partitions atomically and
      commits what is safely done
- [ ] jqwik property tests for the four `OffsetTracker` properties in
      `ARCHITECTURE.md` §4.1
- [ ] Chaos test: randomised completion order + forced rebalances

**Exit:** property tests pass over ≥10,000 randomised interleavings; the chaos
test reconciles 100% of records with no offset regression and no loss.

---

## ☐ Phase 3 — Backpressure

- [ ] `pause()` / `resume()` driven by window saturation
- [ ] Permit-wait time and pause/resume counters as metrics
- [ ] `max.poll.interval.ms` safety margin asserted at startup against the
      configured window size and worst-case model latency

**Exit — the headline result of the project:** fake model pinned at 5 req/s,
producer at 5,000 events/s (1000:1 oversubscription), 30 minutes:

| Metric | Requirement |
|---|---|
| Rebalances | **0** |
| Heap | bounded, no upward trend |
| Records lost | **0** |
| Lag | grows linearly (correct behaviour) |

Run the **Phase 1 naive build under the identical load** and record how it fails.
That contrast is the point of the repository.

---

## ☐ Phase 4 — Ollama backend

- [ ] `OllamaModelClient` against the native API, `format` used for structured output
- [ ] Prompt templates, versioned; `prompt.version` in config
- [ ] JSON schema validation of responses (`ARCHITECTURE.md` §5)
- [ ] Provenance stamping: model, digest, prompt version, schema version
- [ ] PII redaction before the call (FR-14)
- [ ] Sample dataset of support-style events committed under `samples/`

**Exit:** real enrichment over the sample dataset; measured CPU req/s and p50/p99
in `RESULTS.md`; malformed and refused responses observed and correctly classified.

---

## ☐ Phase 5 — Batching and cost

- [ ] Micro-batcher: size or linger deadline
- [ ] **Per-record failure attribution** by stable batch index (FR-7)
- [ ] Adaptive batch sizing
- [ ] Content-hash result cache with hit-rate metric
- [ ] Token accounting and a per-event cost estimate

**Exit:** tokens/event and events/s against the Phase 4 baseline, with cache hit
rate. A batch containing one poison record dead-letters **exactly** that record —
asserted by test, not observed by eye.

---

## ☐ Phase 6 — Adaptive concurrency and retry

- [ ] AIMD limiter, plus a gradient variant to compare
- [ ] Token bucket for hard quotas
- [ ] Error classifier: retryable vs terminal (`ARCHITECTURE.md` §5)
- [ ] Full-jitter exponential backoff
- [ ] `events.retry` topic with delayed consumption for long backoffs

**Exit:** the fake injects a 60-second 429 storm. Throughput degrades and fully
recovers; zero loss; the retry send-rate histogram shows **no synchronised
spike** — the jitter is verified, not assumed.

---

## ☐ Phase 7 — Dead-letter operations

- [ ] Full envelope (`ARCHITECTURE.md` §6)
- [ ] `enricher-replay` CLI: filter, `--dry-run`, rate limit
- [ ] Quarantine for records that fail replay repeatedly

**Exit:** kill the model mid-stream, N records land in the dead-letter topic,
replay recovers exactly N to the enriched topic with correct dedupe keys.

---

## ☐ Phase 8 — Observability and SLOs

- [ ] Micrometer → Prometheus; Grafana dashboard as code
- [ ] OpenTelemetry traces spanning consume → model → produce
- [ ] HdrHistogram latency capture (merged, never averaged)
- [ ] Cost meter
- [ ] Alert rules: lag, dead-letter rate, permit saturation, rebalance count

**Exit:** one dashboard answers *is it healthy*, *what is it costing*, *where is
the bottleneck* — demonstrated by using it to diagnose a deliberately injected
fault.

---

## ☐ Phase 9 — Soak and published envelope

- [ ] 4-hour soak with a mixed workload
- [ ] Broker restart mid-run
- [ ] Rebalance storm (rolling instance restarts)
- [ ] Oversized and pathological payloads in the mix

**Exit:** zero loss, bounded memory over 4 hours, published throughput/latency
envelope in `RESULTS.md` with the config file that produced it.

---

## ☐ Phase 10 — Deployment (stretch)

- [ ] Distroless or jlink image
- [ ] Kubernetes manifests
- [ ] Autoscale on consumer lag (KEDA)

---

## Open questions

| # | Question | Blocking |
|---|---|---|
| Q-1 | Which small instruct model gives usable structured output on CPU — `llama3.2:1b` or `qwen2.5:1.5b`? Pin by digest. | Phase 4 |
| Q-2 | Does the in-process cache hit rate justify a distributed cache at all? | Phase 5 stretch |
| Q-3 | Is the delayed `events.retry` topic worth its complexity, or does in-process backoff within the window suffice at the observed error rates? | Phase 6 |
| Q-4 | AIMD or gradient limiter for the Ollama latency curve? Decide from measurement. | Phase 6 |
