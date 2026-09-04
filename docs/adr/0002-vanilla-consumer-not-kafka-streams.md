# 0002 — Vanilla consumer, not Kafka Streams or Flink

- Status: accepted
- Date: 2026-09-03

## Context

Three plausible runtimes for a Kafka enrichment stage:

- **Kafka Streams** — DSL, state stores, EOS v2, rebalance handling for free.
- **Flink** — `AsyncWaitOperator` is purpose-built for enrichment RPCs, with
  ordered and unordered modes and a built-in cap on in-flight requests.
- **Vanilla `KafkaConsumer` / `KafkaProducer`** — everything by hand.

## Decision

**Vanilla consumer and producer, Java 21 on JDK 24, virtual threads for the
in-flight calls.**

## Consequences

### Why not Kafka Streams

Streams owns its threading model. A `StreamTask` processes records on a stream
thread, and the punctuator and commit machinery assume that returning from
`process()` means the work is done. Asynchronous external calls fight this
directly: either you block the stream thread (which is the naive failure mode) or
you complete out of band, at which point Streams' own offset management is no
longer telling the truth about what has been processed.

More importantly, **backpressure stops being something you can steer.** The whole
subject of this project is deciding when to admit work, when to pause partitions,
and how many calls to have in flight. Streams places that machinery behind an
abstraction and offers a small number of tuning knobs in its place.

### Why not Flink

`AsyncWaitOperator` is genuinely the right tool for this in a shop that already
runs Flink, and this ADR should not be read as a criticism of it. It caps
in-flight requests, offers ordered and unordered emission, and integrates async
completion with checkpointing correctly.

It is rejected here for one reason: **it already solves the interesting part.**
The value of this repository is a worked, tested, measured implementation of
bounded in-flight admission, out-of-order watermark commit and pause/resume
backpressure. Delegating those to Flink leaves a thin job and a dependency on a
cluster, and demonstrates Flink rather than the mechanism.

It also raises the barrier to running the project. A goal (NFR-8) is that the
whole system runs on one laptop with no API keys and no GPU.

### Why not Python

Fastest to write, and asyncio maps naturally onto per-event calls. Rejected on
two grounds: the throughput story is materially weaker at the rates this project
needs to demonstrate saturation, and the operational tooling most organisations
run around this kind of stage is JVM-shaped.

### What this costs us

Everything Streams and Flink would have given us has to be built and tested:
rebalance-safe offset handling, graceful shutdown, drain-on-revoke, poison-record
handling, retry topics. `docs/WORKPLAN.md` Phase 2 exists precisely because this
decision moves that risk onto us, and it is gated on property tests rather than
integration tests for the same reason.

This is an accepted cost. The mechanism *is* the deliverable.
