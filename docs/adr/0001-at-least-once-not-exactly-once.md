# 0001 — At-least-once, not exactly-once

- Status: accepted
- Date: 2026-09-03

## Context

Kafka offers transactional exactly-once semantics for read-process-write
topologies. The obvious question for this project is whether the enricher should
use them, and the obvious marketing answer is yes.

The processing step here is an HTTP call to a model endpoint. That endpoint is
external, non-transactional, metered, and stateful in ways we do not control
(rate-limit counters, billing, provider-side logs).

## Decision

**The delivery contract is at-least-once.** The idempotent producer is enabled,
every enriched record carries a stable `dedupe_key` derived from
`sourceTopic/partition/offset`, and downstream consumers are required to
deduplicate on it. Kafka transactions and `sendOffsetsToTransaction` are not used.

## Consequences

### Why exactly-once is not available here

1. **The external call is outside any transaction.** If the process dies after
   the model call returns and before the offset commit, the call has already
   happened. It was billed. Whatever the provider recorded, it recorded. A
   transaction abort rolls back the Kafka write and nothing else, so the
   "exactly-once" boundary does not contain the expensive, observable part of the
   work. Calling that exactly-once is a claim about the wrong system.

2. **It conflicts with the design that makes the project work at all.**
   Transactional EOS wants records processed and produced in a defined order
   within the transaction. Surviving the rate mismatch
   (`BUSINESS_REQUIREMENTS.md` §2.1) requires many concurrent in-flight calls
   completing out of order. Serialising them to preserve transactional ordering
   reintroduces the head-of-line blocking the design exists to remove.

3. **Transaction lifetime versus model latency.** A model call takes hundreds of
   milliseconds to tens of seconds. A transaction spanning a batch of those is
   open long enough to interact badly with `transaction.timeout.ms`, and an
   aborted long transaction re-does all of its expensive calls.

### What we provide instead

- The idempotent producer prevents broker-side retry duplication.
- The `dedupe_key` lets any downstream sink be effectively-once at its own
  boundary — a compacted topic, an upsert, or a unique index all work.
- The content-hash result cache means a duplicate delivery usually does not
  re-pay the model cost, so the practical penalty of at-least-once is bounded.

### Cost of this decision

Downstream consumers must deduplicate. This is written in the requirements, in
the README, and stamped into every record's `_meta`, because a consumer who
assumes exactly-once will build without deduplication and discover the mistake
once, under load, during an incident.

## Alternatives considered

**Transactional EOS with synchronous processing.** Honest semantics, but caps
throughput at one model call per partition at a time — the naive design, with
the failure mode from `BUSINESS_REQUIREMENTS.md` §2.2. Rejected.

**A transactional outbox with a separate committer.** Moves the boundary but does
not close it; the external call still sits outside. Adds a component. Rejected as
complexity that buys a nicer diagram rather than a stronger guarantee.

**Claiming EOS and hoping.** This is what most implementations in this category
do. Rejected explicitly, and this ADR exists so the rejection is on record.
