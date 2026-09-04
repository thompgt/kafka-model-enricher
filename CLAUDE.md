# CLAUDE.md — kafka-model-enricher

Guidance for Claude Code (and humans) working in this repository.

## What this project is

A production-grade Kafka streaming enricher that calls a model once per event.
The whole subject of the project is the mechanics that make that survivable:
bounded in-flight work, correct out-of-order offset commits, `pause()`/`resume()`
backpressure, adaptive concurrency, micro-batching with per-record failure
attribution, and a dead-letter path you can actually replay from.

**The deliverable is the mechanism and the evidence it works, not feature count.**
A change that adds capability while weakening one of the invariants below is the
wrong change. See `docs/BUSINESS_REQUIREMENTS.md` §2.

## Non-negotiable invariants

Breaking one of these is a data-loss or cost bug, not a style issue. A change that
touches one requires an ADR.

1. **Never commit an offset whose record has not completed.** Only the contiguous
   watermark is ever committed. Model calls finish out of order; committing the
   highest completed offset tells Kafka that the gaps beneath it are done, and a
   restart then skips them forever. This loses data and raises no error.
2. **Backpressure is `pause()` / `resume()`.** Never block inside the poll loop —
   no sleeping for capacity, no `future.get()`, no bounded-queue `put()`. Never
   let in-flight work grow unbounded. Blocking causes `max.poll.interval.ms`
   eviction and a rebalance storm; unbounded growth causes an OOM with offsets
   already committed.
3. **`enricher-core` contains no Kafka and no Spring types.** Not
   `TopicPartition`, not `ConsumerRecord`, not `@Component`. The correctness core
   must be testable over thousands of randomised interleavings in milliseconds.
   `enricher-kafka` adapts at the boundary.
4. **Three doors, no fourth.** Every consumed record leaves via `events.enriched`,
   via `events.dlq`, or is still in flight. A silent drop is the
   highest-severity bug in this repository. `events.retry` is not a door — those
   records re-enter.
5. **Every dead-letter record is replayable from its envelope alone**, without the
   source stream. By the time anyone looks, retention may have expired. Original
   key and value bytes are preserved verbatim, never re-serialised.
6. **Never average percentiles.** Merge HdrHistograms, then compute. A mean of
   p99s is not a p99. Latency is measured from the *intended* send time, not the
   actual one — measuring from the actual send time is coordinated omission and
   hides exactly the stalls this project exists to survive.
7. **Model and prompt version are stamped on every output and every dead-letter
   envelope.** An enrichment that cannot be attributed to a prompt version cannot
   be reproduced, audited, or selectively re-derived.
8. **At-least-once is the contract.** The idempotent producer is on; a
   `dedupe_key` is on every record; exactly-once is **not** claimed. See
   `docs/adr/0001`.
9. **A model response is not trusted until it has parsed and validated** against
   the declared schema. Retry a malformed response a small, bounded number of
   times; a repeated schema violation is terminal, because retrying it is pure
   cost.

## Repository layout

```
docs/              Requirements, architecture, workplan, results
  adr/             Architecture decision records, NNNN-title.md
enricher-core/     Framework-free correctness core: OffsetTracker, InFlightWindow,
                   limiter, retry policy, error classifier, batcher, cache, ports
enricher-model/    ModelClient implementations: Ollama, and the injectable fake
enricher-kafka/    Poll loop, pause/resume, rebalance listener, producer, DLQ writer
enricher-app/      Spring Boot wiring, config, Actuator/Micrometer
enricher-replay/   Dead-letter inspection and replay CLI
loadgen/           Open-loop generator and latency measurement
infra/compose/     One Compose file per concern, combined via profiles
infra/conf/        Broker, Prometheus, Grafana config mounted into containers
scripts/           Developer entry points, wrapped by the Makefile
samples/           Sample event data
results/           Local scratch for run output — gitignored
```

## Conventions

### Java

- **Java 21 release level on JDK 24.** Set `<maven.compiler.release>21`; do not
  rely on JDK 24 APIs.
- Virtual threads for the in-flight model calls. Platform threads for the poll
  loop — it must never be descheduled behind a carrier thread doing blocking work.
- `enricher-core` keeps dependencies to the JDK and a JSON library. Every
  dependency there is a dependency in the correctness path.
- Records and sealed interfaces for the domain types. Prefer immutability; the
  window and tracker are the only mutable state and they are explicitly owned by
  the poll-loop thread or explicitly synchronised.
- Tests: JUnit 5, jqwik for properties, Testcontainers for integration, AssertJ.

### Configuration

- Nothing operational is hardcoded. Broker addresses, topic names, window sizes,
  batch sizes, model name, prompt version, retry limits all live in config.
- A published result must name the config file that produced it.

### Compose

- One file per concern, never a monolith; combine with profiles.
- Every service declares explicit `cpus` and `mem_limit`. Unbounded containers
  make measurements non-comparable across runs.
- **Bitnami images are prohibited** — the free catalogue was restricted in 2025.
  Use `apache/*` and Docker Official images.
- Pin image tags and record the resolved digest for anything that affects a
  published number.

### Git

- Commit and push after each small logical unit, not at the end of a phase.
- Imperative subject lines; the body explains *why*, not *what*.
- Work on `main` for scaffolding. Once Phase 2 lands, changes to
  `enricher-core/src/main/java/.../{offset,window}` go via a branch and PR — they
  can silently invalidate every correctness claim in the repository.

## Toolchain facts for this machine

Cost real time to discover on previous JVM projects here.

- **No local `mvn` or `gradle`.** The only-script Maven Wrapper is used; `./mvnw`
  bootstraps Maven itself with no pre-existing install.
- **Committing from Windows drops the exec bit on `mvnw`**, and Linux CI then
  fails instantly with `./mvnw: Permission denied`. Fixed here with
  `git update-index --chmod=+x mvnw`; re-check it if the file is ever recreated.
- **Surefire's default `<includes>` skip `*Properties` classes, and specifying
  `<includes>` replaces the defaults rather than extending them.** A build can go
  green having run zero tests. Assert the test count; do not trust the colour.
- **Since JDK 23, `javac` does not run classpath annotation processors.** Anything
  needing one (JMH, for instance) requires explicit `annotationProcessorPaths`.
- **LF line endings are enforced** via `.gitattributes`. A CRLF shebang fails
  inside a Linux container with an opaque "bad interpreter" error.
- **`export MSYS_NO_PATHCONV=1` before any `docker exec` with an absolute
  container path.** Git Bash rewrites `/opt/kafka/bin/...` into
  `C:/Program Files/Git/opt/kafka/bin/...` and the exec fails bewilderingly.
- **`make` is not installed in Git Bash.** Every Makefile target is a thin wrapper
  over a script in `scripts/`, so `bash scripts/up.sh core` always works.
- **Run performance-sensitive work from WSL2**, not the Windows host. Windows
  quantises poll waits to the ~15.6 ms scheduler tick, which is larger than some
  of the intervals this project measures.
- **No GPU on this machine.** Ollama is CPU-only; expect single-digit requests per
  second from a 1–2B model. That is a feature for demonstrating backpressure and a
  problem for throughput tests, which is why the fake backend exists.

## Running things

Entry points live in the Makefile, each a wrapper over `scripts/`. Most do not
exist until their phase lands.

```bash
bash scripts/up.sh core          # Kafka + topics
bash scripts/up.sh ollama        # local model
bash scripts/up.sh obs           # Prometheus + Grafana
bash scripts/down.sh             # tear down, volumes included
./mvnw verify                    # build and test
```

## Where to look first

- Why the project exists → `docs/BUSINESS_REQUIREMENTS.md` §2
- How a mechanism works → `docs/ARCHITECTURE.md`, §4 for the three core ones
- What to build next → `docs/WORKPLAN.md`
- Why a decision was made → `docs/adr/`
- Changing offset or window logic → re-read invariants 1–4, then write the ADR
