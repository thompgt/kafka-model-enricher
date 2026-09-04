# 0003 — Local Ollama as the default backend, with a deterministic fake alongside

- Status: accepted
- Date: 2026-09-03

## Context

The enricher needs something behind the `ModelClient` port. Options: a hosted API
(Anthropic, OpenAI), a local model server (Ollama, vLLM), or a simulator only.

Constraints that decide it:

- NFR-8: the whole system must run on one laptop with **no API keys**.
- The reference machine has **no GPU**. CPU inference with a 1–2B model will serve
  roughly single-digit requests per second.
- `BUSINESS_REQUIREMENTS.md` §2.7: regulated organisations often require inference
  on infrastructure they control.
- Phase 3's headline result needs a producer at 5,000 events/s sustained for 30
  minutes, and Phase 9 needs a 4-hour soak.

## Decision

**Two implementations of one port, both first-class.**

- `OllamaModelClient` — the default, real backend. `ollama/ollama` in Compose with
  a small instruct model (~1–2B), pinned by digest, using the `format` parameter
  for structured output.
- `FakeModelServer` — an in-process HTTP server with injectable behaviour: latency
  drawn from a configured distribution, error rates by class (429, 5xx, timeout),
  malformed and truncated JSON, refusals, and a hard concurrency ceiling. This is
  what CI, unit tests, load tests and chaos tests run against.

A hosted API backend is explicitly **out of scope** but not designed out; the port
exists and a third implementation would be additive.

## Consequences

### Why the fake is not a testing shortcut but a primary artifact

Every exit criterion in the workplan that proves the mechanism works requires
behaviour a real model cannot be asked to produce on demand:

| Phase | Needs the model to |
|---|---|
| 3 | Serve exactly 5 req/s, unwaveringly, for 30 minutes |
| 5 | Fail exactly one record inside a batch of ten |
| 6 | Return HTTP 429 for exactly 60 seconds, then stop |
| 9 | Sustain a mixed latency distribution for 4 hours |

None of these are reproducible against a real endpoint. Reproducibility is the
whole point — a chaos test that cannot be re-run identically after a fix is not a
regression test.

The fake also keeps CI free, fast and offline. A CI run that pulls a 1 GB model
and does CPU inference is a CI run nobody waits for.

### Why the real backend is Ollama and not a hosted API

**No keys, no bill, no network.** A reader can clone and run.

**The rate mismatch becomes real rather than simulated.** CPU inference at
single-digit requests per second against a stream at thousands per second *is*
the 1000:1 oversubscription the project is about. With a hosted API on a fast
connection, the mismatch has to be manufactured by throttling. Here it is simply
the truth, which makes the backpressure demonstration more honest, not less.

**It exercises the adaptive limiter for real.** Ollama has no quota but does have
a hard concurrency ceiling and a latency curve that degrades sharply past it —
which is exactly the signal an AIMD or gradient limiter is supposed to find. A
hosted API mostly teaches the limiter to respect a 429; Ollama teaches it to find
an inflection point.

**It answers the compliance question** from §2.7 concretely instead of with a
disclaimer.

### Costs

- Absolute throughput numbers from the Ollama path characterise *this laptop's
  CPU*, not the enricher. `RESULTS.md` must report the two paths separately and
  never blend them: the fake measures the pipeline, Ollama measures end-to-end
  reality on known hardware.
- Small models produce lower-quality enrichment and violate schemas more often
  than a frontier model. For this project that is nearly a benefit — the validate
  and dead-letter paths get exercised by real failures rather than only by
  injected ones — but the README must not present sample output as a quality
  demonstration.
- Two backends is a maintenance surface. Mitigated by keeping the port narrow.

### Open

Q-1 in the workplan: `llama3.2:1b` versus `qwen2.5:1.5b` for usable structured
output on CPU. Decide by measurement in Phase 4 and pin the winner by digest.
