# Pharos

A single-binary LLM router for small teams running mixed inference engines (Ollama, llama.cpp, vLLM, …) on a handful of machines.

Read [docs/STRATEGY.md](docs/STRATEGY.md) first. It defines the target customer, the routing policy, the engine support tiers and the success criteria.

Then read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): package layout, engine adapter model (probes per signal, recipes, per-backend plans, log probes, own probes), state model, cost-model policy, scheduler, testing layers and performance budget.

## Rules

- Don't trust engine docs. Endpoint paths, metric names and behavior drift between engine versions. Treat a doc-derived fact as unverified until an integration test against the real engine confirms it.
- Clients are third-party too. Open WebUI, SDKs and the `ollama` CLI differ in body encoding, routes and headers. Take their behavior from captures of what they really send, not from requests a Go test builds.
- Numbers in fakes and the simulator come from a capture or a scenario run, or are listed as assumed in ARCHITECTURE §14's assumptions ledger. A routing claim is settled by a scenario run on real engines (`examples/mac-native`).
- Before planning or writing tests, load the `testing` skill ([.claude/skills/testing/SKILL.md](.claude/skills/testing/SKILL.md)). It decides which test layer, TDD vs spike, and which cases come first.
- Engine adapters are combinations of probes, one probe per signal (a metric, a JSON field or a log line), not code written per engine or per version (ARCHITECTURE §4). When a version changes a metric, field or endpoint, add a newer probe ahead of the old one; the plan keeps whichever answers. A probe enters the library only after its name was seen in a real capture. Add a version guard only when a name changed meaning.
- Every engine adapter ships with a live integration test that asserts behavior (e.g. N concurrent requests → running = N), not just that fields exist.
- A missing or unrecognized signal is *unknown*, never 0.
- Stay in scope: no KV-event routing, prefill/decode disaggregation or Kubernetes operator. See STRATEGY.md §3.
