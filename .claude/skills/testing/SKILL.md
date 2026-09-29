---
name: testing
description: Load BEFORE planning, writing or changing tests or Go code in Pharos. Decides the test layer (unit, fixture replay, component, live engine, benchmark), whether to TDD, spike first or skip tests, and which cases matter most. Required for engine adapters, new engine versions, bug fixes (especially wrong engine signals), fixture recording, choosing defaults for values an engine does not report, and "how should I test this?".
---

# Testing in Pharos

A test is a document of a contract. If you can't write the contract down yet, you can't write a test worth keeping. Find the contract first, then pick the cheapest layer that can prove it.

Layers are defined in [docs/ARCHITECTURE.md §14](../../../docs/ARCHITECTURE.md). Short version:

| # | Layer | Proves | Speed | Runs |
|---|---|---|---|---|
| 1 | Unit (pure) | our logic | ms | `go test ./...` |
| 2 | Fixture replay | our code against **recorded** engine output | ms | `go test ./...` |
| 3 | Component | packages wired together, fake engines, multi-instance | s | `go test ./...` |
| 4 | Live integration | the engine really behaves that way | min | `go test -tags integration ./...` + docker compose |
| 5 | Bench / simulation | performance budget, routing quality | min | `go test -bench`, simulator |

Truth flows **down**: layer 4 observes real engines → records layer-2 fixtures → layer-3 fake engines serve those fixtures. Nothing below layer 4 may claim an engine behavior that layer 4 never observed.

The same holds for **clients** (Open WebUI, OpenAI SDKs, `requests`/`aiohttp`, the `ollama` CLI): how they encode bodies, which routes they call, which headers they send. A test request built with Go's `json.Marshal` is our assumption, not a client's behavior. Take client shapes from real captures (a sink that records what the client sends: `examples/mac-native/webui.py` for Open WebUI, replayed by `TestOpenWebUIChatReplays`), never from what a Go test happens to produce.

**Numbers in fakes and the simulator need a source.** Latency, cache size, capacity, load time and bytes per token are either measured (a capture or a scenario run, cited) or listed as assumed in the assumptions ledger (ARCHITECTURE §14). A fake that shares the router's assumption can't catch the router's mistake. When a fake needs a number nobody has measured, the answer is a live run, not a better guess.

**Scenario runs** (`examples/mac-native`): real engines on Metal, real conversations, three views compared (client, Pharos `/metrics`, engine logs). They report, never assert. A routing-quality claim is settled only after a scenario run confirms it; the simulator compares policies with each other, not with reality. A hypothesis formed by reading code is [U] until a run shows it: "non-English text breaks the estimate" turned out to be "JSON-escaped text does".

This skill overrides generic "write one small check" or minimal-test guidance. The case lists below are the minimum for this repo.

## Step 1: classify the change

Answer in order. Stop at the first "yes".

1. **Does it depend on how a third-party engine or client behaves** (endpoint, metric name, field, status code, timing, caching; a client's body encoding, routes or headers)?
   → **Engine path.** Spike against the real engine, then layer 4, then record layer 2. Never write a probe or its fixture from docs. See [Engine path](#engine-path). If it's a fix for a wrong engine signal, use the bug-fix order in item 6.
   Choosing a **default that stands in for an unknown engine value** (e.g. Ollama capacity when `OLLAMA_NUM_PARALLEL` is unset) is both an engine question and a tuning question: see [Defaults for unknown engine values](#defaults-for-unknown-engine-values).
2. **Is the contract unclear** — you're exploring, tuning constants, or you can't name the inputs, outputs and dependencies?
   → **Spike path.** Fail fast with real data. No tests yet. See [Spike path](#spike-path).
3. **Is it pure logic with edge cases** (math, parsing, ordering, state machines, merges, hashing, windows)?
   → **TDD at layer 1.** See [TDD path](#tdd-path).
4. **Is it wiring** (handlers, goroutines, scheduler ↔ proxy ↔ state, peers, drain, config reload)?
   → **Layer 3** with fake engines. No per-function unit tests for glue.
5. **Is it a performance or routing-quality claim?**
   → **Layer 5**, then a scenario run on real engines. Never assert timings in layers 1–3.
6. **Is it a bug fix?**
   → Reproduce with a failing test at the **lowest layer that can reproduce it**, then fix. For a wrong engine signal the order is: capture the real output (`pharos doctor -url <engine> -record <dir>` or a spike) → add it as a fixture → layer-2 replay goes red → fix (usually a newer probe for that signal) → layer 4 on that version goes green.
7. **Trivial** (field rename, log text, one-line delegation, types only)?
   → No new test. Existing tests covering the caller are enough.

## TDD path

Use when you can state the rule before writing the code. Pharos examples: `policy.Pick`, prefix chain hashing, Prometheus line parser, the `Prom` and `LogLine` constructors, own-probe config parsing, plan merge and redundancy rules, stream tap line scanning, fair-queue ordering, EWMA, RPM window, every `Merge`.

- Write the table first. Each row name is a sentence from the spec: `warm_busy_host_is_last_resort`, `missing_metric_is_unknown_not_zero`.
- Inject time (`now func() time.Time`) and randomness (seeded or injected). No `time.Sleep`.
- Assert **policy, not constants.** The cost model's numbers will be tuned. Prefer relative assertions ("warm target beats cold when load time > prefill saved") over exact floats. When you need a number, compute it by hand in the test comment, never by calling the same formula.
- `Merge` functions get a `testing/quick` property test: any order, any duplicates → same state.
- Test through the public function. If a private helper needs its own table, it's probably a separate unit with its own contract; move it or leave it covered by the caller.

## Engine path

Engine docs lie (renamed metrics that silently read 0 are the canonical failure; see STRATEGY §6). The order is fixed:

1. **Spike against the real engine.** Run the pinned version in docker, `curl` the endpoints, send concurrent requests, watch the numbers move. Write findings down. Anything you only read in docs stays marked **[U]**.
2. **Write the layer-4 behavioral test.** Assert behavior, not schema:
   - N concurrent slow requests → `Running == N`, or `Running` unknown — never a wrong number. Unknown passes only for a version where the matrix records the signal as unsupported. Once a signal is verified on a version, unknown there is a failure.
   - load model → `Loaded`; wait past keep-alive → `Cold` and `gen` bumped.
   - same long prefix twice → cached tokens > 0 on the second.
   - overload → `Waiting > 0` or saturation fallback.
   - kill engine mid-stream → lease released, target ejected, next request goes elsewhere.
   A check that the engine can't support is recorded as *unknown* in the support matrix, not skipped silently.
3. **Record fixtures** with `PHAROS_RECORD=1 go test -tags integration -run TestLive ./internal/engine` (it runs `test/engines/capture.sh`, which records each state with `pharos doctor -record`, the one recorder) into `internal/engine/testdata/<engine>/<version>/<state>/` (e.g. `idle/`, `loaded/`) with `meta.yaml` (version, capture date, capture command). Record every path in the recipe, 404s included, plus a response stream per API format, so replay can resolve the plan and check the stream tap. If the engine has log probes, also record its log lines to `engine.log`, and review them for prompt content before committing. Fixtures are **raw endpoint bodies and log lines**, never parsed `Snapshot`s. Replaying the parser's own output back into it proves nothing. Review the diff; a fixture diff is an engine behavior change.
4. **Write layer-2 replay tests** from those fixtures. Every library probe gets these cases, run against every capture dir that contains its feed:
   - each capture dir that contains the probe's path or log → expected value for its signal (`OK=true`, correct value). A capture the probe isn't expected to match → unknown;
   - metric/field **absent** → `OK=false`;
   - metric **present but unparseable** → error surfaced;
   - a log probe: a matching line → value; a non-matching line → no change; stream disconnected → unknown;
   - a **replaced probe** (a newer probe ahead of an older one for the same signal) → on the old version's capture the old probe answers, on the new version's capture the new one does, and `Resolve` keeps exactly one;
   - two probes with complementary `When` guards → captures from both sides of the change, plus unknown version → neither runs.
   Also replay `engine.Resolve` against each whole capture dir → expected plan (active and dropped probes) and merged `Snapshot`.
   Edge-case inputs (absent, garbage) are short inline strings in the table, derived from a real capture. Expected values live in the Go test file, keyed `engine/*` (every version), `engine/*/state`, `engine/version` or `engine/version/state`; the most specific key wins, so a behavior every version shares is written once and a new version needs rows only where it differs. `testdata/<engine>/<version>/` holds only real captures.
   **A probe enters the library only if its metric, field or log line was seen in a real capture** (any version, ours or user-submitted), never from docs or a guess. A name seen only in docs gets no probe and stays **[U]** in the support matrix. Own probes from config are the operator's responsibility and are not in the fixture suite.
5. Fill the support-matrix cells for that version: which probe supplies each signal, verified live or by fixture only, or unknown.

Adding a new engine **version** is steps 1, 3, 4 (new directory) — no new code unless a fixture test fails, and then usually one newer probe for the signal that changed.

**Can't run the engine here** (no docker, or no image for this OS/arch or CPU; for example, whether SGLang or vLLM CPU images run on arm64 is [U])? Don't substitute docs or a hand-written fixture. Build what you can, report layer 4 as not run, and leave every signal from that engine [U]. Layer 4 then runs where the engine does (CI or a GPU box) before the adapter counts as supported.

## Spike path

For work where the dependency or the contract isn't defined yet: tuning cost-model defaults, answering an open question in ARCHITECTURE §16, figuring out what Ollama reports, a simulator scenario. A data scientist runs the script on real data; a DevOps engineer reads the `terraform plan`. Same idea.

- Spike in the scratchpad or with `go run`, against real engines or real traces. Optimize for learning in minutes.
- **Run it, don't read about it.** Reading engine source at a pinned tag is a good way to form a hypothesis. It is still [U] until you've observed it.
- The output is a **finding** (numbers, a captured payload, a resolved [U]), not code.
- Then choose: **delete** the spike, or **stabilize** it — now that the contract is known, re-enter Step 1 and write the tests that document it.
- Don't merge spike code without tests. Don't write unit tests during the spike; they'll be thrown away with the code.

## Defaults for unknown engine values

When an engine doesn't report a value and Pharos must assume one (capacity, load time, prefill rate):

1. **Measure, don't look it up.** Spike against the real engine. For capacity, fire 1, 2, 4, 8 concurrent slow requests and watch where per-request throughput or TTFT stops scaling.
2. Put the number in config, with a comment citing the measurement: engine version, hardware, date.
3. **Test the fallback behavior, not the number.** At layer 1 or 3: the value is unknown → the configured default is used; config overrides it; it is never treated as 0.
4. If the engine exposes something the default relies on, assert that behavior at layer 4 (e.g. with 2 slots, the third request waits). Don't assert that a field is absent; that's a schema check.

## Which cases to write first

Budget is finite. Write cases in this order and stop when the next one wouldn't catch a realistic bug:

1. **Unknown vs zero.** Every path where a signal can be missing, stale (> 3× scrape interval, or its log stream disconnected) or unparseable. This is the product's core promise.
2. **Behavior under concurrency.** Counts under N parallel requests; two requests racing for the last slot; lease released on success, upstream error, client cancel and engine death. Inflight never goes negative.
3. **Safety invariants.** Never evict a model with in-flight requests. Never log prompt content or engine log lines. Quota overshoot bounded by one tick per instance while connected, and by one quota per side while partitioned. No stream cut on graceful drain.
4. **Merge laws.** Commutative, idempotent, tolerant of unknown targets and dropped ops.
5. **Boundaries.** Empty candidate list, all signals unknown, exact thresholds (KV 0.9), ties (break on utilization then random — assert no herding, not a specific winner). Test each boundary in the package that owns its input. For example, staleness (3× the scrape interval) is tested in `state`, where the clock is. `policy` has no clock and only ever sees signals already marked unknown.
6. **Version drift.** One capture dir per engine version; replaced probes, plan resolution and every `When` guard covered.
7. Happy path. Usually already covered by the above.

**Skip:** getters/setters, struct defaults, re-testing stdlib or `net/http`, call-count assertions on mocks of our own packages, exact `Reason` wording (assert it mentions the deciding factor), coverage-percentage padding.

## Rules

- **Don't mock what you don't own from imagination.** Engine doubles (fixtures, fake engines) must serve recorded behavior. Only the fake's latency/cache model is synthetic, and it's labeled as such.
- **Prefer fakes over mocks.** `httptest` servers and in-memory implementations, not interaction-verifying mocks.
- **No sleeps** in layers 1–3. Inject clocks; for goroutines, wait on channels or conditions with a timeout.
- **Deterministic by default.** Seed randomness. A flaky test is a bug in the test or the code; fix it, don't retry it.
- **Every test must be able to fail.** When writing a test for existing code, break the code once and watch it go red.
- **Tests read as documentation.** Names are spec sentences. A reader should learn the contract from the test file without opening the implementation.

## When to run what

| Situation | Run |
|---|---|
| Any change, inner loop | `go test ./...` (layers 1–3) |
| Before pushing, or merging to main without waiting for CI | `test/check.sh`: vet, layers 1–3, rolling restart, and layer 4 when engine-signal files changed. Green there = green in CI |
| Touched `serve`, drain, peers or usage handover | + `go test -tags integration -run TestRollingRestart ./cmd/pharos` (three real `serve` loops on fake engines, ~15 s, no Docker) |
| Touched `internal/engine` (probes, recipes, `Resolve`, `Follow`), the stream tap, or the scrape and log-follow loops | + layer 4 for affected engines locally: `PHAROS_LIVE_ENGINES=ollama,vllm go test -tags integration -timeout 90m -v -run TestLive ./internal/engine` |
| Touched the `Dockerfile`, Docker discovery or `serve` | + `go test -tags integration -timeout 20m ./test/e2e` (the image in front of two Ollama containers and a llama.cpp, found by labels; needs Docker) |
| Touched Docker discovery, `DockerLogs` or the log-follow loop in `state` | + `go test -tags integration -v -run TestLiveDocker ./internal/discovery` (needs Docker; pulls `ollama/ollama:0.34.4` and the model) |
| PR (CI) | layers 1–3 + layer 4 tier-1 at pinned versions |
| Nightly (CI) | layer 4 against each engine's `latest`; failure = drift, open issue with fixture diff |
| Release gate | full matrix: every supported engine (Ollama, llama.cpp, vLLM, SGLang, …) × every supported version, plus layer 5 against the performance budget |
| Touched routing, the prefix index, the cost model, defaults for unknown values, or anything a client sends | + a scenario run: `examples/mac-native/up.sh`, then `uv run --with duckdb replay.py` (escaped) and again with `--raw`; read the disagreements |
| Hot-path change | + `go test -bench` on the affected path, compare against the budget in ARCHITECTURE §15 |

## Before you report done

State plainly:

- what's verified and at which layer (e.g. "vLLM 0.x: Running verified live; Waiting verified by fixture replay only");
- what's still **[U]** (doc-only, never observed against a real engine);
- which layers you did **not** run and why (e.g. no docker available → layer 4 not run).

Worked examples for each path: [examples.md](examples.md).

## Further reading

- Kent Beck, [Test Desiderata](https://testdesiderata.com/): the properties a test trades off (fast, deterministic, behavioral, structure-insensitive, …).
- Martin Fowler, [Self-Initializing Fake](https://martinfowler.com/bliki/SelfInitializingFake.html) and [Contract Test](https://martinfowler.com/bliki/ContractTest.html): record real responses, replay them, and keep checking the double against the real thing. That's layers 4 → 2.
- Google Testing Blog, [Test Sizes](https://testing.googleblog.com/2010/12/test-sizes.html): classify by what the test touches (process, network), not by name.
- Ham Vocke, [The Practical Test Pyramid](https://martinfowler.com/articles/practical-test-pyramid.html).
- Kent C. Dodds, [Write tests. Not too many. Mostly integration.](https://kentcdodds.com/blog/write-tests), and DHH, [Test-induced design damage](https://dhh.dk/2014/test-induced-design-damage.html): the case against unit-testing glue.
