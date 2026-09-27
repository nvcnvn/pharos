# Next sessions: build order step 1 (`internal/engine`)

Paste one prompt per fresh session, in order. Each prompt is sized to fit in one context window. CLAUDE.md is loaded automatically, so the prompts don't repeat its rules. Delete this file when step 1 is done.

State as of 2026-09-27:
- No Go code yet.
- The spike on the latest engine releases is done. Findings are in [spikes/2026-09-27-latest-engines.md](spikes/2026-09-27-latest-engines.md).
- Raw captures are in `internal/engine/testdata/<engine>/<version>/{idle,loaded,busy,cold,streams}/`. Each state directory holds one body per path (`/api/ps` → `api_ps`) and a `paths.tsv` with each path's status and content type. 404s appear only in `paths.tsv`.
- `test/engines/capture.sh <engine> [version]` re-records a capture from `test/engines/<engine>/{profile.sh,compose.yaml}`. The `engine-captures` workflow runs it as a CI matrix.

---

## Session 1: module, core types, Prometheus parser, `Prom` constructor

```
Pharos, build order step 1, session 1 of 5 (plan: docs/NEXT.md).
Read docs/ARCHITECTURE.md §2–§4 and §14, docs/spikes/2026-09-27-latest-engines.md, and load the testing skill.

Do:
- `go mod init` (module path github.com/nvcnvn/pharos). Stdlib only for now.
- internal/engine: Opt[T], Signal, Snapshot, ModelInfo, Load, Probe, Feed (types only, as in §4).
- A small Prometheus text parser: gauges and counters, labels, float formats including NaN/+Inf.
  It must accept both `version=0.0.4` and vLLM's `version=1.0.0`. TDD at layer 1.
- The `Prom(name, path, signal, metric, opts...)` constructor with PerModel(label) and a scale option.
- A layer-2 replay test that runs every Prom probe we are allowed to write (only metric names seen
  in the captures: llamacpp:requests_processing/deferred, vllm:num_requests_running/waiting,
  vllm:kv_cache_usage_perc) against every capture dir that has /metrics. Expected values: busy → 2/2
  on llama.cpp and vLLM. On other engines' /metrics (llama-swap, idle states) → unknown, never 0.
Done when: go test ./... is green, and each test has been seen to fail at least once.
```

## Session 2: capture replay helper and JSON probes

```
Pharos, build order step 1, session 2 of 5 (plan: docs/NEXT.md). Session 1 added the types, the Prom parser and Prom probes.
Load the testing skill. Reread ARCHITECTURE §4 (probe library) and the spike findings.

Do:
- A test helper that serves one capture state dir (paths.tsv + bodies) as an httptest server,
  with the recorded statuses, 404s included. Put it in internal/engine, no new package.
- The JSON probes, only for fields that appear in the captures: ollamaVersion (/api/version),
  ollamaPSResidency + ollamaPSVRAM (/api/ps), ollamaTagsSize (/api/tags), llamacppProps
  (total_slots, build_info), llamacppSlotsRunning (/slots is_processing), llamaswapRunning (/running),
  vllmVersion (/version), openaiModels (/v1/models).
- Layer-2 table: every probe × every capture dir that contains its path. Include the cross-engine
  cases: llama-swap's /api/version must not read as an Ollama version; llama.cpp's Ollama-shaped
  /models must not feed Ollama probes. Also absent field → OK=false, and garbage body → error
  (short inline strings derived from a real capture).
- Residency across states: Ollama and llama-swap captures go loaded → cold after keep-alive.
Done when: go test ./... is green, and the probe list maps to the cells of the spike findings table.
```

## Session 3: recipes, `Resolve`, `Scrape`, auto-detect

```
Pharos, build order step 1, session 3 of 5 (plan: docs/NEXT.md). Sessions 1–2 added the types, Prom
probes, JSON probes and a capture-replay httptest helper.
Load the testing skill. Reread ARCHITECTURE §4 (Merge, plan, Kind: auto).

Do:
- Recipes for Ollama, llama.cpp, llama-swap, vLLM and the generic OpenAI kind (mlx-lm uses the generic one).
- Resolve: drop reasons (404, parse error, no value, redundant, version guard). Plan.Scrape: one GET
  per path. Merge: the first known value in plan order wins, and From records which probe.
  Layer 1 for the merge and redundancy rules.
- Auto-detect with the corrected order: llama-swap has to be checked before Ollama, because it also
  answers /api/version (spike finding 2). Layer-2 test: every capture dir → expected kind.
- Layer-2 Resolve test: every capture dir → expected active/dropped probes and merged Snapshot.
Done when: go test ./... is green, and ARCHITECTURE §4 is updated wherever the code had to differ from it.
```

## Session 4: log probes, own probes, `pharos doctor`

```
Pharos, build order step 1, session 4 of 5 (plan: docs/NEXT.md). Sessions 1–3 built the probes, recipes, Resolve and Scrape.
Load the testing skill. Reread ARCHITECTURE §4 (Log probes, Own probes) and §10.

Do:
- The LogLine constructor and Plan.Follow, with a layer-1 test. Ship no built-in log probe unless its line
  is in a captured engine.log. The Ollama "server config ... OLLAMA_NUM_PARALLEL:N" line is a candidate
  for Capacity (see the spike findings). If you add it, replay it against ollama/v0.34.4/engine.log.
- Own probes: parse them from YAML config (gopkg.in/yaml.v3), and put them ahead of the recipe.
- cmd/pharos with `doctor`: for each configured backend print kind, version, active and dropped probes
  with reasons, and unknown signals. Add `doctor --record <dir>`, which writes the same layout that
  capture.sh writes (paths.tsv + bodies; logs only with --logs, plus a prompt-content warning).
Done when: `pharos doctor` against a running `test/engines/capture.sh`-style engine prints a correct plan,
and go test ./... is green.
```

## Session 5: layer-4 live tests and CI

```
Pharos, build order step 1, session 5 of 5 (plan: docs/NEXT.md). The engine package and doctor exist.
Load the testing skill. Reread ARCHITECTURE §14 (layer 4, CI cadence).

Do:
- Go integration tests (build tag `integration`) that start engines from test/engines/<engine>/ compose
  profiles. Reuse the profiles; don't copy them. Assert behavior through Resolve/Scrape: busy → Running=2,
  Waiting=2 where the engine reports it, and unknown where it doesn't (Ollama, llama-swap). Cold after
  keep-alive. Cached tokens > 0 on the repeated prefix.
- Choose one recorder. Either PHAROS_RECORD=1 calls capture.sh, or `doctor --record` replaces its
  recording step. Update the testing skill and ARCHITECTURE §14 to match.
- CI: a PR job with layers 1–3 plus layer 4 for tier-1 engines (Ollama, llama.cpp, vLLM) at pinned versions.
  The nightly job keeps running latest.
- Write docs/SUPPORT.md, the support matrix: engine version × signal → probe, verified live or by
  fixture only, or unknown.
Done when: the layer-4 tests pass locally for Ollama, llama.cpp, llama-swap and vLLM, and the PR job is green.
```
