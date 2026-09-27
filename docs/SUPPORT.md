# Support matrix

Engine version × signal (ARCHITECTURE §4). Each cell names the probe that supplies the signal on that version, and how we know:

- **live**: the layer-4 test (`internal/engine/live_test.go`, `TestLive`) runs the engine and asserts the behavior: busy → 2 running and 2 waiting, loaded → cold after keep-alive, capacity 2, cached tokens on a repeated prefix.
- **fixture**: layer-2 replay asserts the exact value on a committed capture of that version (`internal/engine/testdata/<engine>/<version>/`). The capture came from a real run, but no live test asserts the behavior.
- **unknown**: no probe reads it on this engine. *unknown (live)* means `TestLive` asserts it stays unknown, so a release that starts reporting it fails the test and gets a new cell here.
- **[U]**: seen in docs or source only.

Every engine ran with Qwen2.5-0.5B-Instruct on CPU, 2 parallel slots. Live runs: Docker Desktop on Apple Silicon (linux/arm64), 2026-09-27. The PR job (`ci.yml`) runs `TestLive` for Ollama, llama.cpp and vLLM at these versions; the nightly job runs every engine at its latest release.

## Ollama

Kind `ollama`, one target per model.

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v0.34.4 | `ollama-version` fixture | `openai-models` fixture | `ollama-ps-residency` **live** | `ollama-ps-size-vram` fixture | `ollama-tags-size` fixture | unknown (live) | unknown (live) | `ollama-log-num-parallel` (log feed) **live** | unknown | `prompt_tokens_details.cached_tokens` **live** |

- Capacity needs a log feed (`logs: docker://<container>`, or Docker discovery later). Without one it is unknown and the config `capacity:` applies. The value is `OLLAMA_NUM_PARALLEL`, which Ollama applies per loaded model [U: from docs].
- v0.12.4 was captured in a spike (docs/spikes/2026-09-27-older-versions.md, not committed): every probe above reads the same shape; cached tokens are absent before v0.33.3 [U: source], so unknown.

## llama.cpp

Kind `llamacpp`, single model (router mode has no recipe yet).

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v0.5.0 (b11146) | `llamacpp-props-build-info` fixture | `openai-models` fixture | unknown (live; always loaded) | unknown | unknown | `llamacpp-running` **live** | `llamacpp-waiting` **live** | `llamacpp-props-total-slots` **live** | unknown | `prompt_tokens_details.cached_tokens` **live** |

- Running falls back to `llamacpp-slots-is-processing` (`/slots`) when `--metrics` is off: fixture only (busy = 2), because the profile runs with `--metrics`.
- b10408 rewrote how `requests_processing` and `requests_deferred` are computed, same names [U: source]. Versions before it are unverified.

## llama-swap

Kind `llama-swap`, one target per model.

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v260 (llama.cpp b11176) | `llamaswap-version` fixture | `openai-models` fixture | `llamaswap-running` **live** | unknown | unknown | unknown (live) | unknown (live) | unknown (live) | unknown | `prompt_tokens_details.cached_tokens` **live** (passed through from llama.cpp) |

- `/running` states other than `ready` read as unknown residency; v218 added `starting` and `stopping` [U: source].
- `/api/version` exists from v173 [U: source]; version is unknown before it.

## vLLM

Kind `vllm`, single model.

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v0.30.0 (CPU image) | `vllm-version` fixture | `openai-models` fixture | unknown (live; always loaded) | unknown | unknown | `vllm-running` **live** | `vllm-waiting` **live** | unknown (live) | `vllm-kv-cache-usage-perc` fixture | `prompt_tokens_details.cached_tokens` **live**, only with `--enable-prompt-tokens-details` |

- `vllm:gpu_cache_usage_perc` is absent in v0.30.0 and has no probe.

## Engines without a recipe (kind `openai`)

These resolve as the generic kind: only Models is read. The signals they expose are candidates for a recipe, not probes.

| Engine, version | Models | Other signals seen in the capture (no probe yet) | Cached tokens in replies | Live |
|---|---|---|---|---|
| SGLang v0.5.20 (`-xeon`, amd64) | `openai-models` fixture | `sglang:num_running_reqs`, `sglang:num_queue_reqs`, `/v1/loads`, `/server_info` `max_running_requests` | `prompt_tokens_details.cached_tokens`, only with `--enable-cache-report` (CI capture) | nightly CI only; the image doesn't run on arm64 here |
| mlx-lm v0.31.3 | `openai-models` fixture | none | `prompt_tokens_details.cached_tokens` (capture) | nightly CI (macOS) only |

## Not covered

LM Studio and TRT-LLM: never run. Every signal is [U].
