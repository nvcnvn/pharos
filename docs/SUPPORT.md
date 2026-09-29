# Support matrix

Engine version × signal (ARCHITECTURE §4). Each cell names the probe that supplies the signal on that version, and how we know:

- **live**: the layer-4 test (`internal/engine/live_test.go`, `TestLive`) runs the engine and asserts the behavior: busy (4 requests) → 2 running and 2 waiting, saturated (8 requests) → 2 running and 6 waiting, loaded → cold after keep-alive, capacity 2, cached tokens on a repeated prefix.
- **fixture**: layer-2 replay asserts the exact value on a committed capture of that version (`internal/engine/testdata/<engine>/<version>/`). The capture came from a real run, but no live test asserts the behavior.
- **unknown**: no probe reads it on this engine. *unknown (live)* means `TestLive` asserts it stays unknown, so a release that starts reporting it fails the test and gets a new cell here.
- **capture**: seen in a committed capture, but no test asserts it.
- **[U]**: seen in docs or source only.

The *Cached tokens in replies* column is what the proxy's stream tap reads from each reply (`engine.ParseUsage`); `TestReplayUsage` replays it on every recorded response stream. For an OpenAI chat stream that didn't ask for usage, the proxy sets `stream_options.include_usage` and strips the usage-only chunk from the reply (ARCHITECTURE §9); every recorded version sends that chunk (`TestTapStripReplaysRecordedStreams`). Completion tokens, which daily quotas count, are read from `usage.completion_tokens`, llama.cpp `timings.predicted_n` or Ollama's native `eval_count`; every recorded stream of every version reports one (fixture).

**Replies other than a streamed chat** (`streams/replies.tsv`, recorded on the pinned Ollama v0.34.4, llama.cpp v0.5.0 and vLLM v0.30.0 only): a non-streamed chat and completion, and Ollama's native non-streamed chat and streamed generate, report prompt and completion tokens on all three (**live**: `TestLive` asserts every served reply reports its prompt tokens). Embeddings report prompt tokens only, which the proxy meters with 0 completion tokens. Ollama v0.34.4 serves embeddings only from an embedding model (all-minilm) and answers 501 from a chat model; the llama.cpp chat server answers 501 (it needs `--embeddings`) and vLLM 404 (no embeddings route for a generative model). The other engines and versions: [U].

Every engine ran with Qwen2.5-0.5B-Instruct on CPU, 2 parallel slots. Live runs of the latest versions: Docker Desktop on Apple Silicon (linux/arm64), 2026-09-27. The older versions come from the `engine-captures` backfill on GitHub's amd64 runners (run 36319408570, [spike](spikes/2026-09-27-older-versions.md)). The PR job (`ci.yml`) runs `TestLive` for Ollama, llama.cpp and vLLM at their pinned versions; the nightly job runs every engine at its latest release.

**Missing saturated captures.** The pinned captures (Ollama v0.34.4, llama.cpp v0.5.0, llama-swap v260, vLLM v0.30.0, SGLang v0.5.20, mlx-lm v0.31.3) were recorded before `capture.sh` added the `saturated` and `cancelled` states. Saturated counts (2 running, 6 waiting) are checked live on every `TestLive` run, but replay checks them only on the older versions. Re-record the pinned versions with `PHAROS_RECORD=1` so replay covers them too.

## Ollama

Kind `ollama`, one target per model.

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v0.34.4 | `ollama-version` fixture | `openai-models` fixture | `ollama-ps-residency` **live** | `ollama-ps-size-vram` fixture | `ollama-tags-size` fixture | unknown (live) | unknown (live) | `ollama-log-num-parallel` (log feed) **live** | unknown | `prompt_tokens_details.cached_tokens` **live**; native `prompt_eval_cached_count` fixture |
| v0.33.3 | `ollama-version` fixture | `openai-models` fixture | `ollama-ps-residency` fixture | `ollama-ps-size-vram` fixture | `ollama-tags-size` fixture | unknown | unknown | `ollama-log-num-parallel` fixture | unknown | `prompt_tokens_details.cached_tokens`, `prompt_eval_cached_count` fixture |
| v0.33.2, v0.30.0, v0.12.4 | `ollama-version` fixture | `openai-models` fixture | `ollama-ps-residency` fixture | `ollama-ps-size-vram` fixture | `ollama-tags-size` fixture | unknown | unknown | `ollama-log-num-parallel` fixture | unknown | none; a cold prefix prefills ~1,000× slower than a warm one (capture) |

- Capacity needs a log feed (`logs: docker://<container>`, `logs: file:///<path>` for a native install, or Docker discovery). Without one it is unknown and the config `capacity:` applies. The value is `OLLAMA_NUM_PARALLEL`, which Ollama applies per loaded model [U: from docs].
- v0.32.15, native on macOS with Metal (not in the table; checked by hand 2026-09-29, no capture): `doctor` reads version, models, residency, size, and capacity from the log file (`logs: file:///…`); the ollama CLI works through `serve`; `load_duration` is 0.5 ms warm and 0.55 s cold for Qwen2.5-0.5B; `/v1/responses` and `/v1/messages` answer 200 (Pharos doesn't route them yet).
- v0.34.4: an OpenAI chat stream through Pharos without `include_usage` is byte-identical to the engine's own (compared by hand, 2026-09-28).
- Cached tokens arrive in v0.33.3 (fixture). v0.30.0 reports `prompt_eval_count: 1` for a warm 3,211-token prefix, where the versions around it report 3,211: it may count only uncached tokens there (one run).

## llama.cpp

Kind `llamacpp`, single model (router mode has no recipe yet).

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v0.5.0 (b11146) | `llamacpp-props-build-info` fixture | `openai-models` fixture | unknown (live; always loaded) | unknown | unknown | `llamacpp-running` **live** | `llamacpp-waiting` **live** | `llamacpp-props-total-slots` **live** | unknown | `prompt_tokens_details.cached_tokens` **live** |
| b8772 | `llamacpp-props-build-info` fixture | `openai-models` fixture | unknown (always loaded) | unknown | unknown | `llamacpp-running` fixture | `llamacpp-waiting` fixture | `llamacpp-props-total-slots` fixture | unknown | `prompt_tokens_details.cached_tokens` fixture |
| b7493 | `llamacpp-props-build-info` fixture | `openai-models` fixture | unknown (always loaded) | unknown | unknown | `llamacpp-running` fixture | unknown: version guard (fixture) | `llamacpp-props-total-slots` fixture | unknown | `timings.cache_n` only (fixture) |
| b7139 | `llamacpp-props-build-info` fixture | `openai-models` fixture | unknown (always loaded) | unknown | unknown | `llamacpp-slots-is-processing` fixture | unknown: version guard (fixture) | `llamacpp-props-total-slots` fixture | unknown | `timings.cache_n` only (fixture) |
| b6602 | `llamacpp-props-build-info` **live** | `openai-models` **live** | unknown (live; always loaded) | unknown | unknown | `llamacpp-running` **live** | unknown: version guard **live** | `llamacpp-props-total-slots` **live** | unknown | `timings.cache_n` only **live** |

- Running falls back to `llamacpp-slots-is-processing` (`/slots`) when `--metrics` is off: fixture only (busy = 2), because the profile runs with `--metrics`.
- Before b8772, `requests_deferred` reads 0 while requests are queued, so `llamacpp-waiting` has a version guard: it runs only from build b8772 (`/props` `build_info`). Between b7494 and b8771 no capture exists, so those builds read unknown too. b10408 rewrote how both metrics are computed [U: source], but b10398 and b10423 read the same.
- Before b8772, with 8 requests on 2 slots the server answers no path at all: every signal goes stale, and auto-detect can't run until load drops.
- v0.5.0 answers 503 on every path, its own and others', for the seconds it takes to load its model (`loading` capture, fixture). Auto-detect waits for it rather than settling on the generic kind.
- b7139 (and b7151) serve `/metrics` as a JSON-quoted string: no Prometheus value, so `/slots` reads Running.
- b6602 **live** means `TestLive` with `PHAROS_LIVE_VERSION=b6602`, run locally for the version guard (2026-09-27); CI doesn't run that version. Its cached-tokens check failed there while `TestLive` read only `prompt_tokens_details`. It now reads usage with the stream tap's parser, which falls back to `timings.cache_n`, and passes (re-run 2026-09-27).
- Cached tokens before b8772 are only in `timings.cache_n`, which every build from b6602 to v0.5.0 reports. The stream tap reads it as the fallback field (fixture).

## llama-swap

Kind `llama-swap`, one target per model.

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v260 (llama.cpp b11176) | `llamaswap-version` fixture | `openai-models` fixture | `llamaswap-running` **live** | unknown | unknown | unknown (live) | unknown (live) | unknown (live) | unknown | `prompt_tokens_details.cached_tokens` **live** (passed through from llama.cpp) |
| v219 | `llamaswap-version` fixture | `openai-models` fixture | `llamaswap-running` fixture | unknown | unknown | unknown | unknown | unknown | unknown | `prompt_tokens_details.cached_tokens` fixture |
| v185 (llama.cpp b7769) | `llamaswap-version` fixture | `openai-models` fixture | `llamaswap-running` fixture | unknown | unknown | unknown | unknown | unknown | unknown | `timings.cache_n` only (fixture) |

- `/running` states other than `ready` read as unknown residency; v218 added `starting` and `stopping` [U: source]. Our captures show only `ready`.
- `/api/version` exists from v173 [U: source]; version is unknown before it. v185 and v219 report `"185"` and `"219"`, which `ollama-version` would accept as an Ollama version; only the recipe keeps it off llama-swap.
- v172, v173 and v217 did not start the model with our config (the bundled llama.cpp can't read the model cache; v217 has no `/app/llama-server`), so they have no row.

## vLLM

Kind `vllm`, single model.

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v0.30.0 (CPU image) | `vllm-version` fixture | `openai-models` fixture | unknown (live; always loaded) | unknown | unknown | `vllm-running` **live** | `vllm-waiting` **live** | unknown (live) | `vllm-kv-cache-usage-perc` fixture | `prompt_tokens_details.cached_tokens` **live**, only with `--enable-prompt-tokens-details` |
| v0.11.1, v0.10.2 | `vllm-version` fixture | `openai-models` fixture | unknown (always loaded) | unknown | unknown | `vllm-running` fixture | `vllm-waiting` fixture | unknown | `vllm-kv-cache-usage-perc` fixture | `prompt_tokens_details.cached_tokens` fixture |

- The backfill also read v0.12.0, v0.15.1, v0.16.0, v0.19.1, v0.20.0, v0.23.0 and v0.24.0 with the same values (not committed). Running, Waiting and KV usage keep name and meaning from v0.10.2 to v0.30.0.
- v0.11.1 and v0.10.2 leave `prompt_tokens_details` out of a reply with nothing cached, so cached tokens read unknown there, not 0 (fixture).
- v0.30.0 moves `system_fingerprint` from the last content chunk to the usage chunk when `include_usage` is on, so a client streaming through Pharos without it doesn't get `system_fingerprint`; the rest of the stream is byte-identical (compared by hand, 2026-09-28).
- `vllm:gpu_cache_usage_perc` exists only in v0.10.2 (same values as `kv_cache_usage_perc`) and has no probe.
- v0.10.2, v0.12.0 and v0.16.0 CPU images need AVX-512 (SIGILL without it).

## SGLang

Kind `sglang`, single model. Detected by `/get_server_info`, which no other captured engine answers.

| Version | Version | Models | Residency | VRAM | Size | Running | Waiting | Capacity | KV usage | Cached tokens in replies |
|---|---|---|---|---|---|---|---|---|---|---|
| v0.5.20 (`-xeon`, amd64) | `sglang-version` fixture | `openai-models` fixture | unknown (always loaded) | unknown | unknown | `sglang-running` fixture | `sglang-waiting` fixture | `sglang-max-running-requests` fixture | unknown | `prompt_tokens_details.cached_tokens`, only with `--enable-cache-report` (fixture) |
| v0.5.11, v0.5.8, v0.5.5.post3 | `sglang-version` fixture | `openai-models` fixture | unknown (always loaded) | unknown | unknown | `sglang-running` fixture | `sglang-waiting` fixture | `sglang-max-running-requests` fixture | unknown | `prompt_tokens_details.cached_tokens` (fixture) |

- **Not live-tested yet.** `TestLive` now asserts SGLang like vLLM (busy → 2 running and 2 waiting, saturated → 2 and 6, capacity 2), but it runs only in the nightly job: the `-xeon` image needs amd64 with AVX-512, and doesn't run on arm64 here. On a host without AVX-512 the test skips (profile `needs()`), not fails. Until a nightly run passes, every SGLang cell is fixture only.
- v0.5.5.post3 to v0.5.11 ran only 1 request with `--max-running-requests 2`: busy reads 1 running and 2 waiting, saturated 1 and 6 (fixture). Capacity still reads 2, the configured value; `Waiting > 0` marks the target full anyway.
- v0.5.11 still counts 4 queued after the clients cancelled (`cancelled` capture); the next scrape after the queue drains reads 0 [U: not captured].
- Running and Waiting are per model (`model_name` label), Capacity is for the whole backend; a target takes each signal from its model, else from the backend.
- `sglang:token_usage` reads 0 under load from v0.5.11 on (4-decimal rounding of a tiny share, going by `/v1/loads` `token_usage: 0.0001` [U]), so KV usage has no probe. `/v1/loads` (from v0.5.8) repeats the metrics as JSON and has no probe.

## Engines without a recipe (kind `openai`)

These resolve as the generic kind: only Models is read.

| Engine, version | Models | Other signals seen in the capture (no probe yet) | Cached tokens in replies | Live |
|---|---|---|---|---|
| mlx-lm v0.31.3 | `openai-models` fixture | none | `prompt_tokens_details.cached_tokens` (fixture) | nightly CI (macOS) only |

## Not covered

LM Studio and TRT-LLM: never run. Every signal is [U].
