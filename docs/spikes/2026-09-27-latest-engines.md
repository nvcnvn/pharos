# Spike: latest engine releases, 2026-09-27

Goal: see what each supported engine's latest release actually reports before writing any probe (testing skill, Engine path step 1). Captures are in `internal/engine/testdata/<engine>/<version>/` and were recorded by `test/engines/capture.sh`.

Setup for every engine: Qwen2.5-0.5B-Instruct, 2 parallel slots, CPU only. Docker Desktop on an Apple Silicon Mac (linux/arm64 VM, 8 CPUs, 16 GB); mlx-lm ran natively on the host. The busy state sends 4 long requests to the 2 slots, so the expected result is 2 running and 2 waiting. The prefix test sends the same ~3200-token system prompt twice.

"Observed" below means we saw it in a capture from a real run. It does **not** mean a layer-4 test asserts it; there are no tests yet.

## Versions

| Engine | Version | Image / package | Ran here |
|---|---|---|---|
| Ollama | v0.34.4 | `ollama/ollama:0.34.4` | yes |
| llama.cpp | v0.5.0 (`/props` build_info `b11146-7fe450e19`) | `ghcr.io/ggml-org/llama.cpp:server-v0.5.0` | yes |
| llama-swap | v260 (bundles llama.cpp b11176) | `ghcr.io/mostlygeek/llama-swap:v260-cpu-b11176` | yes |
| vLLM | v0.30.0 | `vllm/vllm-openai-cpu:v0.30.0` (has an arm64 build) | yes |
| mlx-lm | v0.31.3 | PyPI, via `uvx` | yes |
| SGLang | v0.5.20 | `lmsysorg/sglang:v0.5.20-xeon` (amd64 only) | **no**: still not up after 5 min under amd64 emulation. Left for the CI amd64 runner |
| LM Studio | — | no official Linux image; not installed here | **no** |
| TRT-LLM | — | needs a GPU | **no** |

llama.cpp has changed its versioning. Its GitHub "latest" release is now `v0.5.0`, while the `bNNNN` builds are marked as prereleases.

## Signals observed

| Signal | Ollama 0.34.4 | llama.cpp v0.5.0 | llama-swap v260 | vLLM 0.30.0 | mlx-lm 0.31.3 |
|---|---|---|---|---|---|
| Version | `/api/version` `{"version":"0.34.4"}` | `/props` `build_info` | `/api/version` `{"version":"v260",...}` | `/version` | none |
| Residency | `/api/ps`: `[]` → loaded → `[]` after keep-alive | always loaded (single model) | `/running` `state:"ready"` → `[]` after ttl; `/models` `status.value` loaded → unloaded | always loaded | always loaded |
| Running / Waiting | none | `llamacpp:requests_processing 2`, `requests_deferred 2`; `/slots` `is_processing` `[true,true]` | none (its `/metrics` has host CPU and memory only) | `vllm:num_requests_running 2`, `num_requests_waiting 2` (label `model_name`, plus `engine`) | none |
| Capacity | none (but see the logs row) | `/props` `total_slots: 2` | none | none seen | none |
| KV usage | none | none | none | `vllm:kv_cache_usage_perc` (0..1). `gpu_cache_usage_perc` is **absent** | none |
| Cached tokens in the response | **yes**: OpenAI `prompt_tokens_details.cached_tokens` 0 → 3208; native `prompt_eval_cached_count` | `timings.cache_n` 0 → 3208, and `cached_tokens` 0 → 3208 | same as llama.cpp (passthrough) | only with `--enable-prompt-tokens-details`: 0 → 3200, plus a new field `created_cache_tokens` | `cached_tokens` 3 → 3208 |
| Logs | a startup `server config` line contains `OLLAMA_NUM_PARALLEL:2`. No load or unload line at INFO | not reviewed | not reviewed | a periodic line: `Running: 2 reqs, Waiting: 2 reqs` | not reviewed |

## Findings that change the docs

1. **Ollama reports cached prompt tokens**, on both its APIs (0.34.4). This resolves STRATEGY §4 [U] and ARCHITECTURE §16 Q2 for this version.
2. **The auto-detect order is wrong.** llama-swap answers `/api/version` with a `version` field, so "`/api/version` → Ollama" would classify llama-swap as Ollama. llama-swap needs to be checked first (`/running`), or the detector has to look at the body.
3. **Emulation is real.** llama.cpp's `/models` returns an Ollama-shaped `models` list with placeholder fields (`size:""`, `digest:""`) next to the OpenAI `data` list. This is exactly the case the "recipes are keyed by kind" rule guards against.
4. **vLLM leaves out cached tokens by default.** The prefix-feedback loop needs `--enable-prompt-tokens-details` on the server. `doctor` should report when it's off.
5. **`vllm:gpu_cache_usage_perc` is not in 0.30.0.** No probe gets written for it until a capture from an older version shows it.
6. **vLLM serves `/metrics` as `text/plain; version=1.0.0`**, which is newer than 0.0.4. Our line parser has to accept it; the replay tests cover this.
7. **Ollama 0.34.4 runs `llama-server` internally** (log line `using llama-server for model`). Worth trying: does it expose that server's port or metrics? [U]
8. **llama-swap proxies `/upstream/<model>/...`** to the backing llama-server [U, not captured]. Calling it loads the model, so the harness doesn't touch it. It is a candidate for a composed llama-swap + llama.cpp recipe.

## Harness notes

- The harness itself was wrong twice. That's why the spike runs before the test. The first busy prompt produced about 10 tokens, and vLLM's full-precision weights stopped early even on the second one. Busy requests now send `ignore_eos: true`.
- vLLM needs `shm_size`, otherwise it crashes at startup with "Insufficient space in /dev/shm".
- No captured log contains prompt text. mlx-lm's log contained the local home path, which the script now replaces with `~`.

## Still [U]

- Everything about SGLang, LM Studio and TRT-LLM.
- Whether the amd64 vLLM CPU image and the SGLang Xeon image run on GitHub's ubuntu runners (CPU flags, disk space).
- Whether mlx-lm runs on GitHub's macOS runners (Metal inside the VM).
- Ollama: whether a full queue returns 503; whether the keep-alive state change bumps anything we could observe besides `/api/ps`.
- Older versions of every engine. Only the latest release was captured.
