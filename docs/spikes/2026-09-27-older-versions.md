# Spike: supporting older engine versions, 2026-09-27

Goal: work out how to cover every engine release from the last 12 months without capturing all ~3,400 of them. The approach is to find the releases where a name we read changes, capture those, and support the rest as ranges between them.

Tags used below:
- **[U]** means the fact comes from source history only; no capture or test has confirmed it.
- **Observed** means a capture from a real run shows it.

## Method

1. **Signature.** `test/engines/signature.sh <engine>/<version>` reduces a capture to its shape, not its values:
   - each path and its HTTP status;
   - metric names with their label keys;
   - JSON key paths in every body;
   - usage key paths in the response streams.

   Two versions with the same signature look the same to every probe, except when a name keeps its spelling but changes meaning. The harness sets up the same states on every version (e.g. busy = 2 running and 2 waiting), so replay expectations keyed by engine and state catch those meaning changes.
2. **Source check.** Run `git log -S<name>` between the first and last tag in the window, for every name a probe reads, and map each hit to the first release that contains it. This is only a hypothesis. We use it to choose which versions to capture, never to write a probe.
3. **Capture** the oldest release in the window, the latest, and the release on each side of every change point. If two neighbouring captures differ and the source check didn't predict it, bisect between them.
4. **Commit only the change points.** The support matrix lists version ranges.

A full run over every release is not worth it. llama.cpp alone has 3,075 releases in the window. The other engines together have about 280.

## Oldest image we can pull, per engine

| Engine | Oldest release in window | CPU image available |
|---|---|---|
| vLLM | v0.11.0 | `vllm/vllm-openai-cpu` (Docker Hub) only from **v0.16.0**. `public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo` has v0.6.5 → v0.29.0 (no v0.11.0; v0.11.1 is the first in the window), and the profile now falls back to it below v0.16.0. v0.10.2's image starts `api_server`, so `vllm/v0.10.2.compose.yaml` switches it to `vllm serve` |
| llama.cpp | b6601 | ghcr `server-bNNNN` exists for 351 of the 3,075 builds in the window (about 1 in 9, and b6603 is missing). That sets how precise bisecting can be. `server-v0.x` tags exist only from b10833 |
| Ollama | v0.12.4 | Docker Hub has every release |
| SGLang | v0.5.3 | `-xeon` images go back to v0.4.7 |
| llama-swap | v163 | `-cpu` images start at **v172**; earlier releases ship only cuda, intel, musa and vulkan. **v218 has no image**, so v219 stands in for the "after" side of that change |

## Change points found in source [U]

The raw report is in the session scratchpad. The ones that matter for probes:

| Engine | Release | Change | Probe impact |
|---|---|---|---|
| vLLM | v0.11.0 / v0.12.0 | `vllm:gpu_cache_usage_perc` is hidden in v0.11.0 and removed in v0.12.0. v0.10.2 is the last release that shows it by default | An old KV probe would serve only v0.11.0 and older. Low value, because v0.11.0 is already the start of the window |
| vLLM | v0.20.0 | `vllm:num_requests_waiting_by_reason{reason}` added | Possible new Waiting probe; not needed |
| vLLM | v0.11.0 → v0.30.0 | running, waiting and kv usage names and the `model_name` label unchanged | Current probes should cover the whole window. Needs confirming at v0.11.x |
| llama.cpp | **b10408** | Metrics rewrite. `requests_processing` and `requests_deferred` keep their names, but their values are recomputed | **Same name, possibly new meaning.** Capture b10398 and b10423 (the nearest published images) and compare busy and saturated values |
| llama.cpp | b7218, b8777 | router mode `/models`; `/props` build_info in router mode | Matters once a router-mode recipe exists |
| llama.cpp | b7492, b10519 | auto-sleep; `/metrics`, `/props` and `/models` answer while asleep | Residency when sleeping is a new state no capture has shown |
| Ollama | v0.33.3 | cached tokens added (`prompt_eval_cached_count`, `prompt_tokens_details.cached_tokens`) | **Observed**, see below |
| Ollama | v0.30.0 | runner backend replaced (llama-server only) | Log lines and load behaviour may change |
| SGLang | v0.5.6 | `/get_server_info` renamed `/server_info`, and the old path stays as an alias | Auto-detect: fingerprint both paths. The alias may be removed after v0.5.20 |
| SGLang | v0.5.8, v0.5.11 | `/v1/loads` added; `sglang:kv_*_tokens` added | Matters once an SGLang recipe exists |
| llama-swap | v173 | `/api/version` added; it returns 404 before this | Version is unknown before v173; auto-detect must not rely on it |
| llama-swap | **v218** | `/running` also lists processes in `starting` or `stopping` | Our probe maps anything other than `ready` to unknown. Capture v217 and v219 (v218 has no image) to confirm |

## Observed: Ollama v0.12.4 compared with v0.34.4

Captured locally with the same harness and setup as the latest-engines spike. The capture is not committed, and `capture.sh ollama v0.12.4` reproduces it in a few minutes.

The signature diff shows exactly two differences:
- `/api/tags` gains `capabilities`, `details.context_length` and `details.embedding_length` in the later release. No probe reads these.
- The cached-token fields appear only in v0.34.4: `prompt_eval_cached_count` in the native API and `usage.prompt_tokens_details.cached_tokens` in the OpenAI API. That matches the source check (added in v0.33.3).

Everything the probes read is the same:
- `/api/version`;
- `/api/ps` residency (loaded → `[]` after keep-alive) and `size_vram` (0 on CPU);
- `/api/tags` size;
- the path statuses;
- the `OLLAMA_NUM_PARALLEL:2` log line.

So on Ollama before v0.33.3, cached tokens are **unknown**, not 0. ARCHITECTURE §16 item 2 plans to fall back on `prompt_eval_duration` anomalies for these versions. This capture can't test that, because both native requests took about 12 ms for 3,209 tokens. The cause is the harness, not Ollama: it sent the native requests after the OpenAI ones with the same prefix, so both were already cached (v0.34.4 reports 3,208 cached tokens on both). The native requests now get their own prefix, which makes the first one cold. A rerun with that change shows the contrast on v0.12.4: the first native request took **8.7 s** to prefill 3,211 tokens, and the second took **12 ms**. Neither reports a cached-token field. So the duration fallback looks workable on old Ollama. Observed on one version, CPU only.

## Signals only some scenarios produce

The signature and the replay tests can only see signals that some workload makes the engine emit. `capture.sh` now also runs:

- **saturated:** 8 requests against 2 slots, so waiting should read 6. This checks that the counts scale and aren't stuck at 2, which is what a "same name, new meaning" change like llama.cpp b10408 would break;
- **cancelled:** every client is killed after `saturated` is captured, then the state is captured again 3 s later. It shows whether the engine stops working for requests nobody is waiting on. This runs after `cold`, so an engine that keeps generating can't break the residency checks;
- **unknown model:** the status and error body for a model the engine doesn't serve;
- **a cold prefix on Ollama's native API** (see above).

Best-effort backlog. These need per-engine flags, so each one is an overlay on a profile:

- **KV pressure:** a small KV cache plus long prompts, to see preemption and waiting reasons (vLLM `waiting_by_reason`).
- **llama.cpp router mode and auto-sleep.**
- **Speculative decoding:** vLLM's ngram method needs no draft model. Whether it runs on the CPU build is [U].
- **llama-swap with a process that is still starting,** captured while it starts.

## CI backfill

`test/engines/candidates.json` lists 49 versions: the change points above, the release on each side of them, and each engine's current latest. mlx-lm is left out, because it runs only on macOS runners and has no probes of its own. A pull request that changes this file runs the backfill. Dispatching `engine-captures` with a `versions` list does the same.

Each matrix job:
- runs `TestLive` at that version with `PHAROS_RECORD=1`. Assertion failures don't stop the capture; they are findings;
- uploads `capture-<engine>-<version>`, which holds the capture and `test.log`.

The `report` job:
- puts the captures back into the testdata layout;
- runs `test/engines/report.sh`. Per version, that prints the numbers the probes read in each state, any `TestLive` failures, and the signature diff against the previous version;
- writes the output to the run summary and a `report` artifact.

No probe or recipe changes until the report has been read.

Cost: the repo is public, so standard GitHub-hosted runners are free. About 20 jobs run at once and the rest queue. The images and the model download on GitHub's runners.

## Next

1. Read the report, then commit the captures at the change points and rekey the replay rows as `engine/*/state` with per-version overrides.
2. Bisect any difference the source check didn't predict.
3. Update STRATEGY §4 (Ollama cached tokens on older versions, `gpu_cache_usage_perc`) and ARCHITECTURE §16 item 2 with what the report confirms.
