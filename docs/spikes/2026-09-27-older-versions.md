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
| llama.cpp | **b10408** | Metrics rewrite. `requests_processing` and `requests_deferred` keep their names, but their values are recomputed | **Same name, possibly new meaning.** Captured b10398 and b10423: no change there. The real change is between b7493 and b8772 (see Results) |
| llama.cpp | b7218, b8777 | router mode `/models`; `/props` build_info in router mode | Matters once a router-mode recipe exists |
| llama.cpp | b7492, b10519 | auto-sleep; `/metrics`, `/props` and `/models` answer while asleep | Residency when sleeping is a new state no capture has shown |
| Ollama | v0.33.3 | cached tokens added (`prompt_eval_cached_count`, `prompt_tokens_details.cached_tokens`) | **Observed**, see below |
| Ollama | v0.30.0 | runner backend replaced (llama-server only) | Log lines and load behaviour may change |
| SGLang | v0.5.6 | `/get_server_info` renamed `/server_info`, and the old path stays as an alias | Auto-detect: fingerprint both paths. The alias may be removed after v0.5.20 |
| SGLang | v0.5.8, v0.5.11 | `/v1/loads` added; `sglang:kv_*_tokens` added | Matters once an SGLang recipe exists |
| llama-swap | v173 | `/api/version` added; it returns 404 before this | Version is unknown before v173; auto-detect must not rely on it |
| llama-swap | **v218** | `/running` also lists processes in `starting` or `stopping` | Our probe maps anything other than `ready` to unknown. v219 onward list only `ready` in our states; v217 did not start with our config (see Results) |

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

So on Ollama before v0.33.3, cached tokens are **unknown**, not 0. ARCHITECTURE §16 item 2 plans to fall back on `prompt_eval_duration` anomalies for these versions. This capture can't test that, because both native requests took about 12 ms for 3,209 tokens. The cause is the harness, not Ollama: it sent the native requests after the OpenAI ones with the same prefix, so both were already cached (v0.34.4 reports 3,208 cached tokens on both). The native requests now get their own prefix, which makes the first one cold. A rerun with that change shows the contrast on v0.12.4: the first native request took **8.7 s** to prefill 3,211 tokens, and the second took **12 ms**. Neither reports a cached-token field. The CI backfill later showed the same contrast on every version from v0.12.4 to v0.33.2 (see Results).

## Signals only some scenarios produce

The signature and the replay tests can only see signals that some workload makes the engine emit. `capture.sh` now also runs:

- **saturated:** 8 requests against 2 slots, so waiting should read 6. This checks that the counts scale and aren't stuck at 2, which is what a "same name, new meaning" change would break;
- **cancelled:** every client is killed after `saturated` is captured, then the state is captured again 3 s later. It shows whether the engine stops working for requests nobody is waiting on. This runs after `cold`, so an engine that keeps generating can't break the residency checks;
- **unknown model:** the status and error body for a model the engine doesn't serve;
- **a cold prefix on Ollama's native API** (see above).

Best-effort backlog. These need per-engine flags, so each one is an overlay on a profile:

- **KV pressure:** a small KV cache plus long prompts, to see preemption and waiting reasons (vLLM `waiting_by_reason`).
- **llama.cpp router mode and auto-sleep.**
- **Speculative decoding:** vLLM's ngram method needs no draft model. Whether it runs on the CPU build is [U].
- **llama-swap with a process that is still starting,** captured while it starts.

## CI backfill

`test/engines/candidates.json` lists 45 versions: the change points above, the release on each side of them, and each engine's current latest. Some candidates are left out:
- **mlx-lm:** it runs only on macOS runners and has no probes of its own;
- **SGLang v0.5.3, v0.5.6, v0.5.7 and v0.5.10.post1:** their images don't start on any runner (missing Python modules, or SGLang's own warm-up request timing out).

The backfill runs only when triggered, by hand (`gh workflow run engine-captures.yml -f versions=candidates`, or a JSON list of versions) or from a schedule added later. It isn't tied to pull requests, because it takes about an hour.

Each matrix job:
- runs `TestLive` at that version with `PHAROS_RECORD=1`. Assertion failures don't stop the capture; they are findings;
- uploads `capture-<engine>-<version>`, which holds the capture, `test.log` and `cpu.txt` (the runner's CPU and AVX-512/AMX flags).

Backfill jobs and SGLang jobs use `continue-on-error`, so they never turn a check red.

The `report` job:
- puts the captures back into the testdata layout;
- runs `test/engines/report.sh`. Per version, that prints the numbers the probes read in each state, any `TestLive` failures, and the signature diff against the previous version;
- writes the output to the run summary and a `report` artifact.

Cost: the repo is public, so standard GitHub-hosted runners are free. About 20 jobs run at once and the rest queue.

### AVX-512

Some vLLM and SGLang CPU images exit with SIGILL (code 132) on runners without AVX-512. GitHub-hosted runners can't be selected by CPU. Across the runs we saw:
- **AMD EPYC 7763:** no AVX-512;
- **AMD EPYC 9V74 and 9V45:** AVX-512 on some runners but not others, with the same model name;
- **Intel Xeon 8573C and 6973P-C:** AVX-512 and AMX.

About a quarter of jobs got AVX-512. `capture.sh` prints `SIGILL: ...` with the host's AVX-512 status when this happens.

- **Needs AVX-512:** vLLM v0.10.2, v0.12.0, v0.16.0; SGLang v0.5.15.post1, v0.5.16, v0.5.20.
- **Runs without it:** vLLM v0.11.1, v0.15.1, v0.19.1 and later; SGLang v0.5.5.post3, v0.5.8, v0.5.11.

Intel SDE emulation was tried and dropped:
- the engines started under it, but none served a request within 600 s;
- it broke the SGLang versions that don't need AVX-512.

No free always-on tier offers AVX-512 x86 with enough memory: Oracle's free tier is Arm or 1 GB AMD, and Google's is a 1 GB `e2-micro`. The AWS free plan (6 months, `m7i-flex.large`, Sapphire Rapids, 8 GB) or AWS open-source credits could host a start-capture-stop runner at release time. Not set up.

## Results, run 36319408570

Captured with the final harness on GitHub runners, 2 slots per engine. States:
- **busy:** 4 requests;
- **saturated:** 8 requests;
- **cancelled:** captured 3 s after every client disconnects.

vLLM v0.12.0 and v0.16.0 come from run 36315618006 (AVX-512 runners, same states). SGLang v0.5.15.post1 and v0.5.16 have no capture (SIGILL). All of the below was **observed**. Anything seen in one run only is marked (1 run).

**vLLM v0.10.2 to v0.30.0.** No drift in the window:
- every version reads 2 running / 2 waiting when busy, 2 / 6 when saturated and 0 / 0 when cancelled;
- `kv_cache_usage_perc` has the same values throughout;
- v0.10.2 also exposes `gpu_cache_usage_perc` with identical values, and it is gone from v0.11.1;
- an unknown model returns 404.

The current probes cover every version, and no probe for the old gauge is needed inside the window.

**llama.cpp.**
- **b6602 to b7493:**
  - `requests_deferred` reads **0** with 2 requests queued. The requests most likely wait for a free HTTP thread before the server accepts them, so the metrics never see a queue. Reading Waiting there gives a wrong number.
  - Under load the server stops answering: with 4 requests, `/health`, `/version` and `/api/version` time out; with 8, every path times out.
- **b8772 onward:** 2 / 2, 2 / 6 and 0 / 0, correct. The change lies between b7493 and b8772, not at b10408 as the source check suggested (b10398 and b10423 read the same).
- **b7139 and b7151:** `/metrics` is a JSON-quoted string with a `text/plain` content type. The parser reads nothing and Waiting stays unknown, which is correct. Running comes from `/slots`.
- **All versions:**
  - `usage.prompt_tokens_details.cached_tokens` exists only from b8772, but **`timings.cache_n` is present in every build from b6602 to v0.5.0** (0 on the first request, 3,208 on the second). It is a fallback field for the stream tap on older builds.
  - Cancelling stops the work within 3 s.
  - An unknown model returns 200: llama.cpp ignores the model name.

**Ollama v0.12.4 to v0.34.4.**
- `/api/ps` residency and `/api/version` are the same on every version.
- Cached tokens are reported from **v0.33.3**, over both APIs.
- **Before v0.33.3, the prefill-duration fallback works:** a cold 3,211-token prefix took 12.8–48 s and a warm one took 20–47 ms on every version.
- **v0.30.0** reports `prompt_eval_count: 1` for the warm request, where v0.24.0 and v0.33.2 report 3,211. On that version the field counts only uncached tokens, the same name with a different meaning (1 run).
- An unknown model returns 404.

**SGLang** (no recipe yet):
- **v0.5.5.post3 to v0.5.11:** only **1** request runs with `--max-running-requests 2` (1 running / 2 queued when busy, 1 / 6 when saturated). v0.5.20 runs 2.
- **v0.5.11:** 4 requests are still queued 3 s after their clients disconnect (1 run).
- **`sglang:token_usage`:** reads above 0 under load up to v0.5.8 and **0.0** from v0.5.11. The same name may mean something different; the `sglang:kv_*_tokens` gauges arrived in v0.5.11.

**llama-swap.**
- **v172, v173 and v217 could not start the model** with our config: the bundled llama.cpp can't read the model cache, and `/app/llama-server` is missing on v217 (exit 127). So their empty `/running` is correct and says nothing about `/running` itself. Those versions need a per-version `config.yaml`.
- **v185 and v186** list only `ready`, as before v218.
- **v219 onward** behave like v260.
- Cached tokens are absent up to v186 (bundled llama.cpp b7769 and b7814), consistent with llama.cpp before b8772.

## Next

1. ~~Commit the change-point captures and rekey the replay rows~~: 15 captures are in `internal/engine/testdata`, and the replay and resolve tests key rows as `engine/*`, `engine/*/state`, `engine/version` and `engine/version/state` (most specific wins). The rest of the 43 are in the artifacts of run 36319408570.
2. Probe and stream-tap changes the results call for, each with its capture as the fixture:
   - Waiting on llama.cpp before b8772: unknown, not 0. The name keeps its spelling but changes meaning, so this is a version guard or a behavioral check.
   - `timings.cache_n` as the fallback field for cached tokens.
   - Ollama v0.30.0's `prompt_eval_count`.
3. Per-version llama-swap configs for v172, v173 and v217.
4. The AVX-512-only versions on an AVX-512 host.
