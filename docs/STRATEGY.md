# Pharos strategy

Status: pre-development. Based on market research done 2026-09-27. Star counts and feature claims are snapshots from that date. Items marked **[U]** were not verified.

## 1. Target customer

**Who:** a small team (roughly 2–20 people) running its own LLM inference on a handful of machines.

- No dedicated platform engineer. Deployment is docker-compose, bare metal, or a small, simple Kubernetes cluster (k3s-style).
- Mixed hardware: consumer GPUs (3090/4090), Apple Silicon Macs, maybe one server GPU.
- Mixed engines: mostly Ollama and llama.cpp. vLLM arrives when the team outgrows those.
- Traffic: chat UIs (Open WebUI), coding agents and internal apps. Prompts are long and repeated.

**Their pain:**

- Reload thrash: requests land on hosts where the model is not loaded.
- One user can hog a host. Ollama's queue is first-in-first-out.
- No per-user API keys or usage tracking without running Postgres.
- Prompt processing (prefill) is slow on consumer hardware, so a lost prompt cache costs seconds.
- No visibility into which box is serving what.

**Not our customer:**

- Kubernetes GPU fleets. SMG, llm-d, Dynamo and AIBrix serve them.
- Cloud-API-only routing. LiteLLM, Bifrost and Portkey serve them.

## 2. Positioning

> Olla routes by which host has a model installed. SMG routes by KV-cache state on H100 fleets. Pharos routes by live engine state across whatever mix of engines and hardware a small team runs.

### Landscape

| Segment | Projects | Why it doesn't serve our customer |
|---|---|---|
| Datacenter routers | SMG (Rust, forked from SGLang's router), llm-d, Gateway API Inference Extension, NVIDIA Dynamo, AIBrix, vLLM production-stack | Assume Kubernetes, vLLM/SGLang/TRT-LLM and homogeneous GPUs. Treat Ollama and llama.cpp as plain OpenAI endpoints. SMG's own benchmark shows its gRPC path gives no gain at low concurrency. |
| Cloud API gateways | LiteLLM (needs Postgres, known scaling pain), Bifrost (Go; clustering is paid), Portkey, Helicone, otari | Offer keys and budgets, but know nothing about which models are loaded on local hosts. |
| Local/self-hosted | **Olla** (Go, ~300★), llama-swap (single host), Paddler (llama.cpp only), and a tail of small Python/C# projects (Ollama Herd, OllamaFlow, lollms_hub, ollama-queue-proxy) | Each covers part of the problem. None combines load-aware routing, keys and quotas, and fair queueing. |
| Reverse-proxy AI features | Traefik Hub AI Gateway (paid), Kong AI (advanced features paid), APISIX, Higress, Envoy AI Gateway / Agent Router | Paid, or heavy and Kubernetes-first. |

**Closest competitor: Olla.** It supports about 12 backend types but, as of commit bb06121 (2026-09-22), reads **no** engine metrics. Its signals are:

- health checks, model lists, and its own in-flight request count;
- its `/api/ps` passthrough returns 501;
- it has no inbound API keys, quotas or fair queueing;
- load-aware dispatch is an open issue ([thushan/olla#216](https://github.com/thushan/olla/issues/216)).

Olla could close these gaps. Speed and correctness matter.

**Why not a Traefik plugin:**

- Traefik routers cannot match on the request body. Middleware runs after the backend is already chosen.
- Yaegi plugins break SSE streaming because `http.Flusher` doesn't work inside them ([traefik/traefik#10269](https://github.com/traefik/traefik/issues/10269)).
- Pharos instead runs *beside* Traefik as a normal container.
- The part of that idea we keep is Docker-label discovery of model backends, which no project offers today.

## 3. Routing policy

A single default pipeline: filter out ineligible backends, then score the rest.

1. **Filter:** keep backends that are healthy and serve the requested model. Admit the request against the caller's key and quota.
2. **Model residency:** prefer backends where the model is already loaded (warm). If none are warm, use a cold host with enough free memory. A warm but busy host is the last resort.
3. **Workload / imbalance:** compute each backend's expected wait from engine-reported running and waiting requests, or from the router's own in-flight count when the engine reports nothing. Normalize by capacity and observed request duration. Prefer the lowest expected wait. Unlike SMG's `cache_aware`, there are no imbalance thresholds; wait is weighed against load and prefill time.
4. **Prefix affinity:**
   - Hash the conversation as a chain at message boundaries: `h_i = hash(h_{i-1}, role, content_i)`.
   - Keep an LRU map from `(model, h_i)` to the backends that recently served that prefix.
   - Prefer the longest match, valued as the prefill time it saves.
   - **Clear on unload:** drop a backend's entries for a model when its residency signal shows the model was unloaded. SMG's tree does not do this.
   - **Correct from feedback:** compare the cached-token count each response reports (llama.cpp `timings.cache_n`, Ollama `prompt_eval_cached_count`, OpenAI-style `usage.prompt_tokens_details.cached_tokens` on vLLM, SGLang, Ollama and mlx-lm; vLLM sends it only with `--enable-prompt-tokens-details`) with the prediction, and prune entries that were wrong.
   - **Privacy:** the router stores hashes only, never raw prompts.
5. **Fair queueing:** when every candidate is saturated, queue per API key so one user can't starve others.

Implementation: [ARCHITECTURE.md §7](ARCHITECTURE.md#7-routing-policy-internalpolicy) folds steps 2–4 into one estimated-time-to-first-token cost.

**Named policies:** `cost` (the default) is the pipeline above. `least-load` uses load only, for pure vLLM or SGLang fleets.

**Signal model.** Pharos tracks these per backend and model:

- residency (loaded, loading or cold)
- occupancy (running and waiting requests)
- capacity (how many requests it can run at once)
- KV-cache pressure (0–1)
- observed speed (moving averages of prefill time per token, model load time and request duration)
- cached-token feedback

Each engine signal is read by a probe: a Prometheus metric, a JSON field or a log line. It records the probe that read it and when. **A missing signal is unknown, never 0.** Scraped values lag by 1–2 seconds, so the router's own in-flight changes are added on top of the last scrape.

**Out of scope:** KV-event routing, prefill/decode disaggregation, gRPC tokenization, a Kubernetes operator, autoscaling or large router clusters, plugins. Cloud spillover is deferred.

## 4. Supported engines

Each signal below becomes one probe in the engine's recipe ([ARCHITECTURE §4](ARCHITECTURE.md#4-engine-adapters-internalengine)). Which probes actually answer is discovered per backend at runtime. The endpoint and metric names are hypotheses from docs and source reading until a live test verifies them for a given version (§6).

| Tier | Engine | Endpoints we expect to use |
|---|---|---|
| **1 (launch)** | Ollama | `/api/ps` (`size_vram`, `expires_at`) for residency, `/api/tags` for model sizes, `/api/version`. No native occupancy metrics (PR #18508 still open), so the router counts its own in-flight requests. Log lines are a candidate for more signals [U]. |
| | llama.cpp `llama-server` | `/slots` (`is_processing`), `/metrics` with `--metrics` (`llamacpp:requests_processing`, `llamacpp:requests_deferred`), `/props` (`total_slots`), router-mode `/models` status, `timings.cache_n` |
| | vLLM (V1) | `/metrics`: `vllm:num_requests_running`, `vllm:num_requests_waiting`, `vllm:kv_cache_usage_perc` (older versions may expose `gpu_cache_usage_perc` instead [U]; that would be its own probe); `/version` |
| | Generic OpenAI-compatible | router-measured signals only (fallback) |
| **2** | SGLang | `/v1/loads` (JSON), or `/metrics` with the `sglang:` or `sglang_` prefix (one probe per prefix [U]); `/server_info` for capacity |
| | LM Studio | `/api/v1/models` `loaded_instances` |
| | llama-swap | `/running` (model state) |
| | mlx-lm | `/v1/models` only (generic OpenAI recipe) |
| **3** | TRT-LLM | `/prometheus/metrics`: `trtllm_num_requests_*`, `trtllm_kv_cache_utilization`. Needs a GPU CI runner. |
| | Cloud providers | spillover only |
| **Skip** | TGI | repo archived 2026-03 |

**Known unknowns [U]:**

- ~~whether Ollama reports cached prompt tokens~~: yes on 0.34.4, over both APIs (`prompt_tokens_details.cached_tokens`, `prompt_eval_cached_count`). Older versions unknown. See [spikes/2026-09-27-latest-engines.md](spikes/2026-09-27-latest-engines.md);
- whether a full Ollama queue returns 503;
- how llama.cpp picks a slot for similar prompts, and its host-memory prompt cache;
- whether any vLLM version we support still exposes `gpu_cache_usage_perc`. It is absent in 0.30.0. A capture from an older version is needed before a probe for it enters the library;
- which engines expose their version at all. A version guard can't run without one. Seen on latest releases: Ollama `/api/version`, vLLM `/version`, llama.cpp `/props` `build_info`, llama-swap `/api/version`, SGLang `/get_server_info`; mlx-lm exposes none;
- which engine log lines carry useful, stable signals.

Ollama does not report total VRAM, so a host's memory size must be configured.

## 5. Product surface

- A single Go binary with no cgo dependencies and no database. Keys live in the config file. Usage is kept in memory and saved to a state file.
- Optional high availability: run 2–3 instances that sync state peer-to-peer, with no database and no consensus. Rolling updates keep keys, usage and learned prefix routing.
- Accepts both the OpenAI-compatible API and the native Ollama API, with real token-by-token streaming.
- Finds backends from a static list or from Docker labels.
- Per-key quotas and fair queueing.
- A Prometheus `/metrics` endpoint and a one-page status view (which host has what loaded, and why a request went where).
- A `doctor` command that checks every configured backend and reports which probes resolved on that engine version, and which signals are unknown.
- Own probes in config (a Prometheus metric or a log-line pattern), so a team can fix a renamed metric or read an unsupported engine without waiting for a release.

## 6. Success criteria: coverage × signal correctness

Success means covering the engines our target users run **and** handling their signals correctly across engine versions. The LLM serving stack changes fast, and **the docs can't be trusted.** For example, both SMG and vLLM production-stack still read vLLM metrics that were renamed. Those reads quietly return 0, and nothing flags the error.

Rules:

- **Every engine adapter ships with live integration tests.** A fact taken from docs is a hypothesis until a test runs the real engine.
- **Test behavior, not schema.** For example:
  - send N concurrent requests and assert running = N;
  - load a model and assert it shows as resident, then wait past the unload timeout and assert it is gone;
  - send the same prefix twice and assert cached tokens > 0 the second time.
- **Test matrix: engine × version.** Each engine version gets a recorded capture and, for tier 1, a live run.
  - Tier-1 engines on CPU runners (tiny GGUF model; vLLM CPU build) on every pull request.
  - Latest engine releases nightly, to catch drift before users do.
  - GPU runner for tier 3 when we get there.
- **Recorded fixtures:** live runs record each engine version's endpoint outputs, and fast unit tests replay them.
- **Adapters are combinations of probes, one probe per signal, not code written per engine or version** (ARCHITECTURE §4). Engines drift one signal at a time, so that is the unit of code. When a version changes a metric, field or endpoint, a newer probe goes ahead of the old one, and per-backend plan discovery keeps whichever answers. A probe enters the library only after its name was seen in a real capture. Version guards are only for a name whose meaning changed. An unknown or missing metric shows up as unknown in `doctor` and on the status page, never as 0.
- **Publish the support matrix:** engine version × signal. Each cell names the probe that supplies the signal and whether it was verified live or by fixture only, or says *unknown*.
- **Routing benchmarks:** the same test setup measures cache-hit rate, reload count and p95 time to first token against round-robin and Olla, to show the policy pays off.

## 7. Risks

- Olla closes its gaps (keys, load-aware dispatch).
- Ollama or Open WebUI ship native multi-host balancing.
- Docker Model Runner already covers the single-host "many models, one endpoint" case.
- Engine APIs churn. The test matrix is the mitigation, and also the moat.

## 8. Key sources

- SMG: https://github.com/smg-project/smg (cache-aware policy: `model_gateway/src/policies/cache_aware.rs`)
- Olla: https://github.com/thushan/olla, issue #216
- llm-d router: https://github.com/llm-d/llm-d-router
- vLLM metrics: https://docs.vllm.ai/en/latest/design/metrics.html, `vllm/v1/metrics/loggers.py`
- SGLang metrics: https://docs.sglang.io/references/production_metrics.html, `srt/entrypoints/v1_loads.py`
- llama.cpp server: https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md
- Ollama `/api/ps`: https://docs.ollama.com/api/ps, PR https://github.com/ollama/ollama/pull/18508
- LM Studio: https://lmstudio.ai/docs/developer/rest/list
- llama-swap: https://github.com/mostlygeek/llama-swap
- TRT-LLM serve: https://github.com/NVIDIA/TensorRT-LLM (`docs/source/commands/trtllm-serve/`)
- Traefik plugin limits: https://github.com/traefik/traefik/issues/10269, https://doc.traefik.io/traefik-hub/ai-gateway/overview
