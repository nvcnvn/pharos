# Pharos
<p align="center"><img src="docs/assets/pharos-logo.svg" width="420"></p>
A single-binary LLM router for small teams that run their own inference on a handful of machines, with a mix of engines: Ollama, llama.cpp, vLLM, llama-swap, SGLang and anything OpenAI-compatible.

Olla routes by which host has a model installed. SMG routes by KV-cache state on H100 fleets. Pharos routes by the **live state of each engine**: which models are loaded, how many requests are running and waiting, and how much cache is left, across whatever hardware you have.

## Status: early, no releases yet

Pharos is built bottom-up (see the [build order](docs/ARCHITECTURE.md#17-build-order)), and all six steps are done:

- engine adapters that read live state from each engine (Ollama, llama.cpp, llama-swap, vLLM, SGLang), and `pharos doctor`, which shows what Pharos can read from your backends;
- `pharos serve`: the router. It streams OpenAI-compatible and native Ollama requests to the backend with the lowest estimated time to first token, weighing which models are loaded, running and waiting requests, and which backend holds the conversation's prompt prefix in cache;
- backends from Docker labels (`pharos.enable=true`), with each container's log as a feed (Ollama's slot count comes from its startup log);
- 2–3 instances sharing prefix routing, in-flight counts, speed estimates and usage peer to peer (`peers:`), and a state file (`state_file:`) that keeps them across restarts;
- API keys with per-key model allow-lists, requests-per-minute and daily token quotas, and weighted fair queueing; usage history per key and model; `/metrics`, a `/status` page, and a graceful drain on shutdown. Keys and backends reload from the config file without a restart.

**Not built yet:** model aliases, config overrides from environment variables, and several of the planned metrics ([ARCHITECTURE §13](docs/ARCHITECTURE.md#13-observability-internalobs)). There are no releases, Docker images or Helm charts for now.

## Try `pharos serve`

Needs Go 1.27 or later. Without `keys:` in the config, Pharos lets every client in (it logs a warning), so keep it on a trusted network until you add keys.

```yaml
# pharos.yaml
listen: :8090
backends:
  - url: http://gpu-box-1:11434   # kind is detected
    memory_gb: 24                 # Ollama doesn't report total VRAM
    capacity: 2                   # OLLAMA_NUM_PARALLEL
  - url: http://gpu-box-2:8080    # llama.cpp llama-server
```

```sh
go install github.com/nvcnvn/pharos/cmd/pharos@latest
pharos version
pharos serve -config pharos.yaml
curl localhost:8090/v1/models
```

Point your clients at `http://localhost:8090/v1`, or at `http://localhost:8090` as an Ollama endpoint: `/api/chat`, `/api/generate` and `/api/embed` go only to Ollama backends, and the `ollama` CLI works (`OLLAMA_HOST=localhost:8090 ollama run qwen2.5:0.5b`) except for commands that manage models (`pull`, `rm`, `create`), which you run on a backend.

**One model, one name.** Pharos balances a model across backends that list it under the same name; model aliases aren't built yet. To spread `qwen2.5:0.5b` over Ollama and llama.cpp, serve it under Ollama's name everywhere: `llama-server --alias qwen2.5:0.5b`, `vllm serve … --served-model-name qwen2.5:0.5b`.

**Ollama installed natively** (Homebrew or the Mac app) reports its slot count only in its log. Point the backend at the log file and Pharos reads it, instead of relying on `capacity:`. It needs `OLLAMA_NUM_PARALLEL` set: left unset, the log says 0 (automatic) and capacity stays unknown.

```yaml
  - url: http://mac-mini:11434
    logs: file:///Users/me/.ollama/logs/server.log   # the Mac app's log; wherever `ollama serve` writes, for Homebrew
```

**Keys and quotas.** Make a key per person or app; the key is printed once and only its SHA-256 goes in the config:

```sh
pharos keys new -name alice
```

```yaml
keys:
  - name: alice
    sha256: 9f86d081…          # from pharos keys new
    rpm: 60                    # requests per minute (optional)
    tokens_per_day: 2000000    # prompt + completion tokens (optional)
    weight: 2                  # share of the fair queue when backends are busy (default 1)
    models: [llama3.1:8b]      # allow-list (optional)
  - name: ops
    sha256: 2c26b46b…
    admin: true                # may open /status and /usage
usage:
  timezone: Europe/Berlin      # where the day ends for tokens_per_day and /usage (default UTC, not the host's zone)
```

Clients send the key as `Authorization: Bearer <key>`, the way OpenAI SDKs send `api_key`. Native Ollama API clients need to send the same header once keys exist; the `ollama` CLI can't (0.32.15 sends none, even with `OLLAMA_API_KEY` set), so it only works with a Pharos that has no keys. Over a limit, they get a 429 with OpenAI's error codes (`rate_limit_exceeded`, `insufficient_quota`) and a `Retry-After`. Pharos counts the tokens the engines report. For a streamed chat request that didn't ask for usage, Pharos asks the engine for it and removes that extra chunk from the reply. A reply that reports no token counts shows up as *unmetered*, never as 0 tokens. On vLLM v0.30.0, asking for usage moves `system_fingerprint` onto that removed chunk; a client that needs the field sets `stream_options.include_usage: true` itself, and then Pharos passes the stream through untouched.

**Status and usage.** `/status` is a page showing each backend, its engine version and resolved probes, what's loaded, the last requests and why each went where, and usage by key. `/usage?by=key|model&from=2026-09-01&to=2026-09-30&format=csv` exports the history. Both need an admin key. `/metrics` is for Prometheus and needs no key.

**Docker labels.** Mount the Docker socket (read-only) and label engine containers `pharos.enable: "true"`; the static `backends:` list may then be empty. Optional labels: `pharos.url` (default: the container IP and its one exposed port), `pharos.port`, `pharos.kind`, `pharos.memory_gb`, `pharos.capacity`. A static backend in Docker gets its log feed with `logs: docker://<container>`.

**Several instances.** Run 2–3 behind any round-robin load balancer, each with:

```yaml
state_file: /data/pharos.state          # also useful for a single instance
peers:
  listen: :8081                         # private network only
  secret_file: /run/secrets/pharos-peer # the same secret on every instance
  members: [pharos-a:8081, pharos-b:8081, pharos-c:8081]
```

Quotas are shared through the peers. If the network splits, each side keeps serving and enforces quotas on what it can see, so a key can use up to its quota once per side until the split heals. To hear about it, alert on how long an instance has gone without a peer's update:

```yaml
- alert: PharosPeerSilent       # quotas and cache routing here miss that peer's traffic
  expr: time() - pharos_peer_last_heard_timestamp_seconds > 10 or pharos_peer_up == 0
  for: 1m
- alert: PharosPeerConfigDiffers # different backend or key set: routing and quotas differ
  expr: pharos_peer_mismatch == 1
  for: 5m
- alert: PharosPeerProtocolDiffers # a release with another wire protocol: the two share nothing
  expr: pharos_peer_proto_mismatch == 1
  for: 5m
```

`/metrics` also counts every decision a request goes through, `pharos_decisions_total{stage,outcome}` (admission, routing, queueing, prefix prediction, upstream result), along with the background ones (plan resolution, scrape results, prefix invalidation, log feed); the list is in [ARCHITECTURE §13](docs/ARCHITECTURE.md#13-observability-internalobs). It also counts Pharos's own overhead per request. Two rules worth having on any deployment:

```yaml
- alert: PharosBackendsEjected  # engines refusing connections or dying mid-reply
  expr: sum by (instance) (increase(pharos_decisions_total{stage="upstream",outcome=~"connect_failed|died_mid_reply"}[5m])) > 0
- alert: PharosOverBudget       # Pharos itself adds more than 2 ms at p99
  expr: histogram_quantile(0.99, sum by (le, instance) (rate(pharos_overhead_seconds_bucket[5m]))) > 0.002
  for: 10m
```

## Try `pharos doctor`

```sh
pharos doctor -url http://localhost:11434
```

Against Ollama 0.34.4 with one model loaded:

```
== http://localhost:11434
  kind     ollama (detected)
  version  0.34.4
  active   version                  ollama-version       /api/version  0.34.4
           models                   openai-models        /v1/models    [qwen2.5:0.5b]
           residency                ollama-ps-residency  /api/ps       [qwen2.5:0.5b=loaded]
           vram_bytes               ollama-ps-size-vram  /api/ps       [qwen2.5:0.5b=0]
           size_bytes               ollama-tags-size     /api/tags     [qwen2.5:0.5b=397821319]
  dropped  ollama-log-num-parallel  no log feed
  unknown  running, waiting, capacity, kv_usage
```

Each **active** line is a probe that answered: the signal, the probe that read it, where it was read from, and the value. **Dropped** probes didn't apply to this backend, with the reason. **Unknown** signals are ones no probe could read. Pharos never treats an unknown as 0.

Ollama only reports its parallel slot count in its log. Give `doctor` the container's log, or the log file of a native install, and capacity becomes known:

```sh
pharos doctor -url http://localhost:11434 -log-feed docker://ollama
pharos doctor -url http://localhost:11434 -log-feed file:///Users/me/.ollama/logs/server.log   # installed natively
```

To check several backends at once, list them in a config file and run `pharos doctor -config pharos.yaml`:

```yaml
backends:
  - url: http://gpu-box:11434        # kind defaults to auto-detect
  - url: http://mac-studio:8080
    kind: llamacpp
  - url: http://gpu-box:8000
    kind: vllm
    logs: docker://vllm-1            # optional log feed
```

If an engine renamed a metric, or has no built-in support, add your own probe to its backend in the config instead of waiting for a release. See [ARCHITECTURE §10](docs/ARCHITECTURE.md#10-config-and-discovery-internalconfig-internaldiscovery).

## Supported engines

[docs/SUPPORT.md](docs/SUPPORT.md) lists, for each engine version, which signals Pharos reads and how that was verified: by a live test against the real engine, by a recorded capture, or not at all. Every PR runs live tests against Ollama, llama.cpp and vLLM, and a nightly job runs every engine at its latest release to catch drift.

Is `doctor` wrong or silent about your engine version? [Open an engine report](https://github.com/nvcnvn/pharos/issues/new?template=engine-signal.yml). A recorded capture from your engine is the most useful contribution you can make.

## Docs

- [STRATEGY.md](docs/STRATEGY.md): who Pharos is for, the routing policy, engine tiers and what's out of scope.
- [ARCHITECTURE.md](docs/ARCHITECTURE.md): packages, the engine adapter model, state, scheduler and testing.
- [SUPPORT.md](docs/SUPPORT.md): the support matrix.
- [CONTRIBUTING.md](CONTRIBUTING.md): how to build, test and add an engine version.

## License

[Apache 2.0](LICENSE)
