# Pharos

A single-binary LLM router for small teams that run their own inference on a handful of machines, with a mix of engines: Ollama, llama.cpp, vLLM, llama-swap, SGLang and anything OpenAI-compatible.

Olla routes by which host has a model installed. SMG routes by KV-cache state on H100 fleets. Pharos routes by the **live state of each engine**: which models are loaded, how many requests are running and waiting, and how much cache is left, across whatever hardware you have.

## Status: early, single instance, no API keys yet

Pharos is being built bottom-up (see the [build order](docs/ARCHITECTURE.md#17-build-order)). Steps 1–3, 5 and 6 are done:

- engine adapters that read live state from each engine (Ollama, llama.cpp, llama-swap, vLLM, SGLang), and `pharos doctor`, which shows what Pharos can read from your backends;
- `pharos serve`: the router. It streams OpenAI-compatible and native Ollama requests to the backend with the lowest estimated time to first token, weighing which models are loaded, running and waiting requests, and which backend holds the conversation's prompt prefix in cache;
- backends from Docker labels (`pharos.enable=true`), with each container's log as a feed (Ollama's slot count comes from its startup log);
- 2–3 instances sharing prefix routing, in-flight counts and speed estimates peer to peer (`peers:`), and a state file (`state_file:`) that keeps them across restarts.

**Not built yet:** API keys, quotas and per-user fair queueing, the status page and `/metrics`, and the graceful drain on shutdown. There are no releases, Docker images or Helm charts for now.

## Try `pharos serve`

Needs Go 1.27 or later. Pharos doesn't authenticate clients yet, so keep it on a trusted network.

```yaml
# pharos.yaml
listen: :8080
backends:
  - url: http://gpu-box-1:11434   # kind is detected
    memory_gb: 24                 # Ollama doesn't report total VRAM
    capacity: 2                   # OLLAMA_NUM_PARALLEL
  - url: http://gpu-box-2:8080    # llama.cpp llama-server
```

```sh
go install github.com/nvcnvn/pharos/cmd/pharos@latest
pharos serve -config pharos.yaml
curl localhost:8080/v1/models
```

Point your clients at `http://localhost:8080/v1` (or at `http://localhost:8080` as an Ollama endpoint for `/api/chat`, which only goes to Ollama backends).

**Docker labels.** Mount the Docker socket (read-only) and label engine containers `pharos.enable: "true"`; the static `backends:` list may then be empty. Optional labels: `pharos.url` (default: the container IP and its one exposed port), `pharos.port`, `pharos.kind`, `pharos.memory_gb`, `pharos.capacity`. A static backend in Docker gets its log feed with `logs: docker://<container>`.

**Several instances.** Run 2–3 behind any round-robin load balancer, each with:

```yaml
state_file: /data/pharos.state          # also useful for a single instance
peers:
  listen: :8081                         # private network only
  secret_file: /run/secrets/pharos-peer # the same secret on every instance
  members: [pharos-a:8081, pharos-b:8081, pharos-c:8081]
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

Ollama only reports its parallel slot count in its log. If it runs in Docker, give `doctor` the container's log and capacity becomes known:

```sh
pharos doctor -url http://localhost:11434 -log-feed docker://ollama
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
