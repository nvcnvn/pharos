# Contributing

Read [docs/STRATEGY.md](docs/STRATEGY.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before a large change. Out of scope: KV-event routing, prefill/decode disaggregation and a Kubernetes operator ([STRATEGY §3](docs/STRATEGY.md#3-routing-policy)).

## Build and test

Go 1.27 or later. No cgo. The only dependency is `gopkg.in/yaml.v3`.

```sh
go build ./cmd/pharos
go test ./...                    # unit, fixture replay and component tests, runs in seconds
```

Live tests start real engines in Docker, on CPU, with Qwen2.5-0.5B. They need Docker with Compose and take minutes per engine:

```sh
PHAROS_LIVE_ENGINES=ollama,llamacpp go test -tags integration -timeout 90m -v -run TestLive ./internal/engine
```

`PHAROS_LIVE_VERSION=latest` runs each engine's latest release instead of the pinned one.

The [testing guide](.claude/skills/testing/SKILL.md) says which kind of test a change needs. The short version: anything that depends on engine behavior is verified against the real engine, never from its docs.

## The rules that matter most

- **Engine docs are hypotheses.** Endpoint paths, metric names and behavior drift between engine versions. A fact counts once a live test against the real engine confirms it.
- **A missing or unreadable signal is unknown, never 0.** A router that reads a renamed metric as 0 sends all traffic to the busiest host.
- **Adapters are probes, not per-engine code.** Each probe reads one signal (a metric, a JSON field or a log line). An engine's recipe is a list of probes. When a version renames something, add a newer probe ahead of the old one. See [ARCHITECTURE §4](docs/ARCHITECTURE.md#4-engine-adapters-internalengine).
- **A probe enters the library only after its name was seen in a real capture.**

## Adding an engine version

This is the most common contribution, and usually needs no Go code.

1. Record a capture. With Docker, from the repo root:

   ```sh
   test/engines/capture.sh vllm v0.31.0     # engine = a directory in test/engines/
   ```

   [capture.sh](test/engines/capture.sh) drives the engine through idle, loaded, busy and cold states and writes the raw responses to `internal/engine/testdata/<engine>/<version>/`. If the version needs a different setup, add `test/engines/<engine>/<version>.compose.yaml`.

2. Run `go test ./...`. The replay tests run every probe against the new capture.
   - Green: add the version to [docs/SUPPORT.md](docs/SUPPORT.md) and open a PR.
   - Red: the engine changed a signal. Add a newer probe for it in `internal/engine/library.go`, ahead of the old one, and add the expected values to the replay tests.

3. Review `engine.log` in the capture before committing. It can contain prompt text.

Can't run the engine in Docker (a GPU-only image, or your own build)? Record what it serves with `pharos doctor -url <engine> -record <dir>` while it is idle, loaded and busy, and attach the directory to an [engine report](https://github.com/nvcnvn/pharos/issues/new?template=engine-signal.yml).

## Pull requests

- Keep a PR to one change, and say which tests you ran. If you didn't run the live tests, say so; CI runs them for Ollama, llama.cpp and vLLM.
- When a change affects a design decision, update every doc that depends on it, not only the section where it lives.

By contributing you agree that your contribution is licensed under [Apache 2.0](LICENSE).
