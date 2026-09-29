# A small team's Mac, natively on Metal

Two Ollama instances, one llama.cpp `llama-server` and one `mlx_lm.server` on one Apple Silicon Mac, with Pharos in front on `:8090`. Then real sessions replayed through it, with a report that compares what the clients saw, what Pharos counted and what the engines logged.

Needs Homebrew `ollama` and `llama.cpp`, [uv](https://docs.astral.sh/uv/), Go and about 6 GB for Qwen2.5-3B in three formats. Docker only for `webui.py`.

```sh
./up.sh                                    # engines + Pharos; logs, pids and pharos.yaml in .run/
uv run --with duckdb replay.py --users 8   # replay 8 WildChat users
uv run --with duckdb replay.py --users 8 --baseline round-robin  # the same users, round-robin, no Pharos
./down.sh
```

`up.sh` serves `qwen2.5:3b` on both Ollama instances and on llama.cpp (`--alias`), so Pharos balances it over all three. mlx-lm can't serve a model under another name, so it is its own pool (`mlx-community/Qwen2.5-3B-Instruct-4bit`), and a quarter of the replayed users go to it. Override the model with `MODEL`, `GGUF` and `MLX`, and the Ollama binary with `OLLAMA`.

The generated config has two keys, `pharos-chat` (admin, for chat users and for reading `/usage`) and `pharos-batch`, and a state file, so usage and learned speeds survive a restart. Pharos runs at `-log-level debug`. Each process's command line is saved to `.run/<name>.args`, so `replay.py` can restart what it kills.

## The replay

Each chat user replays one real conversation from [WildChat-1M](https://huggingface.co/datasets/allenai/WildChat-1M) (ODC-BY): its user turns in order, the engine's own replies as the assistant turns, and the real gap between replies, scaled down, as think time. All languages are kept. Each coding user replays one [SWE-agent trajectory](https://huggingface.co/datasets/nebius/SWE-agent-trajectories) (CC-BY-4.0) with its recorded replies and tool output, two users per issue. Datasets are downloaded once to `~/.cache/pharos-scenarios`, never committed.

Request bodies are JSON-escaped by default (`\uXXXX` for non-ASCII), as Open WebUI and Python's `requests` and `aiohttp` send them. `--raw` sends UTF-8 as is, like current OpenAI SDKs.

| Flag | Default | |
|---|---|---|
| `--scenario` | `chat` | a preset, below; its flags can still be overridden |
| `--users` | 8 | sessions replayed at once |
| `--burst` | off | every user starts at once, instead of over `--ramp` seconds |
| `--lang` | all | e.g. `Russian`, to target one language |
| `--raw` | off | send UTF-8 instead of `\u` escapes |
| `--mlx-share` | 0.25 | share of users on the mlx-lm model |
| `--baseline` | off | `round-robin`: skip Pharos and send each request to the next backend in `.run/pharos.yaml` that lists the model |
| `--url` | Pharos | point at an engine directly, e.g. `--url http://127.0.0.1:18082 --mlx-share 1` |
| `--repeat` | 1 | run N times; the summary file holds every run and the report ends with each metric's median and range |
| `--compare A B` | | print two summary files side by side (medians) and exit |
| `--fetch` | | only download the dataset (`--dataset swe` for trajectories) and print its file, for the simulator's `SIM_CONVERSATIONS` |

The same `--seed` (default 1) replays the same users on the same models, so a Pharos run and a baseline run compare directly. The second run finds some of the first run's prompts still cached, and the engines as the first left them (loaded or not); restart the engines between runs (`down.sh`, `up.sh`) for a clean comparison, or use `--repeat` and read the ranges.

## Scenarios

Each preset is a claim, printed with the numbers that decide it under **Claim** in the report.

| `--scenario` | Claim | What it does |
|---|---|---|
| `chat` | prefix hits and reloads beat round-robin | 8 WildChat users over 20 s |
| `cold-burst` | the model loads once per engine, and a second Ollama is used once the queue grows | unloads `qwen2.5:3b` from both Ollamas (`keep_alive: 0`), waits 12 s so Pharos's 5 s residency scrape sees it, then 16 users at once, 2 turns each |
| `coding` | prefix affinity saves TTFT on long, growing, shared context | 8 SWE-agent trajectories (every one shares SWE-agent's ~5k-token system prompt, and two share each issue), up to 30 steps or 48k characters, 2 s between steps (assumed tool time), 64-token replies |
| `noisy` | fair queueing holds the chat p95 | 4 requests at a time on the batch key (first turns of other conversations, 256-token replies) beside the chat users, until they finish |
| `ops` | no stream is cut except on the killed engine, and usage totals match | 12 users, 8 turns; at 15 s kills llama.cpp, at 35 s starts it again, at 50 s edits the config, at 65 s restarts Pharos (`--ops` sets the times). Requests refused while Pharos restarts are retried for 60 s and counted |

llama.cpp and mlx-lm have no unload, so `cold-burst` makes only the Ollamas cold. llama.cpp b6890 rejects requests that carry `tools` unless started with `--jinja`; `up.sh` leaves it off, which matters for `webui.py` (see ISSUES).

## The report

- **Disagreements** first: prompt, cached and completion token counts the clients saw against Pharos's `/metrics` (not after a restart) and `/usage`; engines Pharos sent cold requests to against engines whose logs show a load; requests without a Pharos decision line; requests where Pharos predicted more than 1.2× the engine's whole prompt as cached; and, per engine, requests sent there against requests its log shows.
- **Clients:** errors, TTFT p50/p95 per key and model, and the cached share of prompt tokens by language.
- **Per engine:** TTFT, cached share and Pharos's prefix outcomes for the requests that went to each engine, and the first few wrong predictions. Each request carries an `X-Request-Id`, so each row is paired with Pharos's debug line for it: target, decisions and predicted cached tokens.
- **Claim:** model loads and their seconds from the Ollama logs, then the scenario's own numbers.
- **Pharos:** every decision and prefix-prediction counter that changed during the run.
- **Engines:** chat requests per engine and Ollama model loads, counted from their logs. The log lines are never printed; they can carry prompts.
- **Calibration:** per engine, TTFT p50 by how many requests the client had in flight to it (Pharos's queue included), prefill seconds per uncached token fitted on requests that ran alone (unknown where the engine doesn't report cached tokens, like Ollama's OpenAI API), and each load's seconds. These are the measured numbers the assumptions ledger (ARCHITECTURE §14) and the fakes should cite.

Every request is saved to `.run/replay-<time>.jsonl`, and every invocation writes `.run/summary-<time>.json`: the arguments, the Pharos and engine versions, and per run the metrics, calibration and ops events.

The log patterns were seen on Ollama 0.32.15, llama.cpp b6890 and mlx-lm 0.31.3. Another version may log differently, and then the engine counts read 0.

All engines share one GPU here, so load counts, cache hits and routing decisions are meaningful, but TTFT comparisons between policies are muddied by the engines slowing each other down.

## A real client: Open WebUI

```sh
uv run --with playwright==1.55.0 webui.py                      # its OpenAI connection
uv run --with playwright==1.55.0 webui.py --connection ollama  # its Ollama connection
```

Runs Open WebUI v0.11.4 in Docker against a sink on `:8091` that records each request and forwards it to Pharos, then types a three-turn chat (with Russian and Chinese in it) into the UI with headless Chromium. Every request lands in `.run/captures/open-webui-<version>-<connection>/`: method, path, headers (Authorization and cookies redacted) and the body exactly as sent, with the reply beside it. `--fixture internal/proxy/testdata/clients/open-webui/<version>-<connection>` copies the requests alone into the testdata that `TestOpenWebUIChatReplays` replays through the proxy.
