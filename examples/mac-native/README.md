# A small team's Mac, natively on Metal

Two Ollama instances, one llama.cpp `llama-server` and one `mlx_lm.server` on one Apple Silicon Mac, with Pharos in front on `:8090`. Then real multi-turn chats replayed through it, with a report that compares what the clients saw, what Pharos counted and what the engines logged.

Needs Homebrew `ollama` and `llama.cpp`, [uv](https://docs.astral.sh/uv/), Go and about 6 GB for Qwen2.5-3B in three formats.

```sh
./up.sh                                    # engines + Pharos; logs, pids and pharos.yaml in .run/
uv run --with duckdb replay.py --users 8   # replay 8 WildChat users
./down.sh
```

`up.sh` serves `qwen2.5:3b` on both Ollama instances and on llama.cpp (`--alias`), so Pharos balances it over all three. mlx-lm can't serve a model under another name, so it is its own pool (`mlx-community/Qwen2.5-3B-Instruct-4bit`), and a quarter of the replayed users go to it. Override the model with `MODEL`, `GGUF` and `MLX`, and the Ollama binary with `OLLAMA`.

## The replay

Each user replays one real conversation from [WildChat-1M](https://huggingface.co/datasets/allenai/WildChat-1M) (ODC-BY; downloaded once to `~/.cache/pharos-scenarios`, never committed): its user turns in order, the engine's own replies as the assistant turns, and the real gap between replies, scaled down, as think time. All languages are kept.

Request bodies are JSON-escaped by default (`\uXXXX` for non-ASCII), as Open WebUI's default serializer and Python's `requests` and `aiohttp` send them. `--raw` sends UTF-8 as is, like current OpenAI SDKs.

| Flag | Default | |
|---|---|---|
| `--users` | 8 | conversations replayed at once |
| `--burst` | off | every user starts at once, instead of over `--ramp` seconds |
| `--lang` | all | e.g. `Russian`, to target one language |
| `--raw` | off | send UTF-8 instead of `\u` escapes |
| `--mlx-share` | 0.25 | share of users on the mlx-lm model |
| `--url` | Pharos | point at an engine directly for a baseline, e.g. `--url http://127.0.0.1:18082 --mlx-share 1` |
| `--fetch` | | only download the conversations and print their file, for the simulator's `SIM_CONVERSATIONS` |

## The report

- **Disagreements** first: request, prompt, cached and completion token counts the clients saw against Pharos's `/metrics`; predictions Pharos judged wrong while the engines reported the prompt cached; cold dispatches against model loads in the Ollama logs; requests Pharos sent upstream against requests the engines logged.
- **Clients:** errors, TTFT p50/p95 per model, and the cached share of prompt tokens by language.
- **Pharos:** every decision and prefix-prediction counter that changed during the run.
- **Engines:** chat requests per engine and Ollama model loads, counted from their logs. The log lines are never printed; they can carry prompts.

Every request is saved to `.run/replay-<time>.jsonl`.

The log patterns were seen on Ollama 0.32.15, llama.cpp b6890 and mlx-lm 0.31.3. Another version may log differently, and then the engine counts read 0.

All engines share one GPU here, so load counts, cache hits and routing decisions are meaningful, but TTFT comparisons between policies are muddied by the engines slowing each other down.
