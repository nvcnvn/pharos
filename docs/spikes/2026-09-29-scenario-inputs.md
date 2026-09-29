# Spike: inputs for scenario runs, 2026-09-29

Goal: pick the datasets and engines for the scenario runs in `ISSUES-outside-in.md` (step 1) before building the harness. Engine facts below were observed on an Apple M4 Pro (64 GB) with native engines on Metal, one run each unless noted. Dataset facts come from fetching real rows; anything else is marked [U].

## mlx-lm 0.31.3 (`uvx --from mlx-lm==0.31.3 mlx_lm.server`)

Model: `mlx-community/Qwen2.5-0.5B-Instruct-4bit`.

| Question | Observed |
|---|---|
| Endpoints | `/health` 200 `{"status":"ok"}`, `/v1/models` 200. `/metrics`, `/version`, `/api/version` 404. `/v1/models/<anything>` 200 (filtered list). No version anywhere except `system_fingerprint` in replies: `0.31.3-0.32.3-macOS-…` (mlx-lm and mlx versions). |
| What `/v1/models` lists | Every MLX-looking repo in the Hugging Face cache (`scan_cache_dir`), whether loaded or not, plus the resolved local path of `--model` when it is a directory. Not the name the model is served under. |
| The request's `model` field | A Hugging Face repo id or local path to load. `default_model` means the `--model` given at startup. `qwen2.5:0.5b` fails (`Repo id must use alphanumeric chars…`) unless a local directory of that name exists; with a symlinked directory `qwen2.5:0.5b` the request succeeds, but `/v1/models` still lists the resolved snapshot path, not that name. |
| Residency | One model at a time (`ModelProvider.load`). A request for another model swaps: an uncached HF id is downloaded and loaded (9.9 s for SmolLM2-135M-8bit, download included), switching back to Qwen took 0.43 s against 0.11 s warm. Nothing reports which model is loaded. |
| Concurrency | Batches when there is no draft model and no `seed` in the request. Defaults: `--decode-concurrency 32`, `--prompt-concurrency 8`, `--prompt-cache-size 10`. 4 concurrent streams with a 4,910-token shared prefix: TTFT 0.33 s and 2.0 s total each, the same as one alone. 16 concurrent: the 13 that connected had TTFT ~1.0–1.1 s, 5.2–5.4 s total. |
| Connection refusals | `ThreadingHTTPServer` with the default listen backlog. A burst of new connections is reset at connect (`Connection reset by peer`, or `EINVAL` on connect): 1 of 8, 4 of 16, 7 of 32. |
| Cached tokens | `usage.prompt_tokens_details.cached_tokens` in a streamed reply with `include_usage`: 5 on a cold prefix, 4,895 of 4,910 on a repeated one, including 4 concurrent requests sharing it. |

**Consequences for Pharos.**

- mlx-lm can't join a same-name pool: Pharos learns models from `/v1/models`, which never lists a chosen name. Balancing one model across mlx-lm and another engine needs model aliases (still open).
- As the generic kind, every cached model looks servable, but serving one evicts the other. Two teams on two models through one mlx-lm would thrash it, and Pharos can't see it. A residency probe would have to infer from `system_fingerprint`-free signals (none exist) or from reply timing; unknown for now.
- Capacity is effectively the connect backlog, not the batch size: a burst above ~5 new connections loses some at connect. Whether Pharos counts these as ejections or retries them cleanly needs a check in the harness. Keep-alive would avoid it, but `http.server` answers HTTP/1.0 by default [U: not checked on the wire].

## LM Studio

Not installed here, so nothing observed. Homebrew offers the app (`lm-studio` cask, 0.4.25). A headless daemon exists per its docs [U]. Installing it is a separate decision.

## Native engines available here

Ollama 0.32.15 and llama.cpp b6890 (Homebrew), both on Metal, as used for the outside-in review.

## Datasets

Each was checked against real rows or CSV heads (HF datasets-server, HF API, GitHub raw files) on 2026-09-29. Tokens are estimated as characters ÷ 4.

| Use case | Dataset | License, gated | Size | Multi-turn | Notes |
|---|---|---|---|---|---|
| Team chat | `allenai/WildChat-1M` | ODC-BY, no | 838k rows, 3.4 GB | mean 2.3 turns, median 1, max 249 | Per-message timestamps and a `hashed_ip` per user, so real think time and users. 56% English. Raw user text with country and user-agent; NSFW present despite the "non-toxic" name. |
| | `anon8231489123/ShareGPT_Vicuna_unfiltered` | apache-2.0 tag, scraped ChatGPT output; no | 673 MB JSON | median 3 human turns | Long chats are split into `_0`, `_1` pieces that lose earlier context, so it doesn't model prefix growth. |
| | `lmsys/lmsys-chat-1m` | gated (license agreement) | [U] | [U] | Not readable without a login. |
| Support bot | ABCD (`asappresearch/abcd`) | MIT, no | 10k conversations, 37 MB gz | median 7 customer turns | Human-to-human. Short messages (customer median 29 chars). Personas carry names and emails that look synthetic [U]. |
| | `bitext/Bitext-customer-support-…` | CDLA-Sharing-1.0, no | 27k rows, 19 MB | no | Single-turn, synthetic, full of `{{placeholders}}`. |
| Coding agent | `nebius/SWE-agent-trajectories` | CC-BY-4.0, no | 80k rows, 1.1 GB | median 35 messages, max 817 | First request ~2.3k tokens; final history median ~23k tokens, p90 ~33k, observations appended in full. Each `instance_id` repeats 5+ times, so sessions share a long prefix. 31% end at the context limit. |
| Arrival timing | BurstGPT (`HPMLL/BurstGPT`) | CC-BY-4.0 | `BurstGPT_1.csv` 50 MB, 61 days; `BurstGPT_3.csv` 232 MB | `_3` only: `Session ID` on conversation-log rows | Token counts only, no text. In a session, request tokens grow (935 → 1136 → 1325). |
| | Azure LLM inference traces 2023/2024 | CC-BY | 2023: ~1 h, <1 MB; 2024: 0.7–1.1 GB | no session id | Timestamp and token counts only. |
| | Mooncake traces (`kvcache-ai/Mooncake`, `FAST25-release/`) | Apache-2.0 | 1–4 MB each, ~1 h | no session id | `hash_ids` per ~512-token block show prefix sharing (37–64% block reuse). Its block hashing doesn't map onto Pharos's message-boundary hashes. |

**Choice.**

- **Team chat: WildChat-1M**, `turn >= 3`, all languages. It's the only one with real users and think time between turns, and the prompt text grows turn by turn the way Open WebUI sends it.
- **Coding agent: nebius SWE-agent-trajectories.** Long, growing context and repeated issue prefixes, which is where prefix affinity pays most on slow Mac prefill.
- **Support bot: ABCD** for multi-turn content. The long shared system prompt is ours to add.
- **Arrival timing: BurstGPT_3** for session-level burstiness if WildChat's own timestamps aren't enough. Mooncake and Azure are left out: no sessions, and nothing that maps onto Pharos's hashing.
- Avoid LMSYS (gated), ShareGPT (split conversations) and Bitext (single-turn).

## Token estimates across languages and JSON escaping

Pharos estimates tokens as the raw JSON bytes of the messages ÷ 4 (`proxy.go`, `sched.go` `bytesPerToken`), escapes included, while prefill speed is learned per real token. This checks that ratio against real tokenizers.

**Bytes per engine token** (min / median / max). WildChat-1M conversations with 3+ turns, cut at the last user turn (15 per language, from the first three parquet shards), and 10 nebius SWE-agent trajectories (first 16 messages). `prompt_tokens` from llama.cpp b6890 on Metal, `max_tokens: 1`. Tokenizers: Qwen2.5-0.5B-Instruct and Llama-3.2-1B-Instruct (Q4_K_M GGUF).

| Text | Tokenizer | Median prompt tokens | Raw UTF-8 JSON | `\u`-escaped JSON |
|---|---|---|---|---|
| English | Qwen2.5 | 936 | 2.9 / 4.1 / 5.2 | 3.0 / 4.1 / 5.2 |
| | Llama 3.2 | 889 | 3.1 / 4.2 / 5.3 | 3.1 / 4.2 / 5.3 |
| Chinese | Qwen2.5 | 1,094 | 3.0 / 4.2 / 5.5 | 4.6 / 7.5 / 10.4 |
| | Llama 3.2 | 1,487 | 2.8 / 3.6 / 4.9 | 4.3 / 6.3 / 8.9 |
| Russian | Qwen2.5 | 833 | 4.1 / 5.0 / 6.2 | 8.5 / 12.3 / 15.8 |
| | Llama 3.2 | 759 | 4.2 / 5.4 / 6.2 | 8.8 / 13.5 / 17.4 |
| Code (SWE-agent) | Qwen2.5 | 5,730 | 4.0 / 4.0 / 4.3 | 4.0 / 4.0 / 4.3 |
| | Llama 3.2 | 5,453 | 4.2 / 4.3 / 4.6 | 4.2 / 4.3 / 4.6 |

Raw UTF-8 stays within 2.8–6.2 bytes per token in every language, so ÷ 4 is off by at most ~1.5×. Escaping is what breaks it: a CJK character becomes 6 bytes instead of 3, a Cyrillic one 6 instead of 2.

**Which clients escape.** A sink recorded each client's request body for `Привет, 你好`:

| Client | Body |
|---|---|
| OpenAI Python SDK 3.20.0 (httpx2) | raw |
| OpenAI Python SDK 1.109.1 with httpx 0.28 | raw |
| OpenAI Python SDK 1.109.1 with httpx < 0.28 | escaped |
| `requests` 2.34.2 (`json=`) | escaped |
| `aiohttp` 3.14.3 (`json=`) | escaped |
| `ollama` Python 0.6.3 (httpx 0.28.1) | raw |
| Node `fetch` + `JSON.stringify`, curl | raw |

Open WebUI v0.11.4 (source read, not captured on the wire) sends `data=JSONCodec.dumps(payload)` through aiohttp to both OpenAI and Ollama backends; with the default `ENABLE_ORJSON=False` that is stdlib `json.dumps`, which escapes. So the main target client sends escaped text by default [U: wire].

**Live, through Pharos** (`98c32f2`+, one llama.cpp b6890 backend): each conversation sent twice with the same body; the engine reported every prompt token but one as cached on the second send.

| Text | Raw | Escaped |
|---|---|---|
| English (2 conversations) | `predicted_hit` ×2 | `predicted_hit` ×2 |
| Chinese (2) | `predicted_hit` ×2 | `predicted_hit` ×2 |
| Russian (2) | `predicted_hit` ×2 | **`wrong_prediction` ×2** |

A prediction is judged wrong when the engine's cached tokens are under half the prediction (`sched.go`), i.e. above 8 bytes per real token. Escaped Russian is always above that; escaped Chinese sits at a median of 6.3–7.5 with a max of 10.4, so some Chinese conversations will cross it too. A wrong prediction prunes the entry (source read), so the next turn of that conversation loses its affinity. Two smaller effects follow from the same cause: the prefill estimate is 1.5–3× too high for escaped non-Latin text, and the same system prompt sent raw by one client and escaped by another hashes differently, so they never share a prefix entry.

The fake engines count tokens the same way (`fake.go`), so no simulator run or layer-3 test could show this.

## Storage

**Download at run time, commit nothing.** Every license above allows committing a sample with attribution, but a pinned dataset revision plus a local cache outside the repo is less to maintain, and keeps raw user text (WildChat carries country and user-agent per message) out of git. The files are parquet or JSON read with DuckDB, with no loader scripts, so reading them runs no dataset code. Keep all languages: non-English text is what exposed the escaping problem above. Revisit if run-time downloads prove flaky in CI.
