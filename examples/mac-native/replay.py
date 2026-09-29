"""Replays real multi-turn chats (WildChat-1M) through Pharos and prints three
views of the run side by side: what the clients saw, what Pharos counted, and
what the engines logged. Disagreements are printed first.

    uv run --with duckdb replay.py --users 8           # after up.sh
    uv run --with duckdb replay.py --raw               # send UTF-8 as is, like current OpenAI SDKs
    uv run --with duckdb replay.py --burst --users 12  # every user starts at once
    uv run --with duckdb replay.py --fetch             # only download; prints the file for SIM_CONVERSATIONS

By default each request body is JSON-escaped (`\\uXXXX` for non-ASCII), as
Open WebUI's default serializer and Python's requests and aiohttp send it
(docs/spikes/2026-09-29-scenario-inputs.md). Each user replays one real
conversation: its user turns in order, with the engine's own replies as the
assistant turns, and the real gap between replies (scaled) as think time.

The dataset is downloaded once into ~/.cache/pharos-scenarios, never into the repo.
"""

import argparse
import json
import random
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

CACHE = Path.home() / ".cache" / "pharos-scenarios"
WILDCHAT = "hf://datasets/allenai/WildChat-1M@7d6490e462285cf85d91eabea0f9a954fbddcd1f/data/train-0000[0-2]-of-00014.parquet"
HERE = Path(__file__).resolve().parent
T0 = 0.0  # when the run started

p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
p.add_argument("--url", default="http://127.0.0.1:8090")
p.add_argument("--users", type=int, default=8)
p.add_argument("--model", default="qwen2.5:3b")
p.add_argument("--mlx-model", default="mlx-community/Qwen2.5-3B-Instruct-4bit")
p.add_argument("--mlx-share", type=float, default=0.25, help="share of users on the mlx-lm model")
p.add_argument("--raw", action="store_true", help="send non-ASCII as UTF-8, not \\u escapes")
p.add_argument("--burst", action="store_true", help="every user starts at once (default: spread over --ramp)")
p.add_argument("--ramp", type=float, default=20, help="seconds over which users start")
p.add_argument("--think-scale", type=float, default=0.05, help="real think time × this (WildChat gaps are minutes)")
p.add_argument("--max-turns", type=int, default=6)
p.add_argument("--max-tokens", type=int, default=128)
p.add_argument("--max-chars", type=int, default=20000, help="skip conversations whose user turns are longer (context is 16k tokens per slot)")
p.add_argument("--lang", help="only conversations in this WildChat language, e.g. Russian")
p.add_argument("--seed", type=int, default=1)
p.add_argument("--fetch", action="store_true", help="download the conversations, print their file and exit")
p.add_argument("--run", default=str(HERE / ".run"), help="where up.sh put the engine logs")
args = p.parse_args()


FILE = CACHE / "wildchat-1m-7d6490e-turn3.jsonl"


def wildchat():
    f = FILE
    if not f.exists():
        import duckdb

        CACHE.mkdir(parents=True, exist_ok=True)
        rows = duckdb.connect().execute(
            f"select conversation_hash, language, to_json(conversation) from '{WILDCHAT}' where turn >= 3 limit 1000"
        ).fetchall()
        with f.open("w") as out:
            for h, lang, conv in rows:
                turns, last = [], None
                for m in json.loads(conv):
                    if m["role"] == "user":
                        turns.append({"user": m["content"], "think": 0.0})
                    elif m.get("timestamp"):
                        t = datetime.fromisoformat(m["timestamp"].replace("Z", "+00:00"))
                        if last and turns:
                            turns[-1]["think"] = (t - last).total_seconds()
                        last = t
                out.write(json.dumps({"id": h, "lang": lang, "turns": turns}, ensure_ascii=False) + "\n")
    return [json.loads(line) for line in f.open()]


def get(path, timeout=10):
    return urllib.request.urlopen(args.url + path, timeout=timeout).read().decode()


def counters():
    try:
        text = get("/metrics")
    except OSError:
        return {}
    out = {}
    for line in text.splitlines():
        if line.startswith(("pharos_requests_total", "pharos_tokens_total", "pharos_decisions_total", "pharos_prefix_predictions_total")):
            k, v = line.rsplit(" ", 1)
            out[k] = float(v)
    return out


def chat(model, msgs):
    body = {"model": model, "messages": msgs, "stream": True, "stream_options": {"include_usage": True}, "max_tokens": args.max_tokens}
    data = json.dumps(body, ensure_ascii=not args.raw).encode()
    row, t0, reply = {"start": time.time() - T0}, time.time(), []
    try:
        req = urllib.request.Request(args.url + "/v1/chat/completions", data, {"content-type": "application/json"})
        for line in urllib.request.urlopen(req, timeout=600):
            if not line.startswith(b"data: ") or line.strip() == b"data: [DONE]":
                continue
            d = json.loads(line[6:])
            delta = (d.get("choices") or [{}])[0].get("delta") or {}
            if delta.get("content"):
                row.setdefault("ttft", time.time() - t0)
                reply.append(delta["content"])
            if u := d.get("usage"):
                row.update(prompt=u.get("prompt_tokens"), completion=u.get("completion_tokens"))
                row["cached"] = (u.get("prompt_tokens_details") or {}).get("cached_tokens")
            if (t := d.get("timings")) and row.get("cached") is None:
                row["cached"] = t.get("cache_n")  # llama.cpp before b8772
        row["total"] = time.time() - t0
    except urllib.error.HTTPError as e:
        row["error"] = f"http {e.code}"
    except Exception as e:
        row["error"] = type(e).__name__
    return row, "".join(reply)


def user(u, conv, model, start, rows, lock):
    time.sleep(start)
    msgs = []
    for i, turn in enumerate(conv["turns"][: args.max_turns]):
        if i:
            time.sleep(min(max(conv["turns"][i - 1]["think"], 2), 600) * args.think_scale)
        msgs.append({"role": "user", "content": turn["user"]})
        row, reply = chat(model, msgs)
        row.update(user=u, turn=i, lang=conv["lang"], model=model)
        with lock:
            rows.append(row)
        if "error" in row:
            return
        msgs.append({"role": "assistant", "content": reply})


def log_sizes():
    return {f: f.stat().st_size for f in Path(args.run).glob("*.log")}


def engine_view(before):
    """Counts chat requests and model loads in each engine's log since the run
    started, as {log name: (requests, loads)}. The log lines themselves are
    never printed: they can carry prompt content."""
    out = {}
    for f in log_sizes():
        if f.stem == "pharos":
            continue
        with f.open("rb") as fh:
            fh.seek(before.get(f, 0))
            text = fh.read().decode(errors="replace")
        # Seen on Ollama 0.32.15 ([GIN] … POST "/v1/chat/completions"), llama.cpp
        # b6890 (request: POST /v1/chat/completions) and mlx-lm 0.31.3 ("POST /v1/…").
        requests = len(re.findall(r'POST\s+"?/v1/chat/completions', text))
        # Ollama 0.32.15 logs its bundled llama-server's "model loaded" once per load.
        out[f.stem] = (requests, len(re.findall(r"llama_server: model loaded", text)))
    return out


def pct(xs, q):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs) - 1, int(len(xs) * q))] if xs else None


def fmt(x):
    return "-" if x is None else f"{x:.2f}s"


def main():
    global T0
    rng = random.Random(args.seed)
    convs = [c for c in wildchat() if sum(len(t["user"]) for t in c["turns"][: args.max_turns]) <= args.max_chars and args.lang in (None, c["lang"])]
    picked = rng.sample(convs, args.users)
    before, logs = counters(), log_sizes()
    rows, lock, threads = [], threading.Lock(), []
    T0 = time.time()
    for u, c in enumerate(picked):
        model = args.mlx_model if rng.random() < args.mlx_share else args.model
        start = 0 if args.burst else rng.uniform(0, args.ramp)
        threads.append(threading.Thread(target=user, args=(u, c, model, start, rows, lock)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.time() - T0
    time.sleep(2)  # let Pharos finish counting the last replies
    after = counters()
    diff = {k: after[k] - before.get(k, 0) for k in after if after[k] != before.get(k, 0)}
    loads = engine_view(logs)

    out = Path(args.run) / f"replay-{int(T0)}.jsonl"
    out.write_text("".join(json.dumps(r) + "\n" for r in rows))

    ok = [r for r in rows if "error" not in r]
    sent = sum(1 for r in rows if r.get("error") not in ("URLError", "ConnectionResetError", "RemoteDisconnected"))
    tok = lambda k: sum(r.get(k) or 0 for r in ok)
    ptok = lambda t: sum(v for k, v in diff.items() if k.startswith("pharos_tokens_total") and f'type="{t}"' in k)
    pm = lambda pat: sum(v for k, v in diff.items() if re.search(pat, k))
    full_hits = sum(1 for r in ok if r.get("cached") and r.get("prompt") and r["cached"] >= 0.9 * r["prompt"])

    print(f"\n{len(picked)} users, {len(rows)} requests in {wall:.0f}s, bodies {'raw UTF-8' if args.raw else 'JSON-escaped'}; rows in {out}")
    checks = [
        ("requests Pharos answered", sent, pm(r"^pharos_requests_total")),
        ("prompt tokens", tok("prompt"), ptok("prompt")),
        ("cached tokens", tok("cached"), ptok("cached")),
        ("completion tokens", tok("completion"), ptok("completion")),
    ]
    print("\n== Disagreements (client vs Pharos)")
    bad = [(n, c, p) for n, c, p in checks if round(c) != round(p)]
    for n, c, p in bad:
        print(f"  {n}: client {c:.0f}, Pharos {p:.0f}")
    wrong = pm(r'^pharos_prefix_predictions_total.*outcome="wrong_prediction"')
    if wrong and full_hits:
        print(f"  prefix: Pharos judged {wrong:.0f} predictions wrong, while the engines reported ≥90% of the prompt cached on {full_hits} requests")
    cold, engine_loads = pm(r'stage="load",outcome="cold_start"'), sum(n for _, n in loads.values())
    if cold != engine_loads:
        print(f"  model loads: Pharos dispatched {cold:.0f} cold, engine logs show {engine_loads} loads")
    upstream, engine_reqs = pm(r'stage="upstream"'), sum(n for n, _ in loads.values())
    if upstream != engine_reqs:
        print(f"  requests reaching engines: Pharos sent {upstream:.0f} upstream, engine logs show {engine_reqs}")
    if not bad and not (wrong and full_hits) and cold == engine_loads and upstream == engine_reqs:
        print("  none")

    print("\n== Clients")
    errors = {}
    for r in rows:
        if "error" in r:
            errors[r["error"]] = errors.get(r["error"], 0) + 1
    print(f"  ok {len(ok)}, errors {errors or 0}")
    for model in sorted({r["model"] for r in rows}):
        rs = [r for r in ok if r["model"] == model]
        print(f"  {model}: TTFT p50 {fmt(pct([r.get('ttft') for r in rs], .5))}, p95 {fmt(pct([r.get('ttft') for r in rs], .95))}, {len(rs)} ok")
    print("  cached share of prompt tokens, turns 2+, by language:")
    for lang in sorted({r["lang"] for r in ok}):
        rs = [r for r in ok if r["lang"] == lang and r["turn"] > 0 and r.get("prompt")]
        if rs:
            known = [r for r in rs if r.get("cached") is not None]
            share = sum(r["cached"] for r in known) / max(1, sum(r["prompt"] for r in known))
            print(f"    {lang:12} {len(rs):3} requests, {100 * share:3.0f}% cached ({len(rs) - len(known)} didn't report)")

    print("\n== Pharos (/metrics, change during the run)")
    for k in sorted(diff):
        if k.startswith(("pharos_decisions_total", "pharos_prefix_predictions_total")):
            print(f"  {k} {diff[k]:.0f}")

    print("\n== Engines (logs)")
    for name, (n, l) in sorted(loads.items()):
        print(f"  {name:13} {n:4} chat requests" + (f", {l} model loads" if name.startswith("ollama") else ""))


if args.fetch:
    wildchat()
    print(FILE)
else:
    main()
