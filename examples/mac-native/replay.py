"""Replays real multi-turn sessions through Pharos and prints three views of
the run side by side: what the clients saw, what Pharos counted, and what the
engines logged. Disagreements are printed first.

    uv run --with duckdb replay.py --users 8               # after up.sh: team chat, WildChat-1M
    uv run --with duckdb replay.py --raw                   # send UTF-8 as is, like current OpenAI SDKs
    uv run --with duckdb replay.py --baseline round-robin  # the same run straight to the engines, in turn
    uv run --with duckdb replay.py --scenario cold-burst   # or coding, noisy, ops (README.md)
    uv run --with duckdb replay.py --repeat 3              # three runs, one summary file
    uv run replay.py --compare .run/summary-A.json .run/summary-B.json
    uv run --with duckdb replay.py --fetch                 # only download; prints the file for SIM_CONVERSATIONS

By default each request body is JSON-escaped (`\\uXXXX` for non-ASCII), as
Open WebUI's default serializer and Python's requests and aiohttp send it
(docs/spikes/2026-09-29-scenario-inputs.md). Each chat user replays one real
conversation: its user turns in order, with the engine's own replies as the
assistant turns, and the real gap between replies (scaled) as think time.
Each coding user replays one SWE-agent trajectory with its recorded replies.

Datasets are downloaded once into ~/.cache/pharos-scenarios, never into the repo.
"""

import argparse
import itertools
import json
import math
import os
import random
import re
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

CACHE = Path.home() / ".cache" / "pharos-scenarios"
WILDCHAT = "hf://datasets/allenai/WildChat-1M@7d6490e462285cf85d91eabea0f9a954fbddcd1f/data/train-0000[0-2]-of-00014.parquet"
SWE = "hf://datasets/nebius/SWE-agent-trajectories@68195a1450865274106246d0d0296a1d6807b88e/data/train-00000-of-00012.parquet"
HERE = Path(__file__).resolve().parent
# Engine log names by backend URL, as up.sh starts them.
LOGS = {"http://127.0.0.1:11500": "ollama-11500", "http://127.0.0.1:11501": "ollama-11501", "http://127.0.0.1:18081": "llamacpp", "http://127.0.0.1:18082": "mlx"}
KEYS = {"chat": "pharos-chat", "batch": "pharos-batch"}  # up.sh puts their hashes in the config

# Each scenario is a claim and the defaults that test it; flags still override.
SCENARIOS = {
    "chat": ("team chat: prefix hits and reloads beat round-robin", {}),
    "cold-burst": ("every Ollama cold, then a burst: the model loads once, and a second Ollama is used once the queue grows",
                   dict(unload=True, burst=True, users=16, mlx_share=0.0, max_turns=2)),
    "coding": ("coding agents (long, growing, shared context): prefix affinity saves TTFT",
               dict(dataset="swe", users=8, mlx_share=0.0, max_turns=30, max_chars=48000, max_tokens=64, think_scale=1.0)),
    "noisy": ("one key runs a batch job beside chat users: fair queueing holds the chat p95", dict(batch=4)),
    "ops": ("kill llama.cpp, bring it back, reload the config and restart Pharos under traffic: no stream cut but on the killed engine, usage totals match",
            dict(users=12, max_turns=8, ops="15,35,50,65")),
}

p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
p.add_argument("--scenario", choices=SCENARIOS, default="chat")
p.add_argument("--url", default="http://127.0.0.1:8090")
p.add_argument("--baseline", choices=["round-robin"], help="bypass Pharos: send each request to the next engine in the config that lists the model")
p.add_argument("--dataset", choices=["wildchat", "swe"], default="wildchat")
p.add_argument("--users", type=int, default=8)
p.add_argument("--model", default="qwen2.5:3b")
p.add_argument("--mlx-model", default="mlx-community/Qwen2.5-3B-Instruct-4bit")
p.add_argument("--mlx-share", type=float, default=0.25, help="share of users on the mlx-lm model")
p.add_argument("--raw", action="store_true", help="send non-ASCII as UTF-8, not \\u escapes")
p.add_argument("--burst", action="store_true", help="every user starts at once (default: spread over --ramp)")
p.add_argument("--ramp", type=float, default=20, help="seconds over which users start")
p.add_argument("--think-scale", type=float, default=0.05, help="recorded think time × this (WildChat gaps are minutes)")
p.add_argument("--max-turns", type=int, default=6)
p.add_argument("--max-tokens", type=int, default=128)
p.add_argument("--max-chars", type=int, default=20000, help="cut a session where its text passes this (context is 16k tokens per slot)")
p.add_argument("--lang", help="only conversations in this WildChat language, e.g. Russian")
p.add_argument("--unload", action="store_true", help="unload the model from every Ollama first (keep_alive 0)")
p.add_argument("--batch", type=int, default=0, help="this many requests at a time on the batch key, until the chat users finish")
p.add_argument("--ops", help="seconds into the run to kill llama.cpp, restart it, reload the config and restart Pharos, e.g. 15,35,50,65")
p.add_argument("--seed", type=int, default=1)
p.add_argument("--repeat", type=int, default=1, help="run this many times; one summary file")
p.add_argument("--compare", nargs=2, metavar="SUMMARY", help="print two summary files side by side and exit")
p.add_argument("--fetch", action="store_true", help="download the dataset, print its file and exit")
p.add_argument("--run", default=str(HERE / ".run"), help="where up.sh put the logs, pids and config")
pre, _ = p.parse_known_args()
p.set_defaults(**SCENARIOS[pre.scenario][1])
args = p.parse_args()
RUN = Path(args.run)
T0 = 0.0  # when the current run started


def wildchat():
    f = CACHE / "wildchat-1m-7d6490e-turn3.jsonl"
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
    return f


def swe():
    """SWE-agent trajectories, 4 per issue for 100 issues, each cut at 64k
    characters. Every trajectory shares SWE-agent's system prompt, and those of
    one issue share the issue text too."""
    f = CACHE / "swe-agent-trajectories-68195a1.jsonl"
    if not f.exists():
        import duckdb

        CACHE.mkdir(parents=True, exist_ok=True)
        rows = duckdb.connect().execute(f"""
            select instance_id, to_json(trajectory) from '{SWE}'
            where instance_id in (select instance_id from '{SWE}' group by 1 having count(*) >= 4 order by 1 limit 100)
            qualify row_number() over (partition by instance_id order by model_name, trajectory[2].text) <= 4""").fetchall()
        with f.open("w") as out:
            for n, (inst, traj) in enumerate(rows):
                system, turns, size = None, [], 0
                for m in json.loads(traj):
                    size += len(m.get("system_prompt") or "") + len(m.get("text") or "")
                    if size > 64000:
                        break
                    if m["role"] == "system":
                        system = m["system_prompt"]
                    elif m["role"] == "user":
                        turns.append({"user": m["text"], "think": 2.0})  # assumed: an agent's tool call takes ~2 s
                    elif m["role"] == "ai" and turns:
                        turns[-1]["assistant"] = m["text"]
                out.write(json.dumps({"id": f"{inst}#{n}", "instance": inst, "lang": "code", "system": system, "turns": turns}, ensure_ascii=False) + "\n")
    return f


def sessions(rng):
    """The sessions to replay: each cut at --max-turns and --max-chars."""
    f = swe() if args.dataset == "swe" else wildchat()
    convs = []
    for line in f.open():
        c = json.loads(line)
        if args.lang not in (None, c["lang"]):
            continue
        size, keep = len(c.get("system") or ""), []
        for t in c["turns"][: args.max_turns]:
            size += len(t["user"]) + len(t.get("assistant") or "")
            if size > args.max_chars:
                break
            keep.append(t)
        if keep:
            convs.append(c | {"turns": keep})
    if args.dataset != "swe":
        return rng.sample(convs, args.users)
    # Two trajectories per issue, so sessions share the issue's prefix too.
    by = {}
    for c in convs:
        by.setdefault(c["instance"], []).append(c)
    issues = sorted(k for k, v in by.items() if len(v) >= 2)
    return [c for i in rng.sample(issues, math.ceil(args.users / 2)) for c in rng.sample(by[i], 2)][: args.users]


def get(path, timeout=10):
    req = urllib.request.Request(args.url + path, headers={"authorization": "Bearer " + KEYS["chat"]})
    return urllib.request.urlopen(req, timeout=timeout).read().decode()


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


def usage():
    """Pharos's /usage totals over its default window; it survives a restart
    through the state file."""
    try:
        rows = json.loads(get("/usage"))["rows"]
    except OSError:
        return None
    return {k: sum(r[k] for r in rows) for k in ("requests", "prompt_tokens", "cached_tokens", "completion_tokens")}


def lines(resp):
    """resp's lines. Iterating resp itself ends quietly when a chunked stream is
    cut; read1 raises IncompleteRead, as httpx (OpenAI SDKs) and aiohttp do."""
    buf = b""
    while chunk := resp.read1():
        *full, buf = (buf + chunk).split(b"\n")
        yield from full
    yield buf


def chat(url, key, model, msgs, rid, max_tokens):
    body = {"model": model, "messages": msgs, "stream": True, "stream_options": {"include_usage": True}, "max_tokens": max_tokens}
    data = json.dumps(body, ensure_ascii=not args.raw).encode()
    row, t0, reply = {"id": rid, "key": key, "start": time.time() - T0}, time.time(), []
    if url != args.url:
        row["target"] = url
    # X-Request-Id pairs the row with Pharos's debug line for it (serve -log-level debug).
    headers = {"content-type": "application/json", "x-request-id": rid, "authorization": "Bearer " + KEYS[key]}
    try:
        while True:
            try:
                resp = urllib.request.urlopen(urllib.request.Request(url + "/v1/chat/completions", data, headers), timeout=600)
                break
            except urllib.error.URLError as e:
                # Refused: Pharos is restarting (--ops). A real client would
                # show an error; this one retries for 60 s and counts it.
                if not isinstance(e.reason, ConnectionRefusedError) or time.time() - t0 > 60:
                    raise
                row["refused"] = row.get("refused", 0) + 1
                time.sleep(0.5)
        done = False
        for line in lines(resp):
            done = done or line.strip() == b"data: [DONE]"
            if not line.startswith(b"data: ") or done:
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
        if not done:  # a clean end without [DONE]: the reply was cut short
            row["error"] = "no [DONE]"
    except urllib.error.HTTPError as e:
        row["error"] = f"http {e.code}"
    except Exception as e:
        row["error"] = type(e).__name__
    if "error" in row and "ttft" in row:
        row["cut"] = True  # the stream broke after it started
    return row, "".join(reply)


def engines(model):
    """The config's backends that list model, for --baseline round-robin."""
    out = []
    for url in re.findall(r"- url: (\S+)", (RUN / "pharos.yaml").read_text()):
        try:
            ids = [m["id"] for m in json.loads(urllib.request.urlopen(url + "/v1/models", timeout=5).read())["data"]]
        except OSError:
            continue
        if model in ids:
            out.append(url)
    if not out:
        raise SystemExit(f"no engine in {RUN}/pharos.yaml lists {model}")
    return out


def user(u, conv, model, start, rows, lock, pick):
    time.sleep(start)
    msgs = [{"role": "system", "content": conv["system"]}] if conv.get("system") else []
    for i, turn in enumerate(conv["turns"]):
        if i:
            time.sleep(min(max(conv["turns"][i - 1]["think"], 2), 600) * args.think_scale)
        msgs.append({"role": "user", "content": turn["user"]})
        with lock:
            url = pick(model)
        row, reply = chat(url, "chat", model, msgs, f"r{int(T0)}-u{u}-t{i}", args.max_tokens)
        row.update(user=u, turn=i, lang=conv["lang"], model=model)
        with lock:
            rows.append(row)
        if "error" in row:
            return
        msgs.append({"role": "assistant", "content": turn.get("assistant") or reply})


def batch(b, convs, stop, rows, lock, pick):
    """One slot of a batch job: first turns of other conversations, back to
    back, with longer replies."""
    for n in itertools.count():
        if stop.is_set():
            return
        with lock:
            url = pick(args.model)
        conv = convs[(b + n * args.batch) % len(convs)]
        row, _ = chat(url, "batch", args.model, [{"role": "user", "content": conv["turns"][0]["user"]}], f"r{int(T0)}-b{b}-{n}", 256)
        row.update(user=f"b{b}", turn=0, lang=conv["lang"], model=args.model)
        with lock:
            rows.append(row)


def unload():
    """Unloads the model from every Ollama in the config, and waits until
    /api/ps no longer lists it."""
    for url, name in LOGS.items():
        if not name.startswith("ollama"):
            continue
        body = json.dumps({"model": args.model, "keep_alive": 0}).encode()
        urllib.request.urlopen(urllib.request.Request(url + "/api/generate", body), timeout=60).read()
        for _ in range(60):
            ps = json.loads(urllib.request.urlopen(url + "/api/ps", timeout=5).read())
            if not any(m["name"] == args.model for m in ps.get("models") or []):
                break
            time.sleep(0.5)
    # Pharos reads residency every 5 s (state.Options.Slow); give it two reads,
    # or the burst races the scrape and Pharos still sees the model loaded.
    time.sleep(12)


def stop_process(name, sig):
    pid = int((RUN / f"{name}.pid").read_text())
    os.kill(pid, sig)
    for _ in range(1200):  # Pharos drains its streams first
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)


def start_process(name):
    """Starts a process again with the command line up.sh saved for it."""
    argv = (RUN / f"{name}.args").read_text().splitlines()
    proc = subprocess.Popen(argv, stdout=(RUN / f"{name}.log").open("ab"), stderr=subprocess.STDOUT, start_new_session=True)
    (RUN / f"{name}.pid").write_text(str(proc.pid))


def ops(at, events):
    """Kills llama.cpp, starts it again, edits the config, restarts Pharos, each
    at its time into the run. Records when each happened."""
    steps = [
        ("kill llamacpp", lambda: stop_process("llamacpp", signal.SIGKILL)),
        ("start llamacpp", lambda: start_process("llamacpp")),
        ("reload config", lambda: (RUN / "pharos.yaml").open("a").write(f"# edited by replay.py at {datetime.now():%H:%M:%S}\n")),
        ("restart pharos", lambda: (stop_process("pharos", signal.SIGTERM), start_process("pharos"))),
    ]
    for when, (what, do) in zip(at, steps):
        time.sleep(max(0, T0 + when - time.time()))
        events.append((round(time.time() - T0, 1), what))
        do()
        events.append((round(time.time() - T0, 1), what + " done"))


def pharos_view(before):
    """Pharos's debug line for each request, by X-Request-Id: where it went and
    what it predicted. Empty unless serve runs at -log-level debug. Also whether
    a config reload was logged."""
    f = RUN / "pharos.log"
    if not f.exists():
        return {}, False
    with f.open("rb") as fh:
        fh.seek(before.get(f, 0))
        text = fh.read().decode(errors="replace")
    out = {}
    for line in text.splitlines():
        if " DEBUG request " not in line:
            continue
        kv = {k: json.loads(v) if v.startswith('"') else v for k, v in re.findall(r'(\w+)=("(?:\\.|[^"\\])*"|\S*)', line)}
        if kv.get("request_id"):
            out[kv["request_id"]] = kv
    return out, "config reloaded" in text


def log_sizes():
    return {f: f.stat().st_size for f in RUN.glob("*.log")}


def engine_view(before):
    """Counts chat requests and model loads in each engine's log since the run
    started, as {log name: (requests, load seconds)}. The log lines themselves
    are never printed: they can carry prompt content."""
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
        # Ollama 0.32.15 logs its bundled llama-server's "model loaded" once per
        # load, then "llama-server started in N seconds" once per waiting request.
        loads = [float(s) for s in re.findall(r"llama_server: model loaded.*?llama-server started in ([\d.]+) seconds", text, re.S)]
        out[f.stem] = (requests, loads)
    return out


def pct(xs, q):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs) - 1, int(len(xs) * q))] if xs else None


def fmt(x):
    return "-" if x is None else f"{x:.2f}s"


def calibration(ok, loads):
    """What each engine did in this run, for the assumptions ledger and the
    fakes: TTFT by how many requests it was running, prefill seconds per
    uncached token (fit on requests that ran alone), and load times."""
    out = {}
    for target in sorted({r["target"] for r in ok if "target" in r}):
        rs = [r for r in ok if r.get("target") == target]
        for r in rs:
            # In flight to this engine as the client counts them, so Pharos's queue is in it.
            r["running"] = sum(1 for o in rs if o["start"] <= r["start"] < o["start"] + o["total"])
        by = {}
        for r in rs:
            by.setdefault(min(r["running"], 4), []).append(r.get("ttft"))
        alone = [(r["prompt"] - r["cached"], r["ttft"]) for r in rs if r["running"] == 1 and r.get("ttft") and r.get("prompt") and r.get("cached") is not None]
        fit = None
        if len(alone) >= 5 and len({x for x, _ in alone}) > 1:
            mx, my = sum(x for x, _ in alone) / len(alone), sum(y for _, y in alone) / len(alone)
            b = sum((x - mx) * (y - my) for x, y in alone) / sum((x - mx) ** 2 for x, _ in alone)
            fit = {"prefill_s_per_token": b, "base_s": my - b * mx, "requests": len(alone)}
        name = LOGS.get(target, target)
        out[name] = {
            "requests": len(rs),
            "ttft_p50_by_running": {("4+" if k == 4 else str(k)): pct(v, 0.5) for k, v in sorted(by.items())},
            "prefill": fit,  # None: too few lone requests, or cached tokens not reported
            "load_s": loads.get(name, (0, []))[1],
        }
    return out


def run(n):
    global T0
    rng = random.Random(args.seed)
    picked = sessions(rng)
    if args.unload:
        unload()
    before, logs, used_before = counters(), log_sizes(), usage()
    rows, lock, threads, events, stop = [], threading.Lock(), [], [], threading.Event()
    models = [args.mlx_model if rng.random() < args.mlx_share else args.model for _ in picked]
    pick = lambda model: args.url
    if args.baseline == "round-robin":
        turn = {m: itertools.cycle(engines(m)) for m in set(models) | {args.model}}
        pick = lambda model: next(turn[model])
    T0 = time.time()
    for u, c in enumerate(picked):
        start = 0 if args.burst else rng.uniform(0, args.ramp)
        threads.append(threading.Thread(target=user, args=(u, c, models[u], start, rows, lock, pick)))
    others = [c for c in map(json.loads, wildchat().open()) if len(c["turns"][0]["user"]) < 8000] if args.batch else []
    random.Random(args.seed + 1).shuffle(others)
    side = [threading.Thread(target=batch, args=(b, others, stop, rows, lock, pick)) for b in range(args.batch)]
    if args.ops:
        side.append(threading.Thread(target=ops, args=([float(x) for x in args.ops.split(",")], events)))
    for t in threads + side:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    for t in side:
        t.join()
    wall = time.time() - T0
    time.sleep(2)  # let Pharos finish counting the last replies
    after, used_after = counters(), usage()
    diff = {k: after[k] - before.get(k, 0) for k in after if after[k] != before.get(k, 0)}
    loads = engine_view(logs)
    decided, reloaded = pharos_view(logs)
    for r in rows:
        if d := decided.get(r["id"]):
            r["target"] = d["target"].split()[0]  # "url model"
            r["decisions"] = d["decisions"]
            r["prefix"] = next((x.split("=")[1] for x in d["decisions"].split() if x.startswith("prefix=")), None)
            m = re.search(r"(\d+) cached tok", d["reason"])
            r["predicted"] = int(m.group(1)) if m else None

    out = RUN / f"replay-{int(T0)}.jsonl"
    out.write_text("".join(json.dumps(r) + "\n" for r in rows))

    ok = [r for r in rows if "error" not in r]
    chats = [r for r in ok if r["key"] == "chat"]
    sent = sum(1 for r in rows if r.get("error") not in ("URLError", "ConnectionRefusedError", "ConnectionResetError", "RemoteDisconnected"))
    tok = lambda k: sum(r.get(k) or 0 for r in ok)
    ptok = lambda t: sum(v for k, v in diff.items() if k.startswith("pharos_tokens_total") and f'type="{t}"' in k)
    pm = lambda pat: sum(v for k, v in diff.items() if re.search(pat, k))
    restarted = any(w == "restart pharos" for _, w in events)  # /metrics started again from 0
    via = not args.baseline and (restarted or pm(r"^pharos_requests_total") > 0)  # not with --url at an engine
    metrics = {"requests": len(rows), "ok": len(ok), "errors": len(rows) - len(ok), "wall_s": wall}

    how = f"{args.baseline} straight to the engines" if args.baseline else "through Pharos" if via else f"straight to {args.url}"
    print(f"\n[{args.scenario} {n + 1}/{args.repeat}] {len(picked)} users, {len(rows)} requests in {wall:.0f}s {how}, bodies {'raw UTF-8' if args.raw else 'JSON-escaped'}; rows in {out}")
    print("\n== Disagreements")
    found = []
    if via:
        checks = [("prompt tokens", tok("prompt"), "prompt_tokens", ptok("prompt")),
                  ("cached tokens", tok("cached"), "cached_tokens", ptok("cached")),
                  ("completion tokens", tok("completion"), "completion_tokens", ptok("completion"))]
        if not restarted:
            if sent != pm(r"^pharos_requests_total"):
                found.append(f"requests Pharos answered: client {sent}, Pharos /metrics {pm(r'^pharos_requests_total'):.0f}")
            for name, c, _, m in checks:
                if round(c) != round(m):
                    found.append(f"{name}: client {c:.0f}, Pharos /metrics {m:.0f}")
        # Pharos notes load=cold_start on every request that waited for a
        # load (unexpected_load: the engine said so after), so compare
        # engines, not counts.
        cold = sorted({LOGS.get(r["target"]) for r in rows if re.search(r"load=(cold_start|unexpected_load)", r.get("decisions", ""))})
        loaded = sorted(k for k, (_, l) in loads.items() if l)
        if cold != loaded:
            found.append(f"model loads: Pharos sent cold requests to {cold or 'none'}, engine logs show loads on {loaded or 'none'}")
        if used_before and used_after:
            for name, c, k, _ in checks:
                if round(c) != used_after[k] - used_before[k]:
                    found.append(f"{name}: client {c:.0f}, Pharos /usage {used_after[k] - used_before[k]}")
        if unpaired := sum(1 for r in ok if "target" not in r):
            found.append(f"{unpaired} answered requests have no Pharos debug line (serve -log-level debug)")
        # The estimate is bytes ÷ 4, so a few percent over is noise; 1.2× is a miscount.
        if over := [r for r in ok if (r.get("predicted") or 0) > 1.2 * (r.get("prompt") or 1e9)]:
            worst = max(r["predicted"] / r["prompt"] for r in over)
            found.append(f"prefix: Pharos predicted over 1.2× the engine's whole prompt as cached on {len(over)} requests (worst {worst:.1f}×)")
    targeted = {r["target"] for r in rows if "target" in r}
    for name, (n_logged, _) in sorted(loads.items()):
        mine = sum(1 for r in rows if LOGS.get(r.get("target")) == name and r.get("error") != "URLError")
        # SIGKILL (--ops) loses the engine's unflushed log lines with it.
        if (targeted or not via) and mine != n_logged and not (args.ops and name == "llamacpp"):
            found.append(f"{name}: sent {mine}, engine log shows {n_logged} chat requests")
    print("\n".join(f"  {x}" for x in found) or "  none")
    metrics["disagreements"] = len(found)

    print("\n== Clients")
    errors = {}
    for r in rows:
        if "error" in r:
            errors[r["error"]] = errors.get(r["error"], 0) + 1
    print(f"  ok {len(ok)}, errors {errors or 0}")
    for key in sorted({r["key"] for r in rows}):
        for model in sorted({r["model"] for r in rows}):
            rs = [r for r in ok if r["model"] == model and r["key"] == key]
            if rs:
                p50, p95 = pct([r.get("ttft") for r in rs], 0.5), pct([r.get("ttft") for r in rs], 0.95)
                print(f"  {key} {model}: TTFT p50 {fmt(p50)}, p95 {fmt(p95)}, {len(rs)} ok")
                metrics[f"ttft_p50 {key} {model}"], metrics[f"ttft_p95 {key} {model}"] = p50, p95
    print("  cached share of prompt tokens, chat turns 2+, by language:")
    for lang in sorted({r["lang"] for r in chats}):
        rs = [r for r in chats if r["lang"] == lang and r["turn"] > 0 and r.get("prompt")]
        if rs:
            known = [r for r in rs if r.get("cached") is not None]
            share = sum(r["cached"] for r in known) / max(1, sum(r["prompt"] for r in known))
            print(f"    {lang:12} {len(rs):3} requests, {100 * share:3.0f}% cached ({len(rs) - len(known)} didn't report)")
            metrics[f"cached_share {lang}"] = share

    print("\n== Per engine (chat turns 2+ for the cached share)")
    for target in sorted({r["target"] for r in ok if "target" in r}):
        rs = [r for r in ok if r.get("target") == target]
        later = [r for r in rs if r["key"] == "chat" and r["turn"] > 0 and r.get("prompt") and r.get("cached") is not None]
        share = f"{100 * sum(r['cached'] for r in later) / sum(r['prompt'] for r in later):3.0f}% cached" if later else "cached unknown"
        outcomes = {}
        for r in rs:
            if r.get("prefix"):
                outcomes[r["prefix"]] = outcomes.get(r["prefix"], 0) + 1
        print(f"  {LOGS.get(target, target):13} {len(rs):4} ok, TTFT p50 {fmt(pct([r.get('ttft') for r in rs], .5))} p95 {fmt(pct([r.get('ttft') for r in rs], .95))}, {share}"
              + (f", prefix {' '.join(f'{k}={v}' for k, v in sorted(outcomes.items()))}" if outcomes else ""))
        metrics[f"requests {LOGS.get(target, target)}"] = len(rs)
    wrong = [r for r in ok if r.get("prefix") == "wrong_prediction"]
    for r in wrong[:5]:
        print(f"  wrong prediction: user {r['user']} turn {r['turn']} ({r['lang']}) on {LOGS.get(r['target'])}: predicted {r['predicted']}, engine cached {r.get('cached')} of {r.get('prompt')}")
    if len(wrong) > 5:
        print(f"  … {len(wrong) - 5} more wrong predictions in the rows file")
    metrics["wrong_predictions"] = len(wrong)

    print(f"\n== Claim: {SCENARIOS[args.scenario][0]}")
    all_loads = {k: l for k, (_, l) in loads.items() if l}
    metrics["loads"] = sum(len(l) for l in all_loads.values())
    print(f"  model loads: {metrics['loads']}" + "".join(f", {k} {len(l)} ({', '.join(f'{s:.1f}s' for s in l)})" for k, l in sorted(all_loads.items())))
    if args.scenario == "cold-burst":
        used = sorted({LOGS.get(r.get("target")) for r in ok if r.get("target") and r["model"] == args.model})
        print(f"  engines that served {args.model}: {', '.join(used)}")
        metrics["engines used"] = len(used)
    if args.batch:
        for key in ("chat", "batch"):
            rs = [r for r in ok if r["key"] == key and r["model"] == args.model]
            print(f"  {key}: {len(rs)} requests, TTFT p50 {fmt(pct([r.get('ttft') for r in rs], .5))}, p95 {fmt(pct([r.get('ttft') for r in rs], .95))}")
    if args.ops:
        for t, what in events:
            print(f"  {t:6.1f}s {what}")
        cut = [r for r in rows if r.get("cut")]
        killed = [r for r in cut if LOGS.get(r.get("target")) == "llamacpp"]
        refused = [r for r in rows if r.get("refused")]
        clean = sum(1 for r in cut if r["error"] == "no [DONE]")  # a client that doesn't wait for [DONE] shows these as complete
        print(f"  streams cut: {len(cut)}, {len(killed)} of them on llamacpp, {clean} ended without an error; other errors: {sum(1 for r in rows if 'error' in r and not r.get('cut'))}")
        print(f"  requests refused while Pharos restarted, then retried: {len(refused)}")
        print(f"  config reload logged by Pharos: {'yes' if reloaded else 'no'}")
        back = [r for r in ok if LOGS.get(r.get("target")) == "llamacpp" and events and r["start"] > next((t for t, w in events if w == "start llamacpp done"), 1e9)]
        print(f"  llamacpp requests after it came back: {len(back)}")
        metrics.update({"streams cut": len(cut), "streams cut elsewhere": len(cut) - len(killed), "streams cut cleanly": clean, "refused": len(refused)})
    if chats:
        later = [r for r in chats if r["turn"] > 0 and r.get("prompt") and r.get("cached") is not None]
        if later:
            metrics["cached_share chat"] = sum(r["cached"] for r in later) / sum(r["prompt"] for r in later)
            print(f"  chat cached share, turns 2+ where reported: {100 * metrics['cached_share chat']:.0f}%")

    print("\n== Pharos (/metrics, change during the run)" + (": restarted, so from the restart on" if restarted else ""))
    for k in sorted(diff):
        if k.startswith(("pharos_decisions_total", "pharos_prefix_predictions_total")):
            print(f"  {k} {diff[k]:.0f}")

    print("\n== Engines (logs)")
    for name, (n_logged, l) in sorted(loads.items()):
        print(f"  {name:13} {n_logged:4} chat requests" + (f", {len(l)} model loads" if name.startswith("ollama") else ""))

    cal = calibration(ok, loads)
    print("\n== Calibration (also in the summary file)")
    for name, c in cal.items():
        fit = c["prefill"]
        pre = f"prefill {1000 * fit['prefill_s_per_token']:.2f} ms/token + {fit['base_s']:.2f}s ({fit['requests']} lone requests)" if fit else "prefill unknown"
        print(f"  {name:13} TTFT p50 by requests in flight {', '.join(f'{k}: {fmt(v)}' for k, v in c['ttft_p50_by_running'].items())}; {pre}")
    return {"started": datetime.fromtimestamp(T0).isoformat(timespec="seconds"), "rows": str(out), "metrics": metrics, "calibration": cal, "events": events}


def compare(a, b):
    """Prints the median of each metric across the runs of two summary files."""
    sa, sb = (json.loads(Path(f).read_text()) for f in (a, b))
    med = lambda s, k: pct([r["metrics"].get(k) for r in s["runs"]], 0.5)
    print(f"{'':40} {'A':>10} {'B':>10}")
    print(f"{'':40} {sa['scenario'] + ' ' + (sa['args']['baseline'] or 'pharos'):>10} {sb['scenario'] + ' ' + (sb['args']['baseline'] or 'pharos'):>10}")
    for k in sorted({k for s in (sa, sb) for r in s["runs"] for k in r["metrics"]}):
        x, y = med(sa, k), med(sb, k)
        f = lambda v: "-" if v is None else f"{v:.2f}" if isinstance(v, float) else str(v)
        print(f"{k:40} {f(x):>10} {f(y):>10}")
    print(f"\nA: {a} ({sa['pharos']}, {len(sa['runs'])} runs)\nB: {b} ({sb['pharos']}, {len(sb['runs'])} runs)")


def main():
    runs = [run(n) for n in range(args.repeat)]
    versions = RUN / "versions"
    summary = {"scenario": args.scenario, "args": vars(args), "pharos": versions.read_text().strip() if versions.exists() else None, "runs": runs}
    out = RUN / f"summary-{int(T0)}.json"
    out.write_text(json.dumps(summary, indent=1, default=str))
    if args.repeat > 1:
        keys = sorted({k for r in runs for k in r["metrics"]})
        print(f"\n== {args.repeat} runs: median (min–max)")
        for k in keys:
            vs = [r["metrics"].get(k) for r in runs if r["metrics"].get(k) is not None]
            if vs:
                print(f"  {k:40} {pct(vs, .5):.2f} ({min(vs):.2f}–{max(vs):.2f})")
    print(f"\nsummary in {out}")


if args.compare:
    compare(*args.compare)
elif args.fetch:
    print(swe() if args.dataset == "swe" else wildchat())
else:
    main()
