"""Records what Open WebUI really sends: runs it in Docker against a sink that
forwards to Pharos, types a multi-turn chat into its UI with a headless
browser, and saves every request the sink saw (client captures, ARCHITECTURE
§14). Needs up.sh running, and Docker.

    uv run --with playwright==1.55.0 webui.py                     # Open WebUI's OpenAI connection
    uv run --with playwright==1.55.0 webui.py --connection ollama # its Ollama connection

Captures go to .run/captures/open-webui-<version>-<connection>/, one JSON file
per request: method, path, headers (Authorization and cookies redacted) and
the body exactly as sent, with the reply Pharos streamed back beside it
(NNN.sse). --fixture copies the requests alone into a testdata directory.
"""

import argparse
import http.server
import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
p.add_argument("--connection", choices=["openai", "ollama"], default="openai")
p.add_argument("--version", default="v0.11.4", help="Open WebUI image tag")
p.add_argument("--model", default="qwen2.5:3b")
p.add_argument("--pharos", default="http://127.0.0.1:8090")
p.add_argument("--sink-port", type=int, default=8091)
p.add_argument("--port", type=int, default=3000, help="Open WebUI on the host")
p.add_argument("--run", default=str(HERE / ".run"))
p.add_argument("--fixture", help="also copy the requests (not the replies) here, with a meta.yaml")
args = p.parse_args()
OUT = Path(args.run) / "captures" / f"open-webui-{args.version}-{args.connection}"
KEY = "pharos-chat"  # up.sh's chat key
# Non-ASCII on purpose: how a client escapes it is what broke prefix feedback.
TURNS = ["Привет, 你好! Say hello in three languages, one line each.", "Now say good morning in Russian.", "Thanks. And in Chinese?"]

seq, inflight, last, lock = [0], [0], [time.time()], threading.Lock()


class Sink(http.server.BaseHTTPRequestHandler):
    """Records each request, then forwards it to Pharos and streams the reply
    back (HTTP/1.0: the reply ends when the connection closes)."""

    def handle_one(self):
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        with lock:
            seq[0] += 1
            n = seq[0]
            inflight[0] += 1
        headers = {k: ("<redacted>" if k.lower() in ("authorization", "cookie") else v) for k, v in self.headers.items()}
        rec = {"seq": n, "method": self.command, "path": self.path, "headers": headers}
        try:
            rec["body"] = body.decode()
        except UnicodeDecodeError:
            rec["body_hex"] = body.hex()
        fwd = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length", "connection")}
        req = urllib.request.Request(args.pharos + self.path, body or None, fwd, method=self.command)
        try:
            resp = urllib.request.urlopen(req, timeout=600)
        except urllib.error.HTTPError as e:
            resp = e
        rec["status"] = resp.status
        self.send_response(resp.status)
        for k, v in resp.headers.items():
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
        self.end_headers()
        got = []
        try:
            while chunk := resp.read1(65536) if hasattr(resp, "read1") else resp.read():
                got.append(chunk)
                self.wfile.write(chunk)
                self.wfile.flush()
        finally:
            (OUT / f"{n:03d}.sse").write_bytes(b"".join(got))  # the model's reply; never copied into testdata
            (OUT / f"{n:03d}.json").write_text(json.dumps(rec, indent=1, ensure_ascii=False) + "\n")
            with lock:
                inflight[0] -= 1
                last[0] = time.time()

    do_GET = do_POST = do_HEAD = do_DELETE = handle_one

    def log_message(self, *a):
        pass


def idle(quiet=8, timeout=300):
    """Waits until no request is in flight and none arrived for quiet seconds:
    Open WebUI sends title and tag requests after the reply."""
    end = time.time() + timeout
    while time.time() < end:
        with lock:
            if inflight[0] == 0 and time.time() - last[0] > quiet:
                return
        time.sleep(0.5)
    raise SystemExit("Open WebUI never went quiet")


def main():
    from playwright.sync_api import sync_playwright

    OUT.mkdir(parents=True, exist_ok=True)
    for f in [*OUT.glob("*.json"), *OUT.glob("*.sse")]:
        f.unlink()
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", args.sink_port), Sink)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    sink = f"http://host.docker.internal:{args.sink_port}"
    env = {"WEBUI_AUTH": "False", "DEFAULT_MODELS": args.model, "ENABLE_OPENAI_API": "True", "ENABLE_OLLAMA_API": "False",
           "OPENAI_API_BASE_URL": sink + "/v1", "OPENAI_API_KEY": KEY}
    if args.connection == "ollama":
        env |= {"ENABLE_OPENAI_API": "False", "ENABLE_OLLAMA_API": "True", "OLLAMA_BASE_URL": sink}
    name = "pharos-open-webui"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-p", f"127.0.0.1:{args.port}:8080",
                    *[x for k, v in env.items() for x in ("-e", f"{k}={v}")], f"ghcr.io/open-webui/open-webui:{args.version}"], check=True)
    try:
        url = f"http://127.0.0.1:{args.port}"
        for _ in range(300):
            try:
                urllib.request.urlopen(url + "/health", timeout=2)
                break
            except OSError:
                time.sleep(1)
        with sync_playwright() as pw:
            page = pw.chromium.launch().new_page()
            page.goto(url)
            box = page.locator("#chat-input")
            box.wait_for(timeout=120_000)
            idle()
            # A first visit opens a "what's new" dialog over the chat.
            dialog = page.locator('div[role="dialog"]')
            for _ in range(5):
                if not dialog.count():
                    break
                ok = dialog.get_by_role("button", name=r"okay|let's go|close|dismiss", exact=False)
                ok.first.click() if ok.count() else page.keyboard.press("Escape")
                time.sleep(1)
            for turn in TURNS:
                box.click()
                page.keyboard.type(turn)
                page.keyboard.press("Enter")
                time.sleep(2)
                idle()
            page.screenshot(path=str(OUT / "chat.png"))
    finally:
        subprocess.run(["docker", "logs", name], stdout=(OUT / "open-webui.log").open("w"), stderr=subprocess.STDOUT)
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        srv.shutdown()
    for f in sorted(OUT.glob("*.json")):
        r = json.loads(f.read_text())
        body = r.get("body") or ""
        print(f"{r['seq']:3} {r['method']:4} {r['path']:32} {r['status']} {len(body):6}B" + (" \\u-escaped" if "\\u" in body else " raw" if any(ord(c) > 127 for c in body) else ""))
    print(f"captures in {OUT}")
    if args.fixture:
        fx = Path(args.fixture)
        fx.mkdir(parents=True, exist_ok=True)
        for f in OUT.glob("*.json"):
            (fx / f.name).write_text(f.read_text())
        (fx / "meta.yaml").write_text(
            f"client: open-webui {args.version} ({args.connection} connection), typed into its UI by examples/mac-native/webui.py\n"
            f"captured: {time.strftime('%Y-%m-%d')}\nturns: {json.dumps(TURNS, ensure_ascii=False)}\n")
        print(f"fixture in {fx}")


main()
