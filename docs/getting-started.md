# Getting started

In about five minutes: two Ollama backends, Pharos in front of them, and three requests that show why it sends each one where it does.

You need [Ollama](https://ollama.com), `curl`, `jq` and Go 1.27 or later (there are no release binaries yet). Everything runs on one machine; on a real team the two Ollamas are two machines, and nothing below changes except their URLs.

## 1. Two backends

Start two Ollama instances on spare ports, so an Ollama you already run on 11434 is left alone. They share one models directory, so the model is downloaded once.

```sh
mkdir pharos-demo && cd pharos-demo
export OLLAMA_NUM_PARALLEL=2 OLLAMA_CONTEXT_LENGTH=4096   # 2 slots each, small KV cache
OLLAMA_HOST=127.0.0.1:11435 ollama serve > a.log 2>&1 &
OLLAMA_HOST=127.0.0.1:11436 ollama serve > b.log 2>&1 &
sleep 2
OLLAMA_HOST=127.0.0.1:11435 ollama pull qwen2.5:0.5b
```

Call them A (11435) and B (11436).

## 2. Install Pharos and ask what it can see

```sh
go install github.com/nvcnvn/pharos/cmd/pharos@latest

cat > pharos.yaml <<EOF
listen: :8090
backends:
  - url: http://127.0.0.1:11435
    logs: file://$PWD/a.log    # Ollama reports its slot count only in its log
  - url: http://127.0.0.1:11436
    logs: file://$PWD/b.log
EOF

pharos doctor -config pharos.yaml
```

```
== http://127.0.0.1:11435
  kind      ollama (detected)
  version   0.32.15
  log feed  file:///…/pharos-demo/a.log
  active    version     ollama-version           /api/version  0.32.15
            models      openai-models            /v1/models    [qwen2.5:0.5b]
            residency   ollama-ps-residency      /api/ps       [qwen2.5:0.5b=cold]
            …
            capacity    ollama-log-num-parallel  log           [=2]
  unknown   running, waiting, kv_usage
== http://127.0.0.1:11436
  …
```

This is what Pharos routes on: which models each backend has and whether they're loaded, how many requests it can run at once, and what it can't read. Ollama doesn't report its running and waiting requests, so they're **unknown**. Pharos counts its own in-flight requests instead and never treats an unknown as 0. Other engines report more: see the [support matrix](SUPPORT.md).

## 3. Start Pharos

Load the model on A only, then start Pharos in a second terminal (same directory), with a log line per request:

```sh
curl -s localhost:11435/api/generate -d '{"model":"qwen2.5:0.5b"}'
pharos serve -config pharos.yaml -log-level debug
```

Pharos speaks the OpenAI API at `http://localhost:8090/v1` and Ollama's at `http://localhost:8090`. Send everything below from the first terminal and watch the second.

## 4. A warm backend beats a cold one

```sh
curl -s localhost:8090/v1/chat/completions -d '{"model":"qwen2.5:0.5b","max_tokens":50,
  "messages":[{"role":"user","content":"Name three lighthouses."}]}'
```

```
target="http://127.0.0.1:11435 qwen2.5:0.5b" reason="warm, 0 cached tok, est 0.04s (wait 0.00, load 0.00, prefill 0.04) vs 10.04 next"
```

For every request Pharos estimates the time to first token on each backend: waiting for a slot, loading the model, and reading the prompt. A has the model loaded: 0.04 s. B would have to load it first: 10 s, a default until Pharos has timed a load on B. Round-robin would have sent every other request to B and paid that load.

## 5. A burst spills over

Eight requests at once, four times what A can run:

```sh
(for i in 1 2 3 4 5 6 7 8; do
  curl -s localhost:8090/v1/chat/completions -o /dev/null -d "{\"model\":\"qwen2.5:0.5b\",\"max_tokens\":150,\"stream\":true,
    \"messages\":[{\"role\":\"user\",\"content\":\"Write a poem about lighthouse $i.\"}]}" &
done; wait)
```

```
target="http://127.0.0.1:11435 …" reason="warm, … est 0.06s … vs 10.06 next"   decisions="… queue=immediate …"
target="http://127.0.0.1:11435 …" reason="warm, … est 0.06s … vs 10.06 next"   decisions="… queue=immediate …"
target="http://127.0.0.1:11436 …" reason="cold, … est 10.06s (wait 0.00, load 10.00, prefill 0.06) vs 10.38 next"   decisions="… queue=waited load=cold_start …"
target="http://127.0.0.1:11436 …" reason="loading, … est 0.06s … vs 6.94 next"   decisions="… queue=waited load=cold_start …"
target="http://127.0.0.1:11435 …" reason="warm, … est 0.06s … vs 11.21 next"   decisions="… queue=waited …"
…
```

The first two take A's slots. The rest wait in Pharos's queue, not the engine's. Once waiting for A is estimated to take longer than loading the model on B, Pharos loads it there; the others wait for a slot on A. No request is refused. The order and numbers differ from run to run.

## 6. A conversation stays where its prompt is cached

Both backends are warm now. Ask two questions about a long document:

```sh
curl -s https://www.apache.org/licenses/LICENSE-2.0.txt > license.txt
jq -n --rawfile doc license.txt '{model: "qwen2.5:0.5b", max_tokens: 100, messages: [
  {role: "system", content: ("Answer questions about this license:\n\n" + $doc)},
  {role: "user", content: "Can I use it in a commercial product?"}]}' > turn1.json
curl -s localhost:8090/v1/chat/completions -d @turn1.json | jq -r '.choices[0].message.content' > reply.txt

jq --rawfile r reply.txt '.messages += [{role: "assistant", content: $r},
  {role: "user", content: "Do I have to publish my changes?"}]' turn1.json > turn2.json
curl -s localhost:8090/v1/chat/completions -d @turn2.json | jq -r '.choices[0].message.content'
```

```
target="http://127.0.0.1:11436 …" reason="warm, 0 cached tok, est 14.30s (wait 0.00, load 0.00, prefill 14.30) vs 14.30 next"
target="http://127.0.0.1:11436 …" reason="warm, 2861 cached tok, est 0.70s (wait 0.00, load 0.00, prefill 0.70) vs 15.00 next"
```

The first turn could go to either backend. The second goes to the one that already holds the document in its cache. That backend's own log (`b.log`) shows the difference:

```
prompt eval time =     747.82 ms /  2297 tokens
cached n_tokens = 2399
prompt eval time =     224.38 ms /    20 tokens
```

Pharos never stores prompts; it remembers a hash of each message and which backend saw it (see [ARCHITECTURE §6](ARCHITECTURE.md#6-prefix-index-internalprefix)). This is what makes it worth putting in front of chat users and coding agents, whose every turn resends the whole history. The seconds are estimates, starting from defaults (here 5 ms per prompt token, where this Mac took 0.3), so compare them with each other rather than with a clock.

## 7. The status page

Open <http://localhost:8090/status>: each backend, what it has loaded, every signal and the probe that read it, the load times Pharos has measured, and the last requests with the same reasons as above.

## Clean up

```sh
kill %1 %2        # the two Ollamas, in the first terminal; Ctrl-C stops Pharos
```

## Next

- Point your clients at it: any OpenAI SDK with `base_url="http://localhost:8090/v1"`, Open WebUI as an OpenAI connection, or `OLLAMA_HOST=localhost:8090 ollama run qwen2.5:0.5b`.
- Before letting the team in, add keys: without `keys:`, Pharos lets every client in and logs a warning. See the README's *Keys and quotas*.
- Other engines (llama.cpp, vLLM, SGLang, llama-swap) and Docker label discovery: the README and [SUPPORT.md](SUPPORT.md).
