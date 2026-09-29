#!/bin/sh
# Starts a small team's Mac setup natively on Metal: two Ollama, one llama.cpp
# and one mlx-lm, with Pharos in front on :8090. Logs, pids and the generated
# config go to $RUN, and each process's command line to $RUN/<name>.args so
# replay.py can restart what it kills. Stop everything with down.sh.
set -eu
cd "$(dirname "$0")"
RUN=${RUN:-$PWD/.run}
MODEL=${MODEL:-qwen2.5:3b}                             # Ollama's name; llama.cpp serves the same weights under it
GGUF=${GGUF:-Qwen/Qwen2.5-3B-Instruct-GGUF:Q4_K_M}
MLX=${MLX:-mlx-community/Qwen2.5-3B-Instruct-4bit}     # its own pool: mlx-lm can't serve under another name
MLX_VERSION=${MLX_VERSION:-0.31.3}
OLLAMA=${OLLAMA:-ollama}
CTX=${CTX:-16384}                                      # tokens per slot; 2 slots per engine
mkdir -p "$RUN"

start() { # name command...: runs in the background, logs to $RUN/name.log
	name=$1
	shift
	printf '%s\n' "$@" >"$RUN/$name.args"
	"$@" >"$RUN/$name.log" 2>&1 &
	echo $! >"$RUN/$name.pid"
}
wait_up() { # url [seconds]: model downloads on first run can take minutes
	i=0
	until curl -sf -o /dev/null "$1"; do
		i=$((i + 1))
		[ "$i" -lt "${2:-60}" ] || { echo "not up: $1 (logs in $RUN)" >&2; exit 1; }
		sleep 1
	done
}

for port in 11500 11501; do
	start "ollama-$port" env OLLAMA_HOST=127.0.0.1:$port OLLAMA_NUM_PARALLEL=2 OLLAMA_CONTEXT_LENGTH=$CTX "$OLLAMA" serve
done
start llamacpp llama-server -hf "$GGUF" --alias "$MODEL" --port 18081 --parallel 2 -c $((CTX * 2)) --metrics --slots
start mlx uvx --from "mlx-lm==$MLX_VERSION" mlx_lm.server --model "$MLX" --port 18082
(cd ../.. && go build -o "$RUN/pharos" ./cmd/pharos)

wait_up http://127.0.0.1:11500/api/version
wait_up http://127.0.0.1:11501/api/version
OLLAMA_HOST=127.0.0.1:11500 "$OLLAMA" pull "$MODEL" >/dev/null # both share one models dir
wait_up http://127.0.0.1:18081/health 900
wait_up http://127.0.0.1:18082/health 900
# The first mlx-lm request downloads the weights if they aren't cached yet.
curl -sf -o /dev/null http://127.0.0.1:18082/v1/chat/completions -d '{"model":"default_model","messages":[{"role":"user","content":"hi"}],"max_tokens":1}'

# Two fixed keys for replay.py: chat users, and a batch job (--scenario noisy).
sha() { printf %s "$1" | shasum -a 256 | cut -d' ' -f1; }
cat >"$RUN/pharos.yaml" <<EOF
listen: 127.0.0.1:8090
state_file: $RUN/pharos.state     # usage and learned speeds survive a restart
keys:
  - name: chat
    sha256: $(sha pharos-chat)
    admin: true                   # replay.py reads /usage with it
  - name: batch
    sha256: $(sha pharos-batch)
backends:
  - url: http://127.0.0.1:11500
    memory_gb: 16                 # one Mac's memory, shared by every engine here
    logs: file://$RUN/ollama-11500.log
  - url: http://127.0.0.1:11501
    memory_gb: 16
    logs: file://$RUN/ollama-11501.log
  - url: http://127.0.0.1:18081
  - url: http://127.0.0.1:18082
EOF
start pharos "$RUN/pharos" serve -config "$RUN/pharos.yaml" -log-level debug # a line per request, for replay.py
wait_up http://127.0.0.1:8090/healthz
echo "Pharos $("$RUN/pharos" version) on http://127.0.0.1:8090 with Ollama $("$OLLAMA" --version 2>&1 | grep -o "[0-9][0-9.]*" | tail -1), llama.cpp $(llama-server --version 2>&1 | grep -o 'version: [0-9]*' | cut -d' ' -f2), mlx-lm $MLX_VERSION" | tee "$RUN/versions"
