#!/usr/bin/env bash
# Spin up one engine version, drive it through idle → loaded → busy → cold → saturated → cancelled, and save
# what it reports as raw layer-2 captures (ARCHITECTURE §14, testing skill "Engine path").
#
#   test/engines/capture.sh <engine> [version]     # version defaults to the latest release
#
# <engine> is a directory next to this script holding profile.sh and usually compose.yaml.
# A version that needs a different setup gets <engine>/<version>.compose.yaml, merged on top.
# Output: internal/engine/testdata/<engine>/<version>/{idle,loaded,busy,cold,saturated,cancelled,streams}/, engine.log, meta.yaml,
# or $CAPTURE_OUT if set (the layer-4 Go tests set it to a temp dir unless PHAROS_RECORD=1).
# Each state dir is written by `pharos doctor -record`: one raw body per path (/api/ps → api_ps)
# and paths.tsv (path, status, content type). doctor also prints the plan it resolves live.
set -euo pipefail
cd "$(dirname "$0")"

ENGINE=${1:?usage: capture.sh <engine> [version]}
export PORT=${PORT:-18080}
BASE=http://127.0.0.1:$PORT
CAPACITY=2      # every profile pins the engine to 2 parallel slots, so busy = 2 running + 2 waiting
BUSY_REQUESTS=4
SATURATED_REQUESTS=8
BUSY_DELAY=${BUSY_DELAY:-2}   # seconds between the slots filling and capturing
BUSY_TIMEOUT=${BUSY_TIMEOUT:-180} # give up waiting for the slots to fill (engine without slot limit)
READY=/v1/models              # profile may override
READY_TIMEOUT=${READY_TIMEOUT:-900}
UNLOAD_WAIT=                  # seconds until the engine unloads an idle model; empty = no cold/ capture
NATIVE=                       # "ollama" = also record /api/chat streams

compose() {
  local f=(-f "$ENGINE/compose.yaml")
  [ -f "$ENGINE/$VERSION.compose.yaml" ] && f+=(-f "$ENGINE/$VERSION.compose.yaml")
  docker compose -p "pharos-$ENGINE" "${f[@]}" "$@"
}
up() { docker volume create pharos-models >/dev/null; compose up -d; }
down() { compose down; }
logs() { compose logs --no-color --no-log-prefix; }
alive() { [ -n "$(compose ps -q --status running)" ]; }
digest() { docker image inspect --format '{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null || echo "$IMAGE"; }

source "$ENGINE/profile.sh" # sets REPO, MODEL, image(); may override the defaults and functions above
export VERSION=${2:-$(gh api "repos/$REPO/releases/latest" --jq .tag_name)}
export IMAGE; IMAGE=$(image)
OUT=${CAPTURE_OUT:-../../internal/engine/testdata/$ENGINE/$VERSION}

# The one recorder: doctor -record GETs every path in engine.RecordPaths.
PHAROS=$(mktemp -d)/pharos
(cd ../.. && go build -o "$PHAROS" ./cmd/pharos)
capture() { # capture <state>
  "$PHAROS" doctor -url "$BASE" -record "$OUT/$1" || echo "(doctor: backend did not answer)"
  echo "captured $1"
}

chat() { # chat <max_tokens> <user> [system] [extra json]: one streamed OpenAI chat completion, SSE on stdout
  jq -n --arg m "$MODEL" --argjson n "$1" --arg u "$2" --arg s "${3:-}" --argjson x "${4:-null}" \
    '{model:$m, stream:true, stream_options:{include_usage:true}, temperature:0, max_tokens:$n,
      messages:([{role:"system",content:$s} | select($s != "")] + [{role:"user",content:$u}])} + ($x // {})' |
    curl -sS -N -m 600 "$BASE/v1/chat/completions" -H 'content-type: application/json' -d @-
}

ollama_chat() { # same as chat, over Ollama's native API (NDJSON)
  jq -n --arg m "$MODEL" --argjson n "$1" --arg u "$2" --arg s "${3:-}" \
    '{model:$m, stream:true, options:{temperature:0, num_predict:$n},
      messages:([{role:"system",content:$s} | select($s != "")] + [{role:"user",content:$u}])}' |
    curl -sS -N -m 600 "$BASE/api/chat" -d @-
}

trap 'logs 2>&1 | sed "s#$HOME#~#g" >"$OUT/engine.log" || true; down >/dev/null 2>&1 || true' EXIT
rm -rf "$OUT"; mkdir -p "$OUT"
echo "== $ENGINE $VERSION ($IMAGE) on :$PORT"
up
for ((i = 0; ; i++)); do
  curl -fsS -m 5 -o /dev/null "$BASE$READY" 2>/dev/null && break
  alive && ((i < READY_TIMEOUT)) || { echo "not ready (exited or ${READY_TIMEOUT}s timeout)"; logs | tail -40; compose ps -a 2>/dev/null; exit 1; }
  sleep 1
done
declare -F setup >/dev/null && setup # e.g. pull the model

capture idle
chat 8 "Say hi." >/dev/null
capture loaded

# Same long prefix twice: the second response should report cached prompt tokens.
mkdir -p "$OUT/streams"
long=$(seq 1 300 | sed 's/.*/Rule &: answer in plain words./' | tr '\n' ' ')
for n in 1 2; do chat 16 "Say hi." "$long" >"$OUT/streams/openai-chat.$n.sse"; done
if [ "$NATIVE" = ollama ]; then # its own prefix, so the first native request is cold too
  for n in 1 2; do ollama_chat 16 "Say hi." "Native. $long" >"$OUT/streams/ollama-chat.$n.ndjson"; done
fi
# Error shape for a model the engine doesn't serve (status line last).
jq -n '{model:"no-such-model", messages:[{role:"user",content:"hi"}], max_tokens:1}' |
  curl -sS -m 30 -w '\n%{http_code}\n' "$BASE/v1/chat/completions" -H 'content-type: application/json' -d @- \
  >"$OUT/streams/unknown-model.txt" || true

# load <n>: fire n slow requests (pids in $pids) and return once the slots are full. ignore_eos keeps
# every request generating (engines without it just ignore the field); the prompt helps those.
load() {
  local i t; pids=(); busy=$(mktemp -d)
  for ((i = 0; i < $1; i++)); do
    chat 1000 "Write the numbers from 1 to 1000, one per line, with no other text." "" '{"ignore_eos":true}' >"$busy/$i" &
    pids+=($!)
  done
  # Wait until CAPACITY streams have produced a token: a queued request can't have one, so the slots
  # are full and the rest are queued. A fixed sleep was too short on a CI runner (vLLM: 1 running, 0 waiting).
  for ((t = 0; t < BUSY_TIMEOUT; t++)); do
    (($(grep -lE '"content": ?"[^"]' "$busy"/* 2>/dev/null | wc -l) >= CAPACITY)) && break
    sleep 1
  done
  sleep "$BUSY_DELAY"
}

# More requests than slots: expect running = CAPACITY, waiting = the rest.
load "$BUSY_REQUESTS"
capture busy
wait "${pids[@]}"
rm -rf "$busy"

if [ -n "$UNLOAD_WAIT" ]; then
  sleep "$UNLOAD_WAIT"
  capture cold
fi

# Saturated: 4x capacity, so waiting should read 6. Then drop every client and see
# whether the engine stops the work (running → 0) or keeps generating for nobody.
# After cold, so an engine that keeps generating can't hold the model in memory.
chat 8 "Say hi." >/dev/null # reload after cold
load "$SATURATED_REQUESTS"
capture saturated
for p in "${pids[@]}"; do pkill -P "$p"; done # chat runs curl in a child; killing the job alone leaves it connected
kill "${pids[@]}" 2>/dev/null || true
wait "${pids[@]}" 2>/dev/null || true
rm -rf "$busy"
sleep 3
capture cancelled

cat >"$OUT/meta.yaml" <<EOF
engine: $ENGINE
version: $VERSION
image: $(digest)
model: $MODEL
capacity: $CAPACITY
busy_requests: $BUSY_REQUESTS
saturated_requests: $SATURATED_REQUESTS
captured: $(date -u +%Y-%m-%dT%H:%M:%SZ)
command: test/engines/capture.sh $ENGINE $VERSION
host: $(uname -s)/$(uname -m)
EOF
echo "done: $OUT (review engine.log for prompt content before committing)"
