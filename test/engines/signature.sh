#!/usr/bin/env bash
# signature.sh <capture version dir>: the shape of a capture, not its values.
# One line per fact: "<state> <file> <kind> <item>", sorted. Two versions with the
# same signature look the same to every probe (meaning changes aside).
#   diff <(signature.sh vllm/v0.29.0) <(signature.sh vllm/v0.30.0)
# ponytail: engine.log is left out; add log line templates when log probes exist.
set -euo pipefail
dir=${1:?usage: signature.sh <engine>/<version>}

keys='[paths | map(if type == "number" then "[]" else tostring end) | join(".")] | unique[]'

for s in "$dir"/*/; do
  state=$(basename "$s")
  if [ "$state" = streams ]; then
    for f in "$s"*; do # SSE "data: {...}" or NDJSON; key union across chunks
      sed -E 's/^data: //' "$f" | grep '^{' | jq -r "$keys" | sed "s|^|$state $(basename "$f" | sed -E 's/\.[0-9]+\./.N./') key |"
    done
    continue
  fi
  awk -F'\t' -v s="$state" '{print s, "paths.tsv", "status", $1, $2}' "$s/paths.tsv"
  for f in "$s"*; do
    b=$(basename "$f"); [ "$b" = paths.tsv ] && continue
    if jq -e . "$f" >/dev/null 2>&1; then
      jq -r "$keys" "$f" | sed "s|^|$state $b key |"
    elif grep -qE '^# (TYPE|HELP) ' "$f"; then # Prometheus text: name + label keys
      grep -v '^#' "$f" | awk -v s="$state" -v b="$b" '{
        n = $0; sub(/[{ ].*/, "", n); l = ""
        if (match($0, /\{[^}]*\}/)) { t = substr($0, RSTART, RLENGTH)
          while (match(t, /[a-zA-Z_][a-zA-Z0-9_]*="/)) { l = l substr(t, RSTART, RLENGTH - 2) ","; t = substr(t, RSTART + RLENGTH) } }
        print s, b, "metric", n "{" l "}" }'
    else
      echo "$state $b opaque"
    fi
  done
done | sort -u
