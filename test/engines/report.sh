#!/usr/bin/env bash
# report.sh <root>: markdown comparing consecutive versions of each engine under
# <root>/<engine>/<version>/ (the testdata layout). Per version: the numbers the
# probes read in each state, TestLive failures (test.log, CI only), and the
# signature diff against the previous version. Raw material for a human; it judges nothing.
#   test/engines/report.sh internal/engine/testdata
set -uo pipefail
root=${1:?usage: report.sh <root>}
sig=$(cd "$(dirname "$0")" && pwd)/signature.sh
metrics='^(vllm:(num_requests_(running|waiting)|kv_cache_usage_perc|gpu_cache_usage_perc)|llamacpp:requests_(processing|deferred)|sglang:(num_running_reqs|num_queue_reqs|token_usage))[{ ]'

values() { # values <version dir>: one line per state
  local s d line
  for s in idle loaded busy cold saturated cancelled; do
    d=$1/$s; [ -f "$d/paths.tsv" ] || continue
    line=""
    [ -f "$d/metrics" ] && line+=$(grep -E "$metrics" "$d/metrics" | sed -E 's/\{[^}]*\}//' | awk '{printf "%s=%s ", $1, $2}')
    [ -f "$d/slots" ] && line+="slots_processing=$(jq '[.[]? | select(.is_processing)] | length' "$d/slots" 2>/dev/null) "
    [ -f "$d/running" ] && line+="running_states=$(jq -c '[.running[]?.state]' "$d/running" 2>/dev/null) "
    [ -f "$d/api_ps" ] && line+="api_ps_models=$(jq '.models | length' "$d/api_ps" 2>/dev/null) "
    echo "- $s: ${line:-(nothing probed)}"
  done
  for f in "$1"/streams/*-chat.*; do # cached tokens, and Ollama's own count and prefill time
    [ -f "$f" ] || continue
    echo "- $(basename "$f"): $(sed -E 's/^data: //' "$f" | grep '^{' | jq -c 'select(.usage or .done) | {cached: (.usage.prompt_tokens_details.cached_tokens // .prompt_eval_cached_count), prompt: (.usage.prompt_tokens // .prompt_eval_count), prefill_ns: .prompt_eval_duration}' 2>/dev/null | tail -1)"
  done
  [ -f "$1/streams/unknown-model.txt" ] && echo "- unknown model: HTTP $(tail -1 "$1/streams/unknown-model.txt")"
}

for e in "$root"/*/; do
  echo "## $(basename "$e")"
  prev=
  for v in $(ls "$e" | sort -V); do
    d=$e$v; [ -d "$d" ] || continue
    echo; echo "### $v"
    [ -f "$d/meta.yaml" ] || echo "**Capture incomplete.** engine.log tail: \`$(tail -3 "$d/engine.log" 2>/dev/null | tr '\n' ' ' | cut -c1-300)\`"
    [ -f "$d/cpu.txt" ] && echo "- runner: $(tr '\n' ' ' <"$d/cpu.txt" | sed 's/model name[[:space:]]*: //')"
    [ -f "$d/test.log" ] && grep -E 'live_test\.go:[0-9]+: .*(want|exit status)|not ready' "$d/test.log" | sed 's/^ */- test: /' | head -20
    values "$d"
    if [ -n "$prev" ]; then # server_info dumps every SGLang flag; it changes each release
      echo; echo "<details><summary>signature diff vs $prev</summary>"; echo; echo '```diff'
      diff <("$sig" "$e$prev" 2>/dev/null) <("$sig" "$d" 2>/dev/null) | grep '^[<>]' | grep -vE ' (get_)?server_info key ' | sed 's/^</-/; s/^>/+/'
      echo '```'; echo "</details>"
    fi
    [ -f "$d/meta.yaml" ] && prev=$v
  done
  echo
done
