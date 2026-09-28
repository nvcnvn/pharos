#!/usr/bin/env bash
# What CI runs, on this machine, in the least wall time. Green here = safe to push straight to main.
#
#   test/check.sh        fast stage; plus the live stage when the change since origin/main touches engine signals
#   test/check.sh fast   vet, layers 1–3, rolling restart of three instances (~15 s, no Docker)
#   test/check.sh live   fast stage + layer 4 for llama.cpp, Ollama, vLLM and Docker discovery (~4–5 min),
#                        then the image end to end (test/e2e, ~1–2 min with the images pulled)
#
# Measured on a Mac Mini (M-series, 14 cores, Docker VM 8 CPUs), images pulled (2026-09-28): layers 1–3 2 s,
# rolling restart 13 s; live llama.cpp 40 s, Ollama 68 s, vLLM 125 s. The engines run one at a time, fastest
# first: in parallel they fight over the Docker VM's CPUs and all three took 270–300 s.
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-auto}
t0=$SECONDS
step() { echo "== $1 ($((SECONDS - t0))s)"; }

step "vet"
go vet ./... && go vet -tags integration ./...
step "layers 1–3"
go test ./...
step "rolling restart" # before the live stage: its instances discover labeled containers
go test -tags integration -run TestRollingRestart ./cmd/pharos

e2e=
if [ "$mode" = auto ]; then
  # The testing skill's "when to run what": engine code, the stream tap, the scrape and log-follow loops.
  changed=$( (git diff --name-only origin/main...HEAD; git diff --name-only HEAD) 2>/dev/null)
  if grep -qE '^(internal/engine|internal/state|internal/discovery|internal/proxy/tap|test/engines)/?' <<<"$changed"; then
    mode=live
  else
    echo "== no engine-signal files changed: live stage skipped (test/check.sh live runs it)"
  fi
  # The image, what only runs inside it (discovery through the mounted socket), and serve.
  if grep -qE '^(Dockerfile|\.dockerignore|test/e2e/|internal/discovery/|cmd/pharos/serve\.go)' <<<"$changed"; then
    e2e=1
  fi
fi
if [ "$mode" = live ]; then
  step "layer 4: llamacpp, ollama, vllm"
  PHAROS_LIVE_ENGINES=llamacpp,ollama,vllm go test -count=1 -tags integration -timeout 50m -run TestLive ./internal/engine
  step "layer 4: docker discovery"
  go test -count=1 -tags integration -timeout 15m -run TestLiveDocker ./internal/discovery
  e2e=1
fi
if [ -n "$e2e" ]; then
  step "end to end: the image in front of real engines"
  go test -count=1 -tags integration -timeout 20m ./test/e2e
fi
step "all green"
