REPO=ollama/ollama
MODEL=qwen2.5:0.5b
READY=/api/version
UNLOAD_WAIT=25 # OLLAMA_KEEP_ALIVE=15s in compose.yaml
NATIVE=ollama
image() { echo "ollama/ollama:${VERSION#v}"; }
setup() { curl -fsS "$BASE/api/pull" -d "{\"model\":\"$MODEL\",\"stream\":false}" >/dev/null; }
