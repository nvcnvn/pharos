REPO=ollama/ollama
MODEL=qwen2.5:0.5b
EMBED_MODEL=all-minilm:22m # 0.34.4 answers 501 to embeddings from a chat model
READY=/api/version
UNLOAD_WAIT=25 # OLLAMA_KEEP_ALIVE=15s in compose.yaml
NATIVE=ollama
image() { echo "ollama/ollama:${VERSION#v}"; }
pull() { curl -fsS "$BASE/api/pull" -d "{\"model\":\"$1\",\"stream\":false}" >/dev/null; }
drop() { curl -sS -X DELETE "$BASE/api/delete" -d "{\"model\":\"$1\"}" >/dev/null || true; }
# The embedding model is on the shared volume only while the replies run: in /api/tags it would be a
# second, cold model in every state capture. setup drops it too, in case a run died before after_replies.
setup() { drop "$EMBED_MODEL"; pull "$MODEL"; }
before_replies() { pull "$EMBED_MODEL"; }
after_replies() { drop "$EMBED_MODEL"; }
