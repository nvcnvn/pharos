# Apple Silicon only, runs natively (no docker). Needs uv.
REPO=ml-explore/mlx-lm
MODEL=mlx-community/Qwen2.5-0.5B-Instruct-4bit
image() { echo "pypi:mlx-lm==${VERSION#v}"; }
digest() { echo "$IMAGE"; }
up() {
  uvx --from "mlx-lm==${VERSION#v}" mlx_lm.server --model "$MODEL" --port "$PORT" >"$OUT/.out" 2>&1 &
  echo $! >"$OUT/.pid"
}
down() { kill "$(cat "$OUT/.pid")"; rm -f "$OUT/.pid" "$OUT/.out"; }
logs() { cat "$OUT/.out"; }
alive() { kill -0 "$(cat "$OUT/.pid")"; }
