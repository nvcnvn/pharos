REPO=mostlygeek/llama-swap
MODEL=qwen2.5-0.5b
UNLOAD_WAIT=25 # ttl: 15 in config.yaml
image() { # release v260 ships as v260-cpu-b<bundled llama.cpp build>; ghcr pages tags 1000 at a time
  local tok hdr next="/v2/$REPO/tags/list?n=1000" tag=
  tok=$(curl -fsS "https://ghcr.io/token?scope=repository:$REPO:pull" | jq -r .token)
  while [ -z "$tag" ] && [ -n "$next" ]; do
    hdr=$(mktemp)
    tag=$(curl -fsS -D "$hdr" -H "Authorization: Bearer $tok" "https://ghcr.io$next" |
      jq -r --arg v "$VERSION" '[.tags[] | select(test("^" + $v + "-cpu-b[0-9]+$"))] | first // empty')
    next=$(sed -nE 's/^[Ll]ink: <([^>]*)>.*/\1/p' "$hdr"); rm -f "$hdr"
  done
  echo "ghcr.io/$REPO:${tag:?no $VERSION-cpu image}"
}
