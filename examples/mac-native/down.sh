#!/bin/sh
# Stops everything up.sh started. Logs stay in $RUN.
cd "$(dirname "$0")"
RUN=${RUN:-$PWD/.run}
for f in "$RUN"/*.pid; do
	[ -e "$f" ] || continue
	kill "$(cat "$f")" 2>/dev/null
	rm -f "$f"
done
