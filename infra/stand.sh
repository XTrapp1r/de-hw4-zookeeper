#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
STAND="${STAND:-$(dirname "$HERE")}"
[ -x "$STAND/bin/rustfs" ] || { echo "не найден стенд ДЗ-2: $STAND/bin/rustfs (задайте STAND=...)"; exit 1; }

case "${1:-status}" in
  up)     "$STAND/scripts/disks.sh" attach && "$STAND/scripts/server.sh" start ;;
  down)   "$STAND/scripts/server.sh" stop && "$STAND/scripts/disks.sh" detach ;;
  status) "$STAND/scripts/disks.sh" status; "$STAND/scripts/server.sh" status ;;
  *) echo "usage: $0 up|down|status"; exit 1 ;;
esac
