#!/usr/bin/env bash
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FENCING=1 RUN=04_fencing LOGFILE="$ROOT/logs/04_fencing.log" exec bash "$ROOT/scripts/failover.sh"
