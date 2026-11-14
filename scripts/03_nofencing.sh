#!/usr/bin/env bash
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FENCING=0 RUN=03_nofencing LOGFILE="$ROOT/logs/03_nofencing.log" exec bash "$ROOT/scripts/failover.sh"
