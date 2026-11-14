#!/usr/bin/env bash
LOGFILE="$(cd "$(dirname "$0")/.." && pwd)/logs/03_single.log"
source "$(dirname "$0")/lib.sh"
: > "$LOGFILE"
RUN=03_single
note "Один координатор без отказов: те же 46 сплитов по 1 МБ, без искусственной задержки"
run "rm -rf work/$RUN logs/$RUN results/$RUN && $PY src/zkctl.py reset"
run "aws s3 rm s3://hh-lake/results/zk/$RUN/ --recursive --quiet; true"
run "RUN=$RUN COORD_ID=A $PY src/coordinator.py --slow 0 | grep -v -E 'map-[0-9]+ attempt|kazoo: Connecting'"
run "$PY src/zkctl.py state"
run "$PY src/zkctl.py compare $RUN"
