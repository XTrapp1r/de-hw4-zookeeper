#!/usr/bin/env bash
set -u
source "$(dirname "$0")/lib.sh"
[ "$LOGFILE" != /dev/null ] && : > "$LOGFILE"
: "${RUN:?}" "${FENCING:?}"
PAUSE_AT_MAPS="${PAUSE_AT_MAPS:-14}"
PAUSE_SEC="${PAUSE_SEC:-40}"
ARGS="--chunk-mb 1 --workers 2 --slow 1.0"
export RUN FENCING SESSION_TIMEOUT=10

note "Подготовка: чистое дерево /hw4/skills в ZooKeeper, пустые work/$RUN и logs/$RUN"
run "rm -rf work/$RUN logs/$RUN results/$RUN && mkdir -p logs/$RUN && $PY src/zkctl.py reset"
run "aws s3 rm s3://hh-lake/results/zk/$RUN/ --recursive --quiet; echo 'старые результаты прогона $RUN в S3 удалены'"

note "Запуск двух копий координатора: A, затем B (FENCING=$FENCING, тайм-аут сессии 10 с, $ARGS)"
COORD_ID=A nohup "$PY" src/coordinator.py $ARGS > /dev/null 2> logs/$RUN/coord-A.stderr &
PA=$!
wait_line logs/$RUN/coord-A.log "СТАЛ ЛИДЕРОМ" 30 || { say "A не стал лидером"; exit 1; }
COORD_ID=B nohup "$PY" src/coordinator.py $ARGS > /dev/null 2> logs/$RUN/coord-B.stderr &
PB=$!
wait_line logs/$RUN/coord-B.log "жду исчезновения" 30 || { say "B не встал в очередь"; exit 1; }
say "A: pid $PA, B: pid $PB"
run "grep -E 'кандидат|СТАЛ ЛИДЕРОМ|запасной' logs/$RUN/coord-A.log logs/$RUN/coord-B.log"
run "$PY src/zkctl.py state"
run "grep -h 'Session created' logs/$RUN/coord-A.log logs/$RUN/coord-B.log"
run "zkcons"

note "Ждём, пока A сделает $PAUSE_AT_MAPS map-задач из 46, и приостанавливаем его (kill -STOP)"
run "$PY src/zkctl.py wait-ckpt $PAUSE_AT_MAPS"
T_PAUSE=$(ts); kill -STOP $PA
say "kill -STOP $PA - лидер A приостановлен (T_PAUSE=$T_PAUSE)"
echo "$T_PAUSE" > logs/$RUN/pause_time.txt
run "ps -o pid,stat,command -p $PA | cut -c1-90"

note "Ждём завершения запасного B (A всё это время стоит)"
wait $PB; RCB=$?
say "B завершился, код выхода $RCB"
run "$PY src/zkctl.py state"
run "$PY src/zkctl.py leader-change $T_PAUSE logs/$RUN/coord-B.log"
while [ "$("$PY" -c "from datetime import datetime as d; t=d.combine(d.today(), d.strptime('$T_PAUSE','%H:%M:%S.%f').time()); print(int((d.now()-t).total_seconds()))")" -lt "$PAUSE_SEC" ]; do sleep 0.5; done

note "Возобновляем A (kill -CONT) после ~$PAUSE_SEC с паузы"
say "kill -CONT $PA"; kill -CONT $PA
wait $PA; RCA=$?
say "A завершился, код выхода $RCA"

note "Журналы координаторов (полностью - logs/$RUN/coord-A.log и coord-B.log)"
run "grep -v -E 'map-[0-9]+ attempt|reduce-[0-9] attempt|kazoo: Connecting' logs/$RUN/coord-A.log"
run "grep -v -E 'map-[0-9]+ attempt|reduce-[0-9] attempt|kazoo: Connecting' logs/$RUN/coord-B.log"

note "Итог: кто записал результат, версия указателя, сверка с ДЗ-3"
run "grep -h -E 'записал результат|ОТКЛОНЕНА|BadVersion' logs/$RUN/coord-*.log | sort"
run "zkcli 127.0.0.1:2181 get -s /hw4/skills/result | grep -E '^\\{|dataVersion|mtime|mzxid'"
run "zkcli 127.0.0.1:2182 get -s /hw4/skills/epoch | grep -E '^[A-Z]$|dataVersion'"
run "$PY src/zkctl.py state"
run "$PY src/zkctl.py leader-change $T_PAUSE logs/$RUN/coord-B.log"
run "$PY src/zkctl.py compare $RUN"
find logs/$RUN -name "*.stderr" -empty -delete
