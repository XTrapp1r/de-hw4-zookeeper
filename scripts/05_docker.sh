#!/usr/bin/env bash
LOGFILE="$(cd "$(dirname "$0")/.." && pwd)/logs/05_docker.log"
source "$(dirname "$0")/lib.sh"
: > "$LOGFILE"
dstatus() { for i in 1 2 3; do printf 'zoo%s (порт 218%s): ' "$i" "$i"; docker exec zoo$i zkServer.sh status 2>&1 | grep -E 'Mode:|Error' || echo 'контейнер остановлен'; done; }
dleader() { for i in 1 2 3; do docker exec zoo$i zkServer.sh status 2>/dev/null | grep -q 'Mode: leader' && echo "$i"; done; }

note "Локальный ансамбль останавливаем: его порты 2181-2183 и 8081-8083 займут контейнеры"
run "scripts/zk.sh stop"

note "Образ: официальный zookeeper:3.9.6-jre-17 (Docker Hub), compose.yaml"
run "docker image ls zookeeper; docker image inspect zookeeper:3.9.6-jre-17 --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E 'JAVA_VERSION|ZOO_(CONF|DATA)_DIR'"
run "docker run --rm --entrypoint java zookeeper:3.9.6-jre-17 -version"
run "grep -v '^#' compose.yaml | grep -v '^$'"

note "Запуск трёх контейнеров: docker compose up -d"
docker compose down -v --remove-orphans > /dev/null 2>&1
run "docker compose up -d 2>&1"
sleep 8
run "docker compose ps --format 'table {{.Name}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'"
run "docker exec zoo1 cat /conf/zoo.cfg"
run 'for i in 1 2 3; do echo "zoo$i: /data/myid = $(docker exec zoo$i cat /data/myid)"; done'
run 'for i in 1 2 3; do echo "--- docker exec zoo$i zkServer.sh status"; docker exec zoo$i zkServer.sh status 2>&1; done'
run 'for i in 1 2 3; do echo "zoo$i: $(curl -s localhost:808$i/commands/leader | tr -d "\n ")"; done'

note "Запись через zoo1 (localhost:2181), чтение через zoo2 и zoo3"
run "zkcli 127.0.0.1:2181 create /hw4-docker 'записано через zoo1'; zkcli 127.0.0.1:2182 get /hw4-docker; zkcli 127.0.0.1:2183 get /hw4-docker"

L1=$(dleader)
note "Останавливаем один контейнер - лидера zoo$L1: 2 из 3, кворум есть"
run "docker compose stop zoo$L1 2>&1"
sleep 4
run dstatus
set -- $(for i in 1 2 3; do [ "$i" != "$L1" ] && echo $i; done)
run "zkcli 127.0.0.1:218$1 set /hw4-docker 'изменено через zoo$1, когда работают 2 из 3'; zkcli 127.0.0.1:218$2 get -s /hw4-docker | grep -E 'изменено|dataVersion'"
run "$PY src/zkctl.py probe"

L2=$(dleader)
LEFT=$(for i in 1 2 3; do [ "$i" != "$L1" ] && [ "$i" != "$L2" ] && echo $i; done)
note "Останавливаем второй - нового лидера zoo$L2: остался zoo$LEFT, кворума нет"
run "docker compose stop zoo$L2 2>&1"
sleep 4
run dstatus
run "docker compose ps --format 'table {{.Name}}\t{{.Status}}'"
run "echo ruok | nc localhost 218$LEFT; echo; echo srvr | nc localhost 218$LEFT"
run "$PY src/zkctl.py probe"
run "with_timeout 20 zk/zk1/bin/zkCli.sh -server 127.0.0.1:218$LEFT get /hw4-docker 2>&1 | grep -v -E '^(WATCHER::|$)' | tail -3"

note "Запускаем оба контейнера обратно"
run "docker compose start zoo$L1 zoo$L2 2>&1"
sleep 8
run dstatus
run "zkcli $ZK_HOSTS get -s /hw4-docker | grep -E 'изменено|dataVersion'"
run "$PY src/zkctl.py probe"

note "Эфемерный узел: zkCli во «втором окне» (клиент на хосте, сервер zoo2), закрываем окно"
FIFO="$ROOT/work/window2.fifo"; mkdir -p work; rm -f "$FIFO"; mkfifo "$FIFO"
W2LOG="$ROOT/logs/05_docker_window2.log"; : > "$W2LOG"
( zk/zk2/bin/zkCli.sh -server 127.0.0.1:2182 < "$FIFO" >> "$W2LOG" 2>&1 & )
exec 3> "$FIFO"; sleep 3
echo 'create -e /hw4-docker-ephemeral "живу, пока открыто окно 2"' >&3; sleep 1
run "grep -o 'Created .*' logs/05_docker_window2.log"
run "zkcli 127.0.0.1:2181 get -s /hw4-docker-ephemeral | grep -E 'живу|ephemeralOwner'"
T0=$(date +%s); say "закрываю окно 2"
pkill -HUP -f "ZooKeeperMain -server 127.0.0.1:2182"; exec 3>&-
for n in $(seq 1 30); do
  if zkcli 127.0.0.1:2181 stat /hw4-docker-ephemeral | grep -q 'does not exist'; then
    say "узел /hw4-docker-ephemeral исчез через $(( $(date +%s) - T0 )) с после закрытия окна"; break
  fi
  say "через $(( $(date +%s) - T0 )) с: узел ещё есть"; sleep 2
done
rm -f "$FIFO"
run "zkcli 127.0.0.1:2181 ls /"

note "Этапы 3-4 на Docker-ансамбле: те же два сценария scripts/failover.sh"
FENCING=0 RUN=05_docker_nofencing LOGFILE="$ROOT/logs/05_docker_nofencing.log" bash scripts/failover.sh > /dev/null 2>&1
FENCING=1 RUN=05_docker_fencing LOGFILE="$ROOT/logs/05_docker_fencing.log" bash scripts/failover.sh > /dev/null 2>&1
run "grep -h -E 'СТАЛ ЛИДЕРОМ|чекпоинт /.*сделали|записал результат|ОТКЛОНЕНА' logs/05_docker_nofencing/coord-*.log | sort"
run "grep -h -E '^/hw4/skills/(epoch|result): (эпоха|version)' logs/05_docker_nofencing.log | tail -2 | cut -c1-140; grep -h -E 'СМЕНА ЛИДЕРА|ИТОГ' logs/05_docker_nofencing.log | sort -u"
run "grep -h -E 'СТАЛ ЛИДЕРОМ|чекпоинт /.*сделали|записал результат|ОТКЛОНЕНА' logs/05_docker_fencing/coord-*.log | sort"
run "grep -h -E '^/hw4/skills/(epoch|result): (эпоха|version)' logs/05_docker_fencing.log | tail -2 | cut -c1-140; grep -h -E 'СМЕНА ЛИДЕРА|ИТОГ' logs/05_docker_fencing.log | sort -u"

note "Останавливаем контейнеры и возвращаем локальный ансамбль"
run "docker compose down 2>&1"
run "scripts/zk.sh start"
sleep 5
run "scripts/zk.sh status"
