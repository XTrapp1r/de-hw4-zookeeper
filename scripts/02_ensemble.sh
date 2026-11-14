#!/usr/bin/env bash
LOGFILE="$(cd "$(dirname "$0")/.." && pwd)/logs/02_ensemble.log"
source "$(dirname "$0")/lib.sh"
: > "$LOGFILE"
for i in 1 2 3; do zk/zk$i/bin/zkServer.sh stop > /dev/null 2>&1; done

note "Этап 1. Java 17 (JAVA_HOME) и ZooKeeper 3.9.6, распакованный в три копии zk/zk1..zk3"
run 'echo "JAVA_HOME=$JAVA_HOME"; java -version'
run "cat downloads/apache-zookeeper-3.9.6-bin.tar.gz.sha512; shasum -a 512 downloads/apache-zookeeper-3.9.6-bin.tar.gz"
run "ls zk/ zk/zk1/ zk/zk1/bin/ | head -20"
run "zk/zk1/bin/zkServer.sh version 2>&1 | tail -1"

note "Этап 2. Конфигурации: строки server.N одинаковые, clientPort / admin.serverPort / dataDir у каждого свои"
run "cat zk/zk1/conf/zoo.cfg"
run "diff zk/zk1/conf/zoo.cfg zk/zk2/conf/zoo.cfg; diff zk/zk1/conf/zoo.cfg zk/zk3/conf/zoo.cfg; true"
run 'for i in 1 2 3; do echo "data/zk$i/myid = $(cat data/zk$i/myid)"; done'

note "Запуск трёх серверов штатной командой zkServer.sh start"
run 'for i in 1 2 3; do zk/zk$i/bin/zkServer.sh start; done'
sleep 5
run 'for i in 1 2 3; do zk/zk$i/bin/zkServer.sh status; done'
run 'for p in 2181 2182 2183; do echo "--- echo srvr | nc localhost $p"; echo srvr | nc localhost $p; done'
run 'for i in 1 2 3; do echo "zk$i: $(curl -s localhost:808$i/commands/leader | tr -d "\n ")"; done'
run 'for i in 1 2 3; do pid=$(pgrep -f "zk/zk$i/bin/../conf/zoo.cfg" | head -1); echo "zk$i (pid $pid) слушает: $(lsof -nP -iTCP -sTCP:LISTEN -a -p $pid | awk "NR>1 {print \$9}" | sort | tr "\n" " ")"; done'

zkcli 127.0.0.1:2181 deleteall /hw4-check > /dev/null
note "Запись через zk1, чтение через zk2 и zk3 - данные реплицированы"
run "zkcli 127.0.0.1:2181 create /hw4-check 'записано через zk1, все 3 сервера'"
run "zkcli 127.0.0.1:2182 get /hw4-check; zkcli 127.0.0.1:2183 get /hw4-check"

L1=$(zkleader)
note "Остановка одного сервера - лидера zk$L1. Осталось 2 из 3, кворум 2 есть: выбирается новый лидер, запись работает"
run "zk/zk$L1/bin/zkServer.sh stop"
sleep 4
run zkstatus
LIVE=$(for i in 1 2 3; do [ "$i" != "$L1" ] && echo $i; done | tr '\n' ' ')
set -- $LIVE
run "zkcli 127.0.0.1:218$1 set /hw4-check 'изменено через zk$1, когда работают 2 из 3'; zkcli 127.0.0.1:218$2 get -s /hw4-check | grep -E 'изменено|dataVersion'"
run "$PY src/zkctl.py probe"

L2=$(zkleader)
LEFT=$(for i in 1 2 3; do [ "$i" != "$L1" ] && [ "$i" != "$L2" ] && echo $i; done)
note "Остановка второго сервера - нового лидера zk$L2. Остался один zk$LEFT из 3: кворума нет, ансамбль не обслуживает клиентов"
run "zk/zk$L2/bin/zkServer.sh stop"
sleep 4
run zkstatus
run "pgrep -fl 'zk/zk$LEFT/' | grep -o 'QuorumPeerMain.*' ; echo \"процесс zk$LEFT жив: \$(pgrep -f 'zk/zk$LEFT/' | head -1)\""
run "echo ruok | nc localhost 218$LEFT; echo; echo srvr | nc localhost 218$LEFT"
run "$PY src/zkctl.py probe"
run "with_timeout 20 zk/zk1/bin/zkCli.sh -server 127.0.0.1:218$LEFT get /hw4-check 2>&1 | grep -v -E '^(WATCHER::|$)' | tail -4"

note "Запуск обоих остановленных серверов: кворум восстановлен, данные на месте"
run "zk/zk$L1/bin/zkServer.sh start; zk/zk$L2/bin/zkServer.sh start"
sleep 6
run zkstatus
run "zkcli $ZK_HOSTS get -s /hw4-check | grep -E 'изменено|dataVersion'"
run "$PY src/zkctl.py probe"

note "Эфемерный узел: zkCli во «втором окне» создаёт узел -e, первое окно его видит"
FIFO="$ROOT/work/window2.fifo"; mkdir -p work; rm -f "$FIFO"; mkfifo "$FIFO"
W2LOG="$ROOT/logs/02_window2.log"; : > "$W2LOG"
( zk/zk2/bin/zkCli.sh -server 127.0.0.1:2182 < "$FIFO" >> "$W2LOG" 2>&1 & )
exec 3> "$FIFO"
sleep 3
for c in 'create -e /hw4-ephemeral "живу, пока открыто окно 2"' 'ls /' 'get -s /hw4-ephemeral'; do echo "$c" >&3; sleep 1; done
say "окно 2 (zkCli -server 127.0.0.1:2182, его вывод - logs/02_window2.log):"
run "grep -v -E '^(WATCHER::|WatchedEvent|Connecting|Welcome|JLine|$)' logs/02_window2.log | sed 's/\\[zk: /\\n[zk: /g' | grep -v '^$'"
run "zkcli 127.0.0.1:2181 get -s /hw4-ephemeral | grep -E 'живу|ephemeralOwner'"
run "zkcli 127.0.0.1:2183 ls /"
run 'for p in 2181 2182 2183; do echo dump | nc localhost $p | grep -A3 "Sessions with Ephemerals"; done'

note "Закрываем окно 2 (SIGHUP процессу zkCli, как при закрытии окна терминала) и опрашиваем узел из окна 1"
T0=$(date +%s); say "закрываю окно 2"
pkill -HUP -f "ZooKeeperMain -server 127.0.0.1:2182"; exec 3>&-
sleep 1; run "pgrep -f ZooKeeperMain > /dev/null && echo 'zkCli окна 2 ещё жив' || echo 'процесса zkCli окна 2 больше нет'"
for n in $(seq 1 60); do
  if zkcli 127.0.0.1:2181 stat /hw4-ephemeral | grep -q 'does not exist'; then
    say "узел /hw4-ephemeral исчез через $(( $(date +%s) - T0 )) с после закрытия окна (сессия zkCli 30 с истекла)"; break
  fi
  say "через $(( $(date +%s) - T0 )) с: /hw4-ephemeral ещё есть (сессия окна 2 ещё не истекла)"; sleep 4
done
run "zkcli 127.0.0.1:2181 ls /; zkcli 127.0.0.1:2181 stat /hw4-ephemeral"

note "Для сравнения: окно закрыто штатно командой quit - сессия закрывается сразу, узел исчезает сразу"
rm -f "$FIFO"; mkfifo "$FIFO"
( zk/zk2/bin/zkCli.sh -server 127.0.0.1:2183 < "$FIFO" >> "$W2LOG" 2>&1 & )
exec 3> "$FIFO"; sleep 3
echo 'create -e /hw4-ephemeral2 "второй эфемерный"' >&3; sleep 1
run "zkcli 127.0.0.1:2181 get -s /hw4-ephemeral2 | grep -E 'второй|ephemeralOwner'"
T0=$(date +%s); echo quit >&3; exec 3>&-; sleep 1
run "zkcli 127.0.0.1:2181 stat /hw4-ephemeral2"
rm -f "$FIFO"
