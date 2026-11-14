#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
infra/stand.sh status | grep -q "запущен, pid" || infra/stand.sh up
bash scripts/02_ensemble.sh
bash scripts/03_single.sh
bash scripts/03_nofencing.sh
bash scripts/04_fencing.sh
[ "${DOCKER:-0}" = 1 ] && bash scripts/05_docker.sh
mkdir -p logs/zk-servers && for i in 1 2 3; do cp zk/zk$i/logs/zookeeper-*-server-*.out logs/zk-servers/zk$i.out; done
echo "Готово: logs/"
