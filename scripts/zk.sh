#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
case "${1:-status}" in
  start|stop) for i in 1 2 3; do zk/zk$i/bin/zkServer.sh "$1" 2>&1 | tail -1; done ;;
  status) zkstatus ;;
  *) echo "usage: $0 start|stop|status"; exit 1 ;;
esac
