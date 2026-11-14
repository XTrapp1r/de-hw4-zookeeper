ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source "$ROOT/infra/env.sh"
PY="$ROOT/.venv/bin/python"
LOGFILE="${LOGFILE:-/dev/null}"

run() {
  printf '\n$ %s\n' "$*" | tee -a "$LOGFILE"
  ( eval "$*" ) 2>&1 | tee -a "$LOGFILE"
  local rc=${PIPESTATUS[0]}
  [ "$rc" -ne 0 ] && printf '[exit code %s]\n' "$rc" | tee -a "$LOGFILE"
  return 0
}
note() { printf '\n# ===== %s =====\n' "$*" | tee -a "$LOGFILE"; }
say()  { printf '%s %s\n' "$(ts)" "$*" | tee -a "$LOGFILE"; }
ts()   { "$PY" -c 'from datetime import datetime; print(datetime.now().strftime("%H:%M:%S.%f")[:-3])'; }
zkcli() { local s="$1"; shift; "$ROOT/zk/zk1/bin/zkCli.sh" -server "$s" "$@" 2>&1 | grep -v -E '^(Connecting to|WATCHER::|WatchedEvent state:SyncConnected|$)|Exiting JVM'; return 0; }
wait_line() { local f="$1" pat="$2" lim="${3:-60}" i=0; until grep -q "$pat" "$f" 2>/dev/null; do sleep 0.2; i=$((i+1)); [ $i -ge $((lim*5)) ] && return 1; done; return 0; }
zkstatus() { for i in 1 2 3; do printf 'zk%s (порт 218%s): ' "$i" "$i"; "$ROOT/zk/zk$i/bin/zkServer.sh" status 2>&1 | grep -E 'Mode:|Error' || echo '?'; done; }
zkleader() { for i in 1 2 3; do "$ROOT/zk/zk$i/bin/zkServer.sh" status 2>/dev/null | grep -q 'Mode: leader' && echo "$i"; done; }
with_timeout() { local t="$1"; shift; perl -e 'alarm shift; exec @ARGV' "$t" "$@"; }
zkcons() { for p in 2181 2182 2183; do echo cons | nc localhost $p | grep -o 'sid=0x[0-9a-f]*\|to=[0-9]*' | paste - - | sed "s/^/сервер :$p  /"; done; }
