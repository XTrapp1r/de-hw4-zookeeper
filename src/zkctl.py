import hashlib
import json
import logging
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mrlib import s3_client

HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
JOB = "/hw4/skills"
EPOCH, RESULT, CKPT, ELECTION = (f"{JOB}/{n}" for n in ("epoch", "result", "checkpoint", "election"))
BUCKET = "hh-lake"
HW3_PREFIX = "results/mapreduce"
FILES = ["skills_k1.csv", "pairs_k2.csv", "triples_k3.csv"]


def connect(hosts=HOSTS, timeout=10):
    zk = KazooClient(hosts=hosts, timeout=10)
    zk.start(timeout=timeout)
    return zk


def reset():
    zk = connect()
    zk.delete(JOB, recursive=True) if zk.exists(JOB) else None
    for p in (ELECTION, CKPT, EPOCH, RESULT):
        zk.ensure_path(p)
    for p in (EPOCH, RESULT):
        st = zk.exists(p)
        print(f"{p}: создан, version {st.version}")
    zk.stop()


def wait_ckpt(n, limit=120):
    zk = connect()
    t0 = time.time()
    while time.time() - t0 < limit:
        maps = [c for c in zk.get_children(CKPT) if c.startswith("map-")]
        if len(maps) >= n:
            print(f"в чекпоинте {len(maps)} map-задач (ждали >= {n})")
            zk.stop()
            return
        time.sleep(0.2)
    sys.exit(f"за {limit} с в чекпоинте не набралось {n} map-задач")


def state():
    zk = connect()
    data, st = zk.get(EPOCH)
    print(f"{EPOCH}: эпоха (version) {st.version}, последний лидер {data.decode() or '-'}")
    data, st = zk.get(RESULT)
    print(f"{RESULT}: version {st.version}, mzxid 0x{st.mzxid:x}, данные: {data.decode() or '(пусто)'}")
    kids = sorted(zk.get_children(ELECTION))
    cands = []
    for c in kids:
        try:
            d, s = zk.get(f"{ELECTION}/{c}")
            cands.append(f"{c} ({d.decode()}, сессия 0x{s.ephemeralOwner:x})")
        except NoNodeError:
            pass
    print(f"{ELECTION}: {', '.join(cands) if cands else 'кандидатов нет'}")
    by = defaultdict(Counter)
    for c in zk.get_children(CKPT):
        info = json.loads(zk.get(f"{CKPT}/{c}")[0])
        by[f"coord-{info['by']} (эпоха {info['epoch']})"][c.split("-")[0]] += 1
    print(f"{CKPT}: " + "; ".join(f"{k}: map {v['map']}, reduce {v['reduce']}" for k, v in sorted(by.items())))
    zk.stop()


def probe(hosts):
    class Count(logging.Handler):
        n = 0

        def emit(self, rec):
            Count.n += "Connection dropped" in rec.getMessage()

    klog = logging.getLogger("kazoo")
    klog.addHandler(Count())
    klog.propagate = False
    t0 = time.time()
    try:
        zk = connect(hosts, timeout=5)
    except Exception as e:
        print(f"НЕТ КВОРУМА: за {time.time() - t0:.1f} с не удалось открыть сессию ({type(e).__name__}); "
              f"попыток соединения, оборванных серверами: {Count.n} - ансамбль не обслуживает клиентов")
        return 1
    path = "/hw4-probe"
    val = datetime.now().isoformat(timespec="seconds").encode()
    zk.ensure_path(path)
    st = zk.set(path, val)
    back = zk.get(path)[0]
    peer = zk._connection._socket.getpeername()
    print(f"кворум есть: сессия 0x{zk.client_id[0]:x} на {peer[0]}:{peer[1]}, "
          f"setData {path}={val.decode()} -> version {st.version}, прочитано {back.decode()}")
    zk.stop()
    return 0


def _hms(s):
    return datetime.combine(datetime.today(), datetime.strptime(s, "%H:%M:%S.%f").time())


def leader_change(pause, logfile):
    t_pause = _hms(pause)
    lines = Path(logfile).read_text(encoding="utf-8").splitlines()
    marks = {}
    for line in lines:
        ts = line.split(" ", 1)[0]
        if "сработал watch" in line and "watch" not in marks:
            marks["watch"] = _hms(ts)
        if "СТАЛ ЛИДЕРОМ" in line and _hms(ts) > t_pause:
            marks["leader"] = _hms(ts)
            break
    w = (marks["watch"] - t_pause).total_seconds()
    led = (marks["leader"] - t_pause).total_seconds()
    print(f"пауза лидера {pause} -> watch у запасного {marks['watch'].strftime('%H:%M:%S.%f')[:-3]} (+{w:.2f} с) "
          f"-> запасной СТАЛ ЛИДЕРОМ {marks['leader'].strftime('%H:%M:%S.%f')[:-3]} (+{led:.2f} с)")
    print(f"СМЕНА ЛИДЕРА: {led:.2f} с от SIGSTOP лидера до нового лидера")


def _get(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def _summary(s3, prefix):
    man = json.loads(_get(s3, f"{prefix}/_manifest.json"))
    out = {"variant_vacancies": man["variant_vacancies"]}
    for f in FILES:
        text = _get(s3, f"{prefix}/{f}")
        rows = text.decode().splitlines()[1:]
        out[f] = (len(rows), sum(int(r.rsplit(",", 1)[1]) for r in rows), hashlib.sha256(text).hexdigest())
    return out


def compare(run):
    s3 = s3_client()
    ref = _summary(s3, HW3_PREFIX)
    print(f"ДЗ-3 s3://{BUCKET}/{HW3_PREFIX}/: вакансий варианта {ref['variant_vacancies']}; "
          + "; ".join(f"{f}: {ref[f][0]} строк, сумма {ref[f][1]}, sha256 {ref[f][2][:16]}" for f in FILES))
    zk = connect()
    ptr = zk.get(RESULT)[0]
    zk.stop()
    current = json.loads(ptr)["result"] if ptr else None
    prefixes = sorted({o["Key"].rsplit("/", 1)[0] for o in s3.list_objects_v2(
        Bucket=BUCKET, Prefix=f"results/zk/{run}/").get("Contents", []) if o["Key"].endswith("_manifest.json")})
    ok_all = True
    for p in prefixes:
        got = _summary(s3, p)
        same = all(got[f] == ref[f] for f in FILES) and got["variant_vacancies"] == ref["variant_vacancies"]
        ok_all &= same
        mark = "  <- на него указывает /hw4/skills/result" if current == f"s3://{BUCKET}/{p}/" else ""
        print(f"{p}/: вакансий {got['variant_vacancies']}; "
              + "; ".join(f"{f}: {got[f][0]}/{got[f][1]}/{got[f][2][:16]}" for f in FILES)
              + f" -> {'СОВПАДАЕТ с ДЗ-3' if same else 'НЕ СОВПАДАЕТ'}{mark}")
    print("ИТОГ: " + ("все результаты прогона побайтно совпали с ДЗ-3" if ok_all and prefixes else "есть расхождения"))
    return 0 if ok_all and prefixes else 1


if __name__ == "__main__":
    cmd, *args = sys.argv[1:] or ["state"]
    rc = 0
    if cmd == "reset":
        reset()
    elif cmd == "wait-ckpt":
        wait_ckpt(int(args[0]))
    elif cmd == "state":
        state()
    elif cmd == "probe":
        rc = probe(args[0] if args else HOSTS)
    elif cmd == "leader-change":
        leader_change(*args)
    elif cmd == "compare":
        rc = compare(args[0])
    else:
        sys.exit("usage: zkctl.py reset | wait-ckpt N | state | probe [HOSTS] | leader-change T LOG | compare RUN")
    sys.exit(rc)
