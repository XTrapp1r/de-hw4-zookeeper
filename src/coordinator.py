import argparse
import hashlib
import json
import logging
import os
import socket
import sys
import threading
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

from mr_skills import BUCKET, DATA_KEY, OUT_FILES, SKILLS_KEY, SkillsJob, write_results
from mrlib import Master, Task, s3_client

ROOT = Path(__file__).resolve().parent.parent
JOB = "/hw4/skills"
ELECTION, EPOCH, CKPT, RESULT = (f"{JOB}/{n}" for n in ("election", "epoch", "checkpoint", "result"))


def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class Log:
    def __init__(self, path: Path, who: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(path, "a", encoding="utf-8")
        self.who = who
        self.lock = threading.Lock()

    def __call__(self, msg):
        line = f"{now()} [{self.who}] {msg}"
        with self.lock:
            print(line, flush=True)
            self.f.write(line + "\n")
            self.f.flush()


class KazooToLog(logging.Handler):

    def __init__(self, log):
        super().__init__(5)
        self.log = log

    def emit(self, rec):
        msg = rec.getMessage()
        if rec.levelno < logging.INFO and "Session created" not in msg:
            return
        if "session_passwd" in msg:
            head, _, tail = msg.partition("session_passwd")
            msg = head + tail[tail.find("negotiated"):] if "negotiated" in tail else head
        self.log("kazoo: " + " ".join(msg.split()))


def ckpt_name(task: Task) -> str:
    return f"map-{task.idx:04d}" if task.kind == "map" else f"reduce-{task.idx:02d}"


def main():
    ap = argparse.ArgumentParser(description="Координатор расчёта пар навыков под ZooKeeper")
    ap.add_argument("--chunk-mb", type=float, default=1, help="размер сплита, МБ")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--kmax", type=int, default=3, choices=[1, 2, 3])
    ap.add_argument("--slow", type=float, default=1.0,
                    help="задержка каждой map-задачи, с (без неё расчёт идёт ~1 с и лидера не приостановить посреди)")
    a = ap.parse_args()

    me = os.environ.get("COORD_ID", socket.gethostname())
    run = os.environ.get("RUN", "manual")
    fencing = os.environ.get("FENCING", "0") == "1"
    timeout = float(os.environ.get("SESSION_TIMEOUT", "10"))
    hosts = os.environ.get("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
    log = Log(ROOT / "logs" / run / f"coord-{me}.log", me)
    log(f"старт: pid {os.getpid()}, прогон {run}, FENCING={int(fencing)}, тайм-аут сессии {timeout:g} с, "
        f"ZooKeeper {hosts}")

    klog = logging.getLogger("kazoo")
    klog.setLevel(5)
    klog.addHandler(KazooToLog(log))
    klog.propagate = False
    zk = KazooClient(hosts=hosts, timeout=timeout)
    closing = threading.Event()
    zk.add_listener(lambda state: log(f"состояние сессии ZooKeeper: {state}"
                                      + (" (сессию закрыли мы сами)" if closing.is_set() else "")))
    zk.start(timeout=15)
    for p in (ELECTION, CKPT, EPOCH, RESULT):
        zk.ensure_path(p)

    node = zk.create(f"{ELECTION}/c_", me.encode(), ephemeral=True, sequence=True)
    mine = node.rsplit("/", 1)[1]
    log(f"кандидат {node} (эфемерный+последовательный, сессия 0x{zk.client_id[0]:x})")
    while True:
        kids = sorted(zk.get_children(ELECTION))
        pos = kids.index(mine)
        if pos == 0:
            break
        pred = kids[pos - 1]
        gone = threading.Event()
        try:
            owner = zk.get(f"{ELECTION}/{pred}", watch=lambda ev: gone.set())[0].decode()
        except NoNodeError:
            continue
        log(f"запасной: лидер сейчас {owner} (узел {pred}), жду исчезновения его узла (watch)")
        gone.wait()
        log(f"сработал watch: узел {pred} удалён -> пересчитываю очередь кандидатов")

    epoch = zk.set(EPOCH, me.encode()).version
    log(f"СТАЛ ЛИДЕРОМ: узел {mine} первый в очереди, эпоха {epoch} (версия {EPOCH})")

    s3 = s3_client()
    size = s3.head_object(Bucket=BUCKET, Key=DATA_KEY)["ContentLength"]
    skills_csv = s3.get_object(Bucket=BUCKET, Key=SKILLS_KEY)["Body"].read().decode("utf-8")
    skills = [tuple(line.split(";", 1)) for line in skills_csv.splitlines()[1:] if line]
    chunk = int(a.chunk_mb * 1024 * 1024)
    splits = [(s, min(s + chunk, size)) for s in range(0, size, chunk)]
    spec = dict(input=f"s3://{BUCKET}/{DATA_KEY}", size=size, chunk=chunk, maps=len(splits),
                reducers=a.reducers, kmax=a.kmax, skills=len(skills))
    raw = zk.get(JOB)[0]
    if not raw:
        zk.set(JOB, json.dumps(spec).encode())
    elif json.loads(raw) != spec:
        log(f"ОШИБКА: параметры задания в {JOB} {raw.decode()} не совпадают с моими {spec}")
        return 2
    workdir = ROOT / "work" / run
    workdir.mkdir(parents=True, exist_ok=True)

    done = {}
    for name in sorted(zk.get_children(CKPT)):
        info = json.loads(zk.get(f"{CKPT}/{name}")[0])
        if (workdir / name).is_dir():
            done[name] = info
        else:
            log(f"{name} отмечена в чекпоинте, но выхода work/{run}/{name} нет -> пересчитаю")
    n_map_done = sum(n.startswith("map-") for n in done)
    n_red_done = len(done) - n_map_done
    who = Counter(f"{v['by']} в эпохе {v['epoch']}" for v in done.values())
    log(f"чекпоинт {CKPT}: готово map {n_map_done} из {len(splits)}, reduce {n_red_done} из {a.reducers}"
        + (f" (сделали: {', '.join(f'{k} - {c}' for k, c in who.items())})" if who else ""))

    ctr = Counter()
    for v in done.values():
        ctr.update(v["counters"])

    already = []

    def mark(task, info, dt):
        ctr.update(info)
        name = ckpt_name(task)
        data = json.dumps({"by": me, "epoch": epoch, "sec": round(dt, 3), "counters": info}).encode()
        try:
            zk.retry(zk.create, f"{CKPT}/{name}", data)
        except NodeExistsError:
            if not already:
                log(f"{name}: уже отмечена в чекпоинте другим координатором (выход совпадает, мой отброшен)")
            already.append(name)

    def phase_note(kind):
        if already:
            log(f"{kind}: {len(already)} моих задач уже были отмечены в чекпоинте другим координатором: "
                f"{already[0]} .. {already[-1]}")
            already.clear()

    conf = dict(bucket=BUCKET, key=DATA_KEY, size=size, skills=skills, kmax=a.kmax, reducers=a.reducers,
                workdir=str(workdir), fail={}, stream=True, materialize=False, combiner=True, lazy_parse=True,
                start_method="spawn", slow=a.slow)
    m = Master(SkillsJob, conf, a.workers)
    m.say = log
    m.start()
    log(f"запущено воркеров: {a.workers}, pid {sorted(m.pids.values())}; сплитов {len(splits)} по {a.chunk_mb:g} МБ, "
        f"редьюсеров {a.reducers}, задержка map-задачи {a.slow:g} с")

    maps = [Task("map", i, {"start": s, "end": e}) for i, (s, e) in enumerate(splits) if f"map-{i:04d}" not in done]
    log(f"map: к запуску {len(maps)} задач" + (f", {n_map_done} пропускаю - они уже в чекпоинте" if n_map_done else ""))
    m.run_phase(maps, on_done=mark)
    phase_note("map")
    map_dirs = [f"map-{i:04d}" for i in range(len(splits))]
    missing = [d for d in map_dirs if not (workdir / d).is_dir()]
    if missing:
        log(f"ОШИБКА: нет выходов map {missing}")
        return 2
    reds = [Task("reduce", r, {"maps": map_dirs}) for r in range(a.reducers) if f"reduce-{r:02d}" not in done]
    log(f"reduce: к запуску {len(reds)} задач")
    m.run_phase(reds, on_done=mark)
    phase_note("reduce")
    m.stop()

    rows_by_k = {}
    for r in range(a.reducers):
        for line in (workdir / f"reduce-{r:02d}" / "part.tsv").read_text(encoding="utf-8").splitlines():
            key, v = line.rsplit("\t", 1)
            k, *names = key.split("|")
            rows_by_k.setdefault(int(k), []).append((*names, int(v)))
    texts = write_results(rows_by_k, a.kmax)
    distinct = {k: len(rows_by_k.get(k, [])) for k in range(1, a.kmax + 1)}
    total = {k: sum(r[-1] for r in rows_by_k.get(k, [])) for k in range(1, a.kmax + 1)}
    sha = {k: hashlib.sha256(t.encode()).hexdigest() for k, t in texts.items()}
    control = (f"вакансий варианта {ctr['variant_vacancies']}, навыков {distinct[1]} (сумма {total[1]}), "
               f"пар {distinct[2]} (сумма {total[2]}), троек {distinct[3]} (сумма {total[3]})")
    log(f"КОНТРОЛЬ: {control}; sha256 pairs_k2.csv {sha[2][:16]}")

    prefix = f"results/zk/{run}/coord-{me}-e{epoch}"
    local = ROOT / "results" / run / f"coord-{me}-e{epoch}"
    local.mkdir(parents=True, exist_ok=True)
    for k, text in texts.items():
        (local / OUT_FILES[k]).write_text(text, encoding="utf-8")
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/{OUT_FILES[k]}", Body=text.encode(), ContentType="text/csv")
    manifest = dict(by=me, epoch=epoch, spec=spec, control=control, distinct=distinct, total=total,
                    variant_vacancies=ctr["variant_vacancies"], records=ctr["records"],
                    sha256={OUT_FILES[k]: h for k, h in sha.items()})
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/_manifest.json",
                  Body=json.dumps(manifest, ensure_ascii=False, indent=1).encode())
    (local / "_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    log(f"CSV выгружены: s3://{BUCKET}/{prefix}/ (и копия {local.relative_to(ROOT)}/)")

    ptr = json.dumps({"by": f"coord-{me}", "epoch": epoch, "result": f"s3://{BUCKET}/{prefix}/",
                      "control": control, "at": datetime.now().isoformat(timespec="milliseconds")},
                     ensure_ascii=False).encode()
    if fencing:
        def fenced_set():
            tx = zk.transaction()
            tx.check(EPOCH, epoch)
            tx.set_data(RESULT, ptr)
            return tx.commit()

        res = zk.retry(fenced_set)
        if isinstance(res[0], Exception):
            who_now, st = zk.get(EPOCH)
            log(f"ЗАПИСЬ РЕЗУЛЬТАТА ОТКЛОНЕНА: {type(res[0]).__name__} в check({EPOCH}, version={epoch}) - "
                f"текущая эпоха {st.version} у {who_now.decode()}; указатель {RESULT} не изменён "
                f"(ответ multi: {[type(r).__name__ for r in res]})")
            closing.set()
            zk.stop()
            zk.close()
            return 3
        stat = res[1]
        how = f"multi: check эпохи {epoch} + setData"
    else:
        stat = zk.retry(zk.set, RESULT, ptr)
        how = "setData без проверки эпохи"
    log(f"записал результат: {RESULT} версия {stat.version} -> s3://{BUCKET}/{prefix}/ ({how})")
    closing.set()
    zk.stop()
    zk.close()
    log("готово, сессия закрыта")
    return 0


if __name__ == "__main__":
    sys.exit(main())
