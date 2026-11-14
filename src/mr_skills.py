import argparse
import csv
import io
import json
import os
import re
import shutil
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from itertools import combinations
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mrlib import Master, Task, s3_client

ROOT = Path(__file__).resolve().parent.parent
BUCKET = "hh-lake"
DATA_KEY = "raw/hh/all_vacancies.csv"
SKILLS_KEY = "ref/skills.csv"
OUT_FILES = {1: "skills_k1.csv", 2: "pairs_k2.csv", 3: "triples_k3.csv"}

CPP = re.compile(r"[cс] ?\+\+|(^|[^a-z])cpp([^a-z]|$)")
C_TOKEN = re.compile(r"(^|[ (/,])c($|[ )/,|])|(^|[ (/,])с($|[)/,|]| [(/|])|(^|[^а-яё])си($|[^а-яё])")
DEV = re.compile(r"программист|програмист|разработчик|developer|programmer|engineer|инженер|software|dev|lead|лид|"
                 r"эксперт|architect|архитектор")
C_FALSE = re.compile(r"чпу|станк|метролог|категори|1 [cс]|[cс] #|[cс] \.net|[cс] опыт|видео с$")
NOT_DEV = re.compile(r"преподав|учител|наставник|писател|автор |(^|[^а-яё])аналитик|документооборот|(^|[^а-яё])водител")


def is_variant(title: str) -> bool:
    if NOT_DEV.search(title):
        return False
    if CPP.search(title):
        return True
    return bool(C_TOKEN.search(title) and DEV.search(title) and not C_FALSE.search(title))


def parse_line(line: bytes) -> list[str]:
    return next(csv.reader([line.decode("utf-8")], delimiter=";", quotechar='"'))


class SkillsJob:
    def __init__(self, conf):
        self.skills = [(name, re.compile(pat)) for name, pat in conf["skills"]]
        self.kmax = conf["kmax"]
        self.lazy = conf["lazy_parse"]

    def map(self, line: bytes, c: Counter):
        c["records"] += 1
        if self.lazy and not line.startswith(b'"'):
            if not is_variant(line[: line.index(b";")].decode("utf-8")):
                return
            fields = parse_line(line)
        else:
            fields = parse_line(line)
            if not is_variant(fields[0]):
                return
        c["variant_vacancies"] += 1
        req = fields[4]
        skills = sorted({name for name, rx in self.skills if rx.search(req)})
        c[f"skills_per_vacancy={len(skills):02d}"] += 1
        for k in range(1, self.kmax + 1):
            for combo in combinations(skills, k):
                c[f"emitted_k{k}"] += 1
                yield f"{k}|" + "|".join(combo), 1

    def reduce(self, acc: Counter):
        yield from acc.items()


def parse_fail(specs):
    out = {}
    for s in specs or []:
        parts = s.split(":")
        out[parts[0]] = (parts[1], int(parts[2]) if len(parts) > 2 else 1)
    return out


def write_results(rows_by_k, kmax):
    out = {}
    for k in range(1, kmax + 1):
        rows = sorted(rows_by_k.get(k, []), key=lambda r: (-r[-1], r[:-1]))
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow((["skill"] if k == 1 else [f"skill_{i}" for i in range(1, k + 1)]) + ["vacancies"])
        w.writerows(rows)
        out[k] = buf.getvalue()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--impl", choices=["v1", "v2"], default="v2")
    ap.add_argument("--chunk-mb", type=float, default=4)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--kmax", type=int, default=3, choices=[1, 2, 3])
    ap.add_argument("--fail", action="append", help="инъекция отказа: map-3:kill | map-7:error[:попыток]")
    ap.add_argument("--out", default="results/mapreduce", help="префикс в бакете и папка в репозитории")
    ap.add_argument("--quiet", action="store_true", help="без построчного лога задач")
    ap.add_argument("--keep-work", action="store_true")
    a = ap.parse_args()

    t_start = time.perf_counter()
    if a.impl == "v2":
        os.environ.setdefault("NO_PROXY", "*")
    s3 = s3_client()
    size = s3.head_object(Bucket=BUCKET, Key=DATA_KEY)["ContentLength"]
    skills_csv = s3.get_object(Bucket=BUCKET, Key=SKILLS_KEY)["Body"].read().decode("utf-8")
    skills = [tuple(l.split(";", 1)) for l in skills_csv.splitlines()[1:] if l]

    chunk = int(a.chunk_mb * 1024 * 1024)
    splits = [(s, min(s + chunk, size)) for s in range(0, size, chunk)]
    workdir = ROOT / "work" / f"mr-{os.getpid()}"
    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True)
    conf = dict(bucket=BUCKET, key=DATA_KEY, size=size, skills=skills, kmax=a.kmax, reducers=a.reducers,
                workdir=str(workdir), fail=parse_fail(a.fail),
                stream=a.impl == "v2", materialize=a.impl == "v1", combiner=a.impl == "v2", lazy_parse=a.impl == "v2",
                start_method="fork" if a.impl == "v2" else "spawn")
    print(f"вход s3://{BUCKET}/{DATA_KEY}: {size} байт; словарь: {len(skills)} навыков; impl={a.impl}")
    print(f"сплитов M={len(splits)} по {a.chunk_mb:g} МБ, редьюсеров R={a.reducers}, воркеров W={a.workers}"
          + (f", отказы: {conf['fail']}" if conf["fail"] else ""))

    m = Master(SkillsJob, conf, a.workers)
    t0 = time.perf_counter()
    m.start()
    t_workers = time.perf_counter() - t0
    m.say(f"запущено воркеров: {a.workers}, pid {sorted(m.pids.values())}")

    t0 = time.perf_counter()
    maps = m.run_phase([Task("map", i, {"start": s, "end": e}) for i, (s, e) in enumerate(splits)], verbose=not a.quiet)
    t_map = time.perf_counter() - t0
    map_dirs = sorted(f"map-{i:04d}" for i in range(len(splits)))
    assert len(maps) == len(splits) and all((workdir / d).is_dir() for d in map_dirs)

    t0 = time.perf_counter()
    m.run_phase([Task("reduce", r, {"maps": map_dirs}) for r in range(a.reducers)], verbose=not a.quiet)
    t_reduce = time.perf_counter() - t0
    m.stop()

    t0 = time.perf_counter()
    rows_by_k = {}
    for r in range(a.reducers):
        for line in (workdir / f"reduce-{r:02d}" / "part.tsv").read_text(encoding="utf-8").splitlines():
            key, v = line.rsplit("\t", 1)
            k, *names = key.split("|")
            rows_by_k.setdefault(int(k), []).append((*names, int(v)))
    texts = write_results(rows_by_k, a.kmax)
    local = ROOT / a.out
    local.mkdir(parents=True, exist_ok=True)
    for k, text in texts.items():
        (local / OUT_FILES[k]).write_text(text, encoding="utf-8")
    put = lambda k: s3.put_object(Bucket=BUCKET, Key=f"{a.out}/{OUT_FILES[k]}", Body=texts[k].encode(), ContentType="text/csv")
    if a.impl == "v2":
        with ThreadPoolExecutor(len(texts)) as ex:
            list(ex.map(put, texts))
    else:
        for k in texts:
            put(k)
    t_out = time.perf_counter() - t0

    c = m.counters
    hist = {int(k.split("=")[1]): v for k, v in sorted(c.items()) if k.startswith("skills_per_vacancy=")}
    summary = {
        "impl": a.impl, "chunk_mb": a.chunk_mb, "splits": len(splits), "workers": a.workers, "reducers": a.reducers,
        "object_bytes": size, "bytes_read": c["bytes_read"], "get_requests": c["get_requests"],
        "tail_requests": c["tail_requests"], "records": c["records"], "variant_vacancies": c["variant_vacancies"],
        "skills_per_vacancy": hist,
        "emitted": {k: c[f"emitted_k{k}"] for k in range(1, a.kmax + 1)},
        "map_emitted": c["map_emitted"], "map_output_records": c["map_output_records"], "shuffle_bytes": c["shuffle_bytes"],
        "distinct": {k: len(rows_by_k.get(k, [])) for k in range(1, a.kmax + 1)},
        "total": {k: sum(r[-1] for r in rows_by_k.get(k, [])) for k in range(1, a.kmax + 1)},
        "failed_map_attempts": c["failed_map_attempts"], "failed_reduce_attempts": c["failed_reduce_attempts"],
        "workers_restarted": c["workers_restarted"],
        "time_s": {"workers_start": round(t_workers, 3), "map": round(t_map, 3), "reduce": round(t_reduce, 3),
                   "output": round(t_out, 3), "total": round(time.perf_counter() - t_start, 3)},
        "tasks": [{"task": n, "attempt": at, "pid": pid, "sec": dt} for n, at, pid, dt in m.task_log],
    }
    s3.put_object(Bucket=BUCKET, Key=f"{a.out}/_manifest.json", Body=json.dumps(summary, ensure_ascii=False, indent=1).encode())
    s3.put_object(Bucket=BUCKET, Key=f"{a.out}/_SUCCESS", Body=b"")
    if not a.keep_work:
        shutil.rmtree(workdir)

    print(f"\nсчётчики: прочитано {summary['bytes_read']} байт за {summary['get_requests']} GET "
          f"(из них дочитываний строки за границей чанка: {summary['tail_requests']}), объект {size} байт")
    print(f"  строк-записей: {summary['records']}, вакансий варианта: {summary['variant_vacancies']}, "
          f"из них с навыками: {summary['variant_vacancies'] - hist.get(0, 0)}")
    print(f"  навыков в вакансии -> число вакансий: {hist}")
    print(f"  map: выдано пар (ключ,1) {summary['map_emitted']}, записей в shuffle {summary['map_output_records']} "
          f"({summary['shuffle_bytes']} байт)")
    print(f"  отказов: map {summary['failed_map_attempts']}, reduce {summary['failed_reduce_attempts']}, "
          f"перезапущено воркеров {summary['workers_restarted']}")
    print(f"{'k':>2} {'сочетаний выдано':>17} {'разных ключей':>14} {'сумма частот':>13}")
    for k in range(1, a.kmax + 1):
        print(f"{k:>2} {summary['emitted'][k]:>17} {summary['distinct'][k]:>14} {summary['total'][k]:>13}")
    t = summary["time_s"]
    print(f"время, с: старт воркеров {t['workers_start']}, map {t['map']}, reduce {t['reduce']}, "
          f"выгрузка {t['output']}, всего {t['total']}")
    print(f"результат: s3://{BUCKET}/{a.out}/{{{','.join(OUT_FILES[k] for k in range(1, a.kmax + 1))}}}")
    print("BENCH " + json.dumps({k: summary[k] for k in ("impl", "chunk_mb", "workers", "time_s", "variant_vacancies")}))


if __name__ == "__main__":
    main()
