import multiprocessing as mp
import multiprocessing.connection
import os
import shutil
import signal
import time
import traceback
import zlib
from collections import Counter, deque
from dataclasses import dataclass, field

TAIL = 8 * 1024


def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))


def _get_range(s3, bucket, key, first, last, stream, stats):
    body = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={first}-{last}")["Body"]
    stats["get_requests"] += 1
    if stream:
        for block in body.iter_chunks(1 << 18):
            stats["bytes_read"] += len(block)
            yield block
    else:
        block = body.read()
        stats["bytes_read"] += len(block)
        yield block


def iter_split_lines(s3, bucket, key, start, end, size, stream, stats):
    first = start - 1 if start > 0 else 0

    def blocks():
        yield from _get_range(s3, bucket, key, first, end - 1, stream, stats)
        pos = end
        while pos < size:
            stats["tail_requests"] += 1
            yield from _get_range(s3, bucket, key, pos, min(pos + TAIL, size) - 1, stream, stats)
            pos += TAIL

    skip = True
    base = first
    buf = b""
    for block in blocks():
        buf = buf + block if buf else block
        p = 0
        while (i := buf.find(b"\n", p)) >= 0:
            line_start = base + p
            line = buf[p:i]
            p = i + 1
            if skip:
                skip = False
                continue
            if line_start >= end:
                return
            yield line.rstrip(b"\r")
        base += p
        buf = buf[p:]
        if base >= end:
            return
        if skip and base + len(buf) >= end:
            return
    if buf and not skip and base < end:
        yield buf.rstrip(b"\r")


@dataclass
class Task:
    kind: str
    idx: int
    payload: dict
    attempt: int = 1

    @property
    def name(self):
        return f"{self.kind}-{self.idx}"


def partition(key: str, n: int) -> int:
    return zlib.crc32(key.encode()) % n


def _write_partitions(attempt_dir, records, n_reducers, sort):
    os.makedirs(attempt_dir)
    parts = [[] for _ in range(n_reducers)]
    for k, v in records:
        parts[partition(k, n_reducers)].append((k, v))
    out_records = out_bytes = 0
    for r, recs in enumerate(parts):
        if sort:
            recs.sort()
        data = "".join(f"{k}\t{v}\n" for k, v in recs).encode()
        with open(os.path.join(attempt_dir, f"part-r{r:02d}.tsv"), "wb") as f:
            f.write(data)
        out_records += len(recs)
        out_bytes += len(data)
    return out_records, out_bytes


def _commit(attempt_dir, final_dir):
    if os.path.exists(final_dir):
        shutil.rmtree(attempt_dir)
    else:
        os.rename(attempt_dir, final_dir)


def _maybe_fail(task, fail, progress):
    spec = fail.get(task.name)
    if not spec or task.attempt > spec[1] or progress < 0.5:
        return
    if spec[0] == "kill":
        os.kill(os.getpid(), signal.SIGKILL)
    raise RuntimeError(f"injected failure in {task.name} attempt {task.attempt}")


def run_map(task, job, conf, s3, stats):
    p = task.payload
    lines = iter_split_lines(s3, conf["bucket"], conf["key"], p["start"], p["end"], conf["size"],
                             stream=conf["stream"], stats=stats)
    if conf["materialize"]:
        lines = list(lines)
    approx_total = max(1, (p["end"] - p["start"]) // 700)
    counters = Counter()

    def records():
        for n, line in enumerate(lines):
            if n % 256 == 0:
                _maybe_fail(task, conf["fail"], n / approx_total)
            yield from job.map(line, counters)

    if conf["combiner"]:
        combined = Counter()
        for k, v in records():
            combined[k] += v
            counters["map_emitted"] += 1
        out = combined.items()
    else:
        out = []
        for kv in records():
            out.append(kv)
            counters["map_emitted"] += 1
    if conf.get("slow"):
        time.sleep(conf["slow"])
    attempt_dir = os.path.join(conf["workdir"], f"map-{task.idx:04d}.attempt{task.attempt}")
    n_out, n_bytes = _write_partitions(attempt_dir, out, conf["reducers"], sort=not conf["combiner"])
    _commit(attempt_dir, os.path.join(conf["workdir"], f"map-{task.idx:04d}"))
    counters["map_output_records"] += n_out
    counters["shuffle_bytes"] += n_bytes
    return counters


def run_reduce(task, job, conf):
    r = task.idx
    acc = Counter()
    for i, m in enumerate(task.payload["maps"]):
        _maybe_fail(task, conf["fail"], i / max(1, len(task.payload["maps"])))
        with open(os.path.join(conf["workdir"], m, f"part-r{r:02d}.tsv"), encoding="utf-8") as f:
            for line in f:
                k, v = line.rstrip("\n").rsplit("\t", 1)
                acc[k] += int(v)
    attempt_dir = os.path.join(conf["workdir"], f"reduce-{r:02d}.attempt{task.attempt}")
    os.makedirs(attempt_dir)
    with open(os.path.join(attempt_dir, "part.tsv"), "w", encoding="utf-8") as f:
        for k, v in job.reduce(acc):
            f.write(f"{k}\t{v}\n")
    _commit(attempt_dir, os.path.join(conf["workdir"], f"reduce-{r:02d}"))
    return Counter()


def _worker(job_factory, conf, conn):
    job = job_factory(conf)
    s3 = s3_client()
    conn.send(("ready", os.getpid()))
    while True:
        task = conn.recv()
        if task is None:
            return
        t0 = time.perf_counter()
        stats = Counter()
        try:
            if task.kind == "map":
                c = run_map(task, job, conf, s3, stats)
            else:
                c = run_reduce(task, job, conf)
            c.update(stats)
            conn.send(("ok", dict(c), time.perf_counter() - t0))
        except Exception:
            conn.send(("error", traceback.format_exc(limit=2), time.perf_counter() - t0))


@dataclass
class Master:
    job_factory: object
    conf: dict
    workers: int
    max_attempts: int = 4
    log: object = print
    t0: float = field(default_factory=time.perf_counter)
    counters: Counter = field(default_factory=Counter)
    task_log: list = field(default_factory=list)

    def say(self, msg):
        self.log(f"[{time.perf_counter() - self.t0:6.2f}s] {msg}")

    def start(self):
        self.ctx = mp.get_context(self.conf.get("start_method", "spawn"))
        self.procs, self.conns, self.pids, self.starting = {}, {}, {}, set()
        for wid in range(self.workers):
            self._spawn(wid)
        for wid in range(self.workers):
            self._ready(wid)

    def _spawn(self, wid):
        parent, child = self.ctx.Pipe()
        p = self.ctx.Process(target=_worker, args=(self.job_factory, self.conf, child), daemon=True)
        p.start()
        child.close()
        self.procs[wid], self.conns[wid] = p, parent

    def _ready(self, wid):
        kind, pid = self.conns[wid].recv()
        assert kind == "ready"
        self.pids[wid] = pid

    def stop(self):
        for wid, p in self.procs.items():
            if wid in self.starting:
                p.terminate()
            elif p.is_alive():
                self.conns[wid].send(None)
        for p in self.procs.values():
            p.join(timeout=5)

    def _retry(self, pending, task, why):
        self.counters[f"failed_{task.kind}_attempts"] += 1
        if task.attempt >= self.max_attempts:
            raise RuntimeError(f"{task.name}: {why}; исчерпаны все {self.max_attempts} попытки, задание провалено")
        self.say(f"{task.name} attempt {task.attempt} FAILED ({why}) -> повтор, attempt {task.attempt + 1}")
        pending.appendleft(Task(task.kind, task.idx, task.payload, task.attempt + 1))

    def run_phase(self, tasks, verbose=True, on_done=None):
        pending = deque(tasks)
        inflight = {}
        starting = self.starting
        idle = deque(w for w in self.procs if w not in starting)
        done = {}
        while pending or inflight:
            while pending and idle:
                wid = idle.popleft()
                inflight[wid] = task = pending.popleft()
                self.conns[wid].send(task)
            watch = set(inflight) | starting
            by_obj = {self.conns[w]: w for w in watch} | {self.procs[w].sentinel: w for w in watch}
            for obj in mp.connection.wait(list(by_obj), timeout=1.0):
                wid = by_obj[obj]
                if self.conns[wid] is not obj and self.procs[wid].sentinel is not obj:
                    continue
                msg = None
                if obj is self.conns[wid]:
                    try:
                        msg = self.conns[wid].recv()
                    except (EOFError, ConnectionResetError):
                        msg = None
                elif self.conns[wid].poll():
                    continue
                if msg is not None and msg[0] == "ready":
                    self.pids[wid] = msg[1]
                    starting.discard(wid)
                    idle.append(wid)
                    self.say(f"вместо него запущен воркер pid {msg[1]}")
                    continue
                if msg is None:
                    task = inflight.pop(wid, None)
                    starting.discard(wid)
                    self.procs[wid].join()
                    why = f"воркер pid {self.pids.get(wid)} умер, exitcode {self.procs[wid].exitcode}"
                    if task:
                        self._retry(pending, task, why)
                    self._spawn(wid)
                    starting.add(wid)
                    self.counters["workers_restarted"] += 1
                    continue
                task = inflight.pop(wid)
                idle.append(wid)
                kind, info, dt = msg
                if kind == "ok":
                    done[task.name] = info
                    self.counters.update(info)
                    self.task_log.append((task.name, task.attempt, self.pids[wid], round(dt, 3)))
                    if verbose:
                        self.say(f"{task.name} attempt {task.attempt} ok, pid {self.pids[wid]}, {dt:.2f}s")
                    if on_done:
                        on_done(task, info, dt)
                else:
                    self._retry(pending, task, info.strip().splitlines()[-1])
        return done
