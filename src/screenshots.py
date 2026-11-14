import html
import re
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "screenshots"
LOGS = ROOT / "logs"

HL = [
    (r"Mode: leader|is_leader\":true|СТАЛ ЛИДЕРОМ|кворум есть|СОВПАДАЕТ с ДЗ-3|побайтно совпали", "#7ee787"),
    (r"Mode: follower", "#79c0ff"),
    (r"Error contacting service|not currently serving|НЕТ КВОРУМА|ConnectionLoss|ОТКЛОНЕНА|BadVersionError"
     r"|kill -STOP|исчез через|does not exist", "#ff7b72"),
    (r"записал результат|dataVersion = \d|СМЕНА ЛИДЕРА|kill -CONT|ephemeralOwner|сработал watch|LOST", "#f2cc60"),
]


def short(line):
    return (line.replace(str(ROOT / ".venv/bin/python"), "python").replace(str(ROOT) + "/", "")
            .replace(str(ROOT), "."))


def blocks(log):
    text = (LOGS / log).read_text(encoding="utf-8")
    return [b.strip("\n") for b in re.split(r"\n(?=\$ |# ===== )", "\n" + text) if b.startswith("$ ")]


def block(log, needle, nth=0, keep=None):
    found = [b for b in blocks(log) if needle in b.splitlines()[0]]
    lines = [short(ln) for ln in found[nth].splitlines()]
    if keep:
        lines = [lines[0]] + [ln for ln in lines[1:] if re.search(keep, ln)]
    return lines


def stamped(log, rx):
    return [short(ln) for ln in (LOGS / log).read_text(encoding="utf-8").splitlines() if re.match(rx, ln)]


def paint(line):
    s = html.escape(line)
    if line.startswith("$ "):
        return f'<span style="color:#d2a8ff">$</span> <b>{s[2:]}</b>'
    for rx, color in HL:
        s = re.sub(rx, lambda m: f'<span style="color:{color};font-weight:600">{m.group(0)}</span>', s)
    return s


def terminal(pg, name, title, *parts, width=1200):
    lines = [ln for p in parts for ln in p + [""]][:-1]
    body = "\n".join(paint(ln) for ln in lines)
    pg.set_viewport_size({"width": width, "height": 400})
    pg.set_content(f"""<html><body style="margin:0;background:#fff">
<div id="w" style="display:inline-block;margin:10px;border-radius:9px;overflow:hidden;
 box-shadow:0 4px 18px rgba(0,0,0,.35);background:#0d1117;font-family:Menlo,monospace">
 <div style="background:#2d333b;padding:7px 12px;color:#adbac7;font:12px -apple-system,sans-serif">
  <span style="color:#ff5f57">&#9679;</span> <span style="color:#febc2e">&#9679;</span>
  <span style="color:#28c840">&#9679;</span>&nbsp;&nbsp; hw4 — {html.escape(title)}</div>
 <pre style="margin:0;padding:12px 14px;color:#e6edf3;font-size:12.5px;line-height:1.38;
  white-space:pre-wrap;max-width:{width - 50}px">{body}</pre></div></body></html>""")
    pg.locator("#w").screenshot(path=str(OUT / f"{name}.png"))
    print(f"screenshots/{name}.png")


def live(pg):
    pg.set_viewport_size({"width": 300, "height": 150})
    for i in (1, 2, 3):
        pg.goto(f"http://localhost:808{i}/commands/leader")
        pg.wait_for_timeout(500)
        pg.screenshot(path=str(OUT / f"admin_zk{i}.png"))
        print(f"screenshots/admin_zk{i}.png")


def logs(pg):
    d = "05_docker.log"
    terminal(pg, "t4_docker", "Docker: compose.yaml, zoo1..zoo3",
             block(d, "docker compose ps --format 'table {{.Name}}\\t{{.Image}}"),
             block(d, "zkServer.sh status 2>&1; done", keep="---|Mode"),
             block(d, "docker compose stop", 0, keep="Stopped"), block(d, "dstatus", 0),
             block(d, "docker compose stop", 1, keep="Stopped"), block(d, "dstatus", 1), block(d, "zkctl.py probe", 1),
             block(d, "05_docker_nofencing/coord"), block(d, "05_docker_nofencing.log"),
             block(d, "05_docker_fencing/coord"), block(d, "05_docker_fencing.log"), width=1500)
    key = (r"кандидат|СТАЛ ЛИДЕРОМ|запасной|сработал|чекпоинт /|к запуску|состояние сессии|КОНТРОЛЬ"
           r"|записал результат|ОТКЛОНЕНА")
    for run, title in (("03_nofencing", "без защиты, FENCING=0"), ("04_fencing", "FENCING=1")):
        log = f"{run}.log"
        a = stamped(f"{run}/coord-A.log", ".")
        b = stamped(f"{run}/coord-B.log", ".")
        marks = stamped(log, r"\d\d:\d\d:\d\d\.\d+ (kill|B завершился|A завершился)")
        merged = sorted([ln for ln in a + b if re.search(key, ln)] + marks)
        merged = [ln.replace("(эфемерный+последовательный, ", "(").replace(" (версия /hw4/skills/epoch)", "")
                  .replace(" (сделали: ", "\n             (сделали: ") for ln in merged]
        merged = [x for ln in merged for x in ln.split("\n")]
        terminal(pg, f"t_{run}_timeline", title + ": журналы A и B по времени", merged, width=1500)


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        pg = browser.new_page(device_scale_factor=2, locale="ru-RU")
        logs(pg)
        live(pg)
        browser.close()
