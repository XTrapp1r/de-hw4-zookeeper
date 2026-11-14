import re
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOGS, GEN = ROOT / "logs", Path(__file__).with_name("gen")
GEN.mkdir(exist_ok=True)
for f in GEN.glob("*.tex"):
    f.unlink()


def short(ln):
    return (ln.replace(str(ROOT / ".venv/bin/python"), "python").replace(str(ROOT) + "/", "")
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


def verb(name, *parts):
    body = "\n".join(ln for p in parts for ln in p).replace("\t", "    ")
    (GEN / f"{name}.tex").write_text("\\begin{Verbatim}\n" + body + "\n\\end{Verbatim}\n", encoding="utf-8")


e = "02_ensemble.log"
verb("install", block(e, "java -version"), block(e, "zkServer.sh version"))
verb("config", block(e, "cat zk/zk1/conf/zoo.cfg"), block(e, "diff zk/zk1", keep="^[<>]"), block(e, "myid"))
verb("roles", block(e, "zkServer.sh status; done", keep="Mode"), block(e, "commands/leader"))
verb("quorum",
     block(e, "zkServer.sh stop", 0, keep="STOPPED"), block(e, "zkstatus", 0), block(e, " set /hw4-check"),
     block(e, "zkctl.py probe", 0),
     block(e, "zkServer.sh stop", 1, keep="STOPPED"), block(e, "zkstatus", 1), block(e, "echo ruok"),
     block(e, "zkctl.py probe", 1), block(e, "with_timeout 20", keep="ConnectionLoss"),
     block(e, "bin/zkServer.sh start; zk/zk", keep="STARTED"), block(e, "zkstatus", 2), block(e, "get -s /hw4-check", 1))
close = stamped(e, r"\d\d:\d\d:\d\d\.\d+ (закрываю|через|узел)")
verb("ephemeral",
     block(e, "logs/02_window2.log", keep=r"Created|ephemeralOwner"), block(e, "echo dump", keep=r"Sessions|0x|/hw4")[:4],
     close[:1] + ["..."] + close[-2:], block(e, "get -s /hw4-ephemeral2"), block(e, "stat /hw4-ephemeral2"))
real = (LOGS / "02_window2_terminal.log").read_text(encoding="utf-8").splitlines()
verb("ephemeral_real", [ln for ln in real if re.search(r"Created|ephemeralOwner =|закрыто$|исчез", ln)])

verb("single", block("03_single.log", "coordinator.py --slow 0", keep="КОНТРОЛЬ"),
     block("03_single.log", "zkctl.py compare", keep="ИТОГ"))
verb("timeout", block("03_nofencing.log", "Session created"), block("03_nofencing.log", "zkcons"))

nums = {}
for run, tag in (("03_nofencing", "Nf"), ("04_fencing", "F"), ("05_docker_nofencing", "DNf"), ("05_docker_fencing", "DF")):
    log = f"{run}.log"
    verb(f"{tag}_result", block(log, "grep -h -E 'записал результат"),
         block(log, "get -s /hw4/skills/result", keep="dataVersion|coord"), block(log, "zkctl.py compare", keep="СОВПАДАЕТ|ИТОГ"))
    lc = " ".join(block(log, "leader-change", 0))
    nums[tag] = dict(pause=re.search(r"пауза лидера ([\d:.]+)", lc).group(1),
                     watch=re.search(r"\(\+([\d.]+) с\) -> запасной", lc).group(1).replace(".", ","),
                     lead=re.search(r"СМЕНА ЛИДЕРА: ([\d.]+) с", lc).group(1).replace(".", ","))

d = "05_docker.log"
verb("docker", block(d, "--entrypoint java", keep="openjdk version"),
     block(d, "zkServer.sh status 2>&1; done", keep="---|Mode"))

with open(GEN / "numbers.tex", "w", encoding="utf-8") as f:
    for tag, v in nums.items():
        for k, x in v.items():
            f.write(f"\\newcommand{{\\{tag}{k.capitalize()}}}{{{x}}}\n")
    gone = [re.search(r"через (\d+) с", t).group(1) for t in close if "исчез" in t]
    real_gone = [re.search(r"через (\d+) с", t).group(1) for t in real if "исчез" in t]
    f.write(f"\\newcommand{{\\EphGone}}{{{gone[0]}}}\n\\newcommand{{\\EphReal}}{{{real_gone[0]}}}\n")

if shutil.which("tectonic"):
    subprocess.run(["tectonic", "-X", "compile", "main.tex"], cwd=Path(__file__).parent, check=True)
    shutil.copy(Path(__file__).with_name("main.pdf"), Path(__file__).with_name("Aprelkov_HW4_report.pdf"))
    print("report/Aprelkov_HW4_report.pdf")
