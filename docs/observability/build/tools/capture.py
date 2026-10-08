"""Capture Grafana panel screenshots for the observability guide (headless Edge)."""

import json
import pathlib
import subprocess
import sys


def find_browser() -> str:
    """A Chromium-based browser for headless screenshots, on any platform.

    Set DOC_BROWSER to the executable to choose one yourself.
    """
    import os
    import shutil

    if os.environ.get("DOC_BROWSER"):
        return os.environ["DOC_BROWSER"]
    candidates = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for name in (
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "microsoft-edge",
        "msedge",
    ):
        found = shutil.which(name)
        if found:
            candidates.append(found)
    for candidate in candidates:
        if pathlib.Path(candidate).exists():
            return candidate
    raise SystemExit("no Chrome, Chromium or Edge found; set DOC_BROWSER to its path")


EDGE = find_browser()
OUT = pathlib.Path(sys.argv[1])
OUT.mkdir(parents=True, exist_ok=True)
G = "http://localhost:3000"

SCENARIOS = {
    "s1": (1791058905000, 1791059370000, [2, 3, 7, 27, 22, 30, 29]),
    "s2": (1791156420000, 1791157080000, [2, 3, 7, 27, 35, 34, 22]),
    "s3": (1791066720000, 1791067140000, [2, 3, 29, 8, 10, 7]),
    "s4": (1791154290000, 1791154770000, [2, 3, 7, 27, 15, 22, 16, 19, 29, 17]),
    "s5": (1791157590000, 1791158670000, [39, 38, 46, 47, 49, 2, 3]),
}
REFERENCE_WINDOW = SCENARIOS["s5"][:2]


def shot(url: str, path: pathlib.Path, width: int, height: int, budget: int = 25000) -> bool:
    if path.exists() and path.stat().st_size > 5000:
        print("skip " + path.name, flush=True)
        return True
    subprocess.run(
        [
            EDGE,
            "--headless=new",
            "--disable-gpu",
            "--hide-scrollbars",
            f"--window-size={width},{height}",
            f"--virtual-time-budget={budget}",
            f"--screenshot={path}",
            url,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=120,
    )
    ok = path.exists() and path.stat().st_size > 5000
    print(("ok   " if ok else "FAIL ") + path.name, flush=True)
    return ok


def panel(pid: int, frm: int, to: int, path: pathlib.Path) -> bool:
    url = f"{G}/d-solo/hub-performance/x?orgId=1&panelId={pid}&from={frm}&to={to}&theme=light"
    return shot(url, path, 1000, 640)


manifest: dict[str, object] = {}
for name, (frm, to, panels) in SCENARIOS.items():
    for i, pid in enumerate(panels, start=1):
        p = OUT / f"{name}_{i:02d}_p{pid}.png"
        panel(pid, frm, to, p)
        manifest[p.name] = {"scenario": name, "panel": pid, "from": frm, "to": to}

# Scenario 1's amplifier is on dashboard 2: "Transaction commit time per stage".
# Its panels have no stored ids, so Grafana numbers them in order (8 here).
s1_from, s1_to = SCENARIOS["s1"][:2]
shot(
    f"{G}/d-solo/hub-pipeline/x?orgId=1&panelId=8&from={s1_from}&to={s1_to}&theme=light",
    OUT / "s1_08_txn.png",
    1000,
    860,
)

frm, to = REFERENCE_WINDOW
dash = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
for p_ in dash["panels"]:
    if p_["type"] == "row":
        continue
    p = OUT / f"ref_p{p_['id']:02d}.png"
    panel(p_["id"], frm, to, p)
    manifest[p.name] = {"reference": p_["title"], "panel": p_["id"]}

for uid, fname in (("hub-run-overview", "dash01.png"), ("hub-pipeline", "dash02.png")):
    shot(
        f"{G}/d/{uid}/x?orgId=1&from={frm}&to={to}&theme=light&kiosk",
        OUT / fname,
        1600,
        2000,
        35000,
    )
shot("http://localhost:9090/targets", OUT / "prom_targets.png", 1400, 1600, 15000)

(OUT / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
print("done")
