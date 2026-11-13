"""Isolated cost window: one option, one workload, N back-to-back runs, billed hours attributable to it.
Starts at hh:01 UTC of the next clock hour (so nothing earlier in that hour can be on the warehouse), requires the
warehouse to be stopped, runs the workload N times, waits for auto-stop, and records the window in
results/isolated2-windows.json. bench.isolated_reconcile reads billing for those hours later (billing lags).
Usage: python -m bench.isolated_cost --workload di|cdc|pipeline --option D2|S1|S2 --scale 10|100 [--repeats 3] [--run-timeout-min N] [--yes] [--print]
Do NOT touch the option's warehouse from anywhere else while a window is open (including probes and enrich of other runs)."""
import os, sys, json, time, glob, subprocess, datetime, yaml
from dotenv import load_dotenv
from .engines import Snowflake
from .pipeline import OPTIONS, opt

CMD = {"di": lambda o, sc: [sys.executable, "-m", "bench.di_etl", "--option", o, "--sf", sc],
       "cdc": lambda o, sc: [sys.executable, "-m", "bench.cdc", "--option", o, "--scale", sc],
       "pipeline": lambda o, sc: [sys.executable, "-m", "bench.pipeline", "--option", o, "--scale", sc, "--runs", "1"]}
PATTERN = {"di": "results/di-{o}-sf{sc}-*.jsonl", "cdc": "results/cdc-{o}-sf{sc}-*.jsonl", "pipeline": "results/pipeline-{o}-sf{sc}-*.jsonl"}

def now(): return datetime.datetime.now(datetime.timezone.utc)

def stopped(option):
    if option == "D2":
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
        return str(w.warehouses.get(os.environ["DATABRICKS_HTTP_PATH"].rsplit("/", 1)[1]).state).endswith("STOPPED")
    cfg = yaml.safe_load(open("config.yaml"))["snowflake"]; e = Snowflake(cfg); cur = e.conn.cursor()
    cur.execute(f"SHOW WAREHOUSES LIKE '{OPTIONS[option][1]}'"); st = cur.fetchone()[1]; e.close(); return st == "SUSPENDED"

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    wl, o, sc = opt("--workload"), opt("--option"), opt("--scale") or "10"
    reps = int(opt("--repeats") or 3)
    if wl not in CMD or o not in ("D2", "S1", "S2"): sys.exit(__doc__)
    if float(sc) >= 100 and "--yes" not in sys.argv: sys.exit("Scale 100 spends real money; re-run with --yes.")
    t = now(); start_at = (t + datetime.timedelta(hours=1)).replace(minute=1, second=0, microsecond=0) if t.minute >= 1 else t.replace(minute=1, second=0, microsecond=0)
    if "--print" in sys.argv:
        print(f"{wl} {o} sf{sc} x{reps}: would start at {start_at.isoformat()} UTC"); return
    print(f"{o} {wl} sf{sc}: waiting until {start_at.isoformat()} UTC", flush=True)
    while now() < start_at: time.sleep(5)
    assert stopped(o), f"{o}: warehouse not stopped at window start; not isolated, refusing"
    start = now().isoformat(); files, codes, walls, timed_out = [], [], [], False
    limit = int(opt("--run-timeout-min") or {("di", "10"): 20, ("cdc", "10"): 20, ("pipeline", "10"): 10}.get((wl, sc), 240)) * 60
    for i in range(1, reps + 1):
        before = set(glob.glob(PATTERN[wl].format(o=o, sc=sc))); t_run = time.time()
        try:
            p = subprocess.run(CMD[wl](o, sc) + (["--yes"] if float(sc) >= 100 else []), capture_output=True, text=True, timeout=limit)
            codes.append(p.returncode)
        except subprocess.TimeoutExpired:   # a stalled run must not silently stretch the window: stop and flag it
            codes.append(-9); timed_out = True; walls.append(round((time.time() - t_run) / 60, 1)); print(f"{o} run {i}/{reps} TIMED OUT after {limit // 60} min; aborting window", flush=True); break
        walls.append(round((time.time() - t_run) / 60, 1))
        new = sorted(set(glob.glob(PATTERN[wl].format(o=o, sc=sc))) - before)
        files += [f for f in new if "enriched" not in f]
        print(f"{o} run {i}/{reps} exit {p.returncode} -> {new[-1] if new else None}", flush=True)
    t0 = time.time()
    while not stopped(o) and time.time() - t0 < 900: time.sleep(10)
    end = now().isoformat(); path = "results/isolated2-windows.json"
    wins = json.load(open(path)) if os.path.exists(path) else []
    med = sorted(walls)[len(walls) // 2] if walls else 0
    reasons = [r for r, c in (("a run exited non-zero or timed out", any(c != 0 for c in codes) or timed_out),
                              ("a run took more than 2x the fastest run (stall)", bool(walls) and max(walls) > 2 * max(min(walls), 0.5) and max(walls) > 3)) if c]
    wins.append({"workload": wl, "option": o, "scale": sc, "repeats": reps, "start_utc": start, "end_utc": end, "exit_codes": codes,
                 "run_wall_min": walls, "valid": not reasons, "invalid_reason": "; ".join(reasons) or None,
                 "result_files": files, "stopped_cleanly": stopped(o)})
    json.dump(wins, open(path, "w"), indent=2)
    print(f"{o} window {start} -> {end} exits {codes} walls {walls} valid={not reasons}", flush=True)

if __name__ == "__main__": main()
