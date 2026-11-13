"""Isolated-window cost runs. Each option runs ALONE on its own warehouse, starting from a stopped warehouse,
3 pipeline runs back to back, then waits until the warehouse auto-stops, so billing for that window is
attributable to this work (including start-up and the idle tail). Records UTC windows in
results/isolated-windows.json; bench.reconcile reads billing/metering later (lags by hours).
Usage: python -m bench.isolated [--scale 10] [--options D2,S1]"""
import os, sys, json, time, subprocess, datetime, yaml
from dotenv import load_dotenv
from .engines import Snowflake
from .pipeline import opt, OPTIONS

def utcnow(): return datetime.datetime.now(datetime.timezone.utc).isoformat()

def warehouse_stopped(option):
    if option == "D2":
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
        return str(w.warehouses.get(os.environ["DATABRICKS_HTTP_PATH"].rsplit("/", 1)[1]).state).endswith("STOPPED")
    cfg = yaml.safe_load(open("config.yaml"))["snowflake"]
    e = Snowflake(cfg); cur = e.conn.cursor(); cur.execute(f"SHOW WAREHOUSES LIKE '{OPTIONS[option][1]}'")
    state = cur.fetchone()[1]; e.close()
    return state == "SUSPENDED"

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    scale = opt("--scale") or "10"
    options = (opt("--options") or "D2,S1").split(",")
    path = "results/isolated-windows.json"
    windows = json.load(open(path)) if os.path.exists(path) else []
    for o in options:
        assert warehouse_stopped(o), f"{o}: warehouse is not stopped; refusing to start (would not be isolated)"
        start = utcnow()
        proc = subprocess.run([sys.executable, "-m", "bench.pipeline", "--option", o, "--scale", scale, "--runs", "3", "--cold"],
                              capture_output=True, text=True)
        files = sorted(__import__("glob").glob(f"results/pipeline-{o}-sf{scale}-*.jsonl"))
        t0 = time.time()
        while not warehouse_stopped(o) and time.time() - t0 < 900: time.sleep(15)
        end = utcnow()
        windows.append({"option": o, "scale": scale, "start_utc": start, "end_utc": end, "exit": proc.returncode,
                        "results_file": [f for f in files if not f.endswith("enriched.jsonl")][-1] if files else None,
                        "stopped_cleanly": warehouse_stopped(o)})
        json.dump(windows, open(path, "w"), indent=2)
        print(o, "window", start, "->", end, "exit", proc.returncode, flush=True)

if __name__ == "__main__": main()
