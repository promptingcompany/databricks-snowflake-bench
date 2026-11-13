"""Phase 2 runner: batch transformation pipeline (see docs/PHASE2_BATCH_ETL_DESIGN.md).
Usage: python -m bench.pipeline --option D2|S1|S2 --scale 10|100 [--runs 3] [--cold] [--yes] [--print]
 D2 = Databricks serverless SQL warehouse (from .env)   S1 = Snowflake BENCH_M   S2 = Snowflake BENCH_L
 D1 = Databricks serverless Jobs compute: NOT IMPLEMENTED (SQL tasks run on SQL warehouses; D1 needs a
      notebook/Python task, so it must be built and reviewed separately).
Writes results/pipeline-<option>-<scale>-<ts>.jsonl. Scale 100 requires --yes (spends real money).
Outputs go to a fresh schema per run which is dropped afterwards; source tables are read only."""
import os, sys, json, time, pathlib, yaml
import sqlglot
from dotenv import load_dotenv
from .engines import Databricks, Snowflake

STEPS = [("T1", "t1_wide"), ("T2", "t2_daily"), ("T3", "t3_cust360"),
         ("T4", "t4a_changes"), ("T4", "t4b_merge"), ("T5", "t5_window")]
CHECKS = {  # output table -> aggregate expressions compared across platforms (rounded to 2 dp)
    "t1_wide": ["COUNT(*)", "SUM(ss_ext_sales_price)", "SUM(ss_quantity)", "SUM(ss_net_paid)"],
    "t2_daily": ["COUNT(*)", "SUM(revenue)", "SUM(qty)", "SUM(discount)"],
    "t3_cust360": ["COUNT(*)", "SUM(total_paid)", "SUM(total_lines)", "SUM(web_paid)"],
    "t4_changes": ["COUNT(*)", "SUM(new_price)"],
    "t5_window": ["COUNT(*)", "SUM(rev)", "SUM(trailing12)", "SUM(rnk)"],
}
OPTIONS = {"D2": ("databricks", None), "S1": ("snowflake", "BENCH_M"), "S2": ("snowflake", "BENCH_L")}

def opt(flag):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else None

def translate(sql, dialect):
    try: return sqlglot.transpile(sql, read="duckdb", write=dialect)[0]
    except Exception: return sqlglot.transpile(sql, write=dialect)[0]

def render(step_file, out, dialect):
    sql = pathlib.Path("pipeline", step_file + ".sql").read_text().replace("{OUT}", out)
    return translate(sql, dialect)

def num(v):
    return None if v is None else f"{round(float(v), 2):.2f}"

def checksums(e, out):
    res = {}
    for t, exprs in CHECKS.items():
        cur = e.conn.cursor()
        cur.execute(f"SELECT {', '.join(exprs)} FROM {out}.{t}")
        res[t] = [num(v) for v in cur.fetchone()]
    return res

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    option, scale = opt("--option"), opt("--scale")
    if option == "D1":
        sys.exit("D1 (serverless Jobs compute) is not implemented: SQL tasks run on SQL warehouses. Needs a notebook task.")
    if option not in OPTIONS or scale not in ("10", "100", "1000"):
        sys.exit(__doc__)
    runs = int(opt("--runs") or 3)
    engine_name, wh = OPTIONS[option]
    cfg = yaml.safe_load(open("config.yaml"))[engine_name]
    cfg["schema"] = (f"tpcds_sf{scale}" if engine_name == "databricks" else f"TPCDS_SF{scale}") + os.environ.get("BENCH_SRC_SUFFIX", "")
    dialect = engine_name
    stamp = time.strftime("%Y%m%d-%H%M%S")
    if "--print" in sys.argv:
        out = "OUTSCHEMA"
        for _, f in STEPS: print(f"-- {f} [{option}]\n{render(f, out, dialect)};\n")
        return
    if scale in ("100", "1000") and "--yes" not in sys.argv:
        sys.exit("Scale 100 spends real warehouse time. Re-run with --yes after reviewing the cost estimate.")
    if wh: os.environ["SNOWFLAKE_WAREHOUSE"] = wh
    path = pathlib.Path("results") / f"pipeline-{option}-sf{scale}-{stamp}.jsonl"
    for run in range(1, runs + 1):
        if "--cold" in sys.argv and run == 1: cold_start(engine_name, wh)
        e = (Databricks if engine_name == "databricks" else Snowflake)(cfg)
        tag = f"out_{option.lower()}_{stamp.replace('-', '')}_{run}"
        out = f"{cfg['catalog']}.{tag}" if engine_name == "databricks" else f"{cfg['database']}.{tag.upper()}"
        e.run(f"CREATE SCHEMA {out}")
        if engine_name == "snowflake":  # CREATE SCHEMA switches the session schema; go back to the source tables
            e.run(f"USE SCHEMA {cfg['database']}.{cfg['schema']}")
        failed, t0 = False, time.perf_counter()
        for step, f in STEPS:
            rec = {"option": option, "engine": engine_name, "scale": scale, "run": run, "step": step, "file": f, "cold": run == 1}
            try:
                dt, qid, _, _ = e.run(render(f, out, dialect))
                rec.update(ok=True, seconds=dt, query_id=qid)
            except Exception as ex:
                rec.update(ok=False, error=str(ex)[:400]); failed = True
            open(path, "a").write(json.dumps(rec) + "\n"); print(option, run, f, rec.get("seconds") or rec.get("error"), flush=True)
            if failed: break  # incomplete pipeline, per design
        rec = {"option": option, "engine": engine_name, "scale": scale, "run": run, "step": "TOTAL",
               "ok": not failed, "seconds": time.perf_counter() - t0}
        if not failed:
            try: rec["checksums"] = checksums(e, out)
            except Exception as ex: rec.update(ok=False, error="checksum: " + str(ex)[:300])
        open(path, "a").write(json.dumps(rec) + "\n")
        e.run(f"DROP SCHEMA IF EXISTS {out} CASCADE")
        e.close()
    print("wrote", path)

def cold_start(engine_name, wh):
    """Stop/suspend the compute so run 1 includes a fresh start, as in the design."""
    try:
        if engine_name == "snowflake":
            e = Snowflake(yaml.safe_load(open("config.yaml"))["snowflake"]); e.run(f"ALTER WAREHOUSE {wh} SUSPEND"); e.close()
        else:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
            w.warehouses.stop(os.environ["DATABRICKS_HTTP_PATH"].rsplit("/", 1)[1]).result()
        time.sleep(10)
    except Exception as ex:
        print("cold start skipped:", str(ex)[:120])

if __name__ == "__main__": main()
