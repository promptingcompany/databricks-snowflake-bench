"""Part A runner (see docs/PHASE3_TUNED_AND_CDC_METHOD.md): build and run TUNED variants of the TPC-DS tables.
Usage: python -m bench.tuned --variant D-A|D-B|D-C|S-A|S-B|S-C --scale 10 [--build] [--run] [--rounds 3] [--yes] [--print]
 D-A  Databricks: liquid clustering on each large table's date key, then OPTIMIZE (+ ANALYZE)
 D-B  Databricks: CLUSTER BY AUTO (needs predictive optimization; reported as unavailable if the syntax fails)
 S-A  Snowflake : clustering key on the same date keys; waits for automatic clustering to settle
 D-C/S-C controls: plain CTAS rebuild with no clustering (separates rebuild effect from tuning effect)
 S-B  Snowflake : query acceleration on a separate warehouse BENCH_Q, default tables (no build step)
Build copies from the already loaded default tables (tpcds_sf<N>) into a variant schema. Every build statement is timed;
background clustering credits must be read later from metering. --print shows statements and touches nothing.
Scale 100 needs --yes. Run results are checked against a reference run's output digests (same results gate)."""
import os, sys, json, time, pathlib, yaml
from dotenv import load_dotenv
from .engines import Databricks, Snowflake
from .run import ENGINES  # noqa: F401  (kept for parity with bench.run)
from .load import TABLES

# table -> date key used for clustering (the same keys on both platforms, per the method doc)
CLUSTER_KEYS = {"store_sales": "ss_sold_date_sk", "catalog_sales": "cs_sold_date_sk", "web_sales": "ws_sold_date_sk",
                "inventory": "inv_date_sk", "store_returns": "sr_returned_date_sk",
                "catalog_returns": "cr_returned_date_sk", "web_returns": "wr_returned_date_sk"}
VARIANTS = {"D-A": "databricks", "D-B": "databricks", "D-C": "databricks", "S-A": "snowflake", "S-B": "snowflake", "S-C": "snowflake"}
SUFFIX = {"D-A": "a", "D-B": "b", "D-C": "c", "S-A": "A", "S-C": "C"}
REFERENCE = "results/run-20261009-141003.jsonl"  # SF10 defaults run (identical digests on both engines)

def opt(flag):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else None

def build_statements(variant, cfg, scale):
    src = cfg["schema"]; dst = f"{src}_{SUFFIX[variant].lower()}" if variant.startswith("D") else f"{src}_{SUFFIX[variant]}"
    if variant.startswith("D"):
        cat = cfg["catalog"]; stm = [f"CREATE SCHEMA IF NOT EXISTS {cat}.{dst}"]
        for t in TABLES:
            if t in CLUSTER_KEYS and variant != "D-C":  # D-C is the control: plain rebuild, no clustering, no OPTIMIZE
                by = f"CLUSTER BY ({CLUSTER_KEYS[t]})" if variant == "D-A" else "CLUSTER BY AUTO"
                stm.append(f"CREATE OR REPLACE TABLE {cat}.{dst}.{t} {by} AS SELECT * FROM {cat}.{src}.{t}")
                if variant == "D-A": stm.append(f"OPTIMIZE {cat}.{dst}.{t}")
            else:
                stm.append(f"CREATE OR REPLACE TABLE {cat}.{dst}.{t} AS SELECT * FROM {cat}.{src}.{t}")
        stm += [f"ANALYZE TABLE {cat}.{dst}.{t} COMPUTE STATISTICS FOR ALL COLUMNS" for t in TABLES]
        return dst, stm
    db = cfg["database"]; stm = [f"CREATE SCHEMA IF NOT EXISTS {db}.{dst}"]
    for t in TABLES:
        by = f" CLUSTER BY ({CLUSTER_KEYS[t]})" if t in CLUSTER_KEYS and variant != "S-C" else ""  # S-C is the plain-rebuild control
        stm.append(f"CREATE OR REPLACE TABLE {db}.{dst}.{t}{by} AS SELECT * FROM {db}.{src}.{t}")
    return dst, stm

def connect(variant, cfg, schema):
    c = dict(cfg, schema=schema)
    if VARIANTS[variant] == "databricks": return Databricks(c)
    os.environ["SNOWFLAKE_WAREHOUSE"] = "BENCH_Q" if variant == "S-B" else "BENCH_M"
    return Snowflake(c)

def wait_clustering(e, db, dst):
    """S-A: poll average clustering depth of the big tables until stable (cap 15 min). Returns snapshots."""
    snaps = {}
    for t, col in CLUSTER_KEYS.items():
        last, t0 = None, time.time()
        while time.time() - t0 < 900:
            cur = e.conn.cursor(); cur.execute(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{db}.{dst}.{t}', '({col})')")
            info = json.loads(cur.fetchone()[0]); depth = info.get("average_depth")
            snaps[t] = info
            if last is not None and abs(depth - last) < 0.05: break
            last = depth; time.sleep(60)
    return snaps

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    variant, scale = opt("--variant"), opt("--scale")
    if variant not in VARIANTS or scale not in ("10", "100"): sys.exit(__doc__)
    if scale == "100" and "--yes" not in sys.argv: sys.exit("Scale 100 spends real money; re-run with --yes after the estimate.")
    plat = VARIANTS[variant]
    cfg = yaml.safe_load(open("config.yaml"))[plat]
    cfg["schema"] = f"tpcds_sf{scale}" if plat == "databricks" else f"TPCDS_SF{scale}"
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = pathlib.Path("results") / f"tuned-{variant}-sf{scale}-{stamp}.jsonl"
    do_build = "--build" in sys.argv and variant != "S-B"
    do_run = "--run" in sys.argv
    if variant == "S-B": dst = cfg["schema"]; stm = ["CREATE WAREHOUSE IF NOT EXISTS BENCH_Q WAREHOUSE_SIZE = MEDIUM AUTO_SUSPEND = 60 "
        "AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE ENABLE_QUERY_ACCELERATION = TRUE QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8"]
    else: dst, stm = build_statements(variant, cfg, scale)
    if "--print" in sys.argv:
        print(f"-- variant {variant}, variant schema: {dst}"); print("\n".join(s + ";" for s in stm)); return
    e = connect(variant, cfg, cfg["schema"])
    if variant == "S-B" or do_build:
        for s in stm:  # S-B: just creates the accelerated warehouse
            t = time.perf_counter()
            try:
                e.run(s); rec = {"variant": variant, "kind": "build", "sql": s[:120], "ok": True, "seconds": time.perf_counter() - t}
            except Exception as ex:
                rec = {"variant": variant, "kind": "build", "sql": s[:120], "ok": False, "error": str(ex)[:300]}
            open(out, "a").write(json.dumps(rec) + "\n"); print(variant, "build", rec["sql"][:70], rec.get("seconds") or rec.get("error"), flush=True)
            if not rec["ok"] and variant == "D-B": print("D-B: CLUSTER BY AUTO not available here; reported as unavailable."); break
        if variant == "S-A":
            snaps = wait_clustering(e, cfg["database"], dst)
            open(out, "a").write(json.dumps({"variant": variant, "kind": "clustering_info", "info": snaps}) + "\n")
    if do_run:
        ref = {}
        for l in open(REFERENCE):
            r = json.loads(l)
            if r.get("ok"): ref[r["query"]] = r["digest"]
        run_schema = dst if variant != "S-B" else cfg["schema"]
        e.close(); e = connect(variant, cfg, run_schema)
        import sqlglot
        rounds = int(opt("--rounds") or 3)
        qs = sorted(pathlib.Path("queries").glob("tpcds_q*.sql"))
        for rd in range(rounds + 1):
            for q in qs:
                sql = sqlglot.transpile(q.read_text(), read="duckdb", write=e.dialect)[0]
                rec = {"variant": variant, "kind": "query", "engine": plat, "query": q.stem, "round": rd, "cold": rd == 0}
                try:
                    dt, qid, n, dg = e.run(sql)
                    rec.update(ok=True, seconds=dt, query_id=qid, rows=n, digest=dg, matches_reference=(ref.get(q.stem) == dg))
                except Exception as ex:
                    rec.update(ok=False, error=str(ex)[:300])
                open(out, "a").write(json.dumps(rec) + "\n")
    e.close()
    print("wrote", out)

if __name__ == "__main__": main()
