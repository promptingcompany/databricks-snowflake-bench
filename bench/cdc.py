"""Part B runner: incremental CDC pipeline.
Usage: python -m bench.cdc --option D2|S1|S2 --scale 10|100 [--batches 10] [--yes] [--print]
Builds the T1 wide fact and T2 daily rollup once, then applies N batches. Each batch touches a 30-day window of sale dates:
 about 5% of its rows updated, 0.5% deleted, 2% inserted (as new keys), all by one MERGE; then T2 is refreshed for only the
 dates that batch touched. Row selection is deterministic key arithmetic, identical on every platform.
Checkpoints (after batches 1, 5, 10 and the base build) record row counts and column sums plus a consistency check
(T2 total revenue equals T1 total). D1 (serverless Jobs) is not implemented here yet. Scale 100 requires --yes."""
import os, sys, json, time, pathlib, yaml
import sqlglot
from dotenv import load_dotenv
from .engines import Databricks, Snowflake
from .pipeline import OPTIONS, opt, render, num

CHECK_BATCHES = {1, 5, 10}
T1_SUMS = ["COUNT(*)", "SUM(ss_ext_sales_price)", "SUM(ss_quantity)", "SUM(ss_net_paid)", "SUM(ss_ext_discount_amt)"]
T2_SUMS = ["COUNT(*)", "SUM(revenue)", "SUM(qty)", "SUM(discount)", "SUM(lines)"]

def render_cdc(name, out, dialect, lo=0, hi=0, b=0):
    sql = pathlib.Path("pipeline_cdc", name + ".sql").read_text()
    sql = sql.replace("{OUT}", out).replace("{LO}", str(lo)).replace("{HI}", str(hi)).replace("{OFF}", str(1000000000 * b))
    try: return sqlglot.transpile(sql, read="duckdb", write=dialect)[0]
    except Exception: return sqlglot.transpile(sql, write=dialect)[0]

def fetch(e, sql):
    cur = e.conn.cursor(); cur.execute(sql); return cur.fetchone()

def checkpoint(e, out):
    t1 = [num(v) for v in fetch(e, f"SELECT {', '.join(T1_SUMS)} FROM {out}.t1_wide")]
    t2 = [num(v) for v in fetch(e, f"SELECT {', '.join(T2_SUMS)} FROM {out}.t2_daily")]
    return {"t1": t1, "t2": t2, "t2_matches_t1": t2[1] == t1[1]}

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    option, scale = opt("--option"), opt("--scale")
    if option == "D1": sys.exit("D1 is not implemented for the CDC pipeline yet.")
    if option not in OPTIONS or scale not in ("10", "100", "1000"): sys.exit(__doc__)
    n = int(opt("--batches") or 10)
    engine_name, wh = OPTIONS[option]
    cfg = yaml.safe_load(open("config.yaml"))[engine_name]
    cfg["schema"] = (f"tpcds_sf{scale}" if engine_name == "databricks" else f"TPCDS_SF{scale}") + os.environ.get("BENCH_SRC_SUFFIX", "")
    dialect = engine_name
    if "--print" in sys.argv:
        out = "OUTSCHEMA"
        for f in ("t1_wide", "t2_daily"): print(f"-- {f}\n{render(f, out, dialect)};\n")
        for nm in ("cdc_changes", "cdc_merge", "cdc_refresh_delete", "cdc_refresh_insert", "cdc_drop"):
            print(f"-- {nm} (batch 1)\n{render_cdc(nm, out, dialect, 1000, 1029, 1)};\n")
        return
    if scale in ("100", "1000") and "--yes" not in sys.argv: sys.exit("Scale 100 spends real money; re-run with --yes after the estimate.")
    if wh: os.environ["SNOWFLAKE_WAREHOUSE"] = wh
    stamp = time.strftime("%Y%m%d-%H%M%S"); path = pathlib.Path("results") / f"cdc-{option}-sf{scale}-{stamp}.jsonl"
    e = (Databricks if engine_name == "databricks" else Snowflake)(cfg)
    tag = f"out_cdc_{option.lower()}_{stamp.replace('-', '')}"
    out = f"{cfg['catalog']}.{tag}" if engine_name == "databricks" else f"{cfg['database']}.{tag.upper()}"
    e.run(f"CREATE SCHEMA {out}")
    if engine_name == "snowflake": e.run(f"USE SCHEMA {cfg['database']}.{cfg['schema']}")
    base_sk = int(fetch(e, "SELECT d_date_sk FROM date_dim WHERE d_date = CAST('2001-01-01' AS DATE)")[0])
    def step(name, sql, **extra):
        rec = {"option": option, "engine": engine_name, "scale": scale, "step": name, **extra}
        try:
            dt, qid, _, _ = e.run(sql); rec.update(ok=True, seconds=dt, query_id=qid)
        except Exception as ex:
            rec.update(ok=False, error=str(ex)[:400])
        open(path, "a").write(json.dumps(rec) + "\n"); print(option, name, rec.get("seconds") or rec.get("error"), flush=True)
        return rec["ok"]
    ok = step("BASE_T1", render("t1_wide", out, dialect)) and step("BASE_T2", render("t2_daily", out, dialect))
    if ok:
        open(path, "a").write(json.dumps({"option": option, "engine": engine_name, "step": "CHECK", "batch": 0, "ok": True, "checksums": checkpoint(e, out)}) + "\n")
    for b in range(1, n + 1) if ok else []:
        lo, hi = base_sk + (b - 1) * 30, base_sk + b * 30 - 1
        for nm in ("cdc_changes", "cdc_merge", "cdc_refresh_delete", "cdc_refresh_insert", "cdc_drop"):
            if not step(f"B{b}_{nm}", render_cdc(nm, out, dialect, lo, hi, b), batch=b): ok = False; break
        if not ok: break
        if b in CHECK_BATCHES:
            open(path, "a").write(json.dumps({"option": option, "engine": engine_name, "step": "CHECK", "batch": b, "ok": True, "checksums": checkpoint(e, out)}) + "\n")
    e.run(f"DROP SCHEMA IF EXISTS {out} CASCADE"); e.close()
    print("wrote", path)

if __name__ == "__main__": main()
