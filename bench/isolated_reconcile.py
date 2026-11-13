"""Read billing and metering for the isolated windows in results/isolated2-windows.json (billing lags: Snowflake up to about 3 hours,
Databricks up to a day or more). For each window: billed total over the clock hours the window spans, cost, cost per run, and
FOREIGN statements (any statement on that warehouse in those hours that started outside the window). Foreign statements mean the
hourly billing is contaminated and the cost must not be used. Also reads Databricks serverless Jobs cost by job run id.
Usage: python -m bench.isolated_reconcile [--skip-databricks] [--json results/isolated2-costs.json]"""
import os, sys, json, glob, yaml, datetime
from dotenv import load_dotenv
from .engines import Databricks, Snowflake

UTC = datetime.timezone.utc
def hr(d): return d.replace(minute=0, second=0, microsecond=0)

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    cfg = yaml.safe_load(open("config.yaml"))
    wins = json.load(open("results/isolated2-windows.json"))
    out = []
    wid = os.environ["DATABRICKS_HTTP_PATH"].rsplit("/", 1)[1]
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import QueryFilter, TimeRange
    w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
    skip_dbx = "--skip-databricks" in sys.argv   # do not start the Databricks warehouse (use while a window's billed hour is still open)
    dbx = None if skip_dbx else Databricks(cfg["databricks"]).conn.cursor()
    sf = Snowflake(dict(cfg["snowflake"], schema="TPCDS_SF10")).conn.cursor()
    for x in wins:
        if skip_dbx and x["option"] == "D2": continue
        if x.get("valid") is False: continue
        s0 = datetime.datetime.fromisoformat(x["start_utc"]); e0 = datetime.datetime.fromisoformat(x["end_utc"])
        h_lo, h_hi = hr(s0), hr(e0) + datetime.timedelta(hours=1)
        lo, hi = h_lo.strftime("%Y-%m-%d %H:%M:%S"), h_hi.strftime("%Y-%m-%d %H:%M:%S")
        rec = {**{k: x[k] for k in ("workload", "option", "scale", "repeats", "start_utc", "end_utc")}, "billed_hours": f"{lo} to {hi} UTC"}
        slack = datetime.timedelta(seconds=30)
        if x["option"] == "D2":
            dbx.execute(f"SELECT usage_start_time, usage_quantity FROM system.billing.usage WHERE usage_metadata.warehouse_id = '{wid}' AND usage_start_time >= '{lo}' AND usage_start_time < '{hi}' ORDER BY 1")
            rows = dbx.fetchall(); total = sum(float(r[1]) for r in rows)
            rec.update(billing_rows=len(rows), billed=round(total, 4), unit="DBU", cost_usd=round(total * cfg["databricks"]["usd_per_dbu"], 3) if rows else None)
            res = w.query_history.list(filter_by=QueryFilter(warehouse_ids=[wid], query_start_time_range=TimeRange(start_time_ms=int(h_lo.timestamp() * 1000), end_time_ms=int(h_hi.timestamp() * 1000))), max_results=1000)
            sts = [datetime.datetime.fromtimestamp(q.query_start_time_ms / 1000, UTC) for q in (res.res or [])]
        else:
            name = {"S1": "BENCH_M", "S2": "BENCH_L"}[x["option"]]
            sf.execute(f"SELECT start_time, credits_used_compute, credits_used_cloud_services FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE warehouse_name = '{name}' AND start_time >= TO_TIMESTAMP_LTZ('{lo}+00:00') AND start_time < TO_TIMESTAMP_LTZ('{hi}+00:00') ORDER BY 1")
            rows = sf.fetchall(); total = sum(float(r[1]) for r in rows)
            rec.update(billing_rows=len(rows), billed=round(total, 4), unit="credits", cost_usd=round(total * cfg["snowflake"]["usd_per_credit"], 3) if rows else None, cloud_services_credits=round(sum(float(r[2]) for r in rows), 4))
            sf.execute(f"SELECT start_time FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(WAREHOUSE_NAME=>'{name}', END_TIME_RANGE_START=>TO_TIMESTAMP_LTZ('{lo}+00:00'), END_TIME_RANGE_END=>TO_TIMESTAMP_LTZ('{hi}+00:00'), RESULT_LIMIT=>10000))")
            sts = [r[0].astimezone(UTC) if r[0].tzinfo else r[0].replace(tzinfo=UTC) for r in sf.fetchall()]
        foreign = [t for t in sts if t < s0 - slack or t > e0 + slack]
        rec.update(statements_in_billed_hours=len(sts), foreign_statements=len(foreign), clean=(len(foreign) == 0 and len(rows) > 0))
        rec["cost_per_run_usd"] = round(rec["cost_usd"] / x["repeats"], 4) if rec.get("cost_usd") is not None else None
        out.append(rec)
        print(f"{x['option']} {x['workload']} sf{x['scale']}: rows={rec['billing_rows']} billed={rec['billed']} {rec['unit']} cost=${rec['cost_usd']} per run=${rec['cost_per_run_usd']} foreign={rec['foreign_statements']} {'CLEAN' if rec['clean'] else 'NOT USABLE (foreign statements or no billing yet)'}")
    # Databricks serverless Jobs by job run id (D1 result files)
    jobs = {}
    for f in ([] if skip_dbx else glob.glob("results/*-D1-*.jsonl")):
        for l in open(f):
            r = json.loads(l)
            if r.get("job_run_id") and not f.endswith("enriched.jsonl"): jobs.setdefault(str(r["job_run_id"]), os.path.basename(f))
    for jid, f in sorted(jobs.items(), key=lambda kv: kv[1]):
        dbx.execute(f"SELECT SUM(usage_quantity) FROM system.billing.usage WHERE usage_metadata.job_run_id = '{jid}'")
        d = dbx.fetchone()[0]
        out.append({"workload": f.split("-")[0], "option": "D1", "file": f, "job_run_id": jid, "billed": None if d is None else round(float(d), 4), "unit": "DBU", "cost_usd": None if d is None else round(float(d) * 0.35, 4)})
        print("D1", f, jid, "DBU", None if d is None else round(float(d), 4), "cost", None if d is None else round(float(d) * 0.35, 4))
    json.dump(out, open(sys.argv[sys.argv.index("--json") + 1] if "--json" in sys.argv else "results/isolated2-costs.json", "w"), indent=2)

if __name__ == "__main__": main()
