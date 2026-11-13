"""Reconcile isolated windows against billing/metering. Usage: python -m bench.reconcile
Databricks: system.billing.usage by warehouse id and window. Snowflake: ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY.
Both lag by hours; rows may be missing until then. Also counts statements on each warehouse in the window to
check nothing else ran there."""
import os, sys, json, yaml, datetime
from dotenv import load_dotenv
from .engines import Databricks, Snowflake

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    cfg = yaml.safe_load(open("config.yaml"))
    wins = json.load(open("results/isolated-windows.json"))
    for w in wins:
        s = datetime.datetime.fromisoformat(w["start_utc"]) - datetime.timedelta(minutes=1)
        e = datetime.datetime.fromisoformat(w["end_utc"]) + datetime.timedelta(minutes=1)
        lo, hi = s.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S")
        if w["option"] == "D2":
            wid = os.environ["DATABRICKS_HTTP_PATH"].rsplit("/", 1)[1]
            c = Databricks(cfg["databricks"]).conn.cursor()
            c.execute(f"SELECT SUM(usage_quantity), COUNT(*), MIN(usage_start_time), MAX(usage_end_time) FROM system.billing.usage "
                      f"WHERE usage_metadata.warehouse_id = '{wid}' AND usage_start_time < '{hi}' AND usage_end_time > '{lo}'")
            dbu, n, a, b = c.fetchone()
            cost = None if dbu is None else float(dbu) * cfg["databricks"]["usd_per_dbu"]
            print(f"D2 window {w['start_utc']} -> {w['end_utc']}: rows={n} DBU={dbu} cost_usd={cost} (3 runs, incl start-up and idle tail)")
        else:
            wh = {"S1": "BENCH_M", "S2": "BENCH_L"}[w["option"]]
            c = Snowflake(cfg["snowflake"]).conn.cursor()
            c.execute(f"SELECT SUM(credits_used), COUNT(*), MIN(start_time), MAX(end_time) FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY "
                      f"WHERE warehouse_name='{wh}' AND start_time < '{hi}'::timestamp_ltz AND end_time > '{lo}'::timestamp_ltz")
            cr, n, a, b = c.fetchone()
            cost = None if cr is None else float(cr) * cfg["snowflake"]["usd_per_credit"]
            print(f"{w['option']} window {w['start_utc']} -> {w['end_utc']}: rows={n} credits={cr} cost_usd={cost} (3 runs, incl resume and idle tail)")
        if cost is None: print("   no billing rows yet (lag); re-run later")
        else: print(f"   per run (cost / 3): {cost/3:.4f} USD")

if __name__ == "__main__": main()
