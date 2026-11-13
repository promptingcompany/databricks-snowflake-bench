"""Attach SERVER-SIDE timings to a run file (removes laptop/network noise).
Usage: python -m bench.enrich results/run-XXXX.jsonl  -> results/run-XXXX.enriched.jsonl
Snowflake: INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION is session-scoped, so we use
QUERY_HISTORY (account-wide, recent). Databricks: REST query history metrics."""
import os, sys, json, yaml
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryFilter
from .engines import Snowflake

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    cfg = yaml.safe_load(open("config.yaml"))
    path = sys.argv[1]
    recs = [json.loads(l) for l in open(path)]
    sf_ids = [r["query_id"] for r in recs if r["engine"] == "snowflake" and r.get("query_id")]
    db_ids = [r["query_id"] for r in recs if r["engine"] == "databricks" and r.get("query_id")]
    server = {}
    if sf_ids:
        cur = Snowflake(cfg["snowflake"]).conn.cursor()
        for i in range(0, len(sf_ids), 500):
            chunk = ",".join(f"'{q}'" for q in sf_ids[i:i+500])
            cur.execute("SELECT query_id, total_elapsed_time, execution_time, compilation_time, "
                        "queued_provisioning_time FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY("
                        f"END_TIME_RANGE_START=>DATEADD('day',-1,CURRENT_TIMESTAMP()), RESULT_LIMIT=>10000)) WHERE query_id IN ({chunk})")
            for q, tot, ex, comp, prov in cur.fetchall():
                server[q] = {"srv_total_ms": tot, "srv_exec_ms": ex, "srv_compile_ms": comp, "srv_wait_ms": prov}
    if db_ids:
        w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
        for i in range(0, len(db_ids), 100):
            res = w.query_history.list(filter_by=QueryFilter(statement_ids=db_ids[i:i+100]), include_metrics=True, max_results=1000)
            for q in (res.res or []):
                m = q.metrics
                server[q.query_id] = {"srv_total_ms": m.total_time_ms, "srv_exec_ms": m.execution_time_ms,
                                      "srv_compile_ms": m.compilation_time_ms, "srv_wait_ms": m.provisioning_queue_start_timestamp and None}
    out = path.replace(".jsonl", ".enriched.jsonl")
    n = 0
    with open(out, "w") as f:
        for r in recs:
            s = server.get(r.get("query_id"))
            if s: r.update(s); n += 1
            f.write(json.dumps(r) + "\n")
    print(f"enriched {n}/{len(recs)} -> {out}")

if __name__ == "__main__": main()
