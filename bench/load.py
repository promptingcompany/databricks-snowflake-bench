"""Create identical TPC-DS tables on both engines from the SAME Parquet in object storage.
No clustering/Z-order/hand-tuned stats: plain CTAS, then each engine's default stats step.
Usage: python -m bench.load [--print]   (--print shows SQL without executing)"""
import os, sys, yaml
from dotenv import load_dotenv
from .engines import ENGINES

TABLES = ["call_center","catalog_page","catalog_returns","catalog_sales","customer",
 "customer_address","customer_demographics","date_dim","household_demographics",
 "income_band","inventory","item","promotion","reason","ship_mode","store",
 "store_returns","store_sales","time_dim","warehouse","web_page","web_returns",
 "web_sales","web_site"]

SKIP_PUT = "--skip-put" in sys.argv

def statements(engine, cfg, data, local_dir=None):
    base = data["uri"].rstrip("/")  # e.g. s3://bucket/tpcds/sf10
    out = []
    if engine == "databricks":
        if local_dir:  # smoke-test path: files uploaded to a UC volume by scripts/upload_databricks.py
            base = f"/Volumes/{cfg['catalog']}/{cfg['schema']}/raw"
        for t in TABLES:
            out.append(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM parquet.`{base}/{t}/`")
        for t in TABLES:
            out.append(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")
    else:
        stage = cfg["stage"]  # external stage pointing at base uri, created by admin
        if local_dir:  # smoke-test path: no S3, PUT local Parquet into an internal stage
            stage = cfg["stage"].rsplit(".", 1)[0] + ".TPCDS_LOCAL_STAGE"
            out.append(f"CREATE STAGE IF NOT EXISTS {stage}")
            if not SKIP_PUT: out.append(f"REMOVE @{stage}")
            for t in ([] if SKIP_PUT else TABLES):
                out.append(f"PUT file://{local_dir}/{t}/*.parquet @{stage}/{t}/ AUTO_COMPRESS=FALSE PARALLEL=8 OVERWRITE=TRUE")
        out.append("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 14400")
        out.append("CREATE OR REPLACE FILE FORMAT bench_parquet TYPE=PARQUET")
        for t in TABLES:
            out.append(f"CREATE OR REPLACE TABLE {t} USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('COLUMN_NAME', UPPER(COLUMN_NAME), 'TYPE', TYPE, 'NULLABLE', NULLABLE)) "
                       f"FROM TABLE(INFER_SCHEMA(LOCATION=>'@{stage}/{t}/', FILE_FORMAT=>'bench_parquet'{", FILES=>'c1.parquet'" if SKIP_PUT else ""})))")
            out.append(f"COPY INTO {t} FROM @{stage}/{t}/ FILE_FORMAT=(FORMAT_NAME='bench_parquet') "
                       f"MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE")
    return out

def main():
    load_dotenv()
    cfg = yaml.safe_load(open(sys.argv[sys.argv.index("--config") + 1] if "--config" in sys.argv else "config.yaml"))
    data = cfg["data"]
    only = sys.argv[sys.argv.index("--engines") + 1].split(",") if "--engines" in sys.argv else None
    for n in ("databricks", "snowflake"):
        if only and n not in only: continue
        local = os.path.abspath(f"data/sf{cfg['scale_factor']}") if "--local" in sys.argv else None
        stmts = statements(n, cfg[n], data, local)
        if "--print" in sys.argv:
            print(f"-- {n}"); print("\n".join(s + ";" for s in stmts)); continue
        e = ENGINES[n](cfg[n])
        for s in stmts:
            e.run(s); print(n, s[:70])
        e.close()

if __name__ == "__main__": main()
