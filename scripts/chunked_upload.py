"""Generate TPC-DS one dsdgen child at a time, convert it to Parquet, upload it to both platforms, then delete it locally.
Keeps peak local disk to about one child (raw .dat plus its Parquet), so SF 10 and SF 100 fit on a small disk.
Usage: KIT=/path/to/tpcds-kit python scripts/chunked_upload.py <scale_factor> <children> [--from <child>] [--to <child>]
Uploads to /Volumes/bench/tpcds_sf<N>/raw/<table>/c<i>.parquet and @BENCH.TPCDS_SF<N>.TPCDS_LOCAL_STAGE/<table>/c<i>.parquet,
the same layout bench.load reads with --local --skip-put."""
import os, sys, re, shutil, subprocess, pathlib, time
import duckdb
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from bench.engines import Snowflake

load_dotenv(os.path.join(os.getcwd(), ".env"))
sf, children = sys.argv[1], int(sys.argv[2])
start = int(sys.argv[sys.argv.index("--from") + 1]) if "--from" in sys.argv else 1
last = int(sys.argv[sys.argv.index("--to") + 1]) if "--to" in sys.argv else children
kit_tools = pathlib.Path(os.environ["KIT"]) / "tools"
gen = pathlib.Path(f"/tmp/tpcds_sf{sf}_from{start}")
out = pathlib.Path("data") / f"sf{sf}_chunk"
volume = f"/Volumes/bench/tpcds_sf{sf}/raw"
stage = f"BENCH.TPCDS_SF{sf}.TPCDS_LOCAL_STAGE"

ddb = duckdb.connect()
ddb.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0.01)")
tables = [r[0] for r in ddb.execute("select table_name from information_schema.tables where table_schema='main'").fetchall()]
columns = {t: ddb.execute(f"select column_name, data_type from information_schema.columns where table_name='{t}' order by ordinal_position").fetchall() for t in tables}

w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
snow = Snowflake({"database": "BENCH", "schema": f"TPCDS_SF{sf}"})
snow.run(f"CREATE STAGE IF NOT EXISTS {stage}")

def with_retry(fn, label):
    for attempt in range(1, 6):
        try:
            return fn()
        except Exception as ex:
            if attempt == 5:
                raise
            print(f"retry {attempt} {label}: {str(ex)[:120]}", flush=True)
            time.sleep(5 * attempt)

for child in range(start, last + 1):
    shutil.rmtree(gen, ignore_errors=True); gen.mkdir(parents=True)
    t0 = time.time()
    subprocess.run(["./dsdgen", "-scale", sf, "-dir", str(gen), "-parallel", str(children), "-child", str(child), "-force"],
                   cwd=kit_tools, check=True, stdout=subprocess.DEVNULL)
    for t in sorted(tables):
        files = sorted(str(f) for f in gen.glob("*.dat") if re.match(rf"^{re.escape(t)}(_\d+_\d+)?\.dat$", f.name))
        if not files:
            continue
        spec = ", ".join(f"'{n}': '{ty}'" for n, ty in columns[t]) + ", '_trail': 'VARCHAR'"
        local = out / t / f"c{child}.parquet"
        local.parent.mkdir(parents=True, exist_ok=True)
        flist = ", ".join(f"'{f}'" for f in files)
        ddb.execute(f"""COPY (SELECT * EXCLUDE (_trail) FROM read_csv([{flist}], delim='|', header=false,
            columns={{{spec}}}, nullstr='', quote='', escape='', dateformat='%Y-%m-%d')) TO '{local}'
            (FORMAT PARQUET, COMPRESSION ZSTD)""")
        rows = ddb.execute(f"select count(*) from '{local}'").fetchone()[0]
        for f in files:
            os.remove(f)
        def to_databricks():
            with open(local, "rb") as fh:
                w.files.upload(f"{volume}/{t}/{local.name}", fh, overwrite=True)
        with_retry(to_databricks, f"databricks {t}")
        with_retry(lambda: snow.run(f"PUT file://{local.resolve()} @{stage}/{t}/ AUTO_COMPRESS=FALSE PARALLEL=8 OVERWRITE=TRUE"), f"snowflake {t}")
        local.unlink()
        print(f"child {child}/{children} {t}: {rows} rows uploaded", flush=True)
    print(f"child {child}/{children} done in {time.time() - t0:.0f}s", flush=True)

shutil.rmtree(gen, ignore_errors=True)
snow.close()
print("all children uploaded")
