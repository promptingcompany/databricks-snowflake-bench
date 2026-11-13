"""Upload local TPC-DS Parquet to a Unity Catalog volume (no S3 needed).
Usage: python scripts/upload_databricks.py [scale_factor]   (default: config scale_factor)
Creates bench.tpcds_sf<N> schema and volume 'raw' if missing, then uploads
data/sf<N>/<table>/*.parquet -> /Volumes/bench/tpcds_sf<N>/raw/<table>/. Resumable: skips
files whose size already matches."""
import os, sys, pathlib, yaml
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from databricks import sql
from databricks.sdk import WorkspaceClient

load_dotenv(os.path.join(os.getcwd(), ".env"))
cfg = yaml.safe_load(open("config.yaml"))
pos = [a for a in sys.argv[1:] if not a.startswith("--")]
sf = pos[0] if pos else str(cfg["scale_factor"])
catalog, schema = cfg["databricks"]["catalog"], cfg["databricks"]["schema"]
root = f"/Volumes/{catalog}/{schema}/raw"

conn = sql.connect(server_hostname=os.environ["DATABRICKS_HOST"],
                   http_path=os.environ["DATABRICKS_HTTP_PATH"],
                   access_token=os.environ["DATABRICKS_TOKEN"])
cur = conn.cursor()
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
cur.execute(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
conn.close()

w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"],
                    token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
files = sorted(pathlib.Path(f"data/sf{sf}").glob("*/*.parquet"))
if "--clean" in sys.argv:  # remove stale files from earlier data before uploading
    for t in sorted({p.parent.name for p in files}):
        try:
            for f in list(w.files.list_directory_contents(f"{root}/{t}")):
                w.files.delete(f.path)
        except Exception as ex:
            print("clean skip", t, str(ex)[:80])
    print("cleaned", root)

def up(p):
    dest = f"{root}/{p.parent.name}/{p.name}"
    try:
        if "--clean" not in sys.argv and w.files.get_metadata(dest).content_length == p.stat().st_size:
            return f"skip {dest}"
    except Exception:
        pass
    import time
    for attempt in range(1, 6):  # transient API errors happen on large uploads
        try:
            with open(p, "rb") as f:
                w.files.upload(dest, f, overwrite=True)
            return f"ok   {dest}" + (f" (attempt {attempt})" if attempt > 1 else "")
        except Exception as ex:
            if attempt == 5: raise
            time.sleep(5 * attempt)

with ThreadPoolExecutor(4) as ex:
    for r in ex.map(up, files):
        print(r, flush=True)
print(len(files), "files processed")
