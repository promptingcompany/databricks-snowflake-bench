"""Upload generated DI-style files to each platform's staging area (one time per SF). Not timed in any benchmark.
Usage: python -m bench.di_upload --sf 1 [--engines snowflake,databricks]
 Snowflake  : internal stage @BENCH.DI_SF<N>.DI_STAGE/sf<N>/batch<B>/<file>
 Databricks : /Volumes/bench/di_sf<N>/raw/batch<B>/<file>"""
import os, sys, pathlib, yaml
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from .engines import Databricks, Snowflake
from .pipeline import opt

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    sf = opt("--sf") or "1"; engines = (opt("--engines") or "snowflake,databricks").split(",")
    root = pathlib.Path("data/di") / f"sf{sf}"
    files = sorted(p for p in root.glob("batch*/*") if p.is_file())
    cfg = yaml.safe_load(open("config.yaml"))
    catalog, database = cfg["databricks"]["catalog"], cfg["snowflake"]["database"]
    if "snowflake" in engines:
        e = Snowflake(cfg["snowflake"])
        e.run(f"CREATE SCHEMA IF NOT EXISTS {database}.DI_SF{sf}"); e.run(f"CREATE STAGE IF NOT EXISTS {database}.DI_SF{sf}.DI_STAGE")
        for p in files:
            e.run(f"PUT file://{p.resolve()} @{database}.DI_SF{sf}.DI_STAGE/sf{sf}/{p.parent.name}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=8")
            print("snowflake put", p.parent.name, p.name, flush=True)
        e.close()
    if "databricks" in engines:
        from databricks.sdk import WorkspaceClient
        e = Databricks(cfg["databricks"])
        e.run(f"CREATE SCHEMA IF NOT EXISTS {catalog}.di_sf{sf}"); e.run(f"CREATE VOLUME IF NOT EXISTS {catalog}.di_sf{sf}.raw"); e.close()
        w = WorkspaceClient(host="https://" + os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"], auth_type="pat")
        def up(p):
            dest = f"/Volumes/{catalog}/di_sf{sf}/raw/{p.parent.name}/{p.name}"
            for attempt in range(1, 6):
                try:
                    with open(p, "rb") as f: w.files.upload(dest, f, overwrite=True)
                    return f"databricks upload {p.parent.name}/{p.name}"
                except Exception:
                    if attempt == 5: raise
                    __import__("time").sleep(5 * attempt)
        with ThreadPoolExecutor(4) as ex:
            for r in ex.map(up, files): print(r, flush=True)
    print("done", len(files), "files")

if __name__ == "__main__": main()
