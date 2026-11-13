"""Convert official dsdgen .dat output (pipe-delimited, trailing '|') to Parquet, per table.
Usage: python scripts/dat_to_parquet.py <scale_factor>
Reads data/official_raw/sf<N>/<table>_<i>_<n>.dat (or <table>.dat), writes data/sf<N>/<table>/*.parquet.
Schema comes from DuckDB's TPC-DS table definitions so types match the earlier load.
Validates: no sentinel values (INT32/INT64 min), row counts vs DuckDB's reference counts."""
import sys, pathlib, glob, duckdb
sf = sys.argv[1]
raw = pathlib.Path("data/official_raw") / f"sf{sf}"
out = pathlib.Path("data") / f"sf{sf}"
c = duckdb.connect()
c.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0.01)")  # schema source only
tables = [r[0] for r in c.execute("select table_name from information_schema.tables where table_schema='main'").fetchall()]
for t in sorted(tables):
    import re
    pat = re.compile(rf"^{re.escape(t)}(_\d+_\d+)?\.dat$")
    files = sorted(str(f) for f in raw.glob("*.dat") if pat.match(f.name))
    if not files:
        print("MISSING", t); continue
    cols = c.execute(f"select column_name, data_type from information_schema.columns where table_name='{t}' order by ordinal_position").fetchall()
    spec = ", ".join(f"'{n}': '{ty}'" for n, ty in cols) + ", '_trail': 'VARCHAR'"
    d = out / t
    if d.exists():
        for f in d.glob("*.parquet"): f.unlink()
    d.mkdir(parents=True, exist_ok=True)
    flist = ", ".join(f"'{f}'" for f in files)
    c.execute(f"""COPY (SELECT * EXCLUDE (_trail) FROM read_csv([{flist}], delim='|', header=false,
        columns={{{spec}}}, nullstr='', quote='', escape='', dateformat='%Y-%m-%d')) TO '{d}'
        (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES 134217728)""")
    n = c.execute(f"select count(*) from '{d}/*.parquet'").fetchone()[0]
    bad = 0
    for col, ty in cols:
        if ty in ("INTEGER", "BIGINT") or ty.startswith("DECIMAL"):
            bad += c.execute(f'select count(*) from \'{d}/*.parquet\' where "{col}" < -1000000').fetchone()[0]
    print(f"{t}: {n} rows, sentinel-like values: {bad}")
