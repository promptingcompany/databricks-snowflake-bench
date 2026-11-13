"""DI-style ETL runner (docs/PHASE4_DI_STYLE_ETL_METHOD.md). Our own design, NOT TPC-DI.
Usage: python -m bench.di_etl --option D2|S1|S2 --sf 1 [--print] [--yes (needed for sf >= 100)]
Per batch: ingest (platform-native readers, timed as phase INGEST), transforms (shared SQL, phase TRANSFORM), audits, checkpoint.
Files must already be uploaded (bench.di_upload). Everything is created in a fresh run schema that is dropped at the end."""
import os, sys, json, time, pathlib, yaml
import sqlglot
from dotenv import load_dotenv
from .engines import Databricks, Snowflake
from .pipeline import OPTIONS, opt, num

BATCHES = (1, 2, 3)
TRANSFORM = ["t_customer_close", "t_customer_insert", "t_account_close", "t_account_insert", "t_security_merge",
             "t_fact_trade_insert", "t_fact_trade_status", "t_fact_market_merge", "t_agg_market_52w", "t_agg_position"]
AUDITS = ["audit_customer_current", "audit_customer_gaps", "audit_account_current", "audit_account_gaps",
          "audit_trade_orphans", "audit_trade_security", "audit_trade_status"]
DDL = ["ddl_dim_customer", "ddl_dim_account", "ddl_dim_security", "ddl_fact_trade", "ddl_fact_market"]
CHECKS = {
    "dim_customer": ["COUNT(*)", "COUNT(DISTINCT customer_id)", "SUM(version)", "SUM(CASE WHEN is_current THEN 1 ELSE 0 END)", "SUM(tier)"],
    "dim_account": ["COUNT(*)", "SUM(version)", "SUM(broker_id)", "SUM(CASE WHEN is_current THEN 1 ELSE 0 END)"],
    "dim_security": ["COUNT(*)", "SUM(shares)", "SUM(dividend_cents)"],
    "fact_trade": ["COUNT(*)", "SUM(quantity)", "SUM(trade_price)", "SUM(CASE WHEN status = 'CMPT' THEN 1 ELSE 0 END)", "SUM(CASE WHEN status = 'PNDG' THEN 1 ELSE 0 END)"],
    "fact_market": ["COUNT(*)", "SUM(close_price)", "SUM(volume)"],
    "agg_market_52w": ["COUNT(*)", "SUM(high_52w)", "SUM(low_52w)"],
    "agg_position": ["COUNT(*)", "SUM(net_qty)", "SUM(traded_value)"],
}
# staging tables: name -> (columns for read_files / COPY, in file order)
CSV_SCHEMAS = {
    "stg_account": "account_id BIGINT, customer_id BIGINT, account_name STRING, tax_status INT, status STRING, broker_id INT, action STRING, effective_ts TIMESTAMP",
    "stg_trade": "trade_id BIGINT, account_id BIGINT, symbol STRING, trade_type STRING, final_status STRING, quantity INT, bid_price DECIMAL(10,2), trade_price DECIMAL(10,2), fee DECIMAL(10,2), commission DECIMAL(12,2), tax DECIMAL(12,2), trade_ts TIMESTAMP",
    "stg_trade_history": "trade_id BIGINT, ts TIMESTAMP, status STRING",
    "stg_market": "dt DATE, symbol STRING, close_price DECIMAL(10,2), high_price DECIMAL(10,2), low_price DECIMAL(10,2), volume BIGINT, restated INT",
    "ref_trade_type": "id STRING, name STRING, is_sell INT, is_market INT",
    "ref_status_type": "id STRING, name STRING",
    "ref_industry": "id INT, name STRING",
    "ref_tax_rate": "id STRING, name STRING, rate DOUBLE",
    "ref_calendar": "day_idx INT, cal_date DATE, day_name STRING, cal_year INT, cal_month INT",
}
FILE_OF = {"stg_account": "Account.txt", "stg_trade": "Trade.csv", "stg_trade_history": "TradeHistory.csv", "stg_market": "DailyMarket.csv",
           "ref_trade_type": "TradeType.csv", "ref_status_type": "StatusType.csv", "ref_industry": "Industry.csv", "ref_tax_rate": "TaxRate.csv", "ref_calendar": "Calendar.csv"}
PIPE = {"stg_account"}
# fixed width layout of Security.dat (matches scripts/di_gen.py layout.json): name, start, length
FIXED = [("symbol", 1, 15), ("issue", 16, 6), ("status", 22, 4), ("name", 26, 40), ("exchange", 66, 6), ("shares", 72, 13),
         ("first_trade", 85, 8), ("dividend", 93, 10), ("company", 103, 30), ("industry_id", 133, 2), ("rating", 135, 4)]

def fixed_select(src):
    f = {n: f"TRIM(SUBSTR({src}, {s}, {l}))" for n, s, l in FIXED}
    return (f"SELECT {f['symbol']} AS symbol, {f['issue']} AS issue, {f['status']} AS status, {f['name']} AS name, {f['exchange']} AS exchange, "
            f"CAST({f['shares']} AS BIGINT) AS shares, CAST(CONCAT(SUBSTR({src}, 85, 4), '-', SUBSTR({src}, 89, 2), '-', SUBSTR({src}, 91, 2)) AS DATE) AS first_trade_date, "
            f"CAST({f['dividend']} AS BIGINT) AS dividend_cents, {f['company']} AS company, CAST({f['industry_id']} AS INT) AS industry_id, {f['rating']} AS rating")

def _names():
    c = yaml.safe_load(open("config.yaml")); return c["databricks"]["catalog"], c["snowflake"]["database"]

def ingest_databricks(b, out, sf):
    base = f"/Volumes/{_names()[0]}/di_sf{sf}/raw/batch{b}"
    st = []
    names = [n for n in CSV_SCHEMAS if (b == 1 or n.startswith("stg_"))]
    for n in names:
        opts = ", delimiter => '|'" if n in PIPE else ""
        st.append((f"ingest_{n}", f"CREATE OR REPLACE TABLE {out}.{n} AS SELECT * FROM read_files('{base}/{FILE_OF[n]}', format => 'csv', header => false{opts}, schema => '{CSV_SCHEMAS[n]}')"))
    st.append(("ingest_stg_customer", f"""CREATE OR REPLACE TABLE {out}.stg_customer AS SELECT customer_id, action, CAST(effective_ts AS TIMESTAMP) AS effective_ts, tax_id, last_name, first_name, gender,
      CAST(dob AS DATE) AS dob, CAST(tier AS INT) AS tier, city, state, postal, phones[0].number AS home_phone, phones[1].number AS mobile_phone, emails[0] AS email,
      tax_info.local_rate AS local_rate, tax_info.national_rate AS national_rate FROM read_files('{base}/Customer.json', format => 'json')"""))
    st.append(("ingest_stg_security", f"CREATE OR REPLACE TABLE {out}.stg_security AS {fixed_select('value')} FROM read_files('{base}/Security.dat', format => 'text')"))
    return st

def ingest_snowflake(b, out, sf):
    stage = f"@{_names()[1]}.DI_SF{sf}.DI_STAGE/sf{sf}/batch{b}"
    st = []
    names = [n for n in CSV_SCHEMAS if (b == 1 or n.startswith("stg_"))]
    for n in names:
        fmt = "TYPE = CSV FIELD_DELIMITER = '|'" if n in PIPE else "TYPE = CSV"
        st.append((f"ingest_{n}_ddl", f"CREATE OR REPLACE TABLE {out}.{n} ({CSV_SCHEMAS[n].replace('STRING', 'VARCHAR')})"))
        st.append((f"ingest_{n}", f"COPY INTO {out}.{n} FROM {stage}/{FILE_OF[n]} FILE_FORMAT = ({fmt}) ON_ERROR = ABORT_STATEMENT"))
    st.append(("ingest_stg_customer_raw_ddl", f"CREATE OR REPLACE TABLE {out}.stg_customer_raw (v VARIANT)"))
    st.append(("ingest_stg_customer_raw", f"COPY INTO {out}.stg_customer_raw FROM {stage}/Customer.json FILE_FORMAT = (TYPE = JSON)"))
    st.append(("ingest_stg_customer", f"""CREATE OR REPLACE TABLE {out}.stg_customer AS SELECT v:customer_id::BIGINT AS customer_id, v:action::VARCHAR AS action, v:effective_ts::TIMESTAMP AS effective_ts,
      v:tax_id::VARCHAR AS tax_id, v:last_name::VARCHAR AS last_name, v:first_name::VARCHAR AS first_name, v:gender::VARCHAR AS gender, v:dob::DATE AS dob, v:tier::INT AS tier,
      v:city::VARCHAR AS city, v:state::VARCHAR AS state, v:postal::VARCHAR AS postal, v:phones[0].number::VARCHAR AS home_phone, v:phones[1].number::VARCHAR AS mobile_phone,
      v:emails[0]::VARCHAR AS email, v:tax_info.local_rate::DOUBLE AS local_rate, v:tax_info.national_rate::DOUBLE AS national_rate FROM {out}.stg_customer_raw"""))
    st.append(("ingest_stg_security_raw_ddl", f"CREATE OR REPLACE TABLE {out}.stg_security_raw (line VARCHAR)"))
    st.append(("ingest_stg_security_raw", f"COPY INTO {out}.stg_security_raw FROM {stage}/Security.dat FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = NONE TRIM_SPACE = FALSE)"))
    st.append(("ingest_stg_security", f"CREATE OR REPLACE TABLE {out}.stg_security AS {fixed_select('line')} FROM {out}.stg_security_raw"))
    return st

def render(name, out, dialect, b=0):
    sql = pathlib.Path("pipeline_di", name + ".sql").read_text().replace("{OUT}", out).replace("{B}", str(b))
    try: return sqlglot.transpile(sql, read="duckdb", write=dialect)[0]
    except Exception: return sqlglot.transpile(sql, write=dialect)[0]

def main():
    load_dotenv(os.path.join(os.getcwd(), ".env"))
    option, sf = opt("--option"), opt("--sf") or "1"
    if option not in OPTIONS: sys.exit(__doc__)
    engine_name, wh = OPTIONS[option]
    cfg = yaml.safe_load(open("config.yaml"))[engine_name]
    # any existing schema works as the connection default; the run creates its own schema
    dialect = engine_name; ingest = ingest_databricks if engine_name == "databricks" else ingest_snowflake
    report = json.load(open(f"data/di/sf{sf}/di_report.json"))
    if "--print" in sys.argv:
        out = "OUTSCHEMA"
        for b in (1, 2):
            for nm, sql in ingest(b, out, sf): print(f"-- b{b} {nm}\n{sql};\n")
        for n in TRANSFORM[:3] + AUDITS[:1]: print(f"-- {n}\n{render(n, out, dialect, 1)};\n")
        return
    if float(sf) >= 100 and "--yes" not in sys.argv: sys.exit("This SF spends real money; re-run with --yes after the estimate.")
    if wh: os.environ["SNOWFLAKE_WAREHOUSE"] = wh
    stamp = time.strftime("%Y%m%d-%H%M%S"); path = pathlib.Path("results") / f"di-{option}-sf{sf}-{stamp}.jsonl"
    e = (Databricks if engine_name == "databricks" else Snowflake)(cfg)
    tag = f"out_di_{option.lower()}_{stamp.replace('-', '')}"
    out = f"{cfg['catalog']}.{tag}" if engine_name == "databricks" else f"{cfg['database']}.{tag.upper()}"
    e.run(f"CREATE SCHEMA {out}")
    base_rec = {"option": option, "engine": engine_name, "sf": sf}
    def log(rec): open(path, "a").write(json.dumps({**base_rec, **rec}) + "\n")
    def step(name, sql, phase, b):
        rec = {"step": name, "phase": phase, "batch": b}
        try:
            dt, qid, _, _ = e.run(sql); rec.update(ok=True, seconds=dt, query_id=qid)
        except Exception as ex:
            rec.update(ok=False, error=str(ex)[:500])
        log(rec); print(option, f"b{b}", name, round(rec["seconds"], 2) if rec.get("ok") else rec["error"][:200], flush=True)
        return rec["ok"]
    def one(sql):
        cur = e.conn.cursor(); cur.execute(sql); return cur.fetchone()
    ok = True
    for n in DDL: ok = ok and step(n, render(n, out, dialect), "INIT", 0)
    for b in BATCHES if ok else []:
        for nm, sql in ingest(b, out, sf):
            if not step(nm, sql, "INGEST", b): ok = False; break
        if not ok: break
        # reconcile ingested row counts against the generator's own report
        for tbl, fn in {**FILE_OF, "stg_customer": "Customer.json", "stg_security": "Security.dat"}.items():
            if fn in report["batches"][str(b)]["files"] and (b == 1 or tbl.startswith("stg_")):
                got = int(one(f"SELECT COUNT(*) FROM {out}.{tbl}")[0]); exp = report["batches"][str(b)]["files"][fn]["rows"]
                log({"step": "RECONCILE", "batch": b, "table": tbl, "got": got, "expected": exp, "ok": got == exp})
        for n in TRANSFORM:
            if not step(n, render(n, out, dialect, b), "TRANSFORM", b): ok = False; break
        if not ok: break
        for n in AUDITS: log({"step": "AUDIT", "name": n, "batch": b, "violations": int(one(render(n, out, dialect))[0])})
        cs = {t: [num(v) for v in one(f"SELECT {', '.join(x)} FROM {out}.{t}")] for t, x in CHECKS.items()}
        log({"step": "CHECK", "batch": b, "ok": True, "checksums": cs})
    e.run(f"DROP SCHEMA IF EXISTS {out} CASCADE"); e.close()
    print("wrote", path)

if __name__ == "__main__": main()
