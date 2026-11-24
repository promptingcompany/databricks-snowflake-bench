"""Generator for the DI-style ETL benchmark. Our own design, NOT TPC-DI.
Usage: python scripts/di_gen.py --sf 1 [--seed 42] [--out data/di]
Writes data/di/sf<SF>/batch{1,2,3}/... plus di_report.json (row counts, sums, versions) and layout.json (fixed-width spec).
Deterministic: every value comes from hash(key, salt, seed), never from random state, so a rerun is byte identical.
SF 1 = 100,000 customers. Fractional SF is allowed for tests (e.g. 0.01)."""
import sys, json, pathlib, argparse, datetime, hashlib
import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--sf", type=float, required=True)
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--out", default="data/di")
a = ap.parse_args()
N = max(int(100_000 * a.sf), 1000)          # customers
NA = 2 * N                                  # accounts
T1 = 20 * N                                 # batch 1 trades
NSEC = 6000                                 # securities (fixed)
D1 = 1250                                   # trading days in batch 1 history
DB = 60                                     # trading days added by each of batches 2 and 3
out = pathlib.Path(a.out) / f"sf{a.sf:g}"
c = duckdb.connect()
c.execute(f"CREATE MACRO r(n, s) AS (hash(CAST(n AS BIGINT) * 7919 + s * 104729 + {a.seed}) % 1000000) / 1000000.0")
c.execute("CREATE MACRO rp(s, n) AS rpad(s, n, ' ')")
c.execute("CREATE MACRO pick(lst, n, s) AS list_extract(lst, 1 + CAST(floor(r(n, s) * len(lst)) AS INTEGER))")
# trading calendar: Mon-Fri starting 2010-01-04
c.execute(f"CREATE TABLE cal AS SELECT i AS d, DATE '2010-01-04' + CAST((i // 5) * 7 + (i % 5) AS INTEGER) AS dt FROM range(0, {D1 + 2 * DB}) t(i)")
FIRST = ["Ava","Ben","Cleo","Dan","Eli","Fay","Gus","Hana","Ivy","Jon","Kai","Lia","Max","Nia","Oto","Pia","Quin","Rae","Sol","Tia"]
LAST = ["Adams","Baker","Cruz","Diaz","Evans","Ford","Gray","Hall","Ito","Jones","Khan","Lopez","Moss","Nash","Ortiz","Park","Quinn","Reyes","Shaw","Tran"]
STATES = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]
CITIES = ["Springfield","Riverton","Fairview","Kingston","Lakeside","Hillcrest","Oakdale","Brookfield","Westport","Eastwood"]
for nm, lst in (("FIRST", FIRST), ("LAST", LAST), ("STATES", STATES), ("CITIES", CITIES)):
    c.execute(f"CREATE MACRO {nm.lower()}_l() AS {lst!r}")

def write(sql, path, fmt):
    path.parent.mkdir(parents=True, exist_ok=True)
    sql = f"SELECT * FROM ({sql}) ORDER BY ALL"  # fixed row order, so reruns are byte identical
    if fmt == "csv": c.execute(f"COPY ({sql}) TO '{path}' (FORMAT CSV, HEADER false, DELIMITER ',')")
    elif fmt == "pipe": c.execute(f"COPY ({sql}) TO '{path}' (FORMAT CSV, HEADER false, DELIMITER '|')")
    elif fmt == "json": c.execute(f"COPY ({sql}) TO '{path}' (FORMAT JSON)")
    elif fmt == "fixed": c.execute(f"COPY ({sql}) TO '{path}' (FORMAT CSV, HEADER false, DELIMITER '\x01', QUOTE '', ESCAPE '')")
    return c.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]

report = {"generator": "di_gen 1.0 (own design, not TPC-DI)", "seed": a.seed, "sf": a.sf, "duckdb": duckdb.__version__,
          "customers_batch1": N, "accounts_batch1": NA, "securities": NSEC, "trades_batch1": T1, "trading_days_batch1": D1, "batches": {}}
effective = {1: "2010-01-01T00:00:00", 2: "2015-01-02T00:00:00", 3: "2015-04-01T00:00:00"}

# ---------- customer record builder (full record at a given batch/version salt) ----------
def cust_sql(id_expr, ver_salt, action, eff):
    return f"""SELECT {id_expr} AS customer_id, '{action}' AS action, '{eff}' AS effective_ts,
      printf('%03d-%02d-%04d', CAST(r(id,1)*899+100 AS INT), CAST(floor(r(id,2) * 99) AS INT), CAST(floor(r(id,3) * 9999) AS INT)) AS tax_id,
      pick(last_l(), id, 11) AS last_name, pick(first_l(), id, 12) AS first_name,
      CASE WHEN r(id,13) < 0.5 THEN 'M' ELSE 'F' END AS gender,
      CAST(DATE '1950-01-01' + CAST(floor(r(id,14) * 20000) AS INT) AS VARCHAR) AS dob,
      1 + CAST(floor(r(id, 15 + {ver_salt}) * 3) AS INT) AS tier,
      pick(cities_l(), id, 16 + {ver_salt}) AS city, pick(states_l(), id, 17 + {ver_salt}) AS state,
      printf('%05d', CAST(floor(r(id, 18 + {ver_salt}) * 99999) AS INT)) AS postal,
      [struct_pack(type := 'home', number := printf('%03d-%04d', CAST(floor(r(id,19) * 999) AS INT), CAST(floor(r(id,20) * 9999) AS INT))),
       struct_pack(type := 'mobile', number := printf('%03d-%04d', CAST(floor(r(id,21+{ver_salt}) * 999) AS INT), CAST(floor(r(id,22) * 9999) AS INT)))] AS phones,
      [printf('%s.%s@example.com', lower(pick(first_l(), id, 12)), lower(pick(last_l(), id, 11)))] AS emails,
      struct_pack(local_rate := round(r(id,23)*0.1, 4), national_rate := round(r(id,24)*0.3, 4)) AS tax_info"""

def cust_from(src_where, id_expr, ver_salt, action, eff, src="range(1, {n}+1) t(id)"):
    return cust_sql(id_expr, ver_salt, action, eff) + f" FROM {src} WHERE {src_where}"

# ---------- Batch 1 ----------
b1 = out / "batch1"
cnt = {}
cnt["Customer.json"] = write(cust_from("TRUE", "id", 0, "NEW", effective[1], f"range(1, {N}+1) t(id)"), b1 / "Customer.json", "json")
cnt["Account.txt"] = write(f"""SELECT id AS account_id, (id + 1) // 2 AS customer_id,
    printf('ACCT-%08d', id) AS account_name, CAST(floor(r(id,31) * 3) AS INT) AS tax_status, 'ACTV' AS status,
    1 + CAST(floor(r(id,32) * 500) AS INT) AS broker_id, 'NEW' AS action, '{effective[1]}' AS effective_ts FROM range(1, {NA}+1) t(id)""", b1 / "Account.txt", "pipe")
# securities, fixed width: symbol(15) issue(6) status(4) name(40) exchange(6) shares(13) first_trade(8) dividend(10) company(30) industry(2) rating(4)
def sec_sql(ver):
    return f"""SELECT rp(printf('SYM%05d', id), 15) || rp('COMMON', 6) || rp('ACTV', 4) || rp(printf('Security %d %s', id, pick(['Corp','Holdings','Group','Partners'], id, 41+{ver})), 40)
      || rp(pick(['NYSE','NASDAQ','AMEX','ARCA'], id, 42), 6) || lpad(CAST(CAST(floor(r(id,43) * 900000000) AS BIGINT) + 1000000 AS VARCHAR), 13, '0')
      || strftime(DATE '2010-01-04' + CAST(floor(r(id,44) * 500) AS INT), '%Y%m%d') || lpad(CAST(CAST(floor(r(id,45) * 500000) AS INT) AS VARCHAR), 10, '0')
      || rp(printf('Company %d', id), 30) || lpad(CAST(1 + CAST(floor(r(id,46) * 12) AS INT) AS VARCHAR), 2, '0')
      || rp(pick(['AAA','AA','A','BBB','BB','B'], id, 47 + {ver}), 4) AS line"""
cnt["Security.dat"] = write(sec_sql(0) + f" FROM range(1, {NSEC}+1) t(id)", b1 / "Security.dat", "fixed")
# reference tables
ref = {"TradeType.csv": "SELECT * FROM (VALUES ('TMB','Market-Buy',0,1),('TMS','Market-Sell',1,1),('TLB','Limit-Buy',0,0),('TLS','Limit-Sell',1,0),('TSL','Stop-Loss',1,0)) v(id,name,is_sell,is_market)",
       "StatusType.csv": "SELECT * FROM (VALUES ('ACTV','Active'),('CMPT','Completed'),('CNCL','Canceled'),('PNDG','Pending'),('SBMT','Submitted'),('INAC','Inactive')) v(id,name)",
       "Industry.csv": "SELECT i AS id, printf('Industry %d', i) AS name FROM range(1, 13) t(i)",
       "TaxRate.csv": "SELECT printf('TX%02d', i) AS id, printf('Tax bracket %d', i) AS name, round(i * 0.012, 4) AS rate FROM range(1, 21) t(i)",
       "Calendar.csv": "SELECT d AS day_idx, CAST(dt AS VARCHAR) AS date, dayname(dt) AS day_name, year(dt) AS year, month(dt) AS month FROM cal"}
for fn, sql in ref.items(): cnt[fn] = write(sql, b1 / fn, "csv")

# trade price model: deterministic close price for (security, trading day)
c.execute("CREATE MACRO base(sec) AS 20 + floor(r(sec, 51) * 180)")
c.execute("CREATE MACRO px(sec, d) AS round(base(sec) * (1 + 0.25 * sin(d / 60.0 + r(sec, 52) * 6.28)) * (1 + 0.02 * (r(sec * 1000 + d, 53) - 0.5)), 2)")
TYPES = "['TMB','TMS','TLB','TLS','TSL']"
def trades_sql(first_id, count, day_lo, day_hi, batch, deleted_filter):
    return f"""WITH t AS (SELECT {first_id} + i AS tid FROM range(0, {count}) g(i)),
      c AS (SELECT tid, 1 + CAST(floor(r(tid,61) * {NA}) AS BIGINT) AS a1, 1 + CAST(floor(r(tid,62) * {NA}) AS BIGINT) AS a2, 1 + CAST(floor(r(tid,63) * {NA}) AS BIGINT) AS a3 FROM t),
      v AS (SELECT tid, CASE WHEN {deleted_filter.format(x="((a1+1)//2)")} THEN a1 WHEN {deleted_filter.format(x="((a2+1)//2)")} THEN a2 WHEN {deleted_filter.format(x="((a3+1)//2)")} THEN a3 ELSE 1 END AS acct FROM c),
      d AS (SELECT tid, acct, 1 + CAST(floor(r(tid,64) * {NSEC}) AS INT) AS sec, {day_lo} + CAST(floor(r(tid,65) * {day_hi - day_lo}) AS INT) AS day,
                   pick({TYPES}, tid, 66) AS ttype, 100 * (1 + CAST(floor(r(tid,67) * 10) AS INT)) AS qty FROM v)
    SELECT tid AS trade_id, acct AS account_id, printf('SYM%05d', sec) AS symbol, ttype AS trade_type,
      CASE WHEN r(tid,68) < 0.05 THEN 'PNDG' WHEN r(tid,68) < 0.10 THEN 'CNCL' ELSE 'CMPT' END AS final_status, qty AS quantity,
      px(sec, day) AS bid_price, round(px(sec, day) * (1 + 0.002 * (r(tid,69) - 0.5)), 2) AS trade_price,
      round(qty * 0.001, 2) AS fee, round(qty * px(sec, day) * 0.0005, 2) AS commission, round(qty * px(sec, day) * 0.0002, 2) AS tax,
      strftime(cal.dt, '%Y-%m-%d') || ' ' || printf('%02d:%02d:%02d', 9 + CAST(floor(r(tid,70) * 7) AS INT), CAST(floor(r(tid,71) * 60) AS INT), CAST(floor(r(tid,72) * 60) AS INT)) AS trade_ts
    FROM d JOIN cal ON cal.d = d.day"""
NODEL = "TRUE"  # no deletions before or during batch 1
def trades_csv(first, count, lo, hi, batch, filt, path):
    n = write(trades_sql(first, count, lo, hi, batch, filt).replace("final_status", "final_status"), path, "csv")
    return n
cnt["Trade.csv"] = trades_csv(1, T1, 0, D1, 1, NODEL, b1 / "Trade.csv")
# trade history: SBMT at trade time, then final status 1 to 3 seconds later unless pending (batch 1 pending trades get SBMT then PNDG)
def hist_sql(first, count, batch):
    return f"""SELECT tid AS trade_id, ts, status FROM (
      SELECT tid, ts, status FROM (SELECT trade_id AS tid, trade_ts AS ts, 'SBMT' AS status FROM trade_src
        UNION ALL SELECT trade_id, trade_ts, final_status FROM trade_src WHERE final_status <> 'PNDG'
        UNION ALL SELECT trade_id, trade_ts, 'PNDG' FROM trade_src WHERE final_status = 'PNDG') x) y"""
c.execute(f"CREATE VIEW trade_src AS {trades_sql(1, T1, 0, D1, 1, NODEL)}")
cnt["TradeHistory.csv"] = write(hist_sql(1, T1, 1), b1 / "TradeHistory.csv", "csv")
# daily market for batch 1: all securities, days 0..D1-1
def mkt_sql(day_lo, day_hi, restate):
    return f"""SELECT strftime(cal.dt, '%Y-%m-%d') AS dt, printf('SYM%05d', s) AS symbol, px(s, d) AS close,
      round(px(s, d) * (1 + 0.01 * r(s * 1000 + d, 54)), 2) AS high, round(px(s, d) * (1 - 0.01 * r(s * 1000 + d, 55)), 2) AS low,
      CAST(100000 * (0.5 + r(s * 1000 + d, 56)) AS BIGINT) AS volume, {restate} AS restated
      FROM range(1, {NSEC}+1) sx(s), cal WHERE cal.d >= {day_lo} AND cal.d < {day_hi} AND cal.d = d""".replace("AND cal.d = d", "").replace("px(s, d)", "px(s, cal.d)").replace("s * 1000 + d", "s * 1000 + cal.d")
cnt["DailyMarket.csv"] = write(mkt_sql(0, D1, 0), b1 / "DailyMarket.csv", "csv")
report["batches"]["1"] = {"files": cnt}

# ---------- Batches 2 and 3 ----------
new_per = max(N // 100, 10)           # 1% new customers per batch
for b in (2, 3):
    bd = out / f"batch{b}"; cnt = {}; eff = effective[b]
    s = 100 * b                        # version salt so changed attributes differ per batch
    prev_del = "(r(id, 902) < 0.005 AND id > 10)" if b == 3 else "FALSE"   # deleted in batch 2: never touched again
    upd = f"(r(id, {900 + b} + 1000) < 0.30) AND id > 10 AND NOT {prev_del}"
    dele = f"(r(id, {900 + b}) < 0.005) AND id > 10 AND NOT {prev_del}"
    lo_new = N + (b - 2) * new_per
    last_day = D1 + (b - 1) * DB
    parts = [cust_sql("id", s, "UPDATE", eff) + f" FROM range(1, {lo_new}+1) t(id) WHERE {upd} AND NOT ({dele})",
             cust_sql("id", s, "DELETE", f"2015-{'03' if b == 2 else '06'}-30T00:00:00") + f" FROM range(1, {lo_new}+1) t(id) WHERE {dele}",
             cust_sql("id", s, "NEW", eff) + f" FROM range({lo_new}+1, {lo_new + new_per}+1) t(id)"]
    cnt["Customer.json"] = write(" UNION ALL ".join(parts), bd / "Customer.json", "json")
    nacct0 = 2 * lo_new
    PDC = "(r((id+1)//2, 902) < 0.005 AND (id+1)//2 > 10)" if b == 3 else "FALSE"   # accounts of customers deleted in batch 2
    acct = f"""SELECT id AS account_id, (id + 1) // 2 AS customer_id, printf('ACCT-%08d', id) AS account_name, CAST(floor(r(id,31) * 3) AS INT) AS tax_status,
        '{{st}}' AS status, 1 + CAST(floor(r(id,32 + {s}) * 500) AS INT) AS broker_id, '{{ac}}' AS action, '{eff}' AS effective_ts FROM range({{lo}}, {{hi}}) t(id) WHERE {{w}}"""
    a_upd = acct.format(st="ACTV", ac="UPDATE", lo=1, hi=nacct0 + 1, w=f"r(id, {950 + b}) < 0.05 AND (id+1)//2 > 10 AND NOT r((id+1)//2, {900 + b}) < 0.005 AND NOT {PDC}")
    a_del = acct.format(st="INAC", ac="DELETE", lo=1, hi=nacct0 + 1, w=f"r((id+1)//2, {900 + b}) < 0.005 AND (id+1)//2 > 10 AND NOT {PDC}")
    a_new = acct.format(st="ACTV", ac="NEW", lo=nacct0 + 1, hi=nacct0 + 2 * new_per + 1, w="TRUE")
    cnt["Account.txt"] = write(" UNION ALL ".join([a_upd, a_del, a_new]), bd / "Account.txt", "pipe")
    cnt["Security.dat"] = write(sec_sql(b) + f" FROM range(1, {NSEC}+1) t(id) WHERE r(id, {960 + b}) < 0.02", bd / "Security.dat", "fixed")
    # new trades: 5% of batch 1, in this batch's trading days, only for accounts whose customer is not deleted in an earlier batch
    first = T1 + (b - 2) * (T1 // 20) + 1
    filt = "NOT (r({x}, 902) < 0.005 AND {x} > 10)" if b == 3 else "TRUE"
    filt = filt  # batch 2 trades happen before batch 2 deletions take effect
    cnt["Trade.csv"] = write(trades_sql(first, T1 // 20, last_day - DB, last_day, b, filt), bd / "Trade.csv", "csv")
    c.execute(f"CREATE OR REPLACE VIEW trade_src AS {trades_sql(first, T1 // 20, last_day - DB, last_day, b, filt)}")
    prior = """SELECT trade_id, strftime(DATE '{d}', '%Y-%m-%d') || ' 10:00:00' AS ts, CASE WHEN r(trade_id, 981) < 0.8 THEN 'CMPT' ELSE 'CNCL' END AS status
               FROM range({lo}, {hi}) t(trade_id) WHERE r(trade_id, 68) < 0.05"""
    pend_lo, pend_hi = (1, T1 + 1) if b == 2 else (T1 + 1, first)
    cnt["TradeHistory.csv"] = write(hist_sql(first, T1 // 20, b) + " UNION ALL " + prior.format(d=f"2015-0{1 if b == 2 else 4}-15", lo=pend_lo, hi=pend_hi), bd / "TradeHistory.csv", "csv")
    cnt["DailyMarket.csv"] = write(mkt_sql(last_day - DB, last_day, 0) + " UNION ALL " + mkt_sql(0, D1, 1).replace("FROM range(1,", "FROM range(1,") + f" AND r(s * 1000 + cal.d, {970 + b}) < 0.01", bd / "DailyMarket.csv", "csv")
    report["batches"][str(b)] = {"files": cnt, "effective_ts": eff}

# ---------- report with checksums so the loaders can reconcile ----------
def sha(p):
    h = hashlib.sha256(); h.update(p.read_bytes()); return h.hexdigest()[:16]
for b in (1, 2, 3):
    for fn in list(report["batches"][str(b)]["files"]):
        p = out / f"batch{b}" / fn
        report["batches"][str(b)]["files"][fn] = {"rows": report["batches"][str(b)]["files"][fn], "bytes": p.stat().st_size, "sha256_16": sha(p)}
(out / "di_report.json").write_text(json.dumps(report, indent=2))
layout = {"Security.dat": [("symbol", 15), ("issue", 6), ("status", 4), ("name", 40), ("exchange", 6), ("shares", 13), ("first_trade_date", 8),
                           ("dividend", 10), ("company", 30), ("industry_id", 2), ("rating", 4)]}
(out / "layout.json").write_text(json.dumps(layout, indent=2))
tot = sum(f["bytes"] for b in report["batches"].values() for f in b["files"].values())
print(f"SF {a.sf:g}: {N} customers, {NA} accounts, {T1} batch-1 trades; wrote {tot/1e6:.1f} MB to {out}")
