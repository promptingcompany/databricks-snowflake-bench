import os, time, hashlib, datetime
from decimal import Decimal
import sqlglot

TIMEOUT_S = int(os.environ.get("BENCH_STATEMENT_TIMEOUT_S", "600"))

def _norm(v):
    if v is None: return '<null>'
    if isinstance(v, (Decimal, float)): return f'{round(float(v), 2):.2f}'
    if isinstance(v, (datetime.date, datetime.datetime)): return v.isoformat()
    return str(v).strip()

def digest(rows):
    lines = sorted('|'.join(_norm(c) for c in r) for r in rows)
    return hashlib.md5('\n'.join(lines).encode()).hexdigest()

class Engine:
    name = ""
    dialect = ""
    def translate(self, sql):
        return sqlglot.transpile(sql, read="duckdb", write=self.dialect)[0]
    def run(self, sql):
        """Returns (wall_seconds, query_id, rows). Raises on failure."""
        raise NotImplementedError
    def close(self): pass

class Databricks(Engine):
    name, dialect = "databricks", "databricks"
    def __init__(self, cfg):
        from databricks import sql
        self.conn = sql.connect(server_hostname=os.environ["DATABRICKS_HOST"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"])
        cur = self.conn.cursor()
        cur.execute("SET use_cached_result = false")  # disable result cache
        cur.execute(f"SET STATEMENT_TIMEOUT = {TIMEOUT_S}")
        cur.execute(f"USE CATALOG {cfg['catalog']}")
        cur.execute(f"USE SCHEMA {cfg['schema']}")
    def run(self, sql):
        cur = self.conn.cursor()
        t = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        dt = time.perf_counter() - t
        return dt, getattr(cur, "query_id", None), len(rows), digest(rows)
    def close(self): self.conn.close()

class Snowflake(Engine):
    name, dialect = "snowflake", "snowflake"
    def __init__(self, cfg):
        import snowflake.connector as sf
        from cryptography.hazmat.primitives import serialization
        with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as f:
            key = serialization.load_pem_private_key(f.read(), password=None)
        pk = key.private_bytes(serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        self.conn = sf.connect(account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"], private_key=pk,
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            role=os.environ.get("SNOWFLAKE_ROLE"),
            database=cfg["database"], schema=cfg["schema"])
        self.conn.cursor().execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        self.conn.cursor().execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {TIMEOUT_S}")
    def run(self, sql):
        cur = self.conn.cursor()
        t = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        dt = time.perf_counter() - t
        return dt, cur.sfqid, len(rows), digest(rows)
    def close(self): self.conn.close()

ENGINES = {"databricks": Databricks, "snowflake": Snowflake}
