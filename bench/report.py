"""Summarise an ENRICHED run: correctness first, then server-side timings.
Usage: python -m bench.report results/run-XXXX.enriched.jsonl"""
import json, sys, statistics, yaml
from collections import defaultdict

def pct(xs, p):
    xs = sorted(xs); return xs[min(len(xs)-1, int(round(p/100*(len(xs)-1))))]

recs = [json.loads(l) for l in open(sys.argv[1])]
# --- correctness: same query + round must give same row count and digest on both engines
g = defaultdict(dict)
for r in recs:
    if r["ok"]: g[(r["query"], r["round"])][r["engine"]] = r
mism = sorted({q for (q, rd), e in g.items() if len(e) == 2 and
               (e["databricks"]["digest"] != e["snowflake"]["digest"])})
fails = defaultdict(set)
for r in recs:
    if not r["ok"]: fails[r["engine"]].add(r["query"])
print(f"## Correctness\n- queries with differing results: {len(mism)} {mism[:20]}")
for e, qs in sorted(fails.items()): print(f"- {e} failures: {len(qs)} {sorted(qs)[:20]}")
# --- timings (warm rounds, server-side execution time, only queries valid on BOTH engines)
bad = set(mism) | fails["databricks"] | fails["snowflake"]
print("\n## Server-side time, warm runs, comparable queries only")
print("| Engine | Queries | Total exec (s) | p50 (s) | p99 (s) | Total elapsed (s) |")
print("|---|---:|---:|---:|---:|---:|")
for e in ("databricks", "snowflake"):
    rs = [r for r in recs if r["engine"] == e and r["ok"] and not r["cold"] and r["query"] not in bad and r.get("srv_exec_ms") is not None]
    if not rs: print(f"| {e} | 0 | - | - | - | - |"); continue
    ex = [r["srv_exec_ms"]/1000 for r in rs]; tot = [r["srv_total_ms"]/1000 for r in rs]
    print(f"| {e} | {len({r['query'] for r in rs})} | {sum(ex):.1f} | {statistics.median(ex):.2f} | {pct(ex,99):.2f} | {sum(tot):.1f} |")

# --- cost: warm-pass server time x hourly rate (query-time basis, NOT warehouse uptime)
cfg = yaml.safe_load(open("config.yaml"))
rate = {"databricks": cfg["databricks"]["dbu_per_hour"] * cfg["databricks"]["usd_per_dbu"],
        "snowflake": cfg["snowflake"]["credits_per_hour"] * cfg["snowflake"]["usd_per_credit"]}
print("\n## Cost of the warm pass (query-time basis)")
print("| Engine | Hourly rate (USD) | Warm server time (s) | Cost (USD) |")
print("|---|---:|---:|---:|")
for e in ("databricks", "snowflake"):
    t = sum(r["srv_total_ms"] for r in recs if r["engine"] == e and r["ok"] and not r["cold"] and r["query"] not in bad and r.get("srv_total_ms") is not None) / 1000
    print(f"| {e} | {rate[e]:.2f} | {t:.1f} | {t/3600*rate[e]:.4f} |")
print("Basis: server-side elapsed x hourly list rate. Excludes idle/auto-suspend time and Snowflake's 60 s resume minimum;")
print("real bills use warehouse uptime. Reconcile against billing tables before publishing.")

# --- per-round suite totals (server-side total time), warm rounds only, comparable queries only
print("\n## Suite total per warm round (server-side, comparable queries), mean and SD across rounds")
print("| Engine | Rounds | Mean suite (s) | SD (s) | Cold round (s) |")
print("|---|---:|---:|---:|---:|")
for e in ("databricks", "snowflake"):
    byr = defaultdict(float); cold = 0.0
    for r in recs:
        if r["engine"] == e and r["ok"] and r["query"] not in bad and r.get("srv_total_ms") is not None:
            if r["cold"]: cold += r["srv_total_ms"] / 1000
            else: byr[r["round"]] += r["srv_total_ms"] / 1000
    v = list(byr.values())
    if v: print(f"| {e} | {len(v)} | {statistics.mean(v):.1f} | {statistics.pstdev(v):.1f} | {cold:.1f} |")
