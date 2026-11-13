"""Summarise DI-style runs: correctness first (reconcile, audits, checkpoint agreement), then time by phase and batch.
Usage: python -m bench.di_report results/di-*.jsonl   (use enriched files for D2/S1 server-side times; D1 times are in-notebook)
Cost: list-price ESTIMATE for D2/S1/S2 from summed step time; D1 is shown as 'billing' (reconcile by job_run_id)."""
import json, sys, yaml
from collections import defaultdict
cfg = yaml.safe_load(open("config.yaml"))
RATE = {"D1": None, "D2": cfg["databricks"]["dbu_per_hour"] * cfg["databricks"]["usd_per_dbu"],
        "S1": cfg["snowflake"]["credits_per_hour"] * cfg["snowflake"]["usd_per_credit"],
        "S2": 2 * cfg["snowflake"]["credits_per_hour"] * cfg["snowflake"]["usd_per_credit"]}
recs = [json.loads(l) for f in sys.argv[1:] for l in open(f)]
sec = lambda r: r["srv_total_ms"] / 1000 if r.get("srv_total_ms") is not None else r["seconds"]
by = defaultdict(list)
for r in recs: by[r["option"]].append(r)
print("## Correctness")
ref = None
for o in sorted(by):
    rs = by[o]
    fail = [r["step"] for r in rs if r.get("ok") is False and "error" in r]
    rec = [r for r in rs if r["step"] == "RECONCILE"]; au = [r for r in rs if r["step"] == "AUDIT"]
    chk = {r["batch"]: r["checksums"] for r in rs if r["step"] == "CHECK"}
    if ref is None: ref = chk
    same = bool(chk) and all(chk.get(b) == ref.get(b) for b in ref)
    print(f"- {o}: failed steps {fail or 'none'} | reconcile {sum(1 for r in rec if r['ok'])}/{len(rec)} ok | audit violations {sum(r['violations'] for r in au)} over {len(au)} checks | checkpoints match first option: {same}")
print("\n## Server-side time by phase (s), summed over each batch")
print("| Option | Batch 1 ingest | Batch 1 transform | Batch 2 ingest | Batch 2 transform | Batch 3 ingest | Batch 3 transform | Total (s) | Est. list-price cost (USD) |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for o in sorted(by):
    t = defaultdict(float)
    for r in by[o]:
        if r.get("ok") and r.get("phase") in ("INGEST", "TRANSFORM"): t[(r["batch"], r["phase"])] += sec(r)
    tot = sum(t.values()); cost = "billing" if RATE[o] is None else f"{tot / 3600 * RATE[o]:.3f}"
    cells = " | ".join(f"{t[(b, p)]:.1f}" for b in (1, 2, 3) for p in ("INGEST", "TRANSFORM"))
    print(f"| {o} | {cells} | {tot:.1f} | {cost} |")
print("\nCost basis: step time x hourly list rate; excludes idle, resume and setup. Reconcile from billing or metering before publishing.")
