"""Summarise CDC runs: checkpoint agreement across options, then time per batch and an ESTIMATED list-price cost.
Usage: python -m bench.cdc_report results/cdc-*.enriched.jsonl   (billing/metering is the real cost source)"""
import json, sys, statistics, yaml
from collections import defaultdict
cfg = yaml.safe_load(open("config.yaml"))
RATE = {"D1": None, "D2": cfg["databricks"]["dbu_per_hour"] * cfg["databricks"]["usd_per_dbu"],
        "S1": cfg["snowflake"]["credits_per_hour"] * cfg["snowflake"]["usd_per_credit"],
        "S2": 2 * cfg["snowflake"]["credits_per_hour"] * cfg["snowflake"]["usd_per_credit"]}
recs = [json.loads(l) for f in sys.argv[1:] for l in open(f)]
sec = lambda r: r["srv_total_ms"] / 1000 if r.get("srv_total_ms") is not None else r["seconds"]
by = defaultdict(list)
for r in recs: by[r["option"]].append(r)
print("## Checkpoints")
ref = None
for o in sorted(by):
    chk = {r["batch"]: r["checksums"] for r in by[o] if r["step"] == "CHECK"}
    failed = [r["step"] for r in by[o] if r.get("ok") is False]
    cons = all(c["t2_matches_t1"] for c in chk.values())
    if ref is None: ref = chk
    same = all(chk.get(k) and {x: chk[k][x] for x in ("t1", "t2")} == {x: ref[k][x] for x in ("t1", "t2")} for k in ref)
    print(f"- {o}: checkpoints {sorted(chk)} | matches first option: {same} | T2 total equals T1 total: {cons} | failed steps: {failed or 'none'}")
print("\n## Time (server side where available)")
print("| Option | Base build (s) | Batch 1 (s) | Batches 2+ mean (s) | All 10 batches (s) | Est. list-price cost of all work (USD) |")
print("|---|---:|---:|---:|---:|---:|")
for o in sorted(by):
    base = sum(sec(r) for r in by[o] if r["step"].startswith("BASE") and r.get("ok"))
    per = defaultdict(float)
    for r in by[o]:
        if r.get("batch") and r["step"] != "CHECK" and r.get("ok"): per[r["batch"]] += sec(r)
    b1 = per.get(1, 0); rest = [v for k, v in per.items() if k > 1]
    allb = sum(per.values())
    cost = "billing" if RATE[o] is None else f"{(base + allb) / 3600 * RATE[o]:.3f}"
    print(f"| {o} | {base:.1f} | {b1:.1f} | {statistics.mean(rest) if rest else 0:.1f} | {allb:.1f} | {cost} |")
print("\nCost basis: server-side time x hourly list rate. Excludes idle and resume time; reconcile from billing or metering before publishing.")
