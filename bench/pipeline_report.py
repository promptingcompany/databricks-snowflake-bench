"""Summarise pipeline runs. Usage: python -m bench.pipeline_report results/pipeline-*.enriched.jsonl
Correctness first (checksums must match across options), then time and an ESTIMATED cost.
Estimate = pipeline server time x hourly list rate; real cost needs billing/metering data
(see design doc). Options with failed steps are shown as incomplete."""
import json, sys, statistics, yaml
from collections import defaultdict
cfg = yaml.safe_load(open("config.yaml"))
RATE = {"D1": None, "D2": cfg["databricks"]["dbu_per_hour"] * cfg["databricks"]["usd_per_dbu"],
        "S1": cfg["snowflake"]["credits_per_hour"] * cfg["snowflake"]["usd_per_credit"],
        "S2": 2 * cfg["snowflake"]["credits_per_hour"] * cfg["snowflake"]["usd_per_credit"]}
recs = [json.loads(l) for f in sys.argv[1:] for l in open(f)]
ms = lambda r: (r.get("srv_total_ms") / 1000) if r.get("srv_total_ms") is not None else r["seconds"]
runs = defaultdict(lambda: defaultdict(list))   # option -> run -> step records
for r in recs: runs[r["option"]][r["run"]].append(r)

print("## Correctness (checksums vs first option listed)")
ref = None
for o in sorted(runs):
    for run, rs in sorted(runs[o].items()):
        tot = next((r for r in rs if r["step"] == "TOTAL"), None)
        if not tot or not tot.get("ok"): print(f"- {o} run {run}: INCOMPLETE {[r.get('error','')[:80] for r in rs if not r['ok']][:1]}"); continue
        cs = tot["checksums"]; ref = ref or cs
        print(f"- {o} run {run}: {'matches reference' if cs == ref else 'DIFFERS from reference'}")
print("\n## Pipeline time and estimated cost (complete runs only)")
print("| Option | Runs | Mean pipeline (s) | SD (s) | Rate (USD/hr) | Est. cost per run (USD) |")
print("|---|---:|---:|---:|---:|---:|")
for o in sorted(runs):
    totals = [sum(ms(r) for r in rs if r["step"] not in ("TOTAL",) and r.get("ok")) for rs in runs[o].values()
              if any(r["step"] == "TOTAL" and r.get("ok") for r in rs)]
    if not totals: print(f"| {o} | 0 | - | - | - | incomplete |"); continue
    sd = statistics.pstdev(totals) if len(totals) > 1 else 0
    m = statistics.mean(totals)
    if RATE[o] is None:
        import os
        b = json.load(open("results/d1_billing.json"))["sf10"] if os.path.exists("results/d1_billing.json") else None
        cost = "pending billing" if not b else f"{statistics.mean(b['dbus_by_run'].values()) * b['rate_usd_per_dbu']:.3f} (measured)"
        print(f"| {o} | {len(totals)} | {m:.1f} | {sd:.1f} | $0.35/DBU | {cost} |"); continue
    print(f"| {o} | {len(totals)} | {m:.1f} | {sd:.1f} | {RATE[o]:.2f} | {m/3600*RATE[o]:.3f} |")
print("\n## Mean time per step (s)")
steps = ["T1", "T2", "T3", "T4", "T5"]
print("| Option | " + " | ".join(steps) + " |"); print("|---|" + "---:|" * len(steps))
for o in sorted(runs):
    cells = []
    for s in steps:
        v = [sum(ms(r) for r in rs if r["step"] == s and r.get("ok")) for rs in runs[o].values() if any(r["step"] == s and r.get("ok") for r in rs)]
        cells.append(f"{statistics.mean(v):.1f}" if v else "-")
    print(f"| {o} | " + " | ".join(cells) + " |")
