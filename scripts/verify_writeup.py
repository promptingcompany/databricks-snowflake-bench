"""Recompute every number in the November write-up from raw files: cost per run, time per run, and output checksums.
Usage: python3 scripts/verify_writeup.py   (reads results/, writes results/writeup-verified.json)"""
import json, glob, os, re, statistics as st
from datetime import datetime, timedelta, timezone

R = "results"
WINDOWS = json.load(open(f"{R}/sf1000-costs-raw.json"))
LOCAL = timezone(timedelta(hours=-8))  # file stamps are local SF time, PST after 2026-11-01

ROWS = [  # write-up row, workload, scale, D1 file, UTC start hour of the D2/S1/S2 windows (S1 clean rerun hour where it differs)
    ("Transformation, 100 GB", "pipeline", "100", "pipeline-D1-sf100-20261110014101.jsonl", {"D2": "2026-11-10T10", "S1": "2026-11-10T10", "S2": "2026-11-10T10"}),
    ("Transformation, 1 TB", "pipeline", "1000", "pipeline-D1-sf1000-20261112001656.jsonl", {"D2": "2026-11-12T09", "S1": "2026-11-12T09", "S2": "2026-11-12T09"}),
    ("Incremental update, 100 GB", "cdc", "100", "cdc-D1-sf100-20261110021142.jsonl", {"D2": "2026-11-10T11", "S1": "2026-11-10T11", "S2": "2026-11-10T11"}),
    ("Incremental update, 1 TB", "cdc", "1000", "cdc-D1-sf1000-20261112012155.jsonl", {"D2": "2026-11-12T10", "S1": "2026-11-12T10", "S2": "2026-11-12T10"}),
    ("ETL from raw files, 10 GB", "di", "10", "di-D1-sf10-20261110111628.jsonl", {"D2": "2026-11-10T20", "S1": "2026-11-11T19", "S2": "2026-11-10T20"}),
    ("Incremental update, tuned tables, 100 GB", "cdc", "100", "cdc-D1-sf100-20261110130610.jsonl", {"D2": "2026-11-10T22", "S1": "2026-11-11T21", "S2": "2026-11-10T22"}),
]

def rows_of(path): return [json.loads(l) for l in open(path) if l.strip()]

def run_totals(recs):
    """One entry per run: total seconds (TOTAL row, else sum of steps), ok flag, checksums."""
    runs = {}
    for r in recs:
        k = r.get("run", 1)
        e = runs.setdefault(k, {"steps": 0.0, "total": None, "ok": True, "checksums": None, "batch_checks": [], "setup_s": None, "exec_s": None,
                                "reconcile": [0, 0], "audit_checks": 0, "audit_violations": 0})
        if r.get("ok") is False: e["ok"] = False
        if r.get("step") == "TOTAL":
            e["total"] = r.get("seconds"); e["checksums"] = r.get("checksums")
        elif r.get("step") == "CHECK":
            e["batch_checks"].append(r.get("checksums"))
        elif r.get("step") == "RECONCILE":
            e["reconcile"][0] += bool(r.get("ok")); e["reconcile"][1] += 1
        elif r.get("step") == "AUDIT":
            e["audit_checks"] += 1; e["audit_violations"] += r.get("violations") or 0
        elif isinstance(r.get("seconds"), (int, float)):
            e["steps"] += r["seconds"]
        if r.get("setup_ms") is not None: e["setup_s"], e["exec_s"] = r["setup_ms"] / 1000, r["exec_ms"] / 1000
    for e in runs.values():
        if e["total"] is None: e["total"] = e["steps"]
        if e["checksums"] is None and e["batch_checks"]: e["checksums"] = e["batch_checks"]
    return [runs[k] for k in sorted(runs)]

def window(wl, opt, hour):
    m = [w for w in WINDOWS if w["workload"] == wl and w["option"] == opt and (w.get("start_utc") or "").startswith(hour)]
    assert len(m) == 1, (wl, opt, hour, len(m))
    return m[0]

def window_files(wl, opt, scale, w):
    lo = datetime.fromisoformat(w["start_utc"]) - timedelta(seconds=60); hi = datetime.fromisoformat(w["end_utc"])
    out = []
    for f in glob.glob(f"{R}/{wl}-{opt}-sf{scale}-*.jsonl"):
        m = re.search(r"-(\d{8})-(\d{6})\.jsonl$", f)
        if not m or "enriched" in f: continue
        t = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=LOCAL)
        if lo <= t <= hi: out.append(f)
    return sorted(out)

def canon(cs): return json.dumps(cs, sort_keys=True) if cs else None

report = []
for label, wl, scale, d1file, hours in ROWS:
    row = {"row": label}
    d1_bills = [w for w in WINDOWS if w["option"] == "D1" and w.get("file") == d1file]
    d1_runs = run_totals(rows_of(f"{R}/{d1file}"))
    assert len(d1_bills) == len(d1_runs) == 3, (label, len(d1_bills), len(d1_runs))
    costs = [w["cost_usd"] for w in d1_bills]
    row["D1"] = {"cost_runs": costs, "cost_per_run": st.mean(costs), "time_runs": [r["total"] for r in d1_runs],
                 "job_setup_s": [r["setup_s"] for r in d1_runs], "job_exec_s": [r["exec_s"] for r in d1_runs],
                 "all_ok": all(r["ok"] for r in d1_runs), "checksums": {canon(r["checksums"]) for r in d1_runs},
                 "reconcile": [r["reconcile"] for r in d1_runs], "audit": [(r["audit_checks"], r["audit_violations"]) for r in d1_runs]}
    for opt, hour in hours.items():
        w = window(wl, opt, hour)
        files = window_files(wl, opt, scale, w)
        runs = [r for f in files for r in run_totals(rows_of(f))]
        assert len(runs) == 3, (label, opt, files)
        row[opt] = {"window": f"{w['start_utc']} to {w['end_utc']}", "clean": w["clean"], "foreign": w.get("foreign_statements"),
                    "billed": w.get("billed"), "unit": w.get("unit"), "cost_per_run": w["cost_per_run_usd"],
                    "files": [os.path.basename(f) for f in files], "time_runs": [r["total"] for r in runs],
                    "all_ok": all(r["ok"] for r in runs), "checksums": {canon(r["checksums"]) for r in runs},
                    "reconcile": [r["reconcile"] for r in runs], "audit": [(r["audit_checks"], r["audit_violations"]) for r in runs]}
    sums = set().union(*(row[o]["checksums"] for o in ("D1", "D2", "S1", "S2")))
    row["outputs_match"] = len(sums) == 1 and None not in sums
    row["distinct_checksum_sets"] = len(sums)
    for o in ("D1", "D2", "S1", "S2"): row[o]["checksums"] = len(row[o]["checksums"])
    report.append(row)

for row in report:
    print(f"\n== {row['row']}  outputs match across all 12 runs: {row['outputs_match']} ({row['distinct_checksum_sets']} distinct)")
    for o in ("D1", "D2", "S1", "S2"):
        x = row[o]; t = x["time_runs"]
        extra = f" clean={x['clean']} foreign={x['foreign']} billed={x['billed']} {x['unit']}" if o != "D1" else f" job setup {x['job_setup_s']} exec {x['job_exec_s']} costs {x['cost_runs']}"
        if x["reconcile"][0][1]: extra += f" reconcile {x['reconcile']} audit(checks,violations) {x['audit']}"
        print(f"  {o}: ${x['cost_per_run']:.4f}/run  time {st.mean(t):7.1f}s (min {min(t):.1f}, max {max(t):.1f}, sd {st.stdev(t):.1f})  ok={x['all_ok']}{extra}")
    print(f"  ratio S1/D1 cost: {row['S1']['cost_per_run'] / row['D1']['cost_per_run']:.2f}x")
json.dump(report, open(f"{R}/writeup-verified.json", "w"), indent=1, default=list)

# Check 2: every cost rebuilt from billed units x list price
PRICE = {"D1": 0.35, "D2": 0.70, "S1": 3.0, "S2": 3.0}
print("\n== Check 2: cost = billed units x price")
for row in report:
    for o in ("D2", "S1", "S2"):
        x = row[o]; rebuilt = x["billed"] * PRICE[o] / 3
        assert abs(rebuilt - x["cost_per_run"]) < 0.002, (row["row"], o, rebuilt, x["cost_per_run"])
    d1 = [w for w in WINDOWS if w["option"] == "D1" and w["cost_usd"] in row["D1"]["cost_runs"]]
    for w in d1: assert abs(w["billed"] * 0.35 - w["cost_usd"]) < 0.001, w
    print(f"  ok  {row['row']}")

# Check 3: every numeric sentence on the page
by = {r["row"]: r for r in report}
c = lambda row, o: by[row][o]["cost_per_run"]
def d1_family(wl, files):
    return [w["cost_usd"] for w in WINDOWS if w["option"] == "D1" and w.get("file") in files]
print("\n== Check 3: prose claims")
t100 = d1_family("pipeline", ["pipeline-D1-sf100-20261110014101.jsonl", "pipeline-D1-sf100-20261110110521.jsonl"])
i100 = d1_family("cdc", ["cdc-D1-sf100-20261110021142.jsonl", "cdc-D1-sf100-20261110124201.jsonl"])
allr = t100 + i100
dev = max(max(abs(v / st.mean(g) - 1) for v in g) for g in (t100, i100))
print(f"  default-table D1 runs at 100 GB: {len(allr)} (transformation {len(t100)}, incremental {len(i100)}); max deviation from own mean {dev:.1%}")
t1k, i1k = by["Transformation, 1 TB"]["D1"]["cost_runs"], by["Incremental update, 1 TB"]["D1"]["cost_runs"]
dev1k = max(max(abs(v / st.mean(g) - 1) for v in g) for g in (t1k, i1k))
print(f"  D1 1 TB max deviation from mean: {dev1k:.1%}")
for a, b in (("Transformation, 100 GB", "Transformation, 1 TB"), ("Incremental update, 100 GB", "Incremental update, 1 TB")):
    print(f"  lead {a}: {c(a,'S1')/c(a,'D1'):.2f}x -> {c(b,'S1')/c(b,'D1'):.2f}x")
print(f"  D2 vs S1 losses: transform 100GB {c('Transformation, 100 GB','D2'):.3f} vs {c('Transformation, 100 GB','S1'):.3f}; "
      f"ETL {c('ETL from raw files, 10 GB','D2'):.3f} vs {c('ETL from raw files, 10 GB','S1'):.3f}; tuned {c('Incremental update, tuned tables, 100 GB','D2'):.3f} vs {c('Incremental update, tuned tables, 100 GB','S1'):.3f}")
print(f"  D2 1 TB transform win: {c('Transformation, 1 TB','D2'):.3f} vs S1 {c('Transformation, 1 TB','S1'):.3f}")
ctrl = {"D1": st.mean(d1_family("cdc", ["cdc-D1-sf100-20261110150620.jsonl"])),
        "D2": window("cdc", "D2", "2026-11-11T00")["cost_per_run_usd"], "S1": window("cdc", "S1", "2026-11-11T23")["cost_per_run_usd"]}
for o in ("D1", "D2", "S1"):
    tuned, base = c("Incremental update, tuned tables, 100 GB", o), c("Incremental update, 100 GB", o)
    print(f"  tuning {o}: default {base:.4f} -> tuned {tuned:.4f} ({tuned/base-1:+.1%}); rebuilt control {ctrl[o]:.4f} -> tuned ({tuned/ctrl[o]-1:+.1%})")

# Speed table for the write-up: mean of the 3 billed runs
def mmss(s): s = round(s); return f"{s // 60}:{s % 60:02d}"
print("\n== Speed (mean of 3 billed runs; work time = the workload's SQL steps, timed as they run)")
speed = []
for row in report:
    d1 = row["D1"]; wall = [a + b for a, b in zip(d1["job_setup_s"], d1["job_exec_s"])]
    rec = {"row": row["row"], "D1_work": st.mean(d1["time_runs"]), "D1_job_wall": st.mean(wall),
           "D1_setup_range": (min(d1["job_setup_s"]), max(d1["job_setup_s"]))}
    for o in ("D2", "S1", "S2"): rec[o] = st.mean(row[o]["time_runs"]); rec[o + "_sd"] = st.stdev(row[o]["time_runs"])
    rec["D1_work_sd"] = st.stdev(d1["time_runs"]); rec["fastest"] = min(("D1", rec["D1_work"]), ("D2", rec["D2"]), ("S1", rec["S1"]), ("S2", rec["S2"]), key=lambda t: t[1])[0]
    speed.append(rec)
    print(f"  {row['row']:<44} D1 work {mmss(rec['D1_work'])}  D1 job incl. start-up {mmss(rec['D1_job_wall'])}  "
          f"D2 {mmss(rec['D2'])}  S1 {mmss(rec['S1'])}  S2 {mmss(rec['S2'])}  | D1 start-up {rec['D1_setup_range']}  fastest {rec['fastest']}  "
          f"S1/D1work {rec['S1']/rec['D1_work']:.2f}  sd D1 {rec['D1_work_sd']:.0f} D2 {rec['D2_sd']:.0f} S1 {rec['S1_sd']:.0f} S2 {rec['S2_sd']:.0f}")
json.dump({"rows": report, "speed": speed}, open(f"{R}/writeup-verified.json", "w"), indent=1, default=list)
