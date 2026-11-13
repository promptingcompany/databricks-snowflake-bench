"""Run isolated windows for several options at the same time (separate warehouses, so they do not interfere), plus optional
Databricks serverless Jobs runs (billed per job run, no window needed).
Usage: python -m bench.isolated_block --workload di|cdc|pipeline --scale 10 --options D2,S1,S2 [--d1] [--repeats 3] [--yes] [--print]"""
import sys, subprocess
from .pipeline import opt
def main():
    wl, sc, opts = opt("--workload"), opt("--scale") or "10", (opt("--options") or "D2,S1,S2").split(",")
    reps = opt("--repeats") or "3"; extra = ["--yes"] if "--yes" in sys.argv else []
    if "--print" in sys.argv: extra.append("--print")
    procs = [subprocess.Popen([sys.executable, "-m", "bench.isolated_cost", "--workload", wl, "--option", o, "--scale", sc, "--repeats", reps] + extra) for o in opts]
    if "--d1" in sys.argv and "--print" not in sys.argv:
        mod = {"di": ["bench.di_d1", "--sf", sc], "cdc": ["bench.cdc_d1", "--scale", sc], "pipeline": ["bench.pipeline_d1", "--scale", sc]}[wl]
        procs.append(subprocess.Popen([sys.executable, "-m"] + mod + ["--runs", reps, "--submit"] + extra))
    sys.exit(max(p.wait() for p in procs))
if __name__ == "__main__": main()
