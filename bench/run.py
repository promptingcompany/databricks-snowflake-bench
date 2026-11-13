"""Paired benchmark runner. Same queries to both engines, order rotated per round.
Round 0 is a cold run (after warehouse idle/resume); rounds 1..N are warm and measured.
Failures stay in the denominator (openbenchmarks convention).
Usage: python -m bench.run [--dry-run] [--engines snowflake] [--queries 1,3,7] [--rounds N]"""
import json, sys, time, pathlib, yaml, statistics
from dotenv import load_dotenv
from .engines import ENGINES

def main():
    load_dotenv()
    cfg = yaml.safe_load(open("config.yaml"))
    dry = "--dry-run" in sys.argv
    def opt(flag, cast=str):
        return cast(sys.argv[sys.argv.index(flag) + 1]) if flag in sys.argv else None
    names = opt("--engines").split(",") if opt("--engines") else list(ENGINES)
    if opt("--queries"): cfg["query_subset"] = [int(x) for x in opt("--queries").split(",")]
    if opt("--rounds", int) is not None: cfg["rounds"] = opt("--rounds", int)
    qs = sorted(pathlib.Path("queries").glob(f"{cfg['suite']}_q*.sql"))
    if cfg["query_subset"]:
        qs = [q for q in qs if int(q.stem.split("_q")[1]) in cfg["query_subset"]]
    out = pathlib.Path("results") / f"run-{time.strftime('%Y%m%d-%H%M%S')}.jsonl"
    engines = {} if dry else {n: ENGINES[n](cfg[n]) for n in names}
    for r in range(cfg["rounds"] + 1):  # round 0 = cold
        order = names[r % len(names):] + names[:r % len(names)]  # rotate launch order
        for q in qs:
            base = q.read_text()
            for n in order:
                sql = ENGINES[n].translate(ENGINES[n], base)
                if dry:
                    print(f"[dry] r{r} {q.stem} {n}: {sql[:60]!r}..."); continue
                rec = {"query": q.stem, "engine": n, "round": r, "cold": r == 0}
                try:
                    dt, qid, nrows, dg = engines[n].run(sql)
                    rec.update(ok=True, seconds=dt, query_id=qid, rows=nrows, digest=dg)
                except Exception as e:
                    rec.update(ok=False, error=str(e)[:300])
                with open(out, "a") as f: f.write(json.dumps(rec) + "\n")
                print(rec["query"], n, r, rec.get("seconds") or rec.get("error"))
    for e in engines.values(): e.close()
    print("wrote", out)

if __name__ == "__main__": main()
