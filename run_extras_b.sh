#!/bin/bash
# Item 4 builds (tuned SF100 tables), then billed windows: ETL at SF10 (item 1), then transform + CDC on tuned (_A) and control (_C) tables.
cd "$(dirname "$0")" && source .venv/bin/activate
for v in D-A D-C S-A S-C; do python -m bench.tuned --variant $v --scale 100 --build --yes > logs/tuned-build-$v.log 2>&1 & done
wait; echo "tuned builds done $(date -u)"; grep -c '"ok": false' results/tuned-*-sf100-*.jsonl
until grep -q "DI PREP DONE" logs/di-prep.log; do sleep 30; done; echo "di prep done"
python -m bench.isolated_block --workload di --scale 10 --options D2,S1,S2 --d1 --repeats 3 --yes; echo "di block exit $?"
for sfx in _A _C; do
  BENCH_SRC_SUFFIX=$sfx python -m bench.isolated_block --workload pipeline --scale 100 --options D2,S1,S2 --d1 --repeats 3 --yes; echo "pipeline$sfx block exit $?"
  BENCH_SRC_SUFFIX=$sfx python -m bench.isolated_block --workload cdc --scale 100 --options D2,S1,S2 --d1 --repeats 3 --yes; echo "cdc$sfx block exit $?"
done
echo "EXTRAS B DONE $(date -u)"
