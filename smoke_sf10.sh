#!/bin/zsh
# SF10 smoke test: one transform run and one incremental-update run per option, options in parallel.
cd ~/databricks-bench/v1-reproducibility-pack
export PYTHONPATH=.
PY=.venv/bin/python
mkdir -p logs
run_opt() {
  local o=$1
  if [[ $o == D1 ]]; then
    $PY -m bench.pipeline_d1 --scale 10 --runs 1 --submit > logs/smoke-pipeline-$o.log 2>&1; echo "pipeline $o exit $?"
    $PY -m bench.cdc_d1 --scale 10 --runs 1 --submit > logs/smoke-cdc-$o.log 2>&1; echo "cdc $o exit $?"
  else
    $PY -m bench.pipeline --option $o --scale 10 --runs 1 > logs/smoke-pipeline-$o.log 2>&1; echo "pipeline $o exit $?"
    $PY -m bench.cdc --option $o --scale 10 > logs/smoke-cdc-$o.log 2>&1; echo "cdc $o exit $?"
  fi
}
for o in D2 D1 S1 S2; do run_opt $o & done
wait
echo "smoke done"
