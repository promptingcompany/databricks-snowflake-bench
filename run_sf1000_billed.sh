#!/bin/bash
# SF1000 billed runs: transform block, then incremental-update block, each in its own clock-hour windows.
cd "$(dirname "$0")" && source .venv/bin/activate
export BENCH_STATEMENT_TIMEOUT_S=3600
python -m bench.isolated_block --workload pipeline --scale 1000 --options D2,S1,S2 --d1 --repeats 3 --yes; echo "pipeline block exit $?"
python -m bench.isolated_block --workload cdc --scale 1000 --options D2,S1,S2 --d1 --repeats 3 --yes; echo "cdc block exit $?"
echo "ALL BLOCKS DONE $(date -u)"
