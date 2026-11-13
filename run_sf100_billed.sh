#!/bin/bash
# SF100 billed runs: transform block, then incremental-update block, each in its own clock-hour windows.
cd "$(dirname "$0")" && source .venv/bin/activate
python -m bench.isolated_block --workload pipeline --scale 100 --options D2,S1,S2 --d1 --repeats 3 --yes; echo "pipeline block exit $?"
python -m bench.isolated_block --workload cdc --scale 100 --options D2,S1,S2 --d1 --repeats 3 --yes; echo "cdc block exit $?"
echo "ALL BLOCKS DONE $(date -u)"
