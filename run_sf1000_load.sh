#!/bin/bash
# Load SF1000 tables on both platforms, then rerun the Snowflake Medium windows that overlapped the upload.
cd "$(dirname "$0")" && source .venv/bin/activate
cp config.sf1000.yaml config.yaml
python -m bench.load --local --skip-put > ../sf1000_load.log 2>&1; echo "sf1000 load exit $?"
cp config.sf100.yaml config.yaml
python -m bench.isolated_block --workload di --scale 10 --options S1 --repeats 3 --yes; echo "S1 di rerun exit $?"
for sfx in _A _C; do
  BENCH_SRC_SUFFIX=$sfx python -m bench.isolated_block --workload pipeline --scale 100 --options S1 --repeats 3 --yes; echo "S1 pipeline$sfx rerun exit $?"
  BENCH_SRC_SUFFIX=$sfx python -m bench.isolated_block --workload cdc --scale 100 --options S1 --repeats 3 --yes; echo "S1 cdc$sfx rerun exit $?"
done
echo "ALL DONE $(date -u)"
