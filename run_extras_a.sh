#!/bin/bash
# Item 3: D1 steadiness, 10 runs each at SF100 (billed by job run id, no window needed).
cd "$(dirname "$0")" && source .venv/bin/activate
python -m bench.pipeline_d1 --scale 100 --runs 10 --submit --yes; echo "d1 pipeline x10 exit $?"
python -m bench.cdc_d1 --scale 100 --runs 10 --submit --yes; echo "d1 cdc x10 exit $?"
echo "EXTRAS A DONE $(date -u)"
