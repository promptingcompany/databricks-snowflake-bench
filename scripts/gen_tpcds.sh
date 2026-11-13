#!/usr/bin/env bash
# Generate TPC-DS data with the official dsdgen from the tpcds-kit source, then convert it to Parquet.
# Usage: KIT=/path/to/tpcds-kit SF=100 ./scripts/gen_tpcds.sh      (run from the repo root)
# Output: data/official_raw/sf<SF>/<table>_<i>_8.dat, then data/sf<SF>/<table>/*.parquet
# Kit used: https://github.com/gregrahn/tpcds-kit at commit 5a3a817. Build it first:
#   cd "$KIT/tools" && make OS=MACOS      # or: make OS=LINUX
set -euo pipefail
: "${KIT:?set KIT to your tpcds-kit checkout}"; : "${SF:?set SF to the scale factor, for example 10 or 100}"
CHILDREN="${CHILDREN:-8}"; DEST="$(pwd)/data/official_raw/sf${SF}"; mkdir -p "$DEST"
# dsdgen crashes (segmentation fault) when -dir is a long path, so generate into a short path and move the files after.
GEN="${GEN:-/tmp/tpcds_sf${SF}}"; mkdir -p "$GEN"
cd "$KIT/tools"   # dsdgen reads tpcds.idx from its working directory
for i in $(seq 1 "$CHILDREN"); do ./dsdgen -scale "$SF" -dir "$GEN" -parallel "$CHILDREN" -child "$i" -force; done
cd - >/dev/null
mv "$GEN"/*.dat "$DEST"/
python scripts/dat_to_parquet.py "$SF"
