#!/bin/bash
# Item 1 prep: generate and upload the ETL source files at SF10 (not timed, not billed windows).
cd "$(dirname "$0")" && source .venv/bin/activate
python scripts/di_gen.py --sf 10; echo "di_gen exit $?"
python -m bench.di_upload --sf 10; echo "di_upload exit $?"
echo "DI PREP DONE $(date -u)"
