# Databricks v. Snowflake benchmark: how it was run

This folder holds the code, SQL, settings and raw result files behind two rounds of tests:

- **October 2026:** the report at https://reports.promptingco.com/databricks/databricks-vs-snowflake/ (query suite,
  transformation, incremental update and ETL at SF 10). Raw files in `results/reported/`.
- **November 2026:** billed cost per run for pipeline work at SF 10, SF 100 and SF 1000 (100 GB and 1 TB). Raw files
  and summaries at the top of `results/`. See "November 2026 results" below.

It lets you check how each number was produced and rerun the tests on your own accounts.

Credentials, private keys, cloud policy files and the generated data are not included. Everything here is read-only
reference unless you add your own `.env`.

## What was compared
| Label | Compute |
|---|---|
| D2 | Databricks serverless SQL warehouse, Medium (24 DBU/h, $0.70 per DBU) |
| D1 | Databricks serverless Jobs compute, standard performance mode, notebook task ($0.35 per DBU, billed per job run) |
| S1 | Snowflake Enterprise, Medium warehouse (4 credits/h, $3 per credit) |
| S2 | Snowflake Enterprise, Large warehouse (8 credits/h), the closest price match to D2 |

Regions and accounts, per round:

| Round | Databricks | Snowflake |
|---|---|---|
| October 2026 | Trial workspace, AWS us-east-2 | Trial account, Enterprise, AWS us-east-2 |
| November 2026 | Premium workspace, AWS us-west-2 | Enterprise account, AWS us-east-2 |

In November the two platforms ran in different regions. The Parquet files were uploaded to each platform from local disk,
so no cross-region data transfer was billed. Default settings unless a run is labelled tuned. Both platforms load the same Parquet files, produced once on a laptop and
uploaded from local disk: into a Snowflake internal stage with `PUT`, and into a Unity Catalog volume on Databricks.
(We first tried reading from an S3 bucket. The Snowflake cross-account role never worked, so the reported runs do not use S3.)
Tables are created with plain CTAS (Databricks) or `COPY INTO` (Snowflake), and each platform's default statistics step. There is no clustering or hand tuning
in the main runs. The separate tuned track is in `bench/tuned.py` and is reported separately.

## Workloads and where the code is
1. **Analytical query suite (99 queries).** `queries/tpcds_q01.sql` to `q99.sql`, run by `bench/run.py`.
2. **Transformation workload (6 steps).** `pipeline/*.sql`, run by `bench/pipeline.py` (`bench/pipeline_d1.py` for Jobs).
3. **Incremental update workload.** `pipeline_cdc/*.sql`, run by `bench/cdc.py` (`bench/cdc_d1.py` for Jobs).
4. **ETL from raw files.** Our own design, modeled on the structure of data integration benchmarks. It is not TPC-DI
   and not an official result. Generator: `scripts/di_gen.py` (deterministic from a seed, no TPC code). SQL:
   `pipeline_di/*.sql`, run by `bench/di_etl.py` (`bench/di_d1.py` for Jobs). Method: `docs/PHASE4_DI_STYLE_ETL_METHOD.md`.

SQL is written once in DuckDB dialect and translated per platform with sqlglot (`bench/engines.py`). Only the file
ingest SQL differs by platform, because parsing is part of what is measured.

## The TPC-DS suite in detail
- **Data:** generated with `dsdgen` built from the community `tpcds-kit` source (github.com/gregrahn/tpcds-kit, commit
  5a3a817). We have not compared that build against the copy on tpc.org. `scripts/gen_tpcds.sh` runs it the way we did
  (8 parallel chunks: `dsdgen -scale <SF> -dir <short path> -parallel 8 -child <1..8> -force`) and then
  `scripts/dat_to_parquet.py` converts the output to Parquet and scans for invalid sentinel values. Scales: SF 10, SF 100 and SF 1000 (about 10 GB, 100 GB and 1 TB of raw data).
  One gotcha: `dsdgen` crashes with a segmentation fault if `-dir` is a long path, so the script generates into `/tmp` and moves the files.
- **An earlier data set was discarded.** DuckDB's generator wrote overflow values into one column of `web_sales`
  (145 rows at SF 10). Those runs are not in the report. `docs/PHASE3_TUNED_AND_CDC_METHOD.md` describes the track
  that followed.
- **Queries:** the 99 files are identical to the 99 queries bundled with DuckDB's TPC-DS extension, which use the
  default substitution values. We did not regenerate them with TPC's `dsqgen`. They are TPC-DS-derived, not an
  official TPC-DS result.
- **Rounds:** round 0 is cold (after the warehouse has been idle), then three warm rounds. The report's suite times are
  the mean of the warm rounds. Query order is rotated per round. Failures count in the denominator.
- **Timing:** server-side execution time from each platform's query history (`bench/enrich.py`), not client-side time.
  Result caches are off on both.
- **Correctness gate:** each query's result is hashed (`digest()` in `bench/engines.py`) and compared across platforms
  by row count, column sums and a result hash. A run counts only if they match.

## Billed cost
Each three-run block (`bench/isolated_cost.py`, several options at once through `bench/isolated_block.py`) starts at
the top of an hour from a stopped warehouse, runs alone on that warehouse, and waits for auto-stop (1 minute). Cost is
read from Databricks `system.billing.usage` and Snowflake `WAREHOUSE_METERING_HISTORY` for the hours the window spans
(`bench/isolated_reconcile.py`). That includes start-up, gaps between statements and the idle tail. The reconcile step
flags any statement on the same warehouse outside the window and rejects the window if it finds one. A window is also
invalid if a run exits non-zero, times out, or one run takes more than twice the fastest.

Windows that were rejected or discarded are in `results/discarded/`. The report's Method section describes the rules.

**Permissions you need to reproduce the cost numbers** (details and SQL in `setup/`):
- Databricks: `SELECT` on `system.billing.usage` and `system.billing.list_prices`, which needs the `billing` system schema enabled
  by an admin. To see other users' statements on the warehouse (the contamination check), the caller must be a workspace
  admin or have CAN MANAGE on it. Billing rows arrive about 4 hours or more after the run.
- Snowflake: `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database (for `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`), and `MONITOR` on
  the warehouses. Metering arrives about 3 hours after the run.

## Rerun it
**1. Set up the accounts.** Run `setup/snowflake_setup.sql` and `setup/databricks_setup.sql` (edit the placeholders). They cover
the Snowflake database, schemas, Medium and Large warehouses, key-pair user and grants, and the Databricks catalog, grants,
billing access and the serverless SQL warehouse. The warehouse itself and its auto-stop are set in the Databricks UI or API.

**2. Credentials.** Copy `env.example` to `.env` (the same content is in the hidden `.env.example`).
- Databricks uses a personal access token. Give the host without `https://` and the warehouse HTTP path.
- **Snowflake uses key-pair login, not a password.** Set `SNOWFLAKE_PRIVATE_KEY_PATH` to an unencrypted PKCS8 PEM key. The commands
  to make the key and register its public half are in `setup/snowflake_setup.sql`.

**3. Install and generate data.**
```
python -m venv .venv && .venv/bin/pip install -r requirements.txt
KIT=/path/to/tpcds-kit SF=10 ./scripts/gen_tpcds.sh          # after building the kit: cd $KIT/tools && make OS=MACOS (or LINUX)
python scripts/di_gen.py --sf 10 --seed 42                    # ETL workload data (seed 42 is the default)
```
**4. Load.** `config.yaml` holds the database, catalog and schema names; edit it for your scale.
```
python scripts/upload_databricks.py 10      # Parquet into a Unity Catalog volume
python -m bench.load --local                # both engines; Snowflake PUTs into an internal stage
python -m bench.di_upload --sf 10           # ETL source files, both engines
```
**5. Run.**
```
python -m bench.run                          # query suite, all rounds
python -m bench.enrich results/run-<stamp>.jsonl
python -m bench.report results/run-<stamp>.enriched.jsonl
python -m bench.pipeline --option D2 --scale 10 --runs 3
python -m bench.cdc --option S1 --scale 10
python -m bench.di_etl --option S2 --sf 10
python -m bench.isolated_block --workload pipeline --scale 10 --options D2,S1,S2 --repeats 3 --yes
python -m bench.isolated_reconcile --json results/my-costs.json
```
Add `--print` to most commands to see the SQL without running it. Scales of 100 and 1000 need `--yes`. At SF 1000, set
`BENCH_STATEMENT_TIMEOUT_S=3600` (the default per-statement limit is 600 seconds); `run_sf1000_billed.sh` does this. The Databricks serverless Jobs
option (D1) is the `*_d1.py` modules with `--submit`.

## Result files
The raw files are JSON Lines (`.jsonl`; one JSON object per line) in `results/reported/` (153 files). If your tool skips that
extension, the same files are bundled in `results/reported-results.tar.gz`. They are named `<workload>-<option>-sf<scale>-<timestamp>.jsonl`. Files ending in
`.enriched.jsonl` add the server-side timings. Billing and variation summaries:
`isolated2-windows.json` (the valid windows), `isolated2-costs-latest.json` and `isolated3-costs-snowflake.json`
(billed cost), `isolated2-variation.json` (run-to-run spread), `d1_billing.json` (Jobs cost per job run).
`results/discarded/` holds the rejected windows.

## November 2026 results
Billed cost per run, US dollars, list prices ($0.35 per DBU for serverless Jobs, $0.70 per DBU for the SQL warehouse,
$3 per Snowflake credit). Snowflake cost counts compute credits only; cloud services credits are recorded separately in each row.

| File | What it holds |
|---|---|
| `results/sf100-costs.json` | Transformation and incremental update at SF 100, every option. D1 rows are one per job run. |
| `results/sf1000-costs.json` | Summary at SF 1000 (per-run costs and the three D1 runs). Raw rows: `results/sf1000-costs-raw.json` |
| `results/extras-costs.json` | Every November window, including ETL at SF 10, tuned tables and windows later rejected |
| `results/s1-clean-reruns.json` | The Snowflake Medium windows rerun because the first window was not clean (`"clean": false`) |
| `results/*-sf100-*.jsonl`, `results/*-sf1000-*.jsonl` | Raw per-statement logs for each run, with row counts and column sums |

**Check every number.** `python3 scripts/verify_writeup.py` recomputes each cost in the November write-up from the billed units, each time from the run logs, and confirms that all 12 runs in each row produced the same output. It reads only `results/` and needs no accounts. Output: `results/writeup-verified.json`.

**Check one number.** Transformation at SF 100 on Snowflake Medium: in `results/sf100-costs.json`, the `pipeline` / `S1`
row shows `billed: 0.3765` credits for the 10:00 to 11:00 UTC hour on 2026-11-10, three runs, no foreign statements.
0.3765 × $3 ÷ 3 = $0.38 per run. The serverless Jobs rows for the same workload show 0.4172, 0.4151 and 0.4047 DBU,
one per job run: × $0.35 = $0.146, $0.145 and $0.142, mean $0.14.

The cost-per-DBU comments in `config*.yaml` were written in the October round (us-east-2). The November cost files apply
the same rates, which are the list prices shown in the November write-up.

## Not yet in these files
- SF 100 and SF 1000 for the ETL workload. It has only run at SF 10.
- Tuned tables at SF 1000. They have only run at SF 100.
- The query suite has not been billed in November. Its October timings are in `results/reported/`.

## Open before this is final
Independent review of the queries and SQL by an expert on each platform, and a check of both vendors' terms and TPC's
fair-use policy on the "TPC-DS-derived" wording.
