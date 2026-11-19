# Databricks Snowflake Bench

[![Workloads](https://img.shields.io/badge/workloads-4-000000?style=for-the-badge)](#workloads)
[![Data](https://img.shields.io/badge/data-10_GB_to_1_TB-000000?style=for-the-badge)](#results)
[![License](https://img.shields.io/badge/license-MIT-000000?style=for-the-badge)](LICENSE)

Databricks Snowflake Bench measures what the same data pipeline costs on Databricks and on Snowflake. Every cost comes from each platform's own billing tables, not from list-price math on run time.

Most warehouse comparisons time queries and multiply by an hourly rate. That misses start-up, idle time and the minimum billing increment. We ran each workload alone on a stopped warehouse and read what the platform actually billed for that window.

## Workloads

| Workload | What runs | Code | Scales run |
|---|---|---|---|
| Analytical query suite | 99 TPC-DS-derived queries, 1 cold and 3 warm rounds | `queries/`, `bench/run.py` | 10 GB |
| Transformation | 6 SQL steps: a wide join, a daily rollup, a customer view, a change set, a merge and a window step | `pipeline/`, `bench/pipeline.py` | 10 GB, 100 GB, 1 TB |
| Incremental update | Build a change set, merge it into existing tables, then refresh the tables built on them | `pipeline_cdc/`, `bench/cdc.py` | 10 GB, 100 GB, 1 TB |
| ETL from raw files | Parse raw files, then build dimension and fact tables | `pipeline_di/`, `bench/di_etl.py` | 10 GB |

Each workload ran on four setups:

| Label | Platform | Compute | List price |
|---|---|---|---|
| D1 | Databricks | Serverless Jobs, standard performance mode, notebook task | $0.35 per DBU, billed per job run |
| D2 | Databricks | Serverless SQL warehouse, Medium (24 DBU/h) | $0.70 per DBU |
| S1 | Snowflake | Enterprise, Medium warehouse (4 credits/h) | $3 per credit |
| S2 | Snowflake | Enterprise, Large warehouse (8 credits/h), the closest price match to D2 | $3 per credit |

SQL is written once in DuckDB dialect and translated per platform with sqlglot (`bench/engines.py`). Only the file ingest SQL differs, because parsing is part of what is measured.

## Results

### Billed cost per run, 2026-11-10 to 2026-11-12

Three runs per setup, in US dollars at list price. Snowflake counts compute credits only; cloud services credits are recorded in each row.

| Workload | Data | Run date (UTC) | D1 | D2 | S1 | S2 | S1 ÷ D1 |
|---|---|---|---|---|---|---|---|
| Transformation | 100 GB | 2026-11-10 | $0.14 | $0.46 | $0.38 | $0.58 | 2.6x |
| Transformation | 1 TB | 2026-11-12 | $1.25 | $1.19 | $1.85 | $2.15 | 1.5x |
| Incremental update | 100 GB | 2026-11-10 | $0.24 | $0.93 | $1.26 | $1.94 | 5.1x |
| Incremental update | 1 TB | 2026-11-12 | $2.91 | $3.02 | $7.39 | $7.72 | 2.5x |
| ETL from raw files | 10 GB | 2026-11-10 (S1 2026-11-11) | $0.23 | $1.46 | $1.17 | $2.26 | 5.1x |
| Incremental update, tuned tables | 100 GB | 2026-11-10 (S1 2026-11-11) | $0.25 | $0.99 | $0.60 | $1.12 | 2.4x |

Serverless Jobs (D1) was the cheapest setup in 5 of 6 rows. At 1 TB the SQL warehouse (D2) beat it on transformation by 6 cents. The gap to Snowflake Medium narrowed as data grew: 2.6x at 100 GB and 1.5x at 1 TB on transformation, so do not assume it holds past 1 TB.

All 12 runs in each row produced the same output (same row counts, column sums and result hash).

### Time per run, same windows

Mean seconds per run. D1 shows two numbers: time spent running SQL, and the whole job including start-up (126 to 315 seconds of setup per job).

| Workload | Data | D1 work | D1 job | D2 | S1 | S2 |
|---|---|---|---|---|---|---|
| Transformation | 100 GB | 169 s | 603 s | 61 s | 59 s | 41 s |
| Transformation | 1 TB | 606 s | 1,008 s | 215 s | 381 s | 213 s |
| Incremental update | 100 GB | 337 s | 757 s | 156 s | 252 s | 189 s |
| Incremental update | 1 TB | 1,138 s | 1,559 s | 595 s | 1,604 s | 825 s |
| ETL from raw files | 10 GB | 348 s | 874 s | 240 s | 223 s | 208 s |
| Incremental update, tuned tables | 100 GB | 341 s | 744 s | 167 s | 107 s | 97 s |

D1 was the slowest setup in 5 of 6 rows by whole-job time; Snowflake Medium was slower on incremental update at 1 TB. D1 trades minutes of start-up for a lower rate and per-job billing.

### Query suite timings, 2026-10-12 and 2026-10-21

The first round ran the 99-query suite and every workload at 10 GB on trial accounts. Those timings are in the [October report](https://reports.promptingco.com/databricks/databricks-vs-snowflake/) and the raw files in `results/reported/`. The query suite was not billed.

## Quickstart

Before you start, have these ready:

- a Databricks workspace with a serverless SQL warehouse and the `billing` system schema enabled
- a Snowflake Enterprise account with a Medium and a Large warehouse
- Python 3.13 (the runs used 3.13.16, pinned in `.python-version`)
- a build of [tpcds-kit](https://github.com/gregrahn/tpcds-kit) (commit 5a3a817)

Check every number without an account. This recomputes each cost from the billed units and each time from the run logs, and confirms that every run in a row produced the same output:

```bash
git clone https://github.com/promptingcompany/databricks-snowflake-bench && cd databricks-snowflake-bench
python3 scripts/verify_writeup.py           # writes results/writeup-verified.json
```

Set up both accounts and your credentials:

```bash
# edit the placeholders, then run each file in that platform's SQL editor
#   setup/snowflake_setup.sql   database, warehouses, key-pair user, grants
#   setup/databricks_setup.sql  catalog, grants, billing access, SQL warehouse
cp env.example .env                          # Databricks token, Snowflake key path
```

Snowflake uses key-pair login, not a password. Set `SNOWFLAKE_PRIVATE_KEY_PATH` to an unencrypted PKCS8 PEM key; the commands to make it are in `setup/snowflake_setup.sql`.

Generate and load 10 GB of data:

```bash
python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt
KIT=/path/to/tpcds-kit SF=10 ./scripts/gen_tpcds.sh
python scripts/di_gen.py --sf 10 --seed 42   # ETL source files
python scripts/upload_databricks.py 10       # Parquet into a Unity Catalog volume
python -m bench.load --local                 # both platforms
python -m bench.di_upload --sf 10            # ETL source files, both platforms
```

Run one workload and read its billed cost:

```bash
python -m bench.isolated_block --workload pipeline --scale 10 --options D2,S1,S2 --repeats 3 --yes
python -m bench.isolated_reconcile --json results/my-costs.json
```

Each run spends real credits on both platforms. Billing rows arrive about 3 hours (Snowflake) to 4 hours or more (Databricks) after the run. Add `--print` to most commands to see the SQL without running it. Scales of 100 and 1000 need `--yes`, and 1 TB needs `BENCH_STATEMENT_TIMEOUT_S=3600` (`run_sf1000_billed.sh` sets it).

The other workloads:

```bash
python -m bench.run                          # query suite, all rounds
python -m bench.pipeline --option D2 --scale 10 --runs 3
python -m bench.cdc --option S1 --scale 10
python -m bench.di_etl --option S2 --sf 10
```

The D1 setup runs through the `*_d1.py` modules with `--submit`.

## How cost is measured

- **One window per warehouse setup.** Each D2, S1 and S2 block of three runs starts at the top of an hour from a stopped warehouse, runs alone on it, and waits for auto-stop (1 minute). `bench/isolated_cost.py` runs it, and `bench/isolated_block.py` runs several setups at once.
- **D1 has no warehouse.** Serverless Jobs bills each job run on its own, so each D1 run's cost is read per job run.
- **Cost comes from billing tables.** Databricks `system.billing.usage` and Snowflake `WAREHOUSE_METERING_HISTORY`, for every hour the window spans (`bench/isolated_reconcile.py`). That includes start-up, gaps between statements and the idle tail.
- **A window is rejected** if any other statement ran on the same warehouse, a run exits non-zero or times out, or one run takes more than twice the fastest. Rejected windows are in `results/discarded/`, and the Snowflake Medium windows rerun after a rejection are in `results/s1-clean-reruns.json`.
- **Outputs must match.** Every run records row counts and column sums. A row counts only if all 12 runs agree.
- **Query suite time is server-side** execution time from each platform's query history (`bench/enrich.py`), with result caches off on both.

To check one number by hand: transformation at 100 GB on Snowflake Medium billed 0.3765 credits for the 10:00 to 11:00 UTC hour on 2026-11-10, across three runs. 0.3765 × $3 ÷ 3 = $0.38 per run. The serverless Jobs rows for the same workload billed 0.4172, 0.4151 and 0.4047 DBU, one per job run, so × $0.35 = $0.146, $0.145 and $0.142, mean $0.14.

To reproduce the cost numbers you need `SELECT` on `system.billing.usage` and `system.billing.list_prices` in Databricks (plus workspace admin or CAN MANAGE on the warehouse, to see other users' statements), and `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database and `MONITOR` on the warehouses in Snowflake. The grants are in `setup/`.

## How we ran it

- **Two rounds.** 2026-10-12 and 2026-10-21 UTC on trial accounts, both in AWS us-east-2. 2026-11-10 10:01 to 2026-11-12 11:23 UTC on a Databricks Premium workspace in AWS us-west-2 and a Snowflake Enterprise account in AWS us-east-2.
- **No cross-region transfer billed.** Both platforms loaded the same Parquet files, produced once on a laptop and uploaded from local disk: into a Snowflake internal stage with `PUT`, and into a Unity Catalog volume on Databricks. (We first tried reading from S3. The Snowflake cross-account role never worked, so no reported run uses S3.)
- **Default settings.** Plain CTAS (Databricks) or `COPY INTO` (Snowflake), each platform's default statistics step, no clustering or hand tuning. The tuned-tables row comes from a separate track in `bench/tuned.py`.
- **No version numbers.** Both are managed services that update without a version bump, so the run dates above are the version record.

### Data

Generated with `dsdgen` from the community [tpcds-kit](https://github.com/gregrahn/tpcds-kit) source, commit 5a3a817, in 8 parallel chunks. `scripts/gen_tpcds.sh` runs it the way we did, and `scripts/dat_to_parquet.py` converts the output and scans for invalid sentinel values. We have not compared this build against the copy on tpc.org.

`dsdgen` crashes with a segmentation fault if `-dir` is a long path, so the script generates into `/tmp` and moves the files.

An earlier data set was discarded. DuckDB's generator wrote overflow values into one column of `web_sales` (145 rows at 10 GB), so those runs are not in any result. `docs/PHASE3_TUNED_AND_CDC_METHOD.md` describes the track that followed.

### Queries

The 99 query files are identical to the queries bundled with DuckDB's TPC-DS extension, with default substitution values. We did not regenerate them with TPC's `dsqgen`. The suite rotates query order each round, and failed queries count in the denominator.

### ETL workload

The ETL workload is our own design, modeled on the structure of data integration benchmarks. It is not TPC-DI and not a TPC-DI result. `scripts/di_gen.py` generates its source files deterministically from a seed, with no TPC code. The method is in `docs/PHASE4_DI_STYLE_ETL_METHOD.md`.

## Result files

| File | What it holds |
|---|---|
| `results/writeup-verified.json` | Every number in the Results tables, recomputed by `scripts/verify_writeup.py` |
| `results/sf100-costs.json` | Transformation and incremental update at 100 GB, every setup; D1 rows are one per job run |
| `results/sf1000-costs.json` | Per-run costs at 1 TB and the three D1 runs; raw rows in `results/sf1000-costs-raw.json` |
| `results/extras-costs.json` | Every window from 2026-11-10 to 2026-11-12, including ETL, tuned tables and rejected windows |
| `results/s1-clean-reruns.json` | Snowflake Medium windows rerun because the first was not clean (`"clean": false`) |
| `results/*-sf100-*.jsonl`, `results/*-sf1000-*.jsonl` | Per-statement logs for each run, with row counts and column sums |
| `results/reported/` | The 153 raw files from the October round, also bundled as `results/reported-results.tar.gz` |
| `results/reported/isolated2-windows.json`, `isolated2-costs-latest.json`, `isolated3-costs-snowflake.json`, `isolated2-variation.json`, `d1_billing.json` | October billing and variation summaries: valid windows, billed cost, run-to-run spread and Jobs cost per job run |

Raw files are JSON Lines, named `<workload>-<setup>-sf<scale>-<timestamp>.jsonl`, with timestamps in local SF time. Files ending in `.enriched.jsonl` add the server-side timings. The cost-per-DBU comments in `config*.yaml` date from the October round; the November cost files use the same list prices.

## What has not run yet

- ETL from raw files at 100 GB and 1 TB. It has run at 10 GB only.
- Tuned tables at 1 TB. They have run at 100 GB only.
- A billed run of the 99-query suite. Its 2026-10 timings are in `results/reported/`, but no billing window was recorded for it.

Still open before this is final: a review of the queries and SQL by an expert on each platform, and sharing the results with Snowflake ahead of public release, as its Acceptable Use Policy (section IV) requires. The TPC fair-use disclaimer is in the License section.

## Citation

If you use Databricks Snowflake Bench, please cite it with the "Cite this repository" button on GitHub or the metadata in [CITATION.cff](CITATION.cff).

## License

Code, SQL and results are [MIT](LICENSE) licensed. The files in `queries/` and the generated data are derived from the TPC-DS kit and remain subject to the TPC's license in [TPC-DS-EULA.txt](TPC-DS-EULA.txt). Built by [The Prompting Company](https://promptingcompany.com).

<sub>The Databricks Snowflake Bench is derived from the TPC-DS Benchmark and as such is not comparable to published TPC-DS results, as the Databricks Snowflake Bench results do not comply with the TPC-DS Specification.</sub>
