# Phase 4 method: DI-style ETL benchmark

Status: draft for review. Written 2026-10-12. Fixed before any code is written or run. Changes after approval go in the change log with date and reason, and earlier results are still reported.

## Purpose
Measure end to end data integration cost and time on each platform: raw source files in, a historized warehouse model out, with incremental batches and audit checks. The shape is modeled on the general structure of TPC-DI (multiple file formats, a historical load, incremental batches, slowly changing dimensions, a trades fact, audits).

## What this is not
- It is not TPC-DI, and no result from it is a TPC-DI result. We do not use DIGen, PDGF, TPC's data files, or any code from third-party TPC-DI implementations. The data generator and all SQL are written by us.
- It must never be labeled as TPC-DI, or as compliant with it. The description to use is: "an ETL benchmark modeled on the structure of TPC-DI, not a TPC-DI result."
- Before anything is published, a person qualified to judge reads TPC's fair-use policy and this wording. Not done yet.

## Rules for every run
1. Method fixed before running. Every planned test is run and every result reported.
2. Same generated data, same logic, same outputs on both platforms. A cross-platform correctness gate (below) decides whether a run counts.
3. Documented defaults only. Any tuning is added later as a separately labeled track using the Phase 3 rules.
4. One run at a time on idle compute, each in a recorded UTC window.
5. Cost comes from billing and metering tables. List-price estimates appear only if billing is unavailable, and are labeled as estimates.
6. A reviewer who knows Snowflake and a reviewer who knows Databricks read this method and the SQL before any numbers are published.
7. Where syntax must differ between platforms, the difference is listed in the write-up. Platform-native file readers are used for parsing, since parsing is part of what is being measured.

## Data
Our own synthetic generator, deterministic from a seed, producing files locally. Files are placed into each platform's own staging area (a Snowflake stage, a Databricks volume) before the timed run. The upload from the laptop is not timed.

### Scale
One size parameter, SF, counts customers: SF 1 is 100,000 customers.

| Entity | Rows per SF unit of 100k customers (approximate) |
|---|---|
| Customers | 100,000 |
| Accounts | 200,000 |
| Securities | 6,000 (fixed, not scaled) |
| Trades (batch 1) | 2,000,000 |
| Daily market rows | about 7.5M (6,000 securities over about 1,250 trading days, fixed) |

Planned sizes: SF 1 for code checks, SF 10 for dry runs, SF 100 for measured runs. Exact row counts will be recorded with the generator version. Sizes are my estimate and have not been measured.

### Source files and formats
| Source | Format | Notes |
|---|---|---|
| Customers | JSON lines | Nested contact and tax records |
| Accounts | pipe delimited | |
| Securities and companies | fixed width text | Parsed by position |
| Trades and trade history | CSV | |
| Daily market | CSV | |
| Reference tables (trade types, statuses, industries, tax rates, calendar) | CSV, small | |

XML is excluded on purpose: its handling is highly vendor specific and would dominate the comparison.

## Batches
- **Batch 1:** the full history load.
- **Batch 2 and Batch 3:** incremental. Each changes about 30 percent of customers as updates (a new version of the record), adds about 1 percent new customers, soft deletes about 0.5 percent, adds trades equal to about 5 percent of batch 1, moves some pending trades to completed, and includes late-arriving market prices.

## Pipeline steps (same logic on every platform)
1. **Ingest:** read each source format into staging tables with the platform's native file reader. Timed separately from transforms.
2. **DimCustomer, SCD Type 2:** effective and end dates and a current flag. Updates close the previous version and open a new one.
3. **DimAccount, SCD Type 2:** with a foreign key to the customer version in force.
4. **DimSecurity, Type 1:** overwrite on change, joined to company attributes.
5. **FactTrade:** surrogate key lookups to the customer and account versions valid at the trade date, handling late-arriving dimension records.
6. **FactMarketHistory:** daily market rows with trailing 52 week high and low by window function.
7. **Position summary:** an aggregate by customer and security.
8. **Audit checks:** row count reconciliation against the generator's own report, no orphan keys, no overlapping or gapped SCD2 date ranges, and column sum checks.

Batches 2 and 3 repeat steps 2 to 8 incrementally.

## Compute options
D1 (serverless Jobs, standard mode, notebook task), D2 (serverless SQL warehouse, Medium), S1 (Snowflake Medium), S2 (Snowflake Large, once created). Prices from your account's tables and Snowflake's consumption table, recorded with the date.

## Metrics
1. Cost per full pipeline (batches 1 to 3) from billing and metering, including start-up and idle tail.
2. Time per step and per batch, ingest and transform shown separately.
3. Audit result and cross-platform checksum agreement.
4. Failures, retries and manual workarounds needed, listed as results, not hidden.
5. Run to run variation over 2 repetitions.

## Correctness gate
After each batch, compare row counts, column sums, and the audit results across platforms. An option that disagrees is flagged, and its speed and cost are reported with the flag. A faster wrong answer is not a result.

## Decision rules
- Report every option and every step, whichever wins.
- A difference under 10 percent of the lower value, or inside run to run variation, is a tie.
- A winner is named per metric and per workload, never overall.

## Dry run first
SF 1 on D2 and S1 to check the generator and SQL, then SF 10 on all options, with a spend estimate before any SF 100 run. No cost estimate is stated here because none has been measured.

## Limitations to state
Our own design, not a standard. Synthetic data. One region (us-east-2), trial accounts, list prices unless billing says otherwise, default settings only in this phase, and no XML. Results do not transfer to other pipelines.

## Open items
- Legal review of the wording and TPC's fair-use policy.
- Two reviewers (Snowflake and Databricks) for this method and the SQL.
- Which parts of fixed-width and JSON parsing need platform-specific SQL, to be found while writing the code.
- Creating the Snowflake Large warehouse for S2.
- Billing and metering for earlier runs is still pending.
- The Snowflake and Databricks vendor terms on benchmark publication.

## Change log
- 2026-10-12: initial draft.
- 2026-10-12: ingest, shared transform SQL (pipeline_di/), audits and runner written (bench/di_etl.py). SF 1 dry run on D2 and S1: all SQL ran on the first live attempt, all 23 ingested row counts match the generator report on both, all 21 audit checks show zero violations on both, and the checksums after each of the three batches are identical across D2 and S1. Only the ingest SQL differs by platform (file readers, JSON paths, fixed-width read); the transform and audit SQL is shared and transpiled. D1 and S2 not yet built for this pipeline. One run each at SF 1, server-side times, so not yet a result.
- 2026-10-12: generator written (scripts/di_gen.py). Measured at SF 1: 100,000 customers, 200,000 accounts, 2,000,000 batch-1 trades, 4,000,000 trade history rows, 7,500,000 market rows, about 814 MB across all three batches, 9 seconds to generate. Reruns are byte identical. Fixed in testing: CAST rounds in DuckDB so ranges overflowed (now floor), and customers deleted in batch 2 were touched again in batch 3 (now excluded). The sizes in the Data section above remain estimates for larger SF.
