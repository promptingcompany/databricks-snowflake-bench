# Phase 3 method: tuned track (Part A) and incremental CDC pipeline (Part B)

Status: draft for review. Written 2026-10-12. Fixed before any run. Changes after approval go in the change log with date and reason, and earlier results are still reported.

## Purpose
Part A answers the main criticism of default-only benchmarks: what happens when each vendor's documented tuning is applied. Part B measures repeated incremental processing (upserts, deletes, inserts, downstream refresh), the batch pattern most production pipelines use.

Neither part is designed to favor a platform. The outcome is unknown and every result is reported.

## Rules for both parts
1. Only optimizations from each vendor's own documentation, each listed below with its source. No query rewrites, hints or custom code that one side has and the other lacks.
2. If a vendor's guidance says a feature is not recommended at our table size, we still test it as a separate labeled variant, and we also report the guidance. Nothing is dropped.
3. The cost of the tuning itself (time, compute, background maintenance credits) is measured and added to the totals. A faster table that cost more to build is shown as such.
4. Same official data (TPC-DS SF100), same results gate (identical outputs on every query), one run at a time on idle compute, result caches off.
5. Defaults results from the earlier run stay in the write-up beside the tuned results.
6. All settings, commands and prices are published with the results. A reviewer who knows each platform reads this method before any run: one Snowflake-experienced and one Databricks-experienced.

## Part A: tuned TPC-DS at SF100

### What the vendors say (summarized, check live docs before running)
- Databricks recommends liquid clustering for all new Delta tables and says OPTIMIZE rewrites files to group data by the clustering keys. It also recommends predictive optimization for Unity Catalog managed tables, and describes CLUSTER BY AUTO for automatic key selection. Sources: [liquid clustering](https://learn.microsoft.com/en-in/azure/databricks/delta/clustering), [optimize data file layout](https://docs.databricks.com/aws/en/tables/operations/optimize).
- Snowflake says clustering keys suit tables with a very large number of micro-partitions (typically multiple terabytes) and queries that filter or sort on a few columns, that clustering has ongoing credit cost, and that query acceleration suits ad hoc large scans with selective filters. Sources: [clustering keys](https://docs.snowflake.com/en/user-guide/tables-clustering-keys), [automatic clustering](https://docs.snowflake.com/en/user-guide/tables-auto-reclustering), [optimizing query performance](https://docs.snowflake.com/en/user-guide/performance-query-options).

Note the asymmetry: Databricks recommends clustering for all new tables, while Snowflake's guidance targets multi-terabyte tables. Our largest table is under 10 GB compressed, so the Snowflake clustering variant is below its stated size range. It is tested anyway and labeled.

### Variants
| Id | Platform | Change from the default load |
|---|---|---|
| D0 | Databricks | Defaults (already measured) |
| D-A | Databricks | Liquid clustering on the date key of each large table, then OPTIMIZE |
| D-B | Databricks | CLUSTER BY AUTO with predictive optimization, if available on this workspace |
| S0 | Snowflake | Defaults (already measured) |
| S-A | Snowflake | Clustering key on the date key of each large table, automatic clustering allowed to finish |
| S-B | Snowflake | Query acceleration service on the warehouse, with its documented scale factor setting (UNAVAILABLE on this account at SF10: feature not enabled) |
| D-C | Databricks | Control: tables rebuilt by plain CTAS from the default tables, no clustering, no OPTIMIZE |
| S-C | Snowflake | Control: tables rebuilt by plain CTAS from the default tables, no clustering key |

The controls separate the effect of the tuning from the effect of rebuilding a table (a rebuild changes file or micro-partition layout by itself). Without them a rebuild gain could be mislabeled as a clustering gain.

Large tables and date keys: store_sales (ss_sold_date_sk), catalog_sales (cs_sold_date_sk), web_sales (ws_sold_date_sk), inventory (inv_date_sk), and the three returns tables (their returned_date_sk columns). If a vendor's documentation names other keys for these tables, the reviewer may change the keys before the run, applied to both sides.

### Protocol
- Build each variant in its own schema from the same loaded data. Record build time, compute used and any background maintenance credits.
- Run all 99 queries: 1 cold round, 3 warm rounds, same warehouse sizes as before (Medium on both).
- Same correctness gate as the defaults run.

### Metrics
Suite time per warm round (mean and SD), per-query results with all 99 shown, cost per round and tuning cost, and cost per suite including amortized tuning cost over 1 and 10 suite runs.

## Part B: incremental CDC pipeline at SF100

### Pipeline
1. Base table: the T1 wide fact from the Phase 2 design, built once at SF100.
2. Ten batches. Each batch, selected deterministically by key arithmetic with the batch number (identical on both platforms):
   - upsert: change about 1 percent of existing rows,
   - delete: remove about 0.1 percent,
   - insert: add about 0.5 percent new rows with new keys.
   Applied as one MERGE.
3. Downstream refresh after every batch: recompute the daily rollup (T2) for only the dates touched by that batch (delete those dates, insert the recomputed rows).
4. Checkpoints after batches 1, 5 and 10: row counts and column sums of the base table and the rollup, compared across platforms.

### Compute options
D1 (serverless Jobs, standard mode), D2 (serverless SQL warehouse Medium), S1 (Snowflake Medium), S2 (Snowflake Large). Each runs alone on idle compute with a recorded UTC window.

### Metrics
- Time per batch and for all ten, with the first batch reported separately.
- Cost measured from billing and metering for each isolated window (D1 by job run id, D2 by warehouse and window, S1 and S2 from metering), including start-up and idle tail. List-price estimates are shown only if billing is unavailable, and labeled as estimates.
- Correctness at checkpoints. An option that differs is flagged and its speed and cost reported with the flag.
- Variation across 2 repetitions per option.

### Tuned variant
After the defaults run, repeat Part B once on the best tuned tables from Part A for each platform, using the same batches.

## Decision rules (fixed now)
- Report every variant and every option, whichever wins.
- A cost or time difference under 10 percent of the lower value, or inside run to run variation, is a tie.
- A winner is named only per metric and per workload, never overall.

## Dry run first
Run both parts at SF10 once to confirm the code works and to measure cost, then give a spend estimate for SF100 before any SF100 run. No estimate is given here because none has been measured.

## Limitations to state
One scale, one region (us-east-2), trial accounts, list prices, synthetic data, one pipeline shape, and a small TPC-DS table size relative to the sizes at which some Snowflake features are recommended.

## Open items
- Snowflake query acceleration setup and cost details are not verified here; confirm from the live docs and the consumption table before running S-B.
- Confirm CLUSTER BY AUTO and predictive optimization are available on this Databricks workspace.
- Billing and metering for the earlier isolated Phase 2 windows are still pending.
- Reviewers for each platform.
- Vendor benchmark and publication terms.

## Change log
- 2026-10-12: initial draft.
- 2026-10-12 (after SF10 dry runs, before any SF100 tuned run): added control variants D-C and S-C because rebuilding a table changes its layout independently of clustering. Recorded S-B as unavailable on this account (Snowflake reported the query acceleration feature is not enabled). SF10 dry-run results for D-A, D-B and S-A are kept in the results folder.
