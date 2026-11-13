# Phase 2 design: batch transformation cost

Status: pre-registered design. Fixed before any run. Written 2026-10-09.
Rule: once approved, steps, metrics and decision rules are not changed after seeing results. Any change is logged here with the date and the reason, and results from before the change are still reported.

## Question
For the same multi-step transformation pipeline over the same SF100 data, what does each platform's batch compute cost, and how long does it take?

The claim this test can support is narrow: batch transformation cost and time for this pipeline, at this scale, with these settings. It does not say which platform is better overall.

## Data
- TPC-DS SF100, official dsdgen output, already loaded identically on both platforms (row counts verified equal).
- Source tables are read only. All outputs go to fresh output schemas per run, dropped afterwards.

## Compute options compared
Each option is run as its platform's normal way to run scheduled batch SQL.

| Option | Platform | Compute | List price used |
|---|---|---|---|
| D1 | Databricks | Serverless Jobs compute (SQL tasks in a job) | $0.35 per DBU (Premium, US East Ohio, from system.billing.list_prices) |
| D2 | Databricks | Serverless SQL warehouse, Medium | $0.70 per DBU, 24 DBU/hr |
| S1 | Snowflake | Warehouse Medium | $3.00 per credit (Enterprise, Ohio), 4 credits/hr |
| S2 | Snowflake | Warehouse Large | $3.00 per credit, 8 credits/hr |

Notes:
- D1 bills by DBUs actually consumed. Its cost comes from billing data (system.billing.usage), not from a size and hourly rate. If billing data cannot be read for these runs, D1 is reported as "cost not measurable" and not estimated.
- Snowflake and D2 cost is uptime based. Each warehouse runs only for the pipeline, with auto-suspend at 60 seconds, and the cost includes the 60 second resume minimum and the idle tail.
- If Snowflake offers a cheaper documented batch option available on this account, add it as S3 before running and list it here. Not added yet because availability on the trial is unconfirmed.

## Pipeline (identical logic on every option)
All steps are written once in standard SQL and transpiled per dialect. No hints, no clustering keys, no manual statistics, no OPTIMIZE or equivalent.

1. **T1 wide fact.** CTAS joining store_sales to date_dim, item, customer, customer_address and store, keeping all fact rows (large join plus a large write).
2. **T2 daily rollup.** From T1, aggregate revenue, quantity and discount by date, store and item category.
3. **T3 customer 360.** Union of store, catalog and web sales aggregated per customer, joined to customer attributes.
4. **T4 upsert.** MERGE a 1 percent sample of changed rows (fixed seed or fixed key set, identical on both) into T1.
5. **T5 window analytics.** From T1 aggregated to customer and month: a 12 month trailing sum per customer and a rank within state and month. (Changed from T3 before any run; see change log.)

Steps run in order as one pipeline. Time and cost are reported per step and for the whole pipeline.

## Run protocol
- 3 full runs per option. Run 1 is the first run after the compute has been idle, runs 2 and 3 follow back to back.
- One option running at a time. No other queries on either platform's compute.
- Result caches off.
- Drop all outputs between runs.

## Correctness gate
After each run, compare across platforms:
- Row counts of T1 to T5.
- A checksum of each output: sums of numeric columns and a hash of sorted key columns, rounded to 2 decimals.
An option whose outputs differ from the others is flagged, and its speed and cost are reported with the flag. A faster wrong answer does not count as a result.

## Metrics
1. **Primary: pipeline cost in USD** (measured as above).
2. Pipeline elapsed time (server side) and per step.
3. Success and failure of each step, retries needed.
4. Variation across the 3 runs (mean and standard deviation).

## Decision rules, fixed in advance
- Report all four options and all steps, whichever is cheaper or faster.
- Cost leader is the option with lowest mean pipeline cost whose outputs pass the correctness gate.
- A difference under 10 percent of the cheaper option, or inside run to run variation, is reported as a tie.
- If a step fails on an option, that option is reported as incomplete for the pipeline, with the failure shown.

## Limitations to state when published
- One pipeline, one scale factor, one region (us-east-2), trial accounts, list prices, no committed-use discounts.
- Default file layout from a plain CTAS. Maintenance operations that either platform documents would change results and are not applied.
- Results do not transfer to other workloads.

## Open items before running
- Confirm D1 is available on this workspace and that system.billing.usage records the run, or accept "not measurable".
- Confirm a Snowflake Large warehouse can be created on this account (S2). Creating it is a Snowflake admin action.
- Estimate spend after a small dry run at SF10, before any SF100 run.
- Vendor benchmark terms check before any publication.

## Change log (all changes made before any run)
- 2026-10-09: T5 now reads T1 by customer and month, not T3. T3 has no time dimension, so a 12 month trailing sum cannot be computed from it. T5 therefore also reads T1 after the T4 merge. No results existed when this was changed.
- 2026-10-09: D1 (serverless Jobs compute) cannot run SQL tasks. Databricks SQL tasks in a job run on SQL warehouses. D1 needs a notebook or Python task executing the same SQL, which is not built yet. The runner refuses D1 until that is written and reviewed. Until then, results cover D2, S1 and S2 only, and the $0.35 Jobs rate is not tested.
- 2026-10-09: Runner requires an explicit scale and --yes for SF100 so a run cannot start by accident.
