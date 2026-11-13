-- Databricks account-side setup for this benchmark. Run in a SQL editor as a metastore or workspace admin.
-- What we did: we ran setup with an admin identity. The grants below are what the code needs, from the SQL it runs;
-- we have not tested them as a separate restricted principal. Check names and permissions against your workspace.

-- 1. Catalog. The data schemas (tpcds_sf10, tpcds_sf100) and volumes are created by scripts/upload_databricks.py.
CREATE CATALOG IF NOT EXISTS bench;
CREATE SCHEMA  IF NOT EXISTS bench.tpcds_sf10;
CREATE SCHEMA  IF NOT EXISTS bench.tpcds_sf100;
-- scripts/upload_databricks.py also runs: CREATE VOLUME IF NOT EXISTS bench.tpcds_sf<N>.raw
-- bench/di_upload.py also runs:          CREATE SCHEMA IF NOT EXISTS bench.di_sf<N>; CREATE VOLUME IF NOT EXISTS bench.di_sf<N>.raw
-- The reported runs loaded from local files into those volumes and then ran:
--   CREATE OR REPLACE TABLE <t> AS SELECT * FROM parquet.`/Volumes/bench/tpcds_sf<N>/raw/<t>/`   (python -m bench.load --local)
-- so no external location or cloud storage credential is needed. (An S3 external location also works; see docs/aws_access.md.)

-- 2. The benchmark principal (a user or service principal) needs:
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG bench TO `<principal>`;           -- the runner creates and drops a schema per run
GRANT ALL PRIVILEGES ON SCHEMA bench.tpcds_sf10  TO `<principal>`;
GRANT ALL PRIVILEGES ON SCHEMA bench.tpcds_sf100 TO `<principal>`;

-- 3. Billing and cost. The cost numbers come from system tables, which a metastore admin must expose:
--      the system schema "billing" must be enabled for the metastore (an account admin can enable it);
GRANT USE CATALOG ON CATALOG system TO `<principal>`;
GRANT USE SCHEMA  ON SCHEMA  system.billing TO `<principal>`;
GRANT SELECT      ON TABLE   system.billing.usage TO `<principal>`;
GRANT SELECT      ON TABLE   system.billing.list_prices TO `<principal>`;     -- used to read the DBU price
-- Billing rows arrive about 4 hours or more after usage.
-- The statement-history check in bench/isolated_reconcile.py (REST query_history) only shows other users' statements
-- if the caller is a workspace admin or has CAN MANAGE on the warehouse.

-- 4. Compute (create in the UI or API; SQL cannot create it):
--    * A serverless SQL warehouse named bench-medium: size Medium, 1 cluster, auto-stop 1 minute. Put its HTTP path in
--      DATABRICKS_HTTP_PATH. The principal needs CAN USE (and CAN MANAGE if it is not an admin).
--    * For serverless Jobs compute (option D1): the principal needs permission to create jobs and run notebooks in its home
--      folder. The runner submits one-time runs through /api/2.1/jobs/runs/submit with performance_target STANDARD.
-- Authentication is a personal access token (DATABRICKS_TOKEN). The code sets auth_type="pat", so no OAuth variables are needed.
