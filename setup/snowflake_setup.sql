-- Snowflake account-side setup for this benchmark. Run as ACCOUNTADMIN (or a role that can create warehouses, users and grants).
-- What we did: we ran the setup with an admin role. The least-privilege grants at the bottom are what the code needs,
-- based on the SQL it runs; we have not tested them as a separate restricted role.

-- 1. Database and schemas (the data schemas match config.yaml; per-run output schemas are created and dropped by the runner)
CREATE DATABASE IF NOT EXISTS BENCH;
CREATE SCHEMA IF NOT EXISTS BENCH.TPCDS_SF10;
CREATE SCHEMA IF NOT EXISTS BENCH.TPCDS_SF100;

-- 2. Warehouses: Medium (S1) and Large (S2). Auto-suspend 60 seconds, as used in the billing windows.
CREATE WAREHOUSE IF NOT EXISTS BENCH_M WAREHOUSE_SIZE = MEDIUM AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE WAREHOUSE IF NOT EXISTS BENCH_L WAREHOUSE_SIZE = LARGE  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- 3. Key-pair login. Make the key on your machine (the code expects an unencrypted PKCS8 PEM):
--      openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--      openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--    Then register the public key body (the lines between BEGIN and END, joined) on the runner user and point
--    SNOWFLAKE_PRIVATE_KEY_PATH in .env at rsa_key.p8.
CREATE ROLE IF NOT EXISTS BENCH_RUNNER;
CREATE USER IF NOT EXISTS BENCH_RUNNER_USER DEFAULT_ROLE = BENCH_RUNNER DEFAULT_WAREHOUSE = BENCH_M;
ALTER USER BENCH_RUNNER_USER SET RSA_PUBLIC_KEY = '<public key body>';
GRANT ROLE BENCH_RUNNER TO USER BENCH_RUNNER_USER;

-- 4. Grants the code needs
GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE BENCH_M TO ROLE BENCH_RUNNER;   -- MONITOR lets the runner see other users' statements on the warehouse
GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE BENCH_L TO ROLE BENCH_RUNNER;
GRANT ALL ON DATABASE BENCH TO ROLE BENCH_RUNNER;                          -- creates schemas, tables, stages
GRANT ALL ON ALL SCHEMAS IN DATABASE BENCH TO ROLE BENCH_RUNNER;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE BENCH_RUNNER;      -- reads SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY for cost

-- 5. Data loading. The reported runs loaded from local files into an internal stage:
--      python -m bench.load --local      (creates BENCH.TPCDS_SF<N>.TPCDS_LOCAL_STAGE, PUTs the Parquet, creates tables with INFER_SCHEMA, COPY INTO)
--    No cloud storage integration is needed for that path. We first tried an S3 external stage with a storage integration
--    (see docs/aws_access.md) and the cross-account role assumption never worked, so the reported runs do not use it.
--    If your account can read the bucket, the S3 path (config.yaml data.uri and the TPCDS_STAGE external stage) also works with bench.load.
-- Note: ACCOUNT_USAGE metering lags by about 3 hours.
