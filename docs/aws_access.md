# Optional: reading the data from S3 instead of loading from local files

Note: this is the route we tried first. The Snowflake cross-account role never worked in our account, so the reported runs loaded from local files (see README). Kept for reference.

Bucket: <your-bucket>, prefix tpcds/sf10. Bucket region MUST equal both
warehouse regions (confirm: us-east-1 vs us-east-2).
Never reuse the uploader's access key for these. Each platform assumes its own IAM role.

## Read-only policy (attach to both roles; name it bench-read)
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],
     "Resource":"arn:aws:s3:::<your-bucket>/tpcds/*"},
    {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],
     "Resource":"arn:aws:s3:::<your-bucket>",
     "Condition":{"StringLike":{"s3:prefix":["tpcds/*"]}}}
  ]
}

## Snowflake (storage integration)
1. IAM: create role `bench-snowflake-read`, attach bench-read. Temporary trust policy: your own
   account root; it is replaced in step 3.
2. Snowflake (ACCOUNTADMIN):
   CREATE STORAGE INTEGRATION bench_s3 TYPE=EXTERNAL_STAGE STORAGE_PROVIDER='S3' ENABLED=TRUE
     STORAGE_AWS_ROLE_ARN='arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/bench-snowflake-read'
     STORAGE_ALLOWED_LOCATIONS=('s3://<your-bucket>/tpcds/');
   DESC INTEGRATION bench_s3;   -- copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
3. IAM: edit the role's trust policy: Principal AWS = STORAGE_AWS_IAM_USER_ARN, condition
   sts:ExternalId = STORAGE_AWS_EXTERNAL_ID.
4. Snowflake:
   GRANT USAGE ON INTEGRATION bench_s3 TO ROLE SYSADMIN;
   USE ROLE SYSADMIN; USE SCHEMA BENCH.TPCDS_SF10;
   CREATE STAGE TPCDS_STAGE URL='s3://<your-bucket>/tpcds/sf10/'
     STORAGE_INTEGRATION=bench_s3;
   LIST @TPCDS_STAGE;          -- should list the table folders

## Databricks (Unity Catalog storage credential + external location)
Catalog -> External data -> Credentials -> Create credential (AWS IAM role). The UI shows the
trust policy (Unity Catalog principal + external ID) and the role ARN field.
1. IAM: create role `bench-databricks-read`, attach bench-read, trust policy as shown by the UI.
2. Create the credential, then External Locations -> Create:
   URL s3://<your-bucket>/tpcds/ ; use that credential; "Test connection".
3. SQL: GRANT READ FILES ON EXTERNAL LOCATION <name> TO `<benchmark principal>`;
   CREATE CATALOG IF NOT EXISTS bench; CREATE SCHEMA IF NOT EXISTS bench.tpcds_sf10;
