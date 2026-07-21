-- R13 Snowflake setup. Run as SYSADMIN (or trial ACCOUNTADMIN).
create warehouse if not exists manifest_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true;

create database if not exists manifest;
create schema if not exists manifest.silver;
create schema if not exists manifest.gold;

create file format if not exists manifest.silver.csv_std
  type = csv skip_header = 1 empty_field_as_null = true
  field_optionally_enclosed_by = '"';

-- Storage integration to the lake bucket (created by R14 Terraform).
-- Fill in the two placeholders, then follow the DESC INTEGRATION dance to
-- grant the returned AWS IAM principal access to the bucket.
create storage integration if not exists manifest_s3
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = '<IAM_ROLE_ARN>'          -- from terraform output
  storage_allowed_locations = ('s3://<LAKE_BUCKET>/silver/');

create stage if not exists manifest.silver.silver_stage
  storage_integration = manifest_s3
  url = 's3://<LAKE_BUCKET>/silver/'
  file_format = manifest.silver.csv_std;
