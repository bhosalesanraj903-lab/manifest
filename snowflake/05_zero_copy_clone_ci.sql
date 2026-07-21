-- R13: zero-copy clone for CI. Instant, zero additional storage; CI runs the
-- dbt build against the clone and drops it.
create or replace database manifest_ci clone manifest;
-- ... CI: dbt build --target snowflake_ci ...
drop database if exists manifest_ci;
