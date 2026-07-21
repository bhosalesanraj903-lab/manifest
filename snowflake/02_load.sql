-- R13: land silver into Snowflake.
create table if not exists manifest.silver.shipment_events (
  event_id string, shipment_id string, carrier string, event_type string,
  planned_ts timestamp_ntz, actual_ts timestamp_ntz,
  origin string, dest string, vessel_mmsi string
);
create table if not exists manifest.silver.exception_queue (
  exception_id string, shipment_id string, carrier string, exception_type string,
  detected_at timestamp_ntz, age_hours float, detail string, probable_cause string
);

-- Scheduled COPY (sufficient at current volume; idempotent on file names)
copy into manifest.silver.shipment_events
  from @manifest.silver.silver_stage/shipment_events.csv
  on_error = abort_statement force = false;
copy into manifest.silver.exception_queue
  from @manifest.silver.silver_stage/exception_queue.csv
  on_error = abort_statement force = false;

-- Snowpipe auto-ingest variant (enable once R14's S3 event notifications exist):
-- create pipe manifest.silver.events_pipe auto_ingest = true as
--   copy into manifest.silver.shipment_events
--   from @manifest.silver.silver_stage/shipment_events/;
