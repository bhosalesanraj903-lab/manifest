
  
  create view "manifest"."main"."stg_shipment_events__dbt_tmp" as (
    select
    event_id,
    shipment_id,
    carrier,
    event_type,
    strptime(planned_ts, '%Y-%m-%dT%H:%M:%SZ') as planned_ts,
    strptime(actual_ts, '%Y-%m-%dT%H:%M:%SZ') as actual_ts,
    origin,
    dest,
    nullif(cast(vessel_mmsi as varchar), '') as vessel_mmsi
from read_csv('../data/silver/shipment_events.csv',
              header = true, all_varchar = true)
  );
