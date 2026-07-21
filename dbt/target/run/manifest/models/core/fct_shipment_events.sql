
  
    
    

    create  table
      "manifest"."main"."fct_shipment_events__dbt_tmp"
  
    as (
      select
    e.event_id,
    e.shipment_id,
    e.carrier,
    c.carrier_name,
    e.event_type,
    e.planned_ts,
    e.actual_ts,
    date_diff('minute', e.planned_ts, e.actual_ts) / 60.0 as delay_hours,
    e.origin,
    lo.port_name as origin_name,
    e.dest,
    ld.port_name as dest_name,
    e.origin || '-' || e.dest as lane,
    e.vessel_mmsi
from "manifest"."main"."stg_shipment_events" e
left join "manifest"."main"."dim_carrier" c on c.carrier_code = e.carrier
left join "manifest"."main"."dim_location" lo on lo.unlocode = e.origin
left join "manifest"."main"."dim_location" ld on ld.unlocode = e.dest
    );
  
  