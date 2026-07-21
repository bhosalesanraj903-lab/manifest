
  
    
    

    create  table
      "manifest"."main"."fct_shipment_legs__dbt_tmp"
  
    as (
      select
    l.shipment_id,
    l.leg_seq,
    l.mode,
    l.vessel_mmsi,
    v.vessel_name,
    l.origin,
    l.dest,
    l.origin || '-' || l.dest as lane
from "manifest"."main"."stg_shipment_legs" l
left join "manifest"."main"."dim_vessel" v on v.mmsi = l.vessel_mmsi
    );
  
  