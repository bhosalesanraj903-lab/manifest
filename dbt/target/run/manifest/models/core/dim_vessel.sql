
  
    
    

    create  table
      "manifest"."main"."dim_vessel__dbt_tmp"
  
    as (
      -- Observed vessel dimension from AIS ShipStaticData (silver, last-write-wins),
-- with names backfilled from the config fleet list for vessels not yet heard
-- on the wire. The Makefile guarantees the silver file exists (header-only
-- before first AIS capture).
with observed as (
    select mmsi, imo, name, type, observed_at
    from read_csv('../data/silver/dim_vessel.csv', header = true, all_varchar = true)
),

configured as (
    select cast(mmsi as varchar) as mmsi, name
    from read_csv('../config/vessels_flat.csv', header = true, all_varchar = true)
)

select
    coalesce(o.mmsi, c.mmsi) as mmsi,
    o.imo,
    coalesce(nullif(o.name, ''), c.name) as vessel_name,
    o.type as ship_type,
    o.observed_at
from configured c
full outer join observed o on o.mmsi = c.mmsi
    );
  
  