select
    l.shipment_id,
    l.leg_seq,
    l.mode,
    l.vessel_mmsi,
    v.vessel_name,
    l.origin,
    l.dest,
    l.origin || '-' || l.dest as lane
from {{ ref('stg_shipment_legs') }} l
left join {{ ref('dim_vessel') }} v on v.mmsi = l.vessel_mmsi
