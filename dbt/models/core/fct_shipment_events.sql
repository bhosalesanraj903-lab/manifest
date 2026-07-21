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
from {{ ref('stg_shipment_events') }} e
left join {{ ref('dim_carrier') }} c on c.carrier_code = e.carrier
left join {{ ref('dim_location') }} lo on lo.unlocode = e.origin
left join {{ ref('dim_location') }} ld on ld.unlocode = e.dest
