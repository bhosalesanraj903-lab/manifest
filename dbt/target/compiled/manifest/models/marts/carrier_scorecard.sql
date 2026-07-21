-- Carrier/lane operational scorecard:
--   on_time_pct        delivered within 24h of plan
--   mis_scan_rate      shipments that went dark (MISSED_MILESTONE) / shipments
--   avg_customs_dwell_h  vessel_arrived -> customs_release
with shipments as (
    select carrier, lane, shipment_id
    from "manifest"."main"."fct_shipment_events"
    group by 1, 2, 3
),

delivered as (
    select carrier, lane, shipment_id,
           max(case when delay_hours <= 24 then 1 else 0 end) as on_time
    from "manifest"."main"."fct_shipment_events"
    where event_type = 'delivered'
    group by 1, 2, 3
),

dark as (
    select shipment_id
    from "manifest"."main"."stg_exception_queue"
    where exception_type = 'MISSED_MILESTONE'
),

customs as (
    select
        carrier, lane, shipment_id,
        date_diff('minute',
                  max(case when event_type = 'vessel_arrived' then actual_ts end),
                  max(case when event_type = 'customs_release' then actual_ts end)
        ) / 60.0 as customs_dwell_h
    from "manifest"."main"."fct_shipment_events"
    where event_type in ('vessel_arrived', 'customs_release')
    group by 1, 2, 3
    having count(distinct event_type) = 2
)

select
    s.carrier,
    s.lane,
    count(distinct s.shipment_id) as shipments,
    round(100.0 * avg(d.on_time), 1) as on_time_pct,
    round(100.0 * avg(case when k.shipment_id is not null then 1.0 else 0.0 end), 1)
        as mis_scan_rate,
    round(avg(c.customs_dwell_h), 1) as avg_customs_dwell_h
from shipments s
left join delivered d using (carrier, lane, shipment_id)
left join dark k on k.shipment_id = s.shipment_id
left join customs c using (carrier, lane, shipment_id)
group by 1, 2
order by 1, 2