-- Median planned-vs-actual offset per lane + carrier + milestone.
-- Consumed by eta/predict.py (R10) as the baseline adjustment.
select
    carrier,
    lane,
    event_type,
    count(*) as observations,
    round(median(delay_hours), 2) as median_offset_h
from {{ ref('fct_shipment_events') }}
group by 1, 2, 3
order by 1, 2, 3
