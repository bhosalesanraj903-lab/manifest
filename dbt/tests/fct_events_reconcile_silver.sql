-- Row-count reconciliation: the fact table must match silver exactly.
-- Any drift fails the build (requirement R9).
select 'fct_shipment_events row count != stg_shipment_events' as failure
where (select count(*) from {{ ref('fct_shipment_events') }})
   <> (select count(*) from {{ ref('stg_shipment_events') }})
