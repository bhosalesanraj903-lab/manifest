
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "manifest"."main"."fct_shipment_events"
    group by event_type

)

select *
from all_values
where value_field not in (
    'booking_confirmed','gate_in','loaded_on_vessel','vessel_departed','vessel_arrived','customs_release','out_for_delivery','delivered'
)



  
  
      
    ) dbt_internal_test