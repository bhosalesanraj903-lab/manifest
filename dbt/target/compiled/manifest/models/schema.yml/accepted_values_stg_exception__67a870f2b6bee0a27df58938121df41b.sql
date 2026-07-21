
    
    

with all_values as (

    select
        probable_cause as value_field,
        count(*) as n_records

    from "manifest"."main"."stg_exception_queue"
    group by probable_cause

)

select *
from all_values
where value_field not in (
    'weather','congestion','disruption','unknown'
)


