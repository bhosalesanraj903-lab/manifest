
    
    

select
    unlocode as unique_field,
    count(*) as n_records

from "manifest"."main"."dim_location"
where unlocode is not null
group by unlocode
having count(*) > 1


