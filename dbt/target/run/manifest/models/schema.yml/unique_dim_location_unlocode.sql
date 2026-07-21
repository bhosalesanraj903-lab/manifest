
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    unlocode as unique_field,
    count(*) as n_records

from "manifest"."main"."dim_location"
where unlocode is not null
group by unlocode
having count(*) > 1



  
  
      
    ) dbt_internal_test