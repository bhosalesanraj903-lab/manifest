
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unlocode
from "manifest"."main"."dim_location"
where unlocode is null



  
  
      
    ) dbt_internal_test