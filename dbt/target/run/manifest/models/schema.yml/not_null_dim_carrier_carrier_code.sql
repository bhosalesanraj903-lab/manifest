
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select carrier_code
from "manifest"."main"."dim_carrier"
where carrier_code is null



  
  
      
    ) dbt_internal_test