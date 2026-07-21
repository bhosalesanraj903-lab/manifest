
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select carrier
from "manifest"."main"."carrier_scorecard"
where carrier is null



  
  
      
    ) dbt_internal_test