
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select exception_id
from "manifest"."main"."stg_exception_queue"
where exception_id is null



  
  
      
    ) dbt_internal_test