
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    exception_id as unique_field,
    count(*) as n_records

from "manifest"."main"."stg_exception_queue"
where exception_id is not null
group by exception_id
having count(*) > 1



  
  
      
    ) dbt_internal_test