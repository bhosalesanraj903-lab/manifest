
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    carrier_code as unique_field,
    count(*) as n_records

from "manifest"."main"."dim_carrier"
where carrier_code is not null
group by carrier_code
having count(*) > 1



  
  
      
    ) dbt_internal_test