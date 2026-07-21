
    
    

select
    exception_id as unique_field,
    count(*) as n_records

from "manifest"."main"."stg_exception_queue"
where exception_id is not null
group by exception_id
having count(*) > 1


