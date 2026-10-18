
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    idempotency_key as unique_field,
    count(*) as n_records

from "dbt_warehouse"."main"."stg_ratings"
where idempotency_key is not null
group by idempotency_key
having count(*) > 1



  
  
      
    ) dbt_internal_test