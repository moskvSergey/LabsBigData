
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idempotency_key
from "dbt_warehouse"."main"."stg_ratings"
where idempotency_key is null



  
  
      
    ) dbt_internal_test