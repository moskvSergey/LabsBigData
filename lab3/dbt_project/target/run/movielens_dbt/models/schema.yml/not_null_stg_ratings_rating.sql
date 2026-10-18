
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rating
from "dbt_warehouse"."main"."stg_ratings"
where rating is null



  
  
      
    ) dbt_internal_test