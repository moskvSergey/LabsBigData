
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select movieId
from "dbt_warehouse"."main"."dim_movies"
where movieId is null



  
  
      
    ) dbt_internal_test