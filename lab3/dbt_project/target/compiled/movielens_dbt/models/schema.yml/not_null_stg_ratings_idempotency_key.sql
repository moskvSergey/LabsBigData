
    
    



select idempotency_key
from "dbt_warehouse"."main"."stg_ratings"
where idempotency_key is null


