
    
    

select
    movieId as unique_field,
    count(*) as n_records

from "dbt_warehouse"."main"."dim_movies"
where movieId is not null
group by movieId
having count(*) > 1


