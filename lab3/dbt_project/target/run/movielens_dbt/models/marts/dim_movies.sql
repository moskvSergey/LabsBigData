
  
  create view "dbt_warehouse"."main"."dim_movies__dbt_tmp" as (
    -- Считаем средний рейтинг и количество оценок для каждого фильма
SELECT 
    movieId,
    COUNT(*) AS total_ratings,
    AVG(rating) AS avg_rating
FROM "dbt_warehouse"."main"."stg_ratings"
GROUP BY movieId
HAVING COUNT(*) > 10
  );
