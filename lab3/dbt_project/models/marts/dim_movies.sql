
-- Считаем средний рейтинг и количество оценок для каждого фильма
SELECT 
    movieId,
    COUNT(*) AS total_ratings,
    AVG(rating) AS avg_rating
FROM {{ ref('stg_ratings') }}
GROUP BY movieId
HAVING COUNT(*) > 10
