-- Очищаем данные: оставляем только валидные рейтинги
SELECT 
    userId,
    movieId,
    rating,
    timestamp,
    idempotency_key
FROM read_parquet('../raw_parquet/*.parquet')
WHERE rating >= 0.5 AND rating <= 5.0