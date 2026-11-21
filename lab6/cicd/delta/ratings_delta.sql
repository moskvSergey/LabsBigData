/* ЛР-6, Часть 4, п.3. Delta Lake: версионирование таблиц.
 *
 * Требования к отчёту: конфигурация Data CI/CD + стратегия безопасного деплоя
 * данных. Delta даёт ACID-транзакции, time travel и откат: «деплой» данных —
 * это запись новой версии, откат — чтение/восстановление предыдущей.
 * Запуск: spark-sql -f cicd/delta/ratings_delta.sql
 */

-- 1. Таблица с версионированием: оценки пользователей (из ЛР-1..ЛР-5)
CREATE TABLE IF NOT EXISTS ratings (
  userId    INT,
  movieId   INT,
  rating    DOUBLE,
  timestamp BIGINT
)
USING DELTA
LOCATION '/data/movielens/ratings'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.enableChangeDataFeed'       = 'true',
  /* Хранение: старые версии уходят в холодный tier (Часть 3, tiering) */
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.logRetentionDuration'         = 'interval 60 days'
);

-- 2. Идемпотентная запись новой версии (MERGE вместо INSERT — повторный
--    прогон пайплайна не создаёт дубликатов; см. ЛР-3 об идемпотентности)
MERGE INTO ratings AS target
USING new_ratings AS source
ON  target.userId = source.userId
AND target.movieId = source.movieId
AND target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;

-- 3. Time travel: сравнение версий до и после «деплоя» данных
SELECT COUNT(*) AS rows_v10 FROM ratings VERSION AS OF 10;
SELECT COUNT(*) AS rows_now  FROM ratings;

-- 4. Проверка безопасного деплоя: что вообще происходило с таблицей
DESCRIBE HISTORY ratings LIMIT 5;

-- 5. Откат «деплоя данных» на предыдущую версию
RESTORE TABLE ratings TO VERSION AS OF 10;

-- 6. Оптимизация стоимости хранения: сжать мелкие файлы и убрать мусор
OPTIMIZE ratings ZORDER BY (userId, movieId);
VACUUM ratings RETAIN 168 HOURS;   -- 7 дней: баланс между cost и возможностью отката
