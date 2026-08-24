-- What landed in bronze, and proof the landing contract holds.
--
-- Run with: make peek
--
-- The point of these four queries: twelve sources arrived in two different native formats,
-- and every one of them carries the same three ingest columns. That uniformity is what lets
-- everything downstream treat bronze as one thing instead of twelve special cases.

-- 1. Everything the catalog knows about.
SHOW TABLES FROM lake.bronze;

-- 2. A CSV-native source.
SELECT _source, _batch_id, _ingested_at
FROM lake.bronze.crm_customers
LIMIT 3;

-- 3. A JSON-native source. Same three columns, different original shape.
SELECT _source, _batch_id, _ingested_at
FROM lake.bronze.web_events
LIMIT 3;

-- 4. One row per batch you have run, oldest first. Run `make seed` again and a
--    second row appears here rather than the first one changing.
SELECT _batch_id,
       count(*)          AS rows,
       min(_ingested_at) AS earliest,
       max(_ingested_at) AS latest
FROM lake.bronze.crm_customers
GROUP BY _batch_id
ORDER BY earliest;
