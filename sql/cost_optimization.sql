-- Cost optimization. Cost is roughly bytes scanned plus compute-seconds. Attack both.

-- Runaway-query guardrail: cap statement runtime (set at session/resource-group level).
SET SESSION query_max_execution_time = '30m';

-- Scan less = pay less. Compact small files so the engine reads fewer, larger objects
-- (also fixes the small-file perf regression from explain.sql).
ALTER TABLE lake.silver.dim_customer EXECUTE optimize;

-- Reclaim storage + shrink metadata: expire old snapshots and orphan files.
ALTER TABLE lake.silver.dim_customer EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE lake.silver.dim_customer EXECUTE remove_orphan_files(retention_threshold => '7d');

-- Archive cold raw data (bronze). See infra/main.tf lifecycle rule for the storage-tier side.
-- Right-sizing compute lives in infra + docker-compose (fewer/smaller executors, auto-suspend).

-- ================================================================================================
-- Glossary
--   query_max_execution_time   Session limit that kills runaway queries (cost + fairness guardrail).
--   SET SESSION                 Sets a Trino session property for the current connection.
--   ALTER TABLE ... EXECUTE     Runs an Iceberg maintenance procedure on the table.
--   optimize                    Compaction: rewrites small files into fewer large ones (scan less).
--   expire_snapshots            Drops snapshots older than the threshold → reclaims storage, trims metadata.
--   remove_orphan_files         Deletes data files no snapshot references (e.g. failed writes).
--   retention_threshold         Age below which snapshots/files are kept (safety window for time travel).
--   Bytes scanned               Primary cost driver in usage-priced engines; scan less, pay less.
--   Right-sizing                Matching compute (warehouse/executor size) to workload; add auto-suspend.
--   Cold data / tiering         Rarely-read data moved to cheaper storage (see infra lifecycle rule).
-- ================================================================================================
