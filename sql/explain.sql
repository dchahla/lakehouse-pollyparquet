-- Performance tuning workflow. EXPLAIN first, then fix what the plan reveals.

-- Baseline: full scan + shuffle join. Read the plan for TableScan (no filter pushdown),
-- the join distribution (PARTITIONED vs BROADCAST), and any large exchanges.
EXPLAIN
SELECT c.region, sum(i.amount) AS revenue
FROM lake.silver.billing_invoices i
JOIN lake.silver.dim_customer c ON i.customer_id = c.id
GROUP BY c.region;

-- Fix 1, partition pruning: filtering on the partition column (region) should drop the scan to
-- a subset of files. Compare the estimated rows on the TableScan node before/after.
EXPLAIN
SELECT c.region, sum(i.amount) AS revenue
FROM lake.silver.billing_invoices i
JOIN lake.silver.dim_customer c ON i.customer_id = c.id
WHERE c.region = 'emea'
GROUP BY c.region;

-- Fix 2, broadcast the small dimension so the fact table isn't reshuffled.
-- (Trino chooses this via stats; the plan node flips to a BROADCAST/REPLICATED join.)

-- MPP-native reminder: the levers are pruning, distribution, sort/cluster, and file sizing.
-- NOT secondary indexes. Small-file explosion and stale stats are the usual sudden-regression cause.
-- Iceberg metadata health check:
SELECT * FROM lake.silver."dim_customer$files";        -- file sizes / counts (small-file smell test)
SELECT * FROM lake.silver."dim_customer$snapshots";    -- unexpired snapshots => metadata bloat

-- ================================================================================================
-- Glossary
--   EXPLAIN             Prints the engine's execution plan without running the query.
--   TableScan node      Reads a table; watch its estimated rows/bytes and whether filters push down.
--   Predicate pushdown  Applying WHERE filters at the scan so fewer files/rows are read.
--   Partition pruning   Skipping whole partitions (files) because they can't match the filter.
--   Exchange / shuffle  A plan node that redistributes data across nodes (costly; minimize).
--   Join distribution   PARTITIONED (both sides shuffled) vs. BROADCAST/REPLICATED (small side copied).
--   Broadcast join      Chosen when one side is small enough; avoids reshuffling the large table.
--   MPP-native levers   Distribution/clustering keys, pruning, sort, file sizing (NOT secondary indexes).
--   $files / $snapshots / $manifests  Iceberg metadata tables exposed by Trino for health checks.
--   Small-file problem  Many tiny data files bloat planning + metadata; fixed by compaction.
--   Stale statistics    Out-of-date table stats → bad plan choices; a common sudden-regression cause.
-- ================================================================================================
