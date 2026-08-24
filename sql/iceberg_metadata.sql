-- Iceberg metadata architecture, made inspectable.
-- catalog -> metadata.json -> manifest list -> manifest files -> data files (Parquet)

-- The snapshot log: every commit is an atomic new snapshot (snapshot isolation, time travel).
SELECT committed_at, snapshot_id, operation, summary
FROM lake.silver."dim_customer$snapshots"
ORDER BY committed_at;

-- Manifests: the inventory layer between metadata.json and data files.
SELECT path, added_data_files_count, existing_data_files_count
FROM lake.silver."dim_customer$manifests";

-- Data files with per-column stats. This is what powers metadata-only pruning.
SELECT file_path, record_count, file_size_in_bytes
FROM lake.silver."dim_customer$files";

-- Time travel: query the table AS OF an earlier snapshot (schema/partition evolution safe).
-- SELECT * FROM lake.silver.dim_customer FOR VERSION AS OF <snapshot_id>;
-- SELECT * FROM lake.silver.dim_customer FOR TIMESTAMP AS OF TIMESTAMP '2026-08-19 00:00:00';

-- Hidden partitioning + partition evolution: the spec is versioned in metadata, so you can
-- repartition future data without rewriting history or breaking existing queries.
-- ALTER TABLE lake.silver.dim_customer SET PARTITION SPEC (segment);

-- ================================================================================================
-- Glossary
--   Metadata tree order   catalog → metadata.json → manifest list → manifest files → data files.
--   catalog               Maps table name → current metadata.json pointer (Nessie here).
--   metadata.json         Top table file: schema, partition specs, and the snapshot list.
--   manifest list         One per snapshot; lists the manifest files that make up that snapshot.
--   manifest file         Inventory of data files + per-file column stats (min/max/nulls).
--   data file             The actual Parquet file holding rows.
--   $snapshots            Iceberg metadata table: the commit/snapshot log (time travel source).
--   $manifests / $files   Metadata tables for inspecting manifests and data-file stats.
--   snapshot_id           Immutable id of a table version; target for time travel.
--   operation             Commit type in a snapshot summary (append / overwrite / delete).
--   Snapshot isolation    Atomic swap of metadata.json → readers never see a partial write.
--   FOR VERSION/TIMESTAMP AS OF   Time-travel syntax to read an older snapshot.
--   Hidden partitioning   Partition value derived from a column via a transform, not stored/queried directly.
--   Partition evolution   Change the partition spec for new data without rewriting old data.
-- ================================================================================================
