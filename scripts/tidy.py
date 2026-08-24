"""Strip interview framing (Topic N) and em/en dashes from the repo's code comments."""
import io, pathlib, re

ROOT = pathlib.Path("/home/air/repos/portfolio/pollydockette/basic-warehouse")

# Explicit rewrites: each is a full-line replacement so no sentence gets mangled.
PAIRS = [
    # --- Topic references -------------------------------------------------------------------
    ("-- Topic 3: performance tuning workflow — EXPLAIN first, then fix what the plan reveals.",
     "-- Performance tuning workflow. EXPLAIN first, then fix what the plan reveals."),
    ("-- Topic 1 — Iceberg metadata architecture, made inspectable.",
     "-- Iceberg metadata architecture, made inspectable."),
    ("-- Topic 10: window functions. Runs on Trino over Iceberg (gold layer).",
     "-- Window functions. Runs on Trino over Iceberg (gold layer)."),
    ("-- Topic 4: cost optimization. Cost ≈ bytes scanned + compute-seconds. Attack both.",
     "-- Cost optimization. Cost is roughly bytes scanned plus compute-seconds. Attack both."),
    ('"""Structured Streaming, exactly-once end-to-end (Topic 8).',
     '"""Structured Streaming, exactly-once end-to-end.'),
    ("#   Exactly-once (Topic 8) — the three pieces, together",
     "#   Exactly-once: the three pieces, together"),
    ("#     .explain(True)             Prints the physical/logical plan — the Topic 3 tuning starting point.",
     "#     .explain(True)             Prints the physical/logical plan; the tuning starting point."),
    ('"""Run the dbt ELT (snapshot + run) via dbt\'s Python API (Topic 5).',
     '"""Run the dbt ELT (snapshot + run) via dbt\'s Python API.'),
    ("-- Topic 6 — SCD Type 2 the declarative way. dbt maintains dbt_valid_from / dbt_valid_to and a",
     "-- SCD Type 2 the declarative way. dbt maintains dbt_valid_from / dbt_valid_to and a"),
    ("-- Topic 5 (ELT) + Topic 10 (windows): a gold mart built by transforming silver in-warehouse.",
     "-- A gold mart built by transforming silver in-warehouse: ELT, plus window functions."),
    ("--   ELT                  This transform runs in-warehouse (Trino) after raw load — Topic 5.",
     "--   ELT                  This transform runs in-warehouse (Trino) after raw load."),

    # --- Em dashes, no Topic involved -------------------------------------------------------
    ("#   IaC                       Infrastructure as Code — infra defined in versioned files, not clicks.",
     "#   IaC                       Infrastructure as Code; infra defined in versioned files, not clicks."),
    ("#   list(string)   An ordered collection of strings — here the medallion + warehouse bucket names.",
     "#   list(string)   An ordered collection of strings; here the medallion + warehouse bucket names."),
    ("-- Fix 1 — partition pruning: filtering on the partition column (region) should drop the scan to",
     "-- Fix 1, partition pruning: filtering on the partition column (region) should drop the scan to"),
    ("-- Fix 2 — broadcast the small dimension so the fact table isn't reshuffled.",
     "-- Fix 2, broadcast the small dimension so the fact table isn't reshuffled."),
    ("-- MPP-native reminder: the levers are pruning, distribution, sort/cluster, and file sizing —",
     "-- MPP-native reminder: the levers are pruning, distribution, sort/cluster, and file sizing."),
    ("--   Broadcast join      Chosen when one side is small enough — avoids reshuffling the large table.",
     "--   Broadcast join      Chosen when one side is small enough; avoids reshuffling the large table."),
    ("-- Data files with per-column stats — this is what powers metadata-only pruning.",
     "-- Data files with per-column stats. This is what powers metadata-only pruning."),
    ('--   DENSE_RANK()        Ranks with no gaps after ties (1,1,2) — used for "top earner per dept".',
     '--   DENSE_RANK()        Ranks with no gaps after ties (1,1,2); used for "top earner per dept".'),
    ("-- Archive cold raw data (bronze) — see infra/main.tf lifecycle rule for the storage-tier side.",
     "-- Archive cold raw data (bronze). See infra/main.tf lifecycle rule for the storage-tier side."),
    ("--   Bytes scanned               Primary cost driver in usage-priced engines — tuning to scan less = save.",
     "--   Bytes scanned               Primary cost driver in usage-priced engines; scan less, pay less."),
    ("-- MERGE in spark/batch.py — shown both ways on purpose.",
     "-- MERGE in spark/batch.py. Shown both ways on purpose."),
    ("--   SCD Type 2           Versioned dimension rows preserving history — same result as the Spark MERGE.",
     "--   SCD Type 2           Versioned dimension rows preserving history; same result as the Spark MERGE."),
    ("#   ELT                 Transform runs inside the warehouse (Trino) after raw load — dbt's model.",
     "#   ELT                 Transform runs inside the warehouse (Trino) after raw load; dbt's model."),
    ("-- Ranks segments by revenue and shows each segment's share — window functions over an aggregate.",
     "-- Ranks segments by revenue and shows each segment's share, via window functions over an aggregate."),
]

EXT = {".py", ".sql", ".yml", ".tf", ""}
hits, misses = 0, []
for f in ROOT.rglob("*"):
    if not f.is_file() or ".git" in f.parts or f.name == "PLAN.md":
        continue
    if f.suffix not in EXT and f.name != "Makefile":
        continue
    try:
        s = io.open(f, encoding="utf-8").read()
    except (UnicodeDecodeError, IsADirectoryError):
        continue
    orig = s
    for a, b in PAIRS:
        if a in s:
            s = s.replace(a, b)
            hits += 1
    if s != orig:
        io.open(f, "w", encoding="utf-8").write(s)
        print(f"rewrote {f.relative_to(ROOT)}")

print(f"\n{hits} replacements applied")
