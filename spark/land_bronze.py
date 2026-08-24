"""Land the file-based bronze sources into Iceberg.

Spark can read the raw files under data/bronze/; Trino and dbt cannot, because the shared `lake`
catalog is Iceberg-only. Registering every landed source as an Iceberg table makes it visible to
Trino, so the dbt snapshot and gold model run in-warehouse on the SAME copy Spark wrote.

Every source directory is landed, whatever its native format. The reader is chosen by what is
actually in the directory, so adding a thirteenth source needs no change here.
"""
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("land-bronze").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")

BRONZE = Path("data/bronze")
if not BRONZE.is_dir():
    raise SystemExit(f"{BRONZE}/ not found. Run `make seed` first.")

landed = 0
for d in sorted(p for p in BRONZE.iterdir() if p.is_dir()):
    # CSV => every column lands as STRING; typing is a downstream concern, which keeps bronze a
    # faithful copy of what arrived. Spark reads JSONL natively (one object per line).
    df = (spark.read.option("header", True).csv(str(d)) if any(d.glob("*.csv"))
          else spark.read.json(str(d)))

    # createOrReplace, not append: `make seed` adds new part FILES to the directory, and this reads
    # the whole directory every time. Appending here would double-count the batches already landed.
    df.writeTo(f"lake.bronze.{d.name}").using("iceberg").createOrReplace()
    print(f"landed lake.bronze.{d.name:<18} {df.count():>6} rows")
    landed += 1

print(f"\n{landed} bronze tables in the catalog. Same three ingest columns on every one.")
spark.stop()

# ==================================================================================================
# Glossary
#   Libraries / API
#     spark.read.csv/.json       Loads the raw landing files (header=True uses the first row as names).
#     writeTo(...).using(...)    DataFrameWriterV2: target an Iceberg catalog table by name.
#     createOrReplace()          Idempotent full refresh: rebuild the table from the whole directory.
#     CREATE NAMESPACE IF NOT EXISTS  Ensure the lake.bronze schema exists before writing.
#     Path.glob                  Pick the reader by what is on disk rather than a hardcoded list.
#   Concepts
#     Bronze as Iceberg          Registering file sources in the catalog makes them Trino/dbt-visible.
#     One copy, many engines     Spark writes; Trino and dbt read the same Iceberg tables via Nessie.
#     Schema-on-read (strings)   Bronze stays untyped; models cast at read (see revenue_by_segment).
#     Append vs full refresh     Seed appends files; this rebuilds from all of them, so counts are right.
# ==================================================================================================
