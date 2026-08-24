"""Bronze -> Silver batch.

AQE (dynamic broadcast + skew join + coalesce), partitioning for pruning, and an SCD Type 2
MERGE that versions dimension rows with effective dating + is_current.
"""
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("batch").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")

# --- Read bronze (many shapes) and conform to a customer entity --------------------------------
# CSV lands every column as STRING; cast id to match the INT column in the target table.
#
# Bronze is append-only, so this directory holds one part file per seed run and every id appears
# once per batch. MERGE needs exactly one source row per business key, so we have to collapse
# them. Which one we keep is the whole ballgame: dropDuplicates() takes an arbitrary row, so the
# history records whichever version Spark happened to hold. Take the newest by _ingested_at.
newest = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
raw = (spark.read.option("header", True).csv("data/bronze/crm_customers")
       .selectExpr("CAST(id AS INT) AS id", "name", "segment", "region", "_ingested_at")
       .withColumn("_rn", F.row_number().over(newest))
       .filter("_rn = 1")
       .drop("_rn", "_ingested_at")
       .withColumn("effective_from", F.current_timestamp()))

# Partitioning is a pruning lever: queries filtered by region skip whole files.
(spark.sql("""
  CREATE TABLE IF NOT EXISTS lake.silver.dim_customer (
    id INT, name STRING, segment STRING, region STRING,
    effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN
  ) USING iceberg PARTITIONED BY (region)
"""))

raw.createOrReplaceTempView("incoming")

# --- SCD Type 2: close changed rows, insert new versions ---------------------------------------
# Step 1: expire current rows whose tracked attributes changed.
spark.sql("""
  MERGE INTO lake.silver.dim_customer t
  USING incoming s ON t.id = s.id AND t.is_current = true
  WHEN MATCHED AND (t.segment <> s.segment OR t.region <> s.region)
    THEN UPDATE SET t.is_current = false, t.effective_to = current_timestamp()
""")
# Step 2: insert the new/changed versions as current.
spark.sql("""
  MERGE INTO lake.silver.dim_customer t
  USING incoming s ON t.id = s.id AND t.is_current = true
  WHEN NOT MATCHED THEN INSERT
    (id, name, segment, region, effective_from, effective_to, is_current)
    VALUES (s.id, s.name, s.segment, s.region, current_timestamp(), NULL, true)
""")

# AQE handles the join strategy + skew at runtime; no manual broadcast hint needed. Inspect with:
spark.sql("SELECT region, count(*) FROM lake.silver.dim_customer GROUP BY region").explain(True)
print("silver.dim_customer written with SCD2 history.")

# --- Conform the sources the window-function demo reads, queried later via Trino ---------------
# CSV lands as STRING; cast numeric columns so Trino's ranking / running-total windows work.
hr = (spark.read.option("header", True).csv("data/bronze/hr_employees")
      .selectExpr("CAST(emp_id AS INT) AS emp_id", "dept", "CAST(salary AS INT) AS salary"))
hr.writeTo("lake.silver.hr_employees").using("iceberg").createOrReplace()

inv = (spark.read.option("header", True).csv("data/bronze/billing_invoices")
       .selectExpr("CAST(invoice_id AS INT) AS invoice_id", "CAST(customer_id AS INT) AS customer_id",
                   "CAST(amount AS DOUBLE) AS amount", "status"))
inv.writeTo("lake.silver.billing_invoices").using("iceberg").createOrReplace()
print("silver.hr_employees + silver.billing_invoices conformed for the window demo.")

spark.stop()

# ==================================================================================================
# Glossary
#   Libraries / API
#     pyspark.sql.SparkSession   Entry point to Spark SQL/DataFrames; .builder configures the app.
#     functions as F             Column expression helpers (F.current_timestamp(), F.col(), ...).
#     Window / row_number       Rank rows within a key so you can keep the newest, not just any.
#     spark.read.csv/.option     Loads external data into a DataFrame; header=True uses the first row.
#     createOrReplaceTempView    Registers a DataFrame as a SQL-queryable view for the session.
#     spark.sql(...)             Runs SQL (DDL + MERGE) against catalog tables.
#     .explain(True)             Prints the physical/logical plan; the tuning starting point.
#     spark.stop()               Releases the cluster resources / ends the app.
#   Iceberg / SQL
#     USING iceberg              Creates an Iceberg-managed table (ACID + snapshots).
#     PARTITIONED BY (region)    Physical partitioning → enables partition pruning on region filters.
#     MERGE INTO ... WHEN ...     Upsert primitive; the two-step MERGE implements SCD Type 2.
#     is_current / effective_from/to  SCD2 versioning columns: which row version is live, and when.
#   Concepts
#     Conform                    Reshape a raw source into a shared entity schema (bronze → silver).
#     AQE                        Adaptive Query Execution picks broadcast/skew strategy at runtime.
#     Surrogate vs. business key business key = id from source; surrogate = per-version identity.
# ==================================================================================================
