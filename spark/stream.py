"""Structured Streaming, exactly-once end-to-end.

The three ingredients, together:
  1. replayable source  -> Kafka offsets
  2. checkpoint / WAL    -> records processed offsets + state (checkpointLocation)
  3. idempotent atomic sink -> each micro-batch commits as ONE Iceberg snapshot; a retried batch
     lands fully once or not at all.

This is why it is NOT the same as "at-least-once + downstream dedupe".
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("floci-stream").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")

schema = (StructType()
          .add("event_id", StringType()).add("customer_id", StringType())
          .add("amount", DoubleType()).add("ts", StringType()))

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")   # broker's in-network address (not localhost)
          .option("subscribe", "orders")
          .option("startingOffsets", "earliest")     # replay from committed offset on restart
          .load()
          .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*"))

query = (events.writeStream
         .format("iceberg")
         .outputMode("append")
         .option("checkpointLocation", "checkpoints/orders")   # the WAL that makes restarts exact
         .toTable("lake.bronze.orders"))                        # atomic snapshot commit per batch

query.awaitTermination()

# ==================================================================================================
# Glossary
#   Libraries / API
#     readStream.format("kafka")  Structured Streaming source that reads a Kafka topic as a stream.
#     StructType/StringType/...   Explicit schema types for parsing the JSON payload.
#     F.from_json                 Parses a JSON string column into typed struct columns.
#     writeStream                 Defines the streaming sink + its execution options.
#     outputMode("append")        Emit only new rows each micro-batch (fits append-only ingestion).
#     toTable("lake.bronze.orders")  Iceberg sink; commits each micro-batch as one atomic snapshot.
#     awaitTermination()          Blocks the driver so the stream keeps running until stopped.
#   Kafka options
#     kafka.bootstrap.servers     Broker address to connect to.
#     subscribe                   Topic(s) to consume.
#     startingOffsets=earliest    Where to begin when there is no committed offset (full replay).
#   Exactly-once: the three pieces, together
#     Replayable source           Kafka offsets let Spark re-read from the last committed position.
#     checkpointLocation (WAL)    Durably records processed offsets + state → exact restarts.
#     Idempotent atomic sink      Iceberg commits a batch fully once or not at all (transactional).
#     Micro-batch                 Structured Streaming's unit of processing (a small bounded batch).
#     vs. at-least-once + dedupe  Weaker guarantee; not equivalent to transactional exactly-once.
# ==================================================================================================
