"""Time the vanilla Spark bronze-landing as the benchmark baseline.

    # from the parent repo; the spark service mounts ./ at /work
    docker compose exec -w /work spark /opt/spark/bin/spark-submit \
        bench-land-bronze/spark_baseline.py

Configured by environment, not flags: BRONZE_DIR (default data/bronze), SOURCES (default
crm_customers,web_events), OUT (default bench-land-bronze/results/spark.json). Override with
`docker compose exec -w /work -e SOURCES=crm_customers spark ...`.

This is spark/land_bronze.py's exact write path — read the raw files, then
`writeTo(...).using("iceberg").createOrReplace()` — but timed per source and emitting the common
result schema (common/result-schema.json), so the out-of-the-box distributed engine sits in the
same comparison table as the bespoke Go/Rust/Java writers.

The real land_bronze.py is left untouched; this is a sibling so the baseline stays honest. Two
clocks, same as the native impls: excl_startup starts after the SparkSession is up (the write+commit
work only); incl_startup counts from process start, so Spark's session/JVM init — a genuine
per-invocation cost of that architecture — is visible rather than hidden. Spark's JSON reader infers
types where the native impls force all-string; that divergence is noted in the README caveats.
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROCESS_START = time.monotonic()   # counts SparkSession/JVM init

from pyspark.sql import SparkSession   # noqa: E402  (import after the clock starts, on purpose)


def main() -> None:
    bronze_dir = os.environ.get("BRONZE_DIR", "data/bronze")
    out_path = os.environ.get("OUT", "bench-land-bronze/results/spark.json")
    sources_arg = os.environ.get("SOURCES", "crm_customers,web_events")
    sources = [s for s in sources_arg.split(",") if s]

    spark = SparkSession.builder.appName("land-bronze-baseline").getOrCreate()
    # excl_startup clock starts here: session is up, everything after is the real work.
    work_start = time.monotonic()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")

    bronze = Path(bronze_dir)
    if not bronze.is_dir():
        raise SystemExit(f"{bronze}/ not found. Run `make seed` first.")

    per_source = []
    total_rows = 0
    for name in sources:
        d = bronze / name
        if not d.is_dir():
            print(f"skipping {name}: no such directory", file=sys.stderr)
            continue

        read_start = time.monotonic()
        df = (spark.read.option("header", True).csv(str(d)) if any(d.glob("*.csv"))
              else spark.read.json(str(d)))
        rows = df.count()   # forces the read; also our row count
        read_ms = int((time.monotonic() - read_start) * 1000)

        # write + commit are one createOrReplace in Spark — same fused shape as iceberg-go, so it all
        # lands in parquet_write_ms and iceberg_commit_ms stays 0.
        write_start = time.monotonic()
        df.writeTo(f"lake.bronze.{name}").using("iceberg").createOrReplace()
        write_ms = int((time.monotonic() - write_start) * 1000)

        per_source.append({
            "source": name,
            "read_ms": read_ms,
            "parquet_write_ms": write_ms,
            "iceberg_commit_ms": 0,      # fused into the write, like Go
            "rows": rows,
            "bytes_parquet": 0,          # Spark doesn't surface it here; left 0
        })
        total_rows += rows
        print(f"landed lake.bronze.{name:<18} {rows:>6} rows")

    result = {
        "impl": "spark",
        "run_id": str(uuid.uuid4()),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
        "totals": {
            "wall_ms_excl_startup": int((time.monotonic() - work_start) * 1000),
            "wall_ms_incl_startup": int((time.monotonic() - PROCESS_START) * 1000),
            "rows": total_rows,
            "bytes_parquet": 0,
        },
        "per_source": per_source,
        "notes": "Spark JSON reader infers types (native impls force all-string); write+commit fused via createOrReplace.",
        "resources": {"peak_rss_mb": 0, "user_cpu_ms": 0, "sys_cpu_ms": 0},
    }

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2))
    print(f"\nbaseline: {len(per_source)} sources, {total_rows} rows -> {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
