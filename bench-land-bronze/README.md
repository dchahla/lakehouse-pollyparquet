# Cross-language Iceberg bronze-landing benchmark

Benchmark the raw-data-to-Iceberg-table pipeline (`spark/land_bronze.py`) reimplemented in Java, Go, and Rust against the Python/Spark baseline. Same input files, same output (Iceberg tables in the same Nessie/MinIO catalog), measured and compared.

## Architecture

- **Replicates**: read CSV/JSONL from `data/bronze/{source}/`, write Parquet, commit to Iceberg REST catalog (Nessie via MinIO)
- **Common harness**: all implementations take the same CLI flags, emit JSON results in a common schema
- **Reuses parent stack**: points at the already-running MinIO/Nessie from `make up` in the parent repo (no duplicate docker-compose)

## Prerequisites

From the parent repo root:
```bash
make up                # start minio + nessie + kafka + spark + trino containers
make seed ROWS=100000  # generate data/bronze/, about 100,200 rows across the two sources
```

Seed at 100k. The Makefile default is `ROWS=50`, and a job that small finishes before it starts, so
all you end up measuring is fixed process overhead wearing a language's name.

Nothing to install in this directory. `common/compare.py` is standard-library only, and
`spark_baseline.py` runs inside the Spark container.

The real-commit round additionally needs `make infra` in the parent repo, because the commit writes to `s3://warehouse/` and without the bucket you get `NoSuchBucketException` instead of a benchmark.

## Running

### Build and run all implementations (5 runs each, median of last 4)
```bash
./common/run_matrix.sh
```

### Build and run specific implementation (real commit)

`run_matrix.sh` passes no catalog arguments and joins no network, so it only exercises the local
write. To drive the real commit, run one implementation at a time on the compose network.

Use the compose network, not `--network host`. From the host you can reach Nessie fine, and then
Nessie hands back its own in-network S3 endpoint (`http://minio:9000`) which your host cannot
resolve. The catalog decides where the data files live, not your client. Compose names the network
after the project, which defaults to the directory the stack was brought up from, so find yours:

```bash
docker network ls
```

```bash
# Java
docker build -f docker/Dockerfile.java -t land-bronze-java .
docker run --rm --network <your-compose-network> -v $(pwd)/../:/work land-bronze-java \
  --bronze-dir /work/data/bronze --catalog-uri http://nessie:19120/iceberg/main \
  --warehouse s3://warehouse/ --s3-endpoint http://minio:9000 \
  --s3-access-key minio --s3-secret-key minio123 \
  --namespace lake.bronze --sources crm_customers,web_events \
  --out /work/bench-land-bronze/results/java-manual.json

# Go and Rust take the same flags; swap the Dockerfile and image name.
```

### Run the Spark baseline

`spark_baseline.py` is not built into an image; it runs in the Spark container from the parent stack,
which already mounts the repo at `/work`. It is configured by environment variables rather than
flags: `BRONZE_DIR`, `SOURCES`, `OUT`.

```bash
# from the parent repo
docker compose exec -w /work spark /opt/spark/bin/spark-submit \
  bench-land-bronze/spark_baseline.py
```

### Compare results
```bash
python3 common/compare.py results/matrix-<stamp>
```
With no argument it reads `results/*.json`. Given a matrix directory it drops the first run per
implementation as a cold-cache warm-up and reports the median of the rest.

## Implementation status

| Language | Phase | Status | Notes |
|----------|-------|--------|-------|
| Java     | 3     | ✓ Parquet write + Iceberg commit | RESTCatalog + AppendFiles, via Iceberg's own `Parquet.write` appender |
| Go       | 3a    | ✓ Parquet write + Iceberg commit | iceberg-go; needs the `io/gocloud` blank import to register the `s3://` scheme |
| Rust     | 3b    | ✓ Parquet write + Iceberg commit | iceberg-rust pre-1.0; needs a fork for the S3 `StorageFactory` |
| Python   | baseline | ✓ Spark wrapper | Runs inside the parent stack's Spark container via `spark-submit` |

All three natives take `--sources all` or a comma-separated subset. The published benchmark used two,
`crm_customers` (CSV) and `web_events` (JSONL).

## Output schema

`run_matrix.sh` writes one JSON per run to `results/matrix-<stamp>/<impl>-run<N>.json`; a manual run
writes wherever `--out` points. Either way the shape is the same:
- `impl`: language identifier
- `run_id`: UUID
- `started_at`: ISO 8601 timestamp
- `sources`: list of source names processed
- `totals.wall_ms_excl_startup`: headline metric (read+parse+write+commit, excluding process startup)
- `totals.wall_ms_incl_startup`: secondary metric (total time including JVM/process startup)
- `per_source[].{read_ms, parquet_write_ms, iceberg_commit_ms, rows, bytes_parquet}`: breakdown by source
- `resources.{peak_rss_mb, user_cpu_ms, sys_cpu_ms}`: self-reported from inside the process (`getrusage` on Go and Rust, `/proc/self/status` on Java), so there is no host-side wrapper to argue about
- `notes`: optional caveats (e.g. Spark's fused write+commit clock)

See `common/result-schema.json` for full schema.

## Caveats

- **Java**: JVM startup overhead is baked into `wall_ms_incl_startup` — a real cost for batch invocation, not a tuning failure. Both metrics are reported so you can choose which "fair" comparison matters to you.
- **Go/Rust**: both commit through their native Iceberg library (`iceberg-go`, `iceberg-rust`). There is no raw-REST fallback path in the code.
- **Fused clocks**: `iceberg-go` performs the Parquet write and the commit inside one `AppendTable`/`Commit` call, so Go reports the whole thing in `parquet_write_ms` and leaves `iceberg_commit_ms` at 0. Spark fuses them too, inside `createOrReplace`. Only Java and Rust split the two phases, so compare per-phase numbers only against those.
- **All-string columns**: every native implementation forces all columns to nullable STRING (schema-on-read), for CSV and JSONL alike, matching Spark's CSV reader.
- **Spark infers JSON types**: Spark's JSON reader types columns (int/float/string) where the natives force string, so the Parquet files are comparable in size but not byte-identical in schema. Compare schemas via Nessie's REST metadata endpoint if you want to check.
- **createOrReplace semantics**: each run rebuilds the whole table from the whole directory, not appended — repeat runs are not faster (idempotency is the point, not incremental updates).

## Verification

After running:
1. Confirm `results/matrix-<stamp>/` holds a JSON per run for all implementations
2. Run `python3 common/compare.py results/matrix-<stamp>` to see the summary table
3. Spot-check one table cross-engine:
   ```bash
   # From parent repo
   make peek
   # or in Trino: SELECT COUNT(*) FROM lake.bronze.crm_customers;
   ```

## Project structure

```
bench-land-bronze/
  README.md                   # this file
  results/                    # gitignored: run outputs
  common/
    result-schema.json        # canonical output schema
    run_matrix.sh             # orchestration script (Phase 4)
    compare.py                # comparison table script
  go/
    go.mod / go.sum
    cmd/land-bronze/main.go
  rust/
    Cargo.toml / Cargo.lock
    src/main.rs
  java/
    pom.xml
    src/main/java/com/bench/landbronze/LandBronze.java
  docker/
    Dockerfile.go
    Dockerfile.rust
    Dockerfile.java
```
