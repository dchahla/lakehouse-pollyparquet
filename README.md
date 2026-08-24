# Polly Parquet — Lakehouse in Your Pocket

A hands-on walkthrough of Parquet, Iceberg, and medallion architecture. Build a working lakehouse on your laptop with no cloud account, no bill. Everything runs in Docker.

Read the full story at [chahla.net/blog](https://chahla.net/blog) (Phase 1: Polly Parquet).

## Stack (decoupled storage / compute, the lakehouse point)

| Layer | Choice | Why |
|-------|--------|-----|
| Storage | MinIO (S3 API) | Object storage; swap for real S3 with one var |
| Table format | Apache Iceberg | Metadata tree, time travel, hidden partitioning |
| Catalog | Nessie (Iceberg REST) | Git-like catalog, multi-engine |
| Batch/stream engine | Spark + AQE | The Must-Have; exactly-once streaming sink |
| Query / warehouse layer | Trino | Reads the same Iceberg tables, no copy |
| Transform (ELT) | dbt | SCD2 snapshots + gold window analytics |
| IaC | Terraform | Buckets + warehouse + cost guardrails, tight |

## 60-second quickstart

Prerequisites
You'll need these installed:

Docker (with Compose) — running
Python (for the data generator in src/)
Terraform (for make infra)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt (dbt-core, dbt-trino, kafka-python, pyarrow)
Run it (from the project root)

```bash
make up        # start minio + nessie + kafka + spark + trino containers
make infra     # terraform: create bronze/silver/gold buckets + warehouse
make seed      # generate 12 heterogeneous sources into bronze (ROWS=200 DAYS=30)
make bronze    # land every file source into iceberg (lake.bronze.*)
make peek      # show what landed + prove the ingest contract holds
make batch     # spark bronze -> silver (AQE + SCD2)
make stream    # spark structured streaming, exactly-once (ctrl-c to stop)
make dbt       # ELT: snapshots (SCD2) + gold marts
make query     # run sql/window_functions.sql on Trino
make down      # stop everything
Run them in order — each step depends on the previous one (containers up → infra → data → transforms → query).
```

`make bronze` is not optional: `make dbt` reads Iceberg tables, not the raw files, and fails on an
unresolved source without it. `ROWS` and `DAYS` are make variables, not flags, so
`make seed ROWS=500 DAYS=90` works and `make seed --days 90` does not. Seeding is append-only: run
it again and a second batch lands alongside the first.

Where things end up once make up is running
MinIO console → http://localhost:9001 (login minio / minio123)
Nessie (Iceberg REST catalog) → http://localhost:19120
Trino → http://localhost:8080
Notes
The stack is deliberately minimal/illustrative — a walkthrough of what actually happens under the hood. MinIO has no volume and Nessie's catalog is in-memory, so `make down` takes your data with it. Perfect for learning, wrong for production.
`make clean` drops local state (warehouse, checkpoints, data, dbt target) if you want a fresh start.

## Glossary

| Term / tool | What it is / does |
|-------------|-------------------|
| **MinIO** | Open-source, S3-API-compatible object store; the local stand-in for AWS S3. |
| **S3 API** | The object-storage protocol both MinIO and AWS S3 speak, so code is portable between them. |
| **Apache Iceberg** | Open table format adding ACID, time travel, and schema/partition evolution over Parquet. |
| **Nessie** | Iceberg REST catalog with git-like branches; tells engines which table metadata is current. |
| **REST catalog** | Standard HTTP catalog API for Iceberg, so Spark and Trino share one source of truth. |
| **Apache Spark** | Distributed batch + streaming engine; the "Must-Have" skill this repo exercises. |
| **AQE** | Adaptive Query Execution — Spark re-optimizes at runtime (coalesce, broadcast, skew). |
| **Trino** | Distributed SQL engine that queries the Iceberg tables directly (no data copy). |
| **dbt** | Runs SQL transformations + SCD2 snapshots in-warehouse; the ELT layer. |
| **Terraform** | Declarative IaC; provisions buckets, the Iceberg warehouse, and cost guardrails. |
| **Docker Compose** | Defines/starts the multi-container local stack from `docker-compose.yml`. |
| **target** | A named command in the `Makefile` (e.g. `make batch`); the repo's command surface. |
| **Decoupled storage/compute** | Data lives once in object storage; multiple engines read it independently. |
| **Bronze / Silver / Gold** | Medallion layers: raw landing / conformed / aggregated marts. |
