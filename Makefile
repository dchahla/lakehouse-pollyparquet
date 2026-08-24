.PHONY: up down infra seed batch bronze peek stream dbt query clean

# Seed knobs: `make seed ROWS=200 DAYS=30`
# (make consumes bare --flags itself, so these are variables, not CLI flags)
ROWS ?= 50
DAYS ?= 0

up:            ## start minio + nessie + kafka + spark + trino
	docker compose up -d
	@echo "MinIO console  http://localhost:9001  (minio/minio123)"
	@echo "Nessie (REST)  http://localhost:19120"
	@echo "Trino          http://localhost:8080"

infra:         ## terraform: buckets + warehouse + cost guardrails
	cd infra && terraform init -input=false && terraform apply -auto-approve

seed:          ## generate heterogeneous sources into bronze (ROWS=200 DAYS=30)
	python3 src/generate.py --mode batch --rows $(ROWS) --days $(DAYS)

bronze:        ## spark: land every file source into iceberg (lake.bronze.*)
	docker compose exec spark /opt/spark/bin/spark-submit /work/spark/land_bronze.py

peek:          ## show what landed in bronze, and that the contract holds
	docker compose exec trino trino --file /work/sql/peek_bronze.sql

batch:         ## spark: bronze -> silver (AQE + SCD2)
	docker compose exec spark /opt/spark/bin/spark-submit /work/spark/batch.py

# One shell for the whole recipe (note the trailing backslashes) so $! is the producer's PID
# and the trap can reach it. Without this each line gets its own shell, the backgrounded
# producer is orphaned, and ctrl-c stops only Spark while events keep piling into Kafka.
stream:        ## spark structured streaming, exactly-once (ctrl-c to stop)
	@python3 src/generate.py --mode stream & \
	 producer=$$!; \
	 trap 'kill $$producer 2>/dev/null' EXIT INT TERM; \
	 docker compose exec spark /opt/spark/bin/spark-submit /work/spark/stream.py

dbt:           ## ELT: snapshots (SCD2) + gold window models
	python3 src/run_dbt.py     # dbt via its Python API (pip3 console script isn't on PATH)

query:         ## run the window-function demo on Trino
	docker compose exec trino trino --file /work/sql/window_functions.sql

down:          ## stop everything
	docker compose down

clean:         ## drop local state
	rm -rf spark-warehouse checkpoints data dbt/target

# ---------------------------------------------------------------------------------------------
# Glossary
#   Makefile        Build tool config; each `target:` is a runnable command (`make <target>`).
#   .PHONY          Declares targets that are commands, not files, so make always runs them.
#   ##              Convention for self-documenting help text after a target.
#   ?=              Assign only if unset, so ROWS/DAYS can be overridden on the command line.
#   $$              Escapes a literal $ through make into the shell (so $$! is the shell's $!).
#   trap            Shell hook that runs a command on exit/interrupt; used to kill the producer.
#   docker compose  Starts/stops the multi-container stack defined in docker-compose.yml.
#   compose exec    Runs a command inside an already-running container (e.g. the spark service).
#   spark-submit    Spark's job launcher; submits a .py app to the cluster/driver.
#   terraform init  Downloads providers + prepares state before `apply`.
#   terraform apply Provisions/updates infra to match the .tf files (-auto-approve = no prompt).
#   dbt snapshot    Builds SCD2 history tables.
#   dbt run         Builds the SQL models (gold marts); the ELT step.
#   trino --file    Runs a .sql script against the Trino query engine.
# ---------------------------------------------------------------------------------------------
