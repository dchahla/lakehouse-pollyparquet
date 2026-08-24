"""Generate heterogeneous sources as batch files, or stream order events to Kafka.

Many shapes (JDBC-like, CSV, JSON, API, CDC) land under one bronze contract with ingest
metadata, so the source count never changes the architecture.

    python src/generate.py --mode batch --rows 200 --days 30
    python src/generate.py --mode stream

Batch mode is append-only: each run writes a new part file per source. --days N backdates
rows across the last N days instead of stamping them all with now.
"""
import argparse, csv, json, random, time, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Heterogeneous sources, each with its own native shape.
SOURCES = {
    "crm_customers":     ("csv",  ["id", "name", "segment", "region"]),
    "erp_products":      ("csv",  ["sku", "category", "unit_cost"]),
    "web_events":        ("json", ["event_id", "customer_id", "url", "ts"]),
    "mobile_events":     ("json", ["event_id", "customer_id", "screen", "ts"]),
    "billing_invoices":  ("csv",  ["invoice_id", "customer_id", "amount", "status"]),
    "support_tickets":   ("json", ["ticket_id", "customer_id", "priority", "state"]),
    "hr_employees":      ("csv",  ["emp_id", "dept", "salary"]),          # feeds window demo
    "inventory_cdc":     ("json", ["sku", "qty", "op", "ts"]),            # CDC-style
    "marketing_api":     ("json", ["campaign_id", "customer_id", "channel"]),
    "partner_feed":      ("csv",  ["partner_id", "customer_id", "referral"]),
    "iot_telemetry":     ("json", ["device_id", "metric", "value", "ts"]),
    "finance_ledger":    ("csv",  ["txn_id", "account", "debit", "credit"]),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _backdate(days: int) -> datetime:
    """A random instant inside the last `days`. days=0 means right now."""
    now = datetime.now(timezone.utc)
    return now if days <= 0 else now - timedelta(seconds=random.uniform(0, days * 86400))


def _row(cols: list[str], i: int, ts: datetime, n_customers: int) -> dict:
    """A plausible value per column; the shape matters more than the data here."""
    v = {c: random.randint(1, 100) for c in cols}
    # Stable business key. Re-running seed gives the same customers with newly-rolled
    # segments, which is what SCD2 needs to version something real.
    if "id" in cols:          v["id"] = i
    # Point foreign keys at ids that exist, so the joins downstream land.
    if "customer_id" in cols: v["customer_id"] = random.randint(0, max(n_customers - 1, 0))
    if "name" in cols:        v["name"] = f"cust_{i}"
    if "segment" in cols:     v["segment"] = random.choice(["smb", "mid", "ent"])
    if "region" in cols:      v["region"] = random.choice(["na", "emea", "apac"])
    if "dept" in cols:        v["dept"] = random.choice(["eng", "sales", "ops"])
    if "salary" in cols:      v["salary"] = random.choice([90, 110, 130, 150, 175]) * 1000
    if "ts" in cols:          v["ts"] = ts.isoformat()
    return v


def batch(out: Path, rows: int, days: int) -> None:
    batch_id = str(uuid.uuid4())     # ONE per run. That is what a batch id means.
    suffix = batch_id[:8]
    for source, (fmt, cols) in SOURCES.items():
        d = out / source
        d.mkdir(parents=True, exist_ok=True)
        records = []
        for i in range(rows):
            ts = _backdate(days)          # one instant for both the row's ts and _ingested_at
            records.append({**_row(cols, i, ts, rows),
                            "_source": source,
                            "_ingested_at": ts.isoformat(),
                            "_batch_id": batch_id})
        # New file per run, never a rewrite. That is what append-only bronze means.
        if fmt == "csv":
            with (d / f"part-{suffix}.csv").open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(records[0]))
                w.writeheader(); w.writerows(records)
        else:
            (d / f"part-{suffix}.jsonl").write_text("\n".join(json.dumps(r) for r in records))
        print(f"bronze/{source:<18} {fmt:<4} {rows:>5} rows -> part-{suffix}")
    span = f"backdated across {days}d" if days > 0 else "stamped now"
    print(f"\n{len(SOURCES)} sources landed under {out}/ ({span}) as batch {suffix}.")
    print("Re-run to append another batch; nothing above was overwritten.")


def stream() -> None:
    """Idempotent event stream for the exactly-once demo. Requires kafka-python + a broker."""
    from kafka import KafkaProducer  # optional dep; only needed for --mode stream
    from kafka.serializer import Serializer

    class JsonSerializer(Serializer):  # subclass silences kafka-python's Serializer type-check warning
        def serialize(self, topic, value):
            return json.dumps(value).encode()

    p = KafkaProducer(bootstrap_servers="localhost:9092",
                      value_serializer=JsonSerializer())
    print("producing order events to topic 'orders' (ctrl-c to stop)")
    while True:
        evt = {"event_id": str(uuid.uuid4()),           # stable key => sink dedupe possible
               "customer_id": random.randint(1, 50),
               "amount": round(random.uniform(5, 500), 2),
               "ts": _now()}
        p.send("orders", evt); p.flush()
        time.sleep(0.5)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["batch", "stream"], default="batch")
    ap.add_argument("--out", default="data/bronze")
    ap.add_argument("--rows", type=int, default=50, help="rows per source per run")
    ap.add_argument("--days", type=int, default=0, help="backdate rows across the last N days")
    args = ap.parse_args()
    batch(Path(args.out), args.rows, args.days) if args.mode == "batch" else stream()

# ==================================================================================================
# Glossary
#   Libraries
#     argparse         Stdlib CLI parser; defines --mode / --out / --rows / --days.
#     csv              Stdlib CSV reader/writer; DictWriter maps dicts -> rows for row-shaped sources.
#     json             Stdlib JSON; used for JSONL (one JSON object per line) semi-structured sources.
#     uuid             Stdlib unique IDs; uuid4() gives the per-run batch id and stable event keys.
#     datetime/timedelta/timezone  ISO-8601 UTC timestamps, and the backdating window.
#     pathlib.Path     Object-oriented filesystem paths; mkdir(parents/exist_ok) creates layout.
#     kafka.KafkaProducer  (kafka-python) Publishes events to a Kafka topic; optional, stream mode only.
#   Concepts
#     Heterogeneous sources  Deliberately mixed shapes (CSV/JSON, JDBC/API/CDC-like).
#     Landing contract   Every row gets _source/_ingested_at/_batch_id so bronze is uniform.
#     Append-only        Each run writes a new part file; bronze never rewrites history.
#     Business key       `id` is stable across runs, so SCD2 can version a real entity.
#     JSONL              Newline-delimited JSON; append-friendly, streamable.
#     CDC                Change Data Capture; inventory_cdc emits op/ts like a change stream.
#     Idempotent key     Stable event_id lets a downstream sink dedupe, supporting exactly-once.
#     bootstrap_servers  Kafka broker address the producer connects to.
#     topic              Named Kafka stream ("orders") that consumers/Spark subscribe to.
#     flush()            Forces buffered records to the broker before continuing.
# ==================================================================================================
