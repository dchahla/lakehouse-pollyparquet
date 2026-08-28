//! Land the file-based bronze sources into Iceberg, timing every step. The Rust arm of the
//! cross-language benchmark; Java and Go do the same work so the numbers line up.
//!
//!   cargo build --release
//!   ./target/release/land-bronze --sources crm_customers,web_events \
//!       --catalog-uri http://nessie:19120/iceberg/main --s3-endpoint http://minio:9000 \
//!       --bronze-dir ../../data/bronze --out ../results/rust.json
//!
//! For each source: read the raw CSV/JSONL, write one Parquet data file straight to S3 (MinIO) via
//! the table's own FileIO, then commit it as a new Iceberg snapshot through the Nessie REST catalog
//! — the same createOrReplace full-refresh spark/land_bronze.py does. read / write / commit are
//! timed separately. Everything lands as string (schema-on-read, like Spark's csv reader). The
//! result is one JSON blob matching common/result-schema.json.
// ================================================================================================
// Glossary
//   RestCatalogBuilder   Builds a Nessie REST catalog client; s3.* props ride along to the FileIO.
//   DataFileWriter       Iceberg's logical writer: arrow RecordBatch in, Vec<DataFile> out on close.
//   RollingFileWriter    Wraps the Parquet file writer, rolling to a new file past a size target.
//   DefaultLocationGenerator / DefaultFileNameGenerator  Put data files under the table's data/ prefix.
//   Transaction::fast_append  One new manifest + one REST commit, no manifest rewrite.
//   Compression::ZSTD    Same codec src/parquet_anatomy.py picks, so file sizes compare.
//   wall_ms_excl_startup Read + write + commit; the clock starts after arg parsing.
//   wall_ms_incl_startup Whole process, binary startup included; near-zero for a native binary.
// ================================================================================================

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;

use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

// Phase 1 works one CSV source and one JSON source, one of each on purpose. Phase 3 opens it up
// to all twelve; until the schema is locked there's no point porting the rest.
const DEFAULT_SOURCES: &[&str] = &["crm_customers", "web_events"];

#[tokio::main]
async fn main() {
    let process_start = Instant::now(); // wall clock, counts binary startup

    let args = Args::parse();

    // Start the "real work" clock only after flags are parsed, so startup lands in its own bucket.
    let work_start = Instant::now();

    let sources: Vec<String> = if args.sources == "all" {
        DEFAULT_SOURCES.iter().map(|s| s.to_string()).collect()
    } else {
        args.sources.split(',').map(|s| s.to_string()).collect()
    };

    let bronze = PathBuf::from(&args.bronze_dir);
    if !bronze.is_dir() {
        eprintln!("{} not found. Run `make seed` in the parent repo first.", args.bronze_dir);
        std::process::exit(1);
    }

    let catalog = build_catalog(&args).await.unwrap_or_else(|e| {
        eprintln!("building catalog: {e}");
        std::process::exit(1);
    });
    let namespace = NamespaceIdent::from_vec(
        args.namespace.split('.').map(|s| s.to_string()).collect(),
    )
    .unwrap();

    let mut result = BenchResult {
        impl_: "rust",
        run_id: uuid::Uuid::new_v4().to_string(),
        started_at: now_rfc3339(),
        sources: sources.clone(),
        totals: Totals::default(),
        per_source: Vec::new(),
        notes: String::new(),
        resources: Resources::default(),
    };

    for source in &sources {
        let dir = bronze.join(source);
        if !dir.is_dir() {
            eprintln!("skipping {source}: no such directory");
            continue;
        }
        let t = land_source(&catalog, &namespace, source, &dir)
            .await
            .unwrap_or_else(|e| {
                eprintln!("error on {source}: {e}");
                std::process::exit(1);
            });
        result.totals.rows += t.rows;
        result.totals.bytes_parquet += t.bytes_parquet;
        result.per_source.push(t);
    }

    result.totals.wall_ms_excl_startup = work_start.elapsed().as_millis() as u64;
    result.totals.wall_ms_incl_startup = process_start.elapsed().as_millis() as u64;
    result.resources = read_resources();

    write_result(&args.out, &result).unwrap_or_else(|e| {
        eprintln!("writing result: {e}");
        std::process::exit(1);
    });
    println!(
        "landed {} sources, {} rows -> {}",
        result.per_source.len(),
        result.totals.rows,
        args.out
    );
}

/// A RestCatalog wired to Nessie + MinIO. The s3.* props ride along on the catalog config; the REST
/// catalog forwards them to each table's FileIO, so data files land in MinIO. path-style + endpoint
/// are what make the S3 client talk to MinIO instead of real AWS.
async fn build_catalog(args: &Args) -> Result<impl Catalog, Box<dyn std::error::Error>> {
    let props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), args.catalog_uri.clone()),
        (REST_CATALOG_PROP_WAREHOUSE.to_string(), args.warehouse.clone()),
        ("s3.endpoint".to_string(), args.s3_endpoint.clone()),
        ("s3.access-key-id".to_string(), args.s3_access.clone()),
        ("s3.secret-access-key".to_string(), args.s3_secret.clone()),
        ("s3.path-style-access".to_string(), "true".to_string()),
        ("s3.region".to_string(), "us-east-1".to_string()),
    ]);
    // The REST catalog needs an S3 StorageFactory to reach MinIO; without it, table ops fail with
    // "StorageFactory must be provided". OpenDalStorageFactory::S3 uses the s3.* props above.
    Ok(RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", props)
        .await?)
}

/// Read one source dir, write its rows to a Parquet data file in S3, and commit to Iceberg.
async fn land_source(
    catalog: &impl Catalog,
    namespace: &NamespaceIdent,
    source: &str,
    dir: &Path,
) -> Result<SourceTiming, Box<dyn std::error::Error>> {
    let mut t = SourceTiming {
        source: source.to_string(),
        ..Default::default()
    };

    let read_start = Instant::now();
    let rows = read_rows(dir)?;
    t.read_ms = read_start.elapsed().as_millis() as u64;
    t.rows = rows.len();
    if rows.is_empty() {
        return Ok(t);
    }

    // Column order fixed from the first row (BTreeMap keeps keys sorted), so the arrow batch and the
    // iceberg schema line up field-for-field. Everything is a nullable string.
    let cols: Vec<String> = rows[0].keys().cloned().collect();
    let arrow_schema = arrow_string_schema(&cols);
    let iceberg_schema = iceberg_string_schema(&cols);

    // createOrReplace, same as spark/land_bronze.py: drop then create, so each run fully refreshes
    // the table from the whole directory rather than appending onto old batches.
    ensure_namespace(catalog, namespace).await?;
    let ident = TableIdent::new(namespace.clone(), source.to_string());
    if catalog.table_exists(&ident).await? {
        catalog.drop_table(&ident).await?;
    }
    let creation = TableCreation::builder()
        .name(source.to_string())
        .schema(iceberg_schema.clone())
        .build();
    let table = catalog.create_table(namespace, creation).await?;

    // WRITE: the data file goes straight to S3 (MinIO) through the table's FileIO. The location +
    // name generators put it under the table's data/ prefix, same layout Spark/Java produce.
    let write_start = Instant::now();
    let batch = to_record_batch(&arrow_schema, &cols, &rows)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default())) // matches parquet_anatomy.py
        .build();
    let parquet_builder =
        ParquetWriterBuilder::new(props, table.metadata().current_schema().clone());
    let location_gen = DefaultLocationGenerator::new(table.metadata())?;
    let name_gen =
        DefaultFileNameGenerator::new(source.to_string(), None, DataFileFormat::Parquet);
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    writer.write(batch).await?;
    let data_files = writer.close().await?;

    t.bytes_parquet = data_files.iter().map(|f| f.file_size_in_bytes()).sum();
    t.parquet_write_ms = write_start.elapsed().as_millis() as u64;

    // COMMIT: register the data files as a new snapshot through the catalog. fast_append writes one
    // new manifest and a single REST commit, no manifest rewrite. Timed on its own.
    let commit_start = Instant::now();
    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;
    tx.commit(catalog).await?;
    t.iceberg_commit_ms = commit_start.elapsed().as_millis() as u64;

    Ok(t)
}

async fn ensure_namespace(
    catalog: &impl Catalog,
    ns: &NamespaceIdent,
) -> Result<(), Box<dyn std::error::Error>> {
    if !catalog.namespace_exists(ns).await? {
        catalog.create_namespace(ns, HashMap::new()).await?;
    }
    Ok(())
}

/// All part files in a source dir, whichever format they're in.
fn read_rows(dir: &Path) -> Result<Vec<Row>, Box<dyn std::error::Error>> {
    let mut parts: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("part-"))
                .unwrap_or(false)
        })
        .collect();
    parts.sort();

    let mut rows = Vec::new();
    for part in parts {
        match part.extension().and_then(|e| e.to_str()) {
            Some("csv") => read_csv(&part, &mut rows)?,
            Some("jsonl") => read_jsonl(&part, &mut rows)?,
            _ => {}
        }
    }
    Ok(rows)
}

fn read_csv(path: &Path, rows: &mut Vec<Row>) -> Result<(), Box<dyn std::error::Error>> {
    let text = fs::read_to_string(path)?;
    let mut lines = text.lines();
    let headers: Vec<&str> = match lines.next() {
        Some(h) => h.split(',').collect(),
        None => return Ok(()),
    };
    for line in lines {
        // The generator writes plain values, so a straight split lines up column-for-column.
        let values: Vec<&str> = line.split(',').collect();
        let mut row = Row::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(v) = values.get(i) {
                row.insert(h.to_string(), v.to_string());
            }
        }
        rows.push(row);
    }
    Ok(())
}

fn read_jsonl(path: &Path, rows: &mut Vec<Row>) -> Result<(), Box<dyn std::error::Error>> {
    let text = fs::read_to_string(path)?;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let raw: serde_json::Map<String, serde_json::Value> = serde_json::from_str(line)?;
        let mut row = Row::new();
        for (k, v) in raw {
            // Stringify every value: bronze is untyped, and it keeps Rust lined up with the CSV path.
            let s = match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            row.insert(k, s);
        }
        rows.push(row);
    }
    Ok(())
}

/// An arrow schema with every named column a nullable Utf8 (feeds the RecordBatch). Each field
/// carries a PARQUET:field_id in its metadata — iceberg-rust matches arrow columns to Iceberg
/// fields by that id, not by position, so without it the writer reports "Field id N not found".
fn arrow_string_schema(cols: &[String]) -> Arc<ArrowSchema> {
    let fields: Vec<Field> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let meta = std::collections::HashMap::from([(
                "PARQUET:field_id".to_string(),
                (i + 1).to_string(), // ids start at 1, matching iceberg_string_schema
            )]);
            Field::new(c, DataType::Utf8, true).with_metadata(meta)
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// The matching Iceberg schema: every column an optional string. Field ids start at 1 and must be
/// stable, same as the Java side.
fn iceberg_string_schema(cols: &[String]) -> IcebergSchema {
    let fields: Vec<_> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            NestedField::optional((i + 1) as i32, c, Type::Primitive(PrimitiveType::String)).into()
        })
        .collect();
    IcebergSchema::builder()
        .with_fields(fields)
        .build()
        .unwrap()
}

/// Pivot the row maps into one arrow RecordBatch, one StringArray per column.
fn to_record_batch(
    schema: &Arc<ArrowSchema>,
    cols: &[String],
    rows: &[Row],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let columns = cols
        .iter()
        .map(|c| {
            let values: Vec<Option<&str>> = rows.iter().map(|r| r.get(c).map(|s| s.as_str())).collect();
            Arc::new(StringArray::from(values)) as arrow::array::ArrayRef
        })
        .collect();
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn write_result(path: &str, r: &BenchResult) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, serde_json::to_vec_pretty(r)?)?;
    Ok(())
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Peak RSS and CPU time from one getrusage call. ru_maxrss is KB on Linux (where the benchmark
/// containers run); ru_utime/ru_stime are timevals (sec + usec).
fn read_resources() -> Resources {
    // SAFETY: getrusage only writes into the rusage we hand it; RUSAGE_SELF is always valid.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return Resources::default();
        }
        Resources {
            peak_rss_mb: ru.ru_maxrss as f64 / 1024.0,
            user_cpu_ms: timeval_ms(ru.ru_utime),
            sys_cpu_ms: timeval_ms(ru.ru_stime),
        }
    }
}

fn timeval_ms(tv: libc::timeval) -> f64 {
    tv.tv_sec as f64 * 1000.0 + tv.tv_usec as f64 / 1000.0
}

// --- rows are ordered string maps so column order is deterministic ------------------------------
type Row = BTreeMap<String, String>;

// --- arg parsing: hand-rolled so there's no clap dependency for the flags ------------------------
struct Args {
    bronze_dir: String,
    out: String,
    sources: String,
    catalog_uri: String,
    warehouse: String,
    s3_endpoint: String,
    s3_access: String,
    s3_secret: String,
    namespace: String,
}

impl Args {
    fn parse() -> Args {
        let mut a = Args {
            bronze_dir: "data/bronze".to_string(),
            out: "result.json".to_string(),
            sources: "all".to_string(),
            catalog_uri: "http://nessie:19120/iceberg/main".to_string(),
            warehouse: "s3://warehouse/".to_string(),
            s3_endpoint: "http://minio:9000".to_string(),
            s3_access: "minio".to_string(),
            s3_secret: "minio123".to_string(),
            // "lake" is the catalog; the namespace within it is "bronze" (see Java's note).
            namespace: "bronze".to_string(),
        };

        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < argv.len() {
            let flag = &argv[i];
            let val = argv.get(i + 1).cloned().unwrap_or_default();
            match flag.as_str() {
                "--bronze-dir" => a.bronze_dir = val,
                "--out" => a.out = val,
                "--sources" => a.sources = val,
                "--catalog-uri" => a.catalog_uri = val,
                "--warehouse" => a.warehouse = val,
                "--s3-endpoint" => a.s3_endpoint = val,
                "--s3-access-key" => a.s3_access = val,
                "--s3-secret-key" => a.s3_secret = val,
                "--namespace" => a.namespace = val,
                other => eprintln!("unknown flag {other}, ignoring"),
            }
            i += 2;
        }
        a
    }
}

// --- result shape, serialized straight to JSON (matches common/result-schema.json) --------------
#[derive(Serialize)]
struct BenchResult {
    #[serde(rename = "impl")]
    impl_: &'static str,
    run_id: String,
    started_at: String,
    sources: Vec<String>,
    totals: Totals,
    per_source: Vec<SourceTiming>,
    notes: String,
    resources: Resources,
}

#[derive(Serialize, Default)]
struct Resources {
    peak_rss_mb: f64,
    user_cpu_ms: f64,
    sys_cpu_ms: f64,
}

#[derive(Serialize, Default)]
struct Totals {
    wall_ms_excl_startup: u64,
    wall_ms_incl_startup: u64,
    rows: usize,
    bytes_parquet: u64,
}

#[derive(Serialize, Default)]
struct SourceTiming {
    source: String,
    read_ms: u64,
    parquet_write_ms: u64,
    iceberg_commit_ms: u64,
    rows: usize,
    bytes_parquet: u64,
}

