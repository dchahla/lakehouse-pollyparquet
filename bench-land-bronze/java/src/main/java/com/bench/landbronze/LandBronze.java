/*
 * Land the file-based bronze sources into Iceberg, timing every step. The Java arm of the
 * cross-language benchmark; Go and Rust do the same work so the numbers line up.
 *
 *   mvn clean package -DskipTests
 *   java -jar target/land-bronze.jar --sources crm_customers,web_events \
 *        --catalog-uri http://nessie:19120/iceberg/main --s3-endpoint http://minio:9000 \
 *        --bronze-dir ../../data/bronze --out ../results/java.json
 *
 * For each source: read the raw CSV/JSONL, write one Parquet data file straight to S3 (MinIO) via
 * the table's own FileIO, then commit it as a new Iceberg snapshot through the Nessie REST catalog
 * — the same createOrReplace full-refresh spark/land_bronze.py does. read / write / commit are
 * timed separately. Everything lands as string (schema-on-read, like Spark's csv reader). The
 * result is one JSON blob matching common/result-schema.json.
 */
package com.bench.landbronze;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

public class LandBronze {
    private static final ObjectMapper JSON = new ObjectMapper();

    // Phase 1 works one CSV source and one JSON source, one of each on purpose. Phase 3 opens it
    // up to all twelve; until the schema is locked there's no point porting the rest.
    private static final List<String> DEFAULT_SOURCES = Arrays.asList("crm_customers", "web_events");

    public static void main(String[] args) throws Exception {
        long processStartMs = System.currentTimeMillis();   // wall clock, counts JVM warmup

        CommandLine cmd = parseArgs(args);
        String bronzeDir = cmd.getOptionValue("bronze-dir", "data/bronze");
        String outPath   = cmd.getOptionValue("out", "result.json");
        String sourcesArg = cmd.getOptionValue("sources", "all");
        String catalogUri = cmd.getOptionValue("catalog-uri", "http://localhost:19120/iceberg/main");
        String warehouse  = cmd.getOptionValue("warehouse", "s3://warehouse/");
        String s3Endpoint = cmd.getOptionValue("s3-endpoint", "http://localhost:9000");
        String s3Access   = cmd.getOptionValue("s3-access-key", "minio");
        String s3Secret   = cmd.getOptionValue("s3-secret-key", "minio123");
        // "lake" is the catalog name (see buildCatalog's initialize); the namespace within it is
        // just "bronze". So lake.bronze = <catalog>.<namespace>, matching spark-defaults.conf.
        String namespace  = cmd.getOptionValue("namespace", "bronze");

        // Start the "real work" clock only after flags are parsed, so startup lands in its own bucket.
        long workStartNs = System.nanoTime();

        List<String> sources = "all".equals(sourcesArg)
            ? DEFAULT_SOURCES
            : Arrays.asList(sourcesArg.split(","));

        Path bronze = Paths.get(bronzeDir);
        if (!Files.isDirectory(bronze)) {
            System.err.println(bronze + " not found. Run `make seed` in the parent repo first.");
            System.exit(1);
        }

        RESTCatalog catalog = buildCatalog(catalogUri, warehouse, s3Endpoint, s3Access, s3Secret);
        Namespace ns = Namespace.of(namespace.split("\\."));

        Result result = new Result();
        result.impl = "java";
        result.run_id = UUID.randomUUID().toString();
        result.started_at = Instant.now().toString();
        result.sources = sources;
        result.per_source = new ArrayList<>();
        result.totals = new Totals();

        for (String source : sources) {
            Path dir = bronze.resolve(source);
            if (!Files.isDirectory(dir)) {
                System.err.println("skipping " + source + ": no such directory");
                continue;
            }
            SourceTiming t = landSource(catalog, ns, source, dir);
            result.per_source.add(t);
            result.totals.rows += t.rows;
            result.totals.bytes_parquet += t.bytes_parquet;
        }
        catalog.close();

        result.totals.wall_ms_excl_startup = (System.nanoTime() - workStartNs) / 1_000_000;
        result.totals.wall_ms_incl_startup = System.currentTimeMillis() - processStartMs;
        result.resources = readResources();

        Files.createDirectories(Paths.get(outPath).toAbsolutePath().getParent());
        Files.write(Paths.get(outPath), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(result));
        System.out.printf("landed %d sources, %d rows -> %s%n",
            result.per_source.size(), result.totals.rows, outPath);
    }

    /**
     * A RESTCatalog wired to Nessie + MinIO. S3FileIO writes data files straight to the bucket;
     * the path-style + endpoint props are what make the AWS SDK talk to MinIO instead of real AWS.
     */
    private static RESTCatalog buildCatalog(String uri, String warehouse, String s3Endpoint,
                                            String s3Access, String s3Secret) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, uri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", s3Endpoint);
        props.put("s3.access-key-id", s3Access);
        props.put("s3.secret-access-key", s3Secret);
        props.put("s3.path-style-access", "true");   // MinIO needs s3://host/bucket, not vhost style

        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("lake", props);           // "lake" = the catalog name, matches Spark's
        return catalog;
    }

    /** Read one source dir, write its rows to a Parquet data file in S3, and commit to Iceberg. */
    private static SourceTiming landSource(RESTCatalog catalog, Namespace ns, String source, Path dir)
            throws Exception {
        SourceTiming t = new SourceTiming();
        t.source = source;

        long readStartMs = System.currentTimeMillis();
        List<Map<String, String>> rows = readRows(dir);
        t.read_ms = System.currentTimeMillis() - readStartMs;
        t.rows = rows.size();
        if (rows.isEmpty()) return t;

        // Schema from the first row's keys. Everything is a string here; typing is a Phase-2 concern,
        // and CSV would be all-string anyway. Keeps the two readers landing the same shape.
        Schema schema = stringSchema(rows.get(0).keySet());

        // createOrReplace semantics, same as spark/land_bronze.py: drop then create, so each run is
        // a full refresh of the whole directory rather than an append onto old batches.
        if (!catalog.namespaceExists(ns)) catalog.createNamespace(ns);
        TableIdentifier id = TableIdentifier.of(ns, source);
        catalog.dropTable(id, false);   // purge=false: just unregister. no-op if it doesn't exist.
        Table table = catalog.createTable(id, schema, PartitionSpec.unpartitioned(),
            Collections.singletonMap("write.parquet.compression-codec", "zstd"));

        // WRITE: the data file goes straight to S3 (MinIO) via the table's own FileIO. The location
        // provider hands us a unique path under the table's data/ prefix.
        long writeStartMs = System.currentTimeMillis();
        String dataPath = table.locationProvider().newDataLocation(source + "-" + UUID.randomUUID() + ".parquet");
        OutputFile outputFile = table.io().newOutputFile(dataPath);

        FileAppender<Record> appender = Parquet.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .set("write.parquet.compression-codec", "zstd")
                .overwrite()
                .build();
        try (appender) {
            GenericRecord template = GenericRecord.create(schema);
            for (Map<String, String> row : rows) {
                GenericRecord rec = template.copy();          // fresh record sharing the schema
                row.forEach(rec::setField);                   // every column is a string here
                appender.add(rec);
            }
        }
        t.parquet_write_ms = System.currentTimeMillis() - writeStartMs;
        t.bytes_parquet = appender.length();   // bytes written to S3

        // COMMIT: register the data file as a new snapshot. This is the catalog round-trip Nessie
        // records; timed on its own so we can see write vs commit separately.
        long commitStartMs = System.currentTimeMillis();
        DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(dataPath)
                .withFileSizeInBytes(t.bytes_parquet)
                .withRecordCount(rows.size())
                .withFormat("PARQUET")
                .build();
        table.newAppend().appendFile(dataFile).commit();
        t.iceberg_commit_ms = System.currentTimeMillis() - commitStartMs;

        return t;
    }

    /** All part files in a source dir, whichever format they're in. */
    private static List<Map<String, String>> readRows(Path dir) throws Exception {
        List<Map<String, String>> rows = new ArrayList<>();
        List<Path> parts = Files.list(dir)
            .filter(p -> p.getFileName().toString().startsWith("part-"))
            .sorted()
            .collect(Collectors.toList());

        for (Path part : parts) {
            String name = part.getFileName().toString();
            if (name.endsWith(".csv")) {
                readCsv(part, rows);
            } else if (name.endsWith(".jsonl")) {
                readJsonl(part, rows);
            }
        }
        return rows;
    }

    private static void readCsv(Path file, List<Map<String, String>> rows) throws Exception {
        List<String> lines = Files.readAllLines(file);
        if (lines.isEmpty()) return;
        String[] headers = lines.get(0).split(",", -1);
        for (String line : lines.subList(1, lines.size())) {
            String[] values = line.split(",", -1);   // -1 keeps trailing empties, so columns stay aligned
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < headers.length && i < values.length; i++) {
                row.put(headers[i], values[i]);
            }
            rows.add(row);
        }
    }

    private static void readJsonl(Path file, List<Map<String, String>> rows) throws Exception {
        for (String line : Files.readAllLines(file)) {
            if (line.isBlank()) continue;
            JsonNode node = JSON.readTree(line);
            Map<String, String> row = new LinkedHashMap<>();
            // Stringify every value: bronze is untyped, and it keeps Java lined up with the CSV path.
            node.fields().forEachRemaining(e -> row.put(e.getKey(), e.getValue().asText()));
            rows.add(row);
        }
    }

    /** An Iceberg schema with every named field an optional (nullable) string. */
    private static Schema stringSchema(Set<String> fields) {
        List<Types.NestedField> cols = new ArrayList<>();
        int id = 1;   // Iceberg field ids must be unique and stable within the schema
        for (String name : fields) {
            cols.add(Types.NestedField.optional(id++, name, Types.StringType.get()));
        }
        return new Schema(cols);
    }

    /**
     * Peak RSS + CPU time from /proc, since the JDK has no getrusage. RSS is VmHWM (high-water mark)
     * in /proc/self/status; user/sys CPU are the utime/stime fields of /proc/self/stat, in clock
     * ticks. The benchmark runs in a Linux container, so /proc is always there; off Linux the
     * fields stay 0, which is fine — the comparison only runs in the containers.
     */
    private static Resources readResources() {
        Resources r = new Resources();
        try {
            for (String line : Files.readAllLines(Paths.get("/proc/self/status"))) {
                if (line.startsWith("VmHWM:")) {          // "VmHWM:    123456 kB"
                    r.peak_rss_mb = Double.parseDouble(line.replaceAll("[^0-9]", "")) / 1024.0;
                    break;
                }
            }
            // /proc/self/stat: fields 14 (utime) and 15 (stime) in clock ticks. comm (field 2) can
            // hold spaces inside parens, so split after the closing ')'.
            String stat = new String(Files.readAllBytes(Paths.get("/proc/self/stat")));
            String[] f = stat.substring(stat.lastIndexOf(')') + 2).split(" ");
            double hz = 100.0;                            // _SC_CLK_TCK is 100 on Linux; no sysconf in the JDK
            r.user_cpu_ms = Double.parseDouble(f[11]) / hz * 1000.0;   // utime: field 14, index 11 after comm
            r.sys_cpu_ms  = Double.parseDouble(f[12]) / hz * 1000.0;   // stime: field 15, index 12 after comm
        } catch (Exception ignored) {
            // not Linux, or no /proc — leave whatever's set at 0
        }
        return r;
    }

    private static CommandLine parseArgs(String[] args) throws Exception {
        Options options = new Options();
        // longOpt so `--bronze-dir` parses; the short name is unused but the API wants one.
        options.addOption(null, "bronze-dir", true, "path to data/bronze/");
        options.addOption(null, "catalog-uri", true, "Iceberg REST catalog URI (Nessie)");
        options.addOption(null, "warehouse", true, "warehouse S3 location");
        options.addOption(null, "s3-endpoint", true, "S3 endpoint, e.g. MinIO");
        options.addOption(null, "s3-access-key", true, "S3 access key");
        options.addOption(null, "s3-secret-key", true, "S3 secret key");
        options.addOption(null, "namespace", true, "Iceberg namespace within the catalog");
        options.addOption(null, "sources", true, "comma-separated sources, or 'all'");
        options.addOption(null, "out", true, "output JSON result file");
        return new DefaultParser().parse(options, args);
    }

    // --- result shape, serialized straight to JSON (matches common/result-schema.json) ----------
    static class Result {
        public String impl, run_id, started_at, notes = "";
        public List<String> sources;
        public Totals totals;
        public List<SourceTiming> per_source;
        public Resources resources;
    }

    static class Resources {
        public double peak_rss_mb, user_cpu_ms, sys_cpu_ms;
    }

    static class Totals {
        public long wall_ms_excl_startup, wall_ms_incl_startup, bytes_parquet;
        public int rows;
    }

    static class SourceTiming {
        public String source;
        public long read_ms, parquet_write_ms, iceberg_commit_ms, bytes_parquet;
        public int rows;
    }
}

// ==================================================================================================
// Glossary
//   Parquet.write        Iceberg's Parquet writer builder; the same path Spark/Trino use to write
//                        Iceberg tables. Files land already Iceberg-shaped for the Phase 2 commit.
//   GenericParquetWriter Iceberg's vectorized record->Parquet writer, wired in via createWriterFunc.
//   FileAppender<Record> Appends Iceberg GenericRecords; batches internally rather than per-row.
//   GenericRecord.copy(map)  Fills a fresh record from a column->value map against the schema.
//   compression-codec=zstd   Same codec src/parquet_anatomy.py picks, so file sizes compare.
//   wall_ms_excl_startup Read + write only; the clock starts after arg parsing.
//   wall_ms_incl_startup Whole process, JVM warmup included — the honest per-invocation cost.
// ==================================================================================================
