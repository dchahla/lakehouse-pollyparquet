// Land the file-based bronze sources into Iceberg, timing every step. The Go arm of the
// cross-language benchmark; Java and Rust do the same work so the numbers line up.
//
//	go build -o land-bronze ./cmd/land-bronze
//	./land-bronze --sources crm_customers,web_events \
//	    --catalog-uri http://nessie:19120/iceberg/main --s3-endpoint http://minio:9000 \
//	    --bronze-dir ../../data/bronze --out ../results/go.json
//
// For each source: read the raw CSV/JSONL, build an arrow table, then let iceberg-go write it to
// Parquet in S3 (MinIO) and commit a new snapshot through the Nessie REST catalog — the same
// createOrReplace full-refresh spark/land_bronze.py does. read / write+commit are timed (iceberg-go
// fuses the Parquet write and the append, so those share one clock). Everything lands as string
// (schema-on-read, like Spark's csv reader). The result matches common/result-schema.json.
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	// Side-effect import: registers the s3:// IO scheme so the catalog can read/write MinIO. Without
	// it: "io scheme not registered for path s3://... (scheme: s3)".
	_ "github.com/apache/iceberg-go/io/gocloud"
)

// Phase 1 works one CSV source and one JSON source, one of each on purpose. Phase 3 opens it up
// to all twelve; until the schema is locked there's no point porting the rest.
var defaultSources = []string{"crm_customers", "web_events"}

func main() {
	processStart := time.Now() // wall clock, counts binary startup

	bronzeDir := flag.String("bronze-dir", "data/bronze", "path to data/bronze/")
	out := flag.String("out", "result.json", "output JSON result file")
	sourcesArg := flag.String("sources", "all", "comma-separated sources, or 'all'")
	catalogURI := flag.String("catalog-uri", "http://nessie:19120/iceberg/main", "Iceberg REST catalog URI")
	warehouse := flag.String("warehouse", "s3://warehouse/", "warehouse S3 location")
	s3Endpoint := flag.String("s3-endpoint", "http://minio:9000", "S3 endpoint (MinIO)")
	s3Access := flag.String("s3-access-key", "minio", "S3 access key")
	s3Secret := flag.String("s3-secret-key", "minio123", "S3 secret key")
	// "lake" is the catalog; the namespace within it is "bronze" (see Java's note).
	namespace := flag.String("namespace", "bronze", "Iceberg namespace within the catalog")
	flag.Parse()

	// Start the "real work" clock only after flags are parsed, so startup lands in its own bucket.
	workStart := time.Now()
	ctx := context.Background()

	sources := defaultSources
	if *sourcesArg != "all" {
		sources = strings.Split(*sourcesArg, ",")
	}

	if fi, err := os.Stat(*bronzeDir); err != nil || !fi.IsDir() {
		fmt.Fprintf(os.Stderr, "%s not found. Run `make seed` in the parent repo first.\n", *bronzeDir)
		os.Exit(1)
	}

	cat, err := buildCatalog(ctx, *catalogURI, *warehouse, *s3Endpoint, *s3Access, *s3Secret)
	if err != nil {
		fmt.Fprintf(os.Stderr, "building catalog: %v\n", err)
		os.Exit(1)
	}

	result := Result{
		Impl:      "go",
		RunID:     uuid.NewString(),
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Sources:   sources,
	}

	for _, source := range sources {
		dir := filepath.Join(*bronzeDir, source)
		if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
			fmt.Fprintf(os.Stderr, "skipping %s: no such directory\n", source)
			continue
		}
		t, err := landSource(ctx, cat, *namespace, source, dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error on %s: %v\n", source, err)
			os.Exit(1)
		}
		result.PerSource = append(result.PerSource, t)
		result.Totals.Rows += t.Rows
		result.Totals.BytesParquet += t.BytesParquet
	}

	result.Totals.WallMsExclStartup = time.Since(workStart).Milliseconds()
	result.Totals.WallMsInclStartup = time.Since(processStart).Milliseconds()
	result.Resources = readResources()

	if err := writeResult(*out, result); err != nil {
		fmt.Fprintf(os.Stderr, "writing result: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("landed %d sources, %d rows -> %s\n", len(result.PerSource), result.Totals.Rows, *out)
}

// buildCatalog wires a Nessie REST catalog to MinIO. The s3.* props ride along via
// WithAdditionalProps; the catalog forwards them to the table's FileIO so data files land in MinIO.
// path-style + endpoint are what make the S3 client talk to MinIO instead of real AWS.
func buildCatalog(ctx context.Context, uri, warehouse, s3Endpoint, s3Access, s3Secret string) (*rest.Catalog, error) {
	props := iceberg.Properties{
		"s3.endpoint":          s3Endpoint,
		"s3.access-key-id":     s3Access,
		"s3.secret-access-key": s3Secret,
		"s3.path-style-access": "true",
		"s3.region":            "us-east-1",
	}
	return rest.NewCatalog(ctx, "lake", uri,
		rest.WithWarehouseLocation(warehouse),
		rest.WithAdditionalProps(props),
	)
}

// landSource reads one source dir, then lets iceberg-go write the rows to Parquet in S3 and commit.
func landSource(ctx context.Context, cat *rest.Catalog, ns, source, dir string) (SourceTiming, error) {
	t := SourceTiming{Source: source}

	readStart := time.Now()
	rows, cols, err := readRows(dir)
	if err != nil {
		return t, err
	}
	t.ReadMs = time.Since(readStart).Milliseconds()
	t.Rows = len(rows)
	if len(rows) == 0 {
		return t, nil
	}

	// createOrReplace, same as spark/land_bronze.py: drop then create, so each run fully refreshes
	// the table from the whole directory rather than appending onto old batches.
	schema := stringSchema(cols)
	nsID := table.Identifier{ns}
	tblID := table.Identifier{ns, source}
	if err := ensureNamespace(ctx, cat, nsID); err != nil {
		return t, err
	}
	if ok, _ := cat.CheckTableExists(ctx, tblID); ok {
		if err := cat.DropTable(ctx, tblID); err != nil {
			return t, err
		}
	}
	tbl, err := cat.CreateTable(ctx, tblID, schema)
	if err != nil {
		return t, err
	}

	// WRITE + COMMIT: iceberg-go fuses the Parquet write (to S3) and the snapshot append, so both
	// share one clock. AppendTable writes the arrow table out as Parquet under the table's data/
	// prefix; Commit records the new snapshot in Nessie.
	writeStart := time.Now()
	arrowTable := toArrowTable(cols, rows)
	defer arrowTable.Release()

	txn := tbl.NewTransaction()
	if err := txn.AppendTable(ctx, arrowTable, arrowTable.NumRows(), nil); err != nil {
		return t, err
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return t, err
	}
	t.ParquetWriteMs = time.Since(writeStart).Milliseconds()
	// iceberg-go fuses the Parquet write and the commit inside AppendTable/Commit, so we can't split
	// them the way Java and Rust do — the whole thing lands in parquet_write_ms, commit stays 0. Not
	// a flaw, just a higher-altitude API; the README caveats call this out.
	t.IcebergCommitMs = 0
	// Byte count isn't returned directly; read it off the committed snapshot's summary instead.
	if snap := committed.CurrentSnapshot(); snap != nil && snap.Summary != nil {
		t.BytesParquet = snap.Summary.Properties.GetInt64("added-files-size", 0)
	}

	return t, nil
}

func ensureNamespace(ctx context.Context, cat *rest.Catalog, ns table.Identifier) error {
	if ok, _ := cat.CheckNamespaceExists(ctx, ns); ok {
		return nil
	}
	return cat.CreateNamespace(ctx, ns, nil)
}

// stringSchema builds an iceberg schema with every column an optional string. Field ids start at 1
// and must be stable, same as the Java and Rust sides.
func stringSchema(cols []string) *iceberg.Schema {
	fields := make([]iceberg.NestedField, len(cols))
	for i, c := range cols {
		fields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     c,
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		}
	}
	return iceberg.NewSchema(0, fields...)
}

// toArrowTable pivots the row maps into an arrow table, one string column each, matching the order
// of cols so it lines up with the iceberg schema field-for-field.
func toArrowTable(cols []string, rows []map[string]string) arrow.Table {
	pool := memory.DefaultAllocator
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = arrow.Field{Name: c, Type: arrow.BinaryTypes.String, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)

	arrays := make([]arrow.Array, len(cols))
	for i, c := range cols {
		b := array.NewStringBuilder(pool)
		for _, row := range rows {
			if v, ok := row[c]; ok {
				b.Append(v)
			} else {
				b.AppendNull()
			}
		}
		arrays[i] = b.NewArray()
		b.Release()
	}

	columns := make([]arrow.Column, len(cols))
	for i := range cols {
		chunked := arrow.NewChunked(fields[i].Type, []arrow.Array{arrays[i]})
		columns[i] = *arrow.NewColumn(fields[i], chunked)
		arrays[i].Release()
	}
	return array.NewTable(schema, columns, int64(len(rows)))
}

// readRows collects all part files in a source dir, returning the rows and the column order (from
// the first row's keys, sorted for stability).
func readRows(dir string) ([]map[string]string, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	var parts []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "part-") {
			parts = append(parts, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(parts)

	var rows []map[string]string
	for _, part := range parts {
		switch {
		case strings.HasSuffix(part, ".csv"):
			rows, err = readCSV(part, rows)
		case strings.HasSuffix(part, ".jsonl"):
			rows, err = readJSONL(part, rows)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	if len(rows) == 0 {
		return rows, nil, nil
	}

	cols := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return rows, cols, nil
}

func readCSV(path string, rows []map[string]string) ([]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return rows, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return rows, err
	}
	if len(records) == 0 {
		return rows, nil
	}
	headers := records[0]
	for _, rec := range records[1:] {
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			if i < len(rec) {
				row[h] = rec[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func readJSONL(path string, rows []map[string]string) ([]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return rows, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024) // some rows are wide; give the scanner room
	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return rows, err
		}
		// Stringify every value: bronze is untyped, and it keeps Go lined up with the CSV path.
		row := make(map[string]string, len(raw))
		for k, v := range raw {
			row[k] = fmt.Sprintf("%v", v)
		}
		rows = append(rows, row)
	}
	return rows, sc.Err()
}

func writeResult(path string, r Result) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// readResources pulls peak RSS and CPU time from a single getrusage call. ru_maxrss is KB on Linux
// (where the benchmark containers run); Utime/Stime are timevals (sec + usec).
func readResources() Resources {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return Resources{}
	}
	return Resources{
		PeakRssMb: float64(ru.Maxrss) / 1024.0,
		UserCpuMs: timevalMs(ru.Utime),
		SysCpuMs:  timevalMs(ru.Stime),
	}
}

func timevalMs(tv syscall.Timeval) float64 {
	return float64(tv.Sec)*1000 + float64(tv.Usec)/1000
}

// --- result shape, serialized straight to JSON (matches common/result-schema.json) --------------

type Result struct {
	Impl      string         `json:"impl"`
	RunID     string         `json:"run_id"`
	StartedAt string         `json:"started_at"`
	Sources   []string       `json:"sources"`
	Totals    Totals         `json:"totals"`
	PerSource []SourceTiming `json:"per_source"`
	Notes     string         `json:"notes"`
	Resources Resources      `json:"resources"`
}

type Resources struct {
	PeakRssMb float64 `json:"peak_rss_mb"`
	UserCpuMs float64 `json:"user_cpu_ms"`
	SysCpuMs  float64 `json:"sys_cpu_ms"`
}

type Totals struct {
	WallMsExclStartup int64 `json:"wall_ms_excl_startup"`
	WallMsInclStartup int64 `json:"wall_ms_incl_startup"`
	Rows              int   `json:"rows"`
	BytesParquet      int64 `json:"bytes_parquet"`
}

type SourceTiming struct {
	Source          string `json:"source"`
	ReadMs          int64  `json:"read_ms"`
	ParquetWriteMs  int64  `json:"parquet_write_ms"`
	IcebergCommitMs int64  `json:"iceberg_commit_ms"`
	Rows            int    `json:"rows"`
	BytesParquet    int64  `json:"bytes_parquet"`
}

// ================================================================================================
// Glossary
//   rest.NewCatalog          Nessie REST catalog client; WithAdditionalProps carries the s3.* config.
//   Transaction.AppendTable  Writes an arrow table out as Parquet to S3 AND stages the append.
//   Transaction.Commit       Records the new snapshot in the catalog (one REST round-trip).
//   arrow.Table              Columnar in-memory table iceberg-go consumes; one string column each.
//   wall_ms_excl_startup     Read + write + commit; the clock starts after flag parsing.
//   wall_ms_incl_startup     Whole process, binary startup included; near-zero for a native binary.
// ================================================================================================
