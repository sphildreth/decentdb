package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	_ "github.com/sphildreth/decentdb-go"
	_ "modernc.org/sqlite"
)

const (
	defaultCount         = 1_000_000
	defaultPointReads    = 10_000
	defaultPointSeed     = 1337
	defaultFetchmanySize = 4096
)

type options struct {
	engine         string
	count          int
	pointReads     int
	pointSeed      int64
	fetchmanyBatch int
	dbPrefix       string
	keepDB         bool
}

type result struct {
	insertSeconds   float64
	insertRowsPerS  float64
	fetchallSeconds float64
	fetchmanySecond float64
	pointP50Ms      float64
	pointP95Ms      float64
}

type benchRow struct {
	id  int64
	val string
	f   float64
}

type metricDef struct {
	name           string
	unit           string
	higherIsBetter bool
	decent         float64
	sqlite         float64
	format         string
}

func main() {
	runtime.GOMAXPROCS(1)

	opts, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	var engines []string
	switch opts.engine {
	case "all":
		engines = []string{"decentdb", "sqlite"}
	case "decentdb", "sqlite":
		engines = []string{opts.engine}
	default:
		fmt.Fprintf(os.Stderr, "error: --engine must be one of all|decentdb|sqlite\n")
		os.Exit(2)
	}

	results := map[string]result{}
	for _, engine := range engines {
		suffix := "ddb"
		if engine == "sqlite" {
			suffix = "db"
		}
		dbPath := fmt.Sprintf("%s_%s.%s", opts.dbPrefix, engine, suffix)
		out, runErr := runEngineBenchmark(engine, dbPath, opts)
		if runErr != nil {
			fmt.Fprintf(os.Stderr, "benchmark failed for %s: %v\n", engine, runErr)
			os.Exit(1)
		}
		results[engine] = out
	}

	printComparison(results)
}

func parseFlags() (options, error) {
	opts := options{}
	flag.StringVar(&opts.engine, "engine", "all", "Engine to run: all|decentdb|sqlite")
	flag.IntVar(&opts.count, "count", defaultCount, "Rows to insert/fetch")
	flag.IntVar(&opts.pointReads, "point-reads", defaultPointReads, "Random indexed point lookups")
	flag.Int64Var(&opts.pointSeed, "point-seed", defaultPointSeed, "RNG seed for point lookups")
	flag.IntVar(&opts.fetchmanyBatch, "fetchmany-batch", defaultFetchmanySize, "Batch size for fetchmany/streaming benchmark")
	flag.StringVar(&opts.dbPrefix, "db-prefix", "go_bench_fetch", "Database file path prefix")
	flag.BoolVar(&opts.keepDB, "keep-db", false, "Keep generated DB files")
	flag.Parse()

	if opts.count <= 0 {
		return options{}, errors.New("--count must be > 0")
	}
	if opts.pointReads <= 0 {
		return options{}, errors.New("--point-reads must be > 0")
	}
	if opts.fetchmanyBatch <= 0 {
		return options{}, errors.New("--fetchmany-batch must be > 0")
	}
	if opts.dbPrefix == "" {
		return options{}, errors.New("--db-prefix cannot be empty")
	}

	return opts, nil
}

func runEngineBenchmark(engine, dbPath string, opts options) (result, error) {
	ctx := context.Background()
	cleanupDBFiles(dbPath)

	fmt.Printf("\n=== %s ===\n", engine)
	fmt.Println("Setting up data...")

	db, err := openEngineDB(engine, dbPath)
	if err != nil {
		return result{}, err
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	conn, err := db.Conn(ctx)
	if err != nil {
		return result{}, err
	}
	defer conn.Close()

	if err := setupSchema(ctx, conn, engine); err != nil {
		return result{}, err
	}

	insertSQL := "INSERT INTO bench VALUES ($1, $2, $3)"
	pointSQL := "SELECT id, val, f FROM bench WHERE id = $1"
	if engine == "sqlite" {
		insertSQL = "INSERT INTO bench VALUES (?, ?, ?)"
		pointSQL = "SELECT id, val, f FROM bench WHERE id = ?"
	}

	if err := warmInsertPath(ctx, conn, insertSQL); err != nil {
		return result{}, err
	}

	insertSeconds, err := runWithGCDisabled(func() (float64, error) {
		started := time.Now()
		tx, txErr := conn.BeginTx(ctx, nil)
		if txErr != nil {
			return 0, txErr
		}

		stmt, stmtErr := tx.PrepareContext(ctx, insertSQL)
		if stmtErr != nil {
			_ = tx.Rollback()
			return 0, stmtErr
		}

		for i := 0; i < opts.count; i++ {
			_, execErr := stmt.ExecContext(ctx, int64(i), fmt.Sprintf("value_%d", i), float64(i))
			if execErr != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				return 0, execErr
			}
		}

		if closeErr := stmt.Close(); closeErr != nil {
			_ = tx.Rollback()
			return 0, closeErr
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return 0, commitErr
		}
		return time.Since(started).Seconds(), nil
	})
	if err != nil {
		return result{}, err
	}
	insertRowsPerS := float64(opts.count) / insertSeconds
	fmt.Printf("Insert %d rows: %.4fs (%.2f rows/sec)\n", opts.count, insertSeconds, insertRowsPerS)

	scanStmt, err := conn.PrepareContext(ctx, "SELECT id, val, f FROM bench")
	if err != nil {
		return result{}, err
	}
	defer scanStmt.Close()

	if err := warmScanPath(ctx, scanStmt); err != nil {
		return result{}, err
	}

	fetchallSeconds, err := runWithGCDisabled(func() (float64, error) {
		started := time.Now()
		rows, queryErr := scanStmt.QueryContext(ctx)
		if queryErr != nil {
			return 0, queryErr
		}
		defer rows.Close()

		out := make([]benchRow, 0, opts.count)
		for rows.Next() {
			var row benchRow
			if scanErr := rows.Scan(&row.id, &row.val, &row.f); scanErr != nil {
				return 0, scanErr
			}
			out = append(out, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return 0, rowsErr
		}
		if len(out) != opts.count {
			return 0, fmt.Errorf("expected %d rows from fetchall, got %d", opts.count, len(out))
		}
		return time.Since(started).Seconds(), nil
	})
	if err != nil {
		return result{}, err
	}
	fmt.Printf("Fetchall %d rows: %.4fs\n", opts.count, fetchallSeconds)

	fetchmanySeconds, err := runWithGCDisabled(func() (float64, error) {
		started := time.Now()
		rows, queryErr := scanStmt.QueryContext(ctx)
		if queryErr != nil {
			return 0, queryErr
		}
		defer rows.Close()

		total := 0
		batch := make([]benchRow, 0, opts.fetchmanyBatch)
		for rows.Next() {
			var row benchRow
			if scanErr := rows.Scan(&row.id, &row.val, &row.f); scanErr != nil {
				return 0, scanErr
			}
			batch = append(batch, row)
			if len(batch) == opts.fetchmanyBatch {
				total += len(batch)
				batch = batch[:0]
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return 0, rowsErr
		}
		total += len(batch)
		if total != opts.count {
			return 0, fmt.Errorf("expected %d rows from fetchmany, got %d", opts.count, total)
		}
		return time.Since(started).Seconds(), nil
	})
	if err != nil {
		return result{}, err
	}
	fmt.Printf("Fetchmany(%d) %d rows: %.4fs\n", opts.fetchmanyBatch, opts.count, fetchmanySeconds)

	pointStmt, err := conn.PrepareContext(ctx, pointSQL)
	if err != nil {
		return result{}, err
	}
	defer pointStmt.Close()

	pointIDs := buildPointReadIDs(opts.count, opts.pointReads, opts.pointSeed)
	warmupID := pointIDs[len(pointIDs)/2]
	{
		var id int64
		var val string
		var f float64
		if err := pointStmt.QueryRowContext(ctx, warmupID).Scan(&id, &val, &f); err != nil {
			return result{}, fmt.Errorf("warmup point read failed: %w", err)
		}
	}

	pointLatenciesMs, err := runWithGCDisabled(func() ([]float64, error) {
		out := make([]float64, 0, len(pointIDs))
		for _, lookupID := range pointIDs {
			started := time.Now()
			var id int64
			var val string
			var f float64
			scanErr := pointStmt.QueryRowContext(ctx, lookupID).Scan(&id, &val, &f)
			if scanErr != nil {
				return nil, fmt.Errorf("point read missed id=%d: %w", lookupID, scanErr)
			}
			out = append(out, float64(time.Since(started).Nanoseconds())/1_000_000.0)
		}
		return out, nil
	})
	if err != nil {
		return result{}, err
	}

	sort.Float64s(pointLatenciesMs)
	pointP50Ms := percentileSorted(pointLatenciesMs, 50)
	pointP95Ms := percentileSorted(pointLatenciesMs, 95)
	fmt.Printf(
		"Random point reads by id (%d, seed=%d): p50=%.6fms p95=%.6fms\n",
		opts.pointReads,
		opts.pointSeed,
		pointP50Ms,
		pointP95Ms,
	)

	if engine == "sqlite" {
		if _, err := conn.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			return result{}, err
		}
	}

	if !opts.keepDB {
		cleanupDBFiles(dbPath)
	}

	return result{
		insertSeconds:   insertSeconds,
		insertRowsPerS:  insertRowsPerS,
		fetchallSeconds: fetchallSeconds,
		fetchmanySecond: fetchmanySeconds,
		pointP50Ms:      pointP50Ms,
		pointP95Ms:      pointP95Ms,
	}, nil
}

func openEngineDB(engine, dbPath string) (*sql.DB, error) {
	switch engine {
	case "decentdb":
		// The Go driver accepts bare file paths reliably across relative/absolute paths.
		return sql.Open("decentdb", dbPath)
	case "sqlite":
		return sql.Open("sqlite", dbPath)
	default:
		return nil, fmt.Errorf("unknown engine %q", engine)
	}
}

func setupSchema(ctx context.Context, conn *sql.Conn, engine string) error {
	if engine == "sqlite" {
		pragmas := []string{
			"PRAGMA journal_mode=WAL",
			"PRAGMA synchronous=FULL",
			"PRAGMA wal_autocheckpoint=0",
		}
		for _, pragma := range pragmas {
			if _, err := conn.ExecContext(ctx, pragma); err != nil {
				return err
			}
		}
	}

	createTable := "CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)"
	if engine == "sqlite" {
		createTable = "CREATE TABLE bench (id INTEGER, val TEXT, f REAL)"
	}
	if _, err := conn.ExecContext(ctx, createTable); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "CREATE INDEX bench_id_idx ON bench(id)"); err != nil {
		return err
	}
	return nil
}

func warmInsertPath(ctx context.Context, conn *sql.Conn, insertSQL string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := stmt.ExecContext(ctx, int64(-1), "__warm__", -1.0); err != nil {
		_ = stmt.Close()
		_ = tx.Rollback()
		return err
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Rollback()
}

func warmScanPath(ctx context.Context, scanStmt *sql.Stmt) error {
	rows, err := scanStmt.QueryContext(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		var id int64
		var val string
		var f float64
		if err := rows.Scan(&id, &val, &f); err != nil {
			return err
		}
	}
	return rows.Err()
}

func cleanupDBFiles(dbPath string) {
	_ = os.Remove(dbPath)
	_ = os.Remove(dbPath + ".wal")
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")
}

func buildPointReadIDs(rowCount, pointReads int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	if pointReads <= rowCount {
		ids := make([]int64, rowCount)
		for i := 0; i < rowCount; i++ {
			ids[i] = int64(i)
		}
		for i := 0; i < pointReads; i++ {
			j := i + rng.Intn(rowCount-i)
			ids[i], ids[j] = ids[j], ids[i]
		}
		out := make([]int64, pointReads)
		copy(out, ids[:pointReads])
		return out
	}

	out := make([]int64, pointReads)
	for i := 0; i < pointReads; i++ {
		out[i] = int64(rng.Intn(rowCount))
	}
	return out
}

func percentileSorted(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Round((pct / 100.0) * float64(len(values)-1)))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func runWithGCDisabled[T any](fn func() (T, error)) (T, error) {
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	return fn()
}

func printComparison(results map[string]result) {
	decent, hasDecent := results["decentdb"]
	sqlite, hasSQLite := results["sqlite"]
	if !hasDecent || !hasSQLite {
		return
	}

	metrics := []metricDef{
		{
			name:           "Insert throughput (higher is better)",
			unit:           " rows/s",
			higherIsBetter: true,
			decent:         decent.insertRowsPerS,
			sqlite:         sqlite.insertRowsPerS,
			format:         "%.2f",
		},
		{
			name:           "Fetchall time (lower is better)",
			unit:           "s",
			higherIsBetter: false,
			decent:         decent.fetchallSeconds,
			sqlite:         sqlite.fetchallSeconds,
			format:         "%.6f",
		},
		{
			name:           "Fetchmany/streaming time (lower is better)",
			unit:           "s",
			higherIsBetter: false,
			decent:         decent.fetchmanySecond,
			sqlite:         sqlite.fetchmanySecond,
			format:         "%.6f",
		},
		{
			name:           "Point read p50 latency (lower is better)",
			unit:           "ms",
			higherIsBetter: false,
			decent:         decent.pointP50Ms,
			sqlite:         sqlite.pointP50Ms,
			format:         "%.6f",
		},
		{
			name:           "Point read p95 latency (lower is better)",
			unit:           "ms",
			higherIsBetter: false,
			decent:         decent.pointP95Ms,
			sqlite:         sqlite.pointP95Ms,
			format:         "%.6f",
		},
	}

	decentBetter := make([]string, 0, len(metrics))
	sqliteBetter := make([]string, 0, len(metrics))
	ties := make([]string, 0, len(metrics))

	for _, metric := range metrics {
		if metric.decent == metric.sqlite {
			ties = append(ties, fmt.Sprintf("%s: tie (%s%s)", metric.name, fmt.Sprintf(metric.format, metric.decent), metric.unit))
			continue
		}

		var decentWins bool
		var winner float64
		var loser float64
		var ratio float64
		var detail string
		if metric.higherIsBetter {
			decentWins = metric.decent > metric.sqlite
			if decentWins {
				winner = metric.decent
				loser = metric.sqlite
			} else {
				winner = metric.sqlite
				loser = metric.decent
			}
			if loser == 0 {
				ratio = math.Inf(1)
			} else {
				ratio = winner / loser
			}
			detail = fmt.Sprintf(
				"%s: %s%s vs %s%s (%.3fx higher)",
				metric.name,
				fmt.Sprintf(metric.format, winner),
				metric.unit,
				fmt.Sprintf(metric.format, loser),
				metric.unit,
				ratio,
			)
		} else {
			decentWins = metric.decent < metric.sqlite
			if decentWins {
				winner = metric.decent
				loser = metric.sqlite
			} else {
				winner = metric.sqlite
				loser = metric.decent
			}
			if winner == 0 {
				ratio = math.Inf(1)
			} else {
				ratio = loser / winner
			}
			detail = fmt.Sprintf(
				"%s: %s%s vs %s%s (%.3fx faster/lower)",
				metric.name,
				fmt.Sprintf(metric.format, winner),
				metric.unit,
				fmt.Sprintf(metric.format, loser),
				metric.unit,
				ratio,
			)
		}

		if decentWins {
			decentBetter = append(decentBetter, detail)
		} else {
			sqliteBetter = append(sqliteBetter, detail)
		}
	}

	fmt.Println("\n=== Comparison (DecentDB vs SQLite) ===")
	fmt.Println("DecentDB better at:")
	if len(decentBetter) == 0 {
		fmt.Println("- none")
	} else {
		for _, line := range decentBetter {
			fmt.Printf("- %s\n", line)
		}
	}

	fmt.Println("SQLite better at:")
	if len(sqliteBetter) == 0 {
		fmt.Println("- none")
	} else {
		for _, line := range sqliteBetter {
			fmt.Printf("- %s\n", line)
		}
	}

	if len(ties) > 0 {
		fmt.Println("Ties:")
		for _, line := range ties {
			fmt.Printf("- %s\n", line)
		}
	}
}
