package decentdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRepeatedOpenQueryCloseKeepsRSSBounded(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("RSS regression is Linux-only")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "memory-leak.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(context.Background(), "CREATE TABLE leak_probe (id INT PRIMARY KEY, payload TEXT)"); err != nil {
		t.Fatalf("failed to create leak probe table: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), "INSERT INTO leak_probe (id, payload) VALUES ($1, $2)", 1, "probe"); err != nil {
		t.Fatalf("failed to seed leak probe table: %v", err)
	}

	for i := 0; i < 25; i++ {
		runRSSLeakIteration(t, dsn)
	}

	trimGoHeap()
	before := readRSSBytes(t)

	for i := 0; i < 160; i++ {
		runRSSLeakIteration(t, dsn)
		if i%10 == 0 {
			trimGoHeap()
		}
	}

	trimGoHeap()
	after := readRSSBytes(t)

	const maxGrowth = 12 << 20
	if diff := after - before; diff > maxGrowth {
		t.Fatalf("RSS grew by %d bytes (before=%d after=%d)", diff, before, after)
	}
}

func runRSSLeakIteration(t *testing.T, dsn string) {
	t.Helper()

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM leak_probe").Scan(&count); err != nil {
		t.Fatalf("failed to query leak probe table: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 leak probe row, got %d", count)
	}
}

func trimGoHeap() {
	for i := 0; i < 3; i++ {
		runtime.GC()
		debug.FreeOSMemory()
		time.Sleep(10 * time.Millisecond)
	}
}

func readRSSBytes(t *testing.T) int64 {
	t.Helper()

	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatalf("failed to read /proc/self/status: %v", err)
	}

	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			break
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			t.Fatalf("failed to parse VmRSS: %v", err)
		}
		return kb * 1024
	}

	t.Fatal("VmRSS not found in /proc/self/status")
	return 0
}
