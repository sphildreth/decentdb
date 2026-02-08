package decentdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDriver(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 1. Create table
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, score FLOAT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// 2. Insert with parameters
	res, err := conn.ExecContext(context.Background(), "INSERT INTO users (id, name, age, score) VALUES ($1, $2, $3, $4)", 1, "Alice", 30, 95.5)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	aff, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if aff != 1 {
		t.Fatalf("expected RowsAffected=1 for insert, got %d", aff)
	}

	res, err = conn.ExecContext(context.Background(), "INSERT INTO users (id, name, age, score) VALUES ($1, $2, $3, $4)", 2, "Bob", 25, 88.0)
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}
	aff, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if aff != 1 {
		t.Fatalf("expected RowsAffected=1 for insert 2, got %d", aff)
	}

	// 2b. Update/Delete rows affected
	res, err = conn.ExecContext(context.Background(), "UPDATE users SET age = age + 1 WHERE age > $1", 20)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	aff, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if aff != 2 {
		t.Fatalf("expected RowsAffected=2 for update, got %d", aff)
	}

	res, err = conn.ExecContext(context.Background(), "DELETE FROM users WHERE id = $1", 2)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	aff, err = res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if aff != 1 {
		t.Fatalf("expected RowsAffected=1 for delete, got %d", aff)
	}

	// 3. Query
	rows, err := conn.QueryContext(context.Background(), "SELECT id, name, age, score FROM users WHERE age > $1 ORDER BY id", 20)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var id int
	var name string
	var age int
	var score float64

	if !rows.Next() {
		t.Fatal("expected first row")
	}
	if err := rows.Scan(&id, &name, &age, &score); err != nil {
		t.Fatal(err)
	}
	if id != 1 || name != "Alice" || age != 31 || score != 95.5 {
		t.Errorf("unexpected row 1: %v %v %v %v", id, name, age, score)
	}

	if rows.Next() {
		// We deleted Bob above.
		t.Error("unexpected second row")
	}

	if rows.Next() {
		t.Error("unexpected third row")
	}

	// 4. Transactions
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(context.Background(), "INSERT INTO users (id, name, age, score) VALUES ($1, $2, $3, $4)", 3, "Charlie", 40, 70.0)
	if err != nil {
		t.Fatal(err)
	}
	tx.Rollback()

	// Verify Charlie is not there
	var count int
	err = conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM users WHERE name = $1", "Charlie").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("Charlie should not exist after rollback, count=%d", count)
	}

	tx, err = conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(context.Background(), "INSERT INTO users (id, name, age, score) VALUES ($1, $2, $3, $4)", 4, "Dave", 35, 80.0)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	err = conn.QueryRowContext(context.Background(), "SELECT id FROM users WHERE name = $1", "Dave").Scan(&id)
	if err != nil {
		// Debug: list all users
		fmt.Println("Dave not found! listing all users:")
		rows, _ := conn.QueryContext(context.Background(), "SELECT id, name FROM users")
		for rows.Next() {
			var rid int
			var rname string
			rows.Scan(&rid, &rname)
			fmt.Printf("id=%d name=%q\n", rid, rname)
		}
		rows.Close()
		t.Fatal(err)
	}
	if id != 4 {
		t.Errorf("Dave should exist after commit, id=%d", id)
	}

	// 5. Prepared statement reuse
	stmt, err := conn.PrepareContext(context.Background(), "SELECT name FROM users WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var got string
	if err := stmt.QueryRowContext(context.Background(), 1).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "Alice" {
		t.Fatalf("expected Alice, got %q", got)
	}
	if err := stmt.QueryRowContext(context.Background(), 4).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "Dave" {
		t.Fatalf("expected Dave, got %q", got)
	}

	// 6. Reject unsupported parameter styles
	if _, err := conn.PrepareContext(context.Background(), "SELECT id FROM users WHERE id = ?"); err == nil {
		t.Fatalf("expected error for '?' parameters")
	}
}

func TestDriver_Decimal(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-decimal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "decimal.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE t (d DECIMAL(18, 9))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert using custom struct
	val := Decimal{Unscaled: 123456789012, Scale: 9}
	_, err = conn.ExecContext(context.Background(), "INSERT INTO t VALUES ($1)", val)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query
	var v interface{}
	err = conn.QueryRowContext(context.Background(), "SELECT d FROM t").Scan(&v)
	if err != nil {
		t.Fatalf("QueryRow failed: %v", err)
	}

	got, ok := v.(Decimal)
	if !ok {
		t.Fatalf("expected Decimal, got %T", v)
	}
	if got.Unscaled != 123456789012 || got.Scale != 9 {
		t.Errorf("expected %v, got %v", val, got)
	}
}

func TestDriver_Bool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-bool-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "bool.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (b BOOL)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO t VALUES ($1)", true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO t VALUES ($1)", false)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT b FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var b bool
	if !rows.Next() {
		t.Fatal("expected row 1")
	}
	if err := rows.Scan(&b); err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Error("expected true")
	}

	if !rows.Next() {
		t.Fatal("expected row 2")
	}
	if err := rows.Scan(&b); err != nil {
		t.Fatal(err)
	}
	if b {
		t.Error("expected false")
	}
}

func TestDriver_UUID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-uuid-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "uuid.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (u UUID)")
	if err != nil {
		t.Fatal(err)
	}

	// Test binding 16 bytes
	u1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	_, err = db.Exec("INSERT INTO t VALUES ($1)", u1)
	if err != nil {
		t.Fatal(err)
	}

	var u2 []byte
	err = db.QueryRow("SELECT u FROM t").Scan(&u2)
	if err != nil {
		t.Fatal(err)
	}
	if len(u2) != 16 {
		t.Errorf("expected 16 bytes, got %d", len(u2))
	}
	for i := range u1 {
		if u1[i] != u2[i] {
			t.Errorf("byte mismatch at %d", i)
		}
	}
}

func TestDriver_Blob(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-blob-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "blob.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INT PRIMARY KEY, data BLOB)")
	if err != nil {
		t.Fatal(err)
	}

	blobs := [][]byte{
		{},
		{0x00},
		{0xDE, 0xAD, 0xBE, 0xEF},
		make([]byte, 256),
	}
	for i := range blobs[3] {
		blobs[3][i] = byte(i)
	}

	for i, b := range blobs {
		_, err = db.Exec("INSERT INTO t (id, data) VALUES ($1, $2)", i, b)
		if err != nil {
			t.Fatalf("INSERT blob[%d] failed: %v", i, err)
		}
	}

	rows, err := db.Query("SELECT data FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for i, expected := range blobs {
		if !rows.Next() {
			t.Fatalf("expected row %d", i)
		}
		var got []byte
		if err := rows.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(expected) {
			t.Errorf("blob[%d]: expected len %d, got %d", i, len(expected), len(got))
		} else {
			for j := range expected {
				if got[j] != expected[j] {
					t.Errorf("blob[%d] byte mismatch at %d", i, j)
					break
				}
			}
		}
	}
}

func TestDriver_Null(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-null-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "null.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INT PRIMARY KEY, i INT, t TEXT, b BOOL, f FLOAT)")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO t (id, i, t, b, f) VALUES ($1, $2, $3, $4, $5)", 1, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	var ni sql.NullInt64
	var nt sql.NullString
	var nb sql.NullBool
	var nf sql.NullFloat64

	err = db.QueryRow("SELECT i, t, b, f FROM t WHERE id = 1").Scan(&ni, &nt, &nb, &nf)
	if err != nil {
		t.Fatal(err)
	}

	if ni.Valid {
		t.Error("expected NULL int")
	}
	if nt.Valid {
		t.Error("expected NULL text")
	}
	if nb.Valid {
		t.Error("expected NULL bool")
	}
	if nf.Valid {
		t.Error("expected NULL float")
	}
}

func TestDriver_Float64Precision(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-float-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "float.ddb")
	dsn := fmt.Sprintf("file:%s", dbPath)

	db, err := sql.Open("decentdb", dsn)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)")
	if err != nil {
		t.Fatal(err)
	}

	values := []float64{0.0, 1.0, -1.0, 3.141592653589793, 1.7976931348623157e+308, 5e-324}
	for i, v := range values {
		_, err = db.Exec("INSERT INTO t (id, v) VALUES ($1, $2)", i, v)
		if err != nil {
			t.Fatalf("INSERT float[%d] failed: %v", i, err)
		}
	}

	rows, err := db.Query("SELECT v FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for i, expected := range values {
		if !rows.Next() {
			t.Fatalf("expected row %d", i)
		}
		var got float64
		if err := rows.Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != expected {
			t.Errorf("float[%d]: expected %v, got %v", i, expected, got)
		}
	}
}

func TestOpenDirect_Checkpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.ddb")
	db, err := OpenDirect(dbPath)
	if err != nil {
		t.Fatalf("OpenDirect failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE chk (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO chk (v) VALUES ($1)", "hello"); err != nil {
		t.Fatal(err)
	}

	err = db.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}
}

func TestOpenDirect_ListTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.ddb")
	db, err := OpenDirect(dbPath)
	if err != nil {
		t.Fatalf("OpenDirect failed: %v", err)
	}
	defer db.Close()

	tables, err := db.ListTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 0 {
		t.Fatalf("expected 0 tables, got %d", len(tables))
	}

	if _, err := db.Exec("CREATE TABLE alpha (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE beta (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	tables, err = db.ListTables()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
}

func TestOpenDirect_GetTableColumns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.ddb")
	db, err := OpenDirect(dbPath)
	if err != nil {
		t.Fatalf("OpenDirect failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)"); err != nil {
		t.Fatal(err)
	}

	cols, err := db.GetTableColumns("users")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}
	if cols[0].Name != "id" {
		t.Errorf("expected first column 'id', got '%s'", cols[0].Name)
	}
	if !cols[0].PrimaryKey {
		t.Error("expected id to be primary key")
	}
	if cols[1].Name != "name" || !cols[1].NotNull {
		t.Error("expected name to be NOT NULL")
	}
}

func TestOpenDirect_ListIndexes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.ddb")
	db, err := OpenDirect(dbPath)
	if err != nil {
		t.Fatalf("OpenDirect failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE INDEX idx_items_name ON items (name)"); err != nil {
		t.Fatal(err)
	}

	indexes, err := db.ListIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) < 1 {
		t.Fatalf("expected at least 1 index, got %d", len(indexes))
	}
	found := false
	for _, idx := range indexes {
		if idx.Name == "idx_items_name" {
			found = true
			if idx.Table != "items" {
				t.Errorf("expected table 'items', got '%s'", idx.Table)
			}
		}
	}
	if !found {
		t.Error("expected to find idx_items_name index")
	}
}

func TestOpenDirect_AutoIncrement(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "decentdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.ddb")
	db, err := OpenDirect(dbPath)
	if err != nil {
		t.Fatalf("OpenDirect failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE auto (id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO auto (val) VALUES ($1)", "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO auto (val) VALUES ($1)", "b"); err != nil {
		t.Fatal(err)
	}

	// Use sql.Open for query since OpenDirect.Exec doesn't return rows
	sqlDB, err := sql.Open("decentdb", fmt.Sprintf("file:%s", dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	rows, err := sqlDB.Query("SELECT id, val FROM auto ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(ids))
	}
	if ids[0] >= ids[1] {
		t.Errorf("auto-increment IDs should be increasing: %d, %d", ids[0], ids[1])
	}
}
