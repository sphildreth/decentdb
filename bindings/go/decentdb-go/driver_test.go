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
