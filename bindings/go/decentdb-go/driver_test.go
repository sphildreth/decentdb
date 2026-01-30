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

	dbPath := filepath.Join(tmpDir, "test.db")
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
	_, err = conn.ExecContext(context.Background(), "INSERT INTO users (id, name, age, score) VALUES ($1, $2, $3, $4)", 1, "Alice", 30, 95.5)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = conn.ExecContext(context.Background(), "INSERT INTO users (id, name, age, score) VALUES ($1, $2, $3, $4)", 2, "Bob", 25, 88.0)
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
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
	if id != 1 || name != "Alice" || age != 30 || score != 95.5 {
		t.Errorf("unexpected row 1: %v %v %v %v", id, name, age, score)
	}

	if !rows.Next() {
		t.Fatal("expected second row")
	}
	if err := rows.Scan(&id, &name, &age, &score); err != nil {
		t.Fatal(err)
	}
	if id != 2 || name != "Bob" || age != 25 || score != 88.0 {
		t.Errorf("unexpected row 2: %v %v %v %v", id, name, age, score)
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
}
