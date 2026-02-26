// Example: DecentDB in-memory database with Go's database/sql interface.
//
// Demonstrates using :memory: for ephemeral databases — no files created on disk.
//
// Build the native library first:
//   nim c -d:release --mm:arc --threads:on --app:lib --out:libdecentdb.so src/c_api.nim
//
// Then run:
//   DECENTDB_LIB_PATH=/path/to/libdecentdb.so go run main_memory.go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/sphildreth/decentdb-go"
)

func main() {
	// Open an in-memory database — no file is created on disk.
	db, err := sql.Open("decentdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a table.
	_, err = db.Exec(`CREATE TABLE users (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE
	)`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert rows.
	users := []struct {
		name  string
		email string
	}{
		{"Alice", "alice@example.com"},
		{"Bob", "bob@example.com"},
		{"Carol", "carol@example.com"},
	}

	for _, u := range users {
		_, err := db.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", u.name, u.email)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Query all users.
	rows, err := db.Query("SELECT id, name, email FROM users ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println("All users:")
	for rows.Next() {
		var id int64
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  id=%d  name=%s  email=%s\n", id, name, email)
	}

	// Prepared statement with parameter.
	var name string
	err = db.QueryRow("SELECT name FROM users WHERE email = $1", "bob@example.com").Scan(&name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nLookup by email: %s\n", name)

	// Transaction example.
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	tx.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "Dave", "dave@example.com")
	tx.Commit()

	// Count after commit.
	var count int64
	db.QueryRow("SELECT count(*) FROM users").Scan(&count)
	fmt.Printf("\nTotal users after transaction: %d\n", count)

	// ── Window Functions ──
	db.Exec(`CREATE TABLE scores (
		id    INTEGER PRIMARY KEY,
		name  TEXT NOT NULL,
		dept  TEXT NOT NULL,
		score INTEGER NOT NULL
	)`)
	for _, s := range []struct {
		name, dept string
		score      int
	}{
		{"Alice", "eng", 95}, {"Bob", "eng", 95},
		{"Carol", "eng", 80}, {"Dave", "sales", 90},
		{"Eve", "sales", 85},
	} {
		db.Exec("INSERT INTO scores (name, dept, score) VALUES ($1, $2, $3)", s.name, s.dept, s.score)
	}

	fmt.Println("\n── Window Functions ──")

	// ROW_NUMBER
	rows, err = db.Query(`
		SELECT name, dept, score,
		       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rn
		FROM scores ORDER BY dept, score DESC`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nROW_NUMBER (ranking within department):")
	for rows.Next() {
		var n, d string
		var s, rn int64
		rows.Scan(&n, &d, &s, &rn)
		fmt.Printf("  %-6s  dept=%-5s  score=%d  rn=%d\n", n, d, s, rn)
	}
	rows.Close()

	// RANK
	rows, err = db.Query(`
		SELECT name, score,
		       RANK() OVER (ORDER BY score DESC) AS rank
		FROM scores ORDER BY score DESC, name`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nRANK (with gaps for ties):")
	for rows.Next() {
		var n string
		var s, r int64
		rows.Scan(&n, &s, &r)
		fmt.Printf("  %-6s  score=%d  rank=%d\n", n, s, r)
	}
	rows.Close()

	// DENSE_RANK
	rows, err = db.Query(`
		SELECT name, score,
		       DENSE_RANK() OVER (ORDER BY score DESC) AS dr
		FROM scores ORDER BY score DESC, name`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nDENSE_RANK (no gaps):")
	for rows.Next() {
		var n string
		var s, dr int64
		rows.Scan(&n, &s, &dr)
		fmt.Printf("  %-6s  score=%d  dense_rank=%d\n", n, s, dr)
	}
	rows.Close()

	// LAG
	rows, err = db.Query(`
		SELECT name, score,
		       LAG(score, 1, 0) OVER (ORDER BY score DESC) AS prev_score
		FROM scores ORDER BY score DESC`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nLAG (previous score):")
	for rows.Next() {
		var n string
		var s, prev int64
		rows.Scan(&n, &s, &prev)
		fmt.Printf("  %-6s  score=%d  prev_score=%d\n", n, s, prev)
	}
	rows.Close()

	// LEAD
	rows, err = db.Query(`
		SELECT name, score,
		       LEAD(score) OVER (PARTITION BY dept ORDER BY score DESC) AS next_score
		FROM scores ORDER BY dept, score DESC`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nLEAD (next score in dept):")
	for rows.Next() {
		var n string
		var s int64
		var next sql.NullInt64
		rows.Scan(&n, &s, &next)
		if next.Valid {
			fmt.Printf("  %-6s  score=%d  next_score=%d\n", n, s, next.Int64)
		} else {
			fmt.Printf("  %-6s  score=%d  next_score=NULL\n", n, s)
		}
	}
	rows.Close()

	// No cleanup needed — in-memory database is automatically discarded.
	fmt.Println("\nDone.")
}
