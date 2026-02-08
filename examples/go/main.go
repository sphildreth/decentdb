// Example: Basic DecentDB usage with Go's database/sql interface.
//
// Build the native library first:
//   nim c -d:release --mm:arc --threads:on --app:lib --out:libdecentdb.so src/c_api.nim
//
// Then run:
//   DECENTDB_LIB_PATH=/path/to/libdecentdb.so go run main.go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/sphildreth/decentdb-go"
)

func main() {
	// Open a database (creates the file if it doesn't exist).
	db, err := sql.Open("decentdb", "file:example.ddb")
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

	// Insert rows. DecentDB uses Postgres-style $1, $2, ... parameters.
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
}
