package decentdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAbiVersion(t *testing.T) {
	ver := AbiVersion()
	if ver <= 0 {
		t.Fatalf("AbiVersion() = %d, want > 0", ver)
	}
}

func TestEngineVersion(t *testing.T) {
	ver := EngineVersion()
	if ver == "" {
		t.Fatal("EngineVersion() returned empty string")
	}
	if !strings.Contains(ver, ".") {
		t.Fatalf("EngineVersion() = %q, expected semver-like string", ver)
	}
}

func TestOpenDirect_GetTableDdl(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "test.ddb")
	db, err := OpenDirect(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)")
	if err != nil {
		t.Fatal(err)
	}

	ddl, err := db.GetTableDdl("items")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(ddl), "items") {
		t.Fatalf("GetTableDdl returned %q, expected to contain 'items'", ddl)
	}
	if !strings.Contains(ddl, "id") {
		t.Fatalf("GetTableDdl returned %q, expected to contain 'id'", ddl)
	}
}

func TestOpenDirect_ListViews(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "test.ddb")
	db, err := OpenDirect(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("CREATE VIEW v_t AS SELECT id, val FROM t WHERE id > 0")
	if err != nil {
		t.Fatal(err)
	}

	viewsJSON, err := db.ListViews()
	if err != nil {
		t.Fatal(err)
	}

	// Parse the JSON to find our view
	var raw []json.RawMessage
	if err := json.Unmarshal([]byte(viewsJSON), &raw); err == nil && len(raw) > 0 {
		// Try as array of strings
		var names []string
		if err := json.Unmarshal([]byte(viewsJSON), &names); err == nil {
			for _, n := range names {
				if n == "v_t" {
					return
				}
			}
		}
		// Try as array of objects with "name" field
		var objs []struct{ Name string }
		if err := json.Unmarshal([]byte(viewsJSON), &objs); err == nil {
			for _, o := range objs {
				if o.Name == "v_t" {
					return
				}
			}
		}
	}

	// If we can't parse structured, at least check the string contains our view
	if !strings.Contains(viewsJSON, "v_t") {
		t.Fatalf("ListViews() = %q, expected to contain 'v_t'", viewsJSON)
	}
}

func TestOpenDirect_GetViewDdl(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "test.ddb")
	db, err := OpenDirect(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("CREATE VIEW v_t AS SELECT id, val FROM t WHERE id > 0")
	if err != nil {
		t.Fatal(err)
	}

	ddl, err := db.GetViewDdl("v_t")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(ddl), "v_t") {
		t.Fatalf("GetViewDdl returned %q, expected to contain 'v_t'", ddl)
	}
}

func TestOpenDirect_ListTriggers(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "test.ddb")
	db, err := OpenDirect(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	triggersJSON, err := db.ListTriggers()
	if err != nil {
		t.Fatal(err)
	}
	// Empty triggers should return "[]"
	if triggersJSON != "[]" && triggersJSON != "" {
		// Try parsing
		var triggers []interface{}
		if err := json.Unmarshal([]byte(triggersJSON), &triggers); err != nil {
			t.Fatalf("ListTriggers() returned invalid JSON: %q", triggersJSON)
		}
	}
}

func TestOpenDirect_InTransaction(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "test.ddb")
	db, err := OpenDirect(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if db.InTransaction() {
		t.Fatal("InTransaction() should be false outside a transaction")
	}

	_, err = db.Exec("BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	if !db.InTransaction() {
		t.Fatal("InTransaction() should be true after BEGIN")
	}

	_, err = db.Exec("COMMIT")
	if err != nil {
		t.Fatal(err)
	}
	if db.InTransaction() {
		t.Fatal("InTransaction() should be false after COMMIT")
	}

	_, err = db.Exec("BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("ROLLBACK")
	if err != nil {
		t.Fatal(err)
	}
	if db.InTransaction() {
		t.Fatal("InTransaction() should be false after ROLLBACK")
	}
}

func TestDsnModeCreate(t *testing.T) {
	// mode=create should fail if the database already exists
	tmp := filepath.Join(t.TempDir(), "test.ddb")

	// First create the database
	db, err := os.Create(tmp)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Now try to open with mode=create — should fail
	d := &Driver{}
	_, err = d.OpenConnector("file:" + tmp + "?mode=create")
	if err != nil {
		// Connector creation itself doesn't fail, but Connect should
	}
	connector, _ := d.OpenConnector("file:" + tmp + "?mode=create")
	if connector != nil {
		_, err = connector.Connect(t.Context())
		if err == nil {
			t.Fatal("mode=create should fail when database already exists")
		}
	}
}

func TestDsnModeOpen(t *testing.T) {
	// mode=open should fail if the database doesn't exist
	tmp := filepath.Join(t.TempDir(), "nonexistent.ddb")
	d := &Driver{}
	connector, err := d.OpenConnector("file:" + tmp + "?mode=open")
	if err != nil {
		t.Fatal(err)
	}
	_, err = connector.Connect(t.Context())
	if err == nil {
		t.Fatal("mode=open should fail when database doesn't exist")
	}
}
