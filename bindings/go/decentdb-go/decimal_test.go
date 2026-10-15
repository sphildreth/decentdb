package decentdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math"
	"strings"
	"testing"
)

func TestDecimalStandardInterfacesPreserveExactValues(t *testing.T) {
	for _, tc := range []struct {
		input    any
		expected Decimal
		text     string
	}{
		{Decimal{Unscaled: 1999, Scale: 2}, Decimal{Unscaled: 1999, Scale: 2}, "19.99"},
		{"0.01", Decimal{Unscaled: 1, Scale: 2}, "0.01"},
		{[]byte("-0.0001"), Decimal{Unscaled: -1, Scale: 4}, "-0.0001"},
		{"1.230e2", Decimal{Unscaled: 1230, Scale: 1}, "123.0"},
		{"1e-255", Decimal{Unscaled: 1, Scale: 255}, "0." + strings.Repeat("0", 254) + "1"},
		{int64(42), Decimal{Unscaled: 42}, "42"},
		{Decimal{Unscaled: math.MinInt64, Scale: 2}, Decimal{Unscaled: math.MinInt64, Scale: 2}, "-92233720368547758.08"},
	} {
		var got Decimal
		if err := got.Scan(tc.input); err != nil || got != tc.expected || got.String() != tc.text {
			t.Fatalf("scan %v: got=%+v string=%q err=%v", tc.input, got, got.String(), err)
		}
		value, err := got.Value()
		if err != nil || value != tc.text || !driver.IsValue(value) {
			t.Fatalf("valuer returned a lossy or invalid driver value: %v, %v", value, err)
		}
	}
}

func TestDecimalInvalidScansLeaveDestinationUntouched(t *testing.T) {
	for _, input := range []any{nil, float64(0.1), "NaN", "1.2.3", "9223372036854775808", "1e-256", Decimal{Unscaled: 1, Scale: -1}} {
		got := Decimal{Unscaled: 42}
		if err := got.Scan(input); err == nil || got != (Decimal{Unscaled: 42}) {
			t.Fatalf("invalid scan %v changed destination: got=%v err=%v", input, got, err)
		}
	}
	for _, scale := range []int{-1, 256} {
		if _, err := (Decimal{Unscaled: 1, Scale: scale}).Value(); err == nil {
			t.Fatalf("invalid scale %d was accepted", scale)
		}
	}
}

func TestDecimalDatabaseSQLScannerAndNativeBinding(t *testing.T) {
	db, err := sql.Open("decentdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE prices (id INT PRIMARY KEY, amount NUMERIC)"); err != nil {
		t.Fatal(err)
	}
	input := Decimal{Unscaled: 1999, Scale: 2}
	if _, err := db.Exec("INSERT INTO prices VALUES ($1, $2)", int64(1), input); err != nil {
		t.Fatal(err)
	}
	var got Decimal
	if err := db.QueryRow("SELECT amount FROM prices WHERE id = $1", int64(1)).Scan(&got); err != nil || got != input {
		t.Fatalf("native decimal round trip: got=%v err=%v", got, err)
	}
	if _, err := db.Exec("CREATE INDEX prices_amount ON prices(amount)"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT amount FROM prices WHERE amount = $1", &input).Scan(&got); err != nil || got != input {
		t.Fatalf("pointer decimal indexed filter: got=%v err=%v", got, err)
	}
	var nullable *Decimal
	if err := db.QueryRow("SELECT CAST(NULL AS NUMERIC)").Scan(&nullable); err != nil || nullable != nil {
		t.Fatalf("nullable decimal: value=%v err=%v", nullable, err)
	}
	if err := db.QueryRow("SELECT CAST(NULL AS NUMERIC)").Scan(&got); err == nil {
		t.Fatal("NULL scanned into non-nullable Decimal")
	}
	if _, err := db.ExecContext(context.Background(), "INSERT INTO prices VALUES (2, $1)", Decimal{Unscaled: 1, Scale: 256}); err == nil {
		t.Fatal("invalid scale reached the native uint8 decimal binding")
	}
}

func TestDecimalDirectBindingRejectsInvalidScales(t *testing.T) {
	db, err := OpenDirect(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.c.Prepare("SELECT $1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, scale := range []int{-1, 256} {
		invalid := Decimal{Unscaled: 1, Scale: scale}
		if _, err := stmt.Exec([]driver.Value{invalid}); err == nil {
			t.Fatalf("direct binding silently changed scale %d", scale)
		}
		args := []driver.NamedValue{{Ordinal: 1, Value: "allocated buffer"}, {Ordinal: 2, Value: invalid}}
		if converted, err := convertQueueArgs(args); err == nil || converted != nil {
			t.Fatalf("queue conversion silently changed scale %d: %v", scale, err)
		}
	}
}
