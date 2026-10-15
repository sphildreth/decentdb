package main

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	decentdb "github.com/sphildreth/decentdb/bindings/go/decentdb-go"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/feature"
)

func TestFileShowcaseCanBeRepeatedAndPersists(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "path with spaces", "demo.ddb")
	for iteration := range 2 {
		var output bytes.Buffer
		if err := run(ctx, path, false, &output); err != nil {
			t.Fatalf("run %d: %v", iteration, err)
		}
		if !strings.Contains(output.String(), "19.99 + 0.01 = 20") ||
			!strings.Contains(output.String(), "Reopened ") {
			t.Fatalf("missing arithmetic or persistence output: %s", output.String())
		}
	}

	db, err := openDB(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var invoice Invoice
	if err := db.NewSelect().Model(&invoice).Relation("Customer").
		Where("invoice.number = ?", "BUN-001").Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if invoice.ID != 1 || invoice.Customer.ID != 1 || invoice.Customer.Name != "Ada O'Neil" ||
		invoice.Amount != (decentdb.Decimal{Unscaled: 20}) || !invoice.Paid || !invoice.IssuedAt.Equal(issuedAt) {
		t.Fatalf("unexpected persisted invoice: %+v", invoice)
	}
	count, err := db.NewSelect().Model((*Invoice)(nil)).Count(ctx)
	if err != nil || count != 3 {
		t.Fatalf("expected three invoices after repeated upsert/draft deletion: count=%d, err=%v", count, err)
	}

	// The Bun relation creates a real native foreign key, not only a Go link.
	bad := &Invoice{Number: "BAD-FK", CustomerID: 999999, Amount: (decentdb.Decimal{Unscaled: 1}),
		IssuedAt: issuedAt, Metadata: invoice.Metadata}
	_, err = db.NewInsert().Model(bad).Exec(ctx)
	var status *decentdb.DecentDBError
	if !errors.As(err, &status) || !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
		t.Fatalf("expected native foreign-key error, got %v", err)
	}
}

func TestInMemoryShowcase(t *testing.T) {
	var output bytes.Buffer
	if err := run(context.Background(), ":memory:", false, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "Reopened ") {
		t.Fatal("in-memory database cannot be reopened for persistence")
	}
}

func TestOrdinaryBunModelsAndDialectWorkWithoutFeatureMasks(t *testing.T) {
	ctx := context.Background()
	db, err := openDB(ctx, ":memory:", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.HasFeature(feature.UpdateTableAlias | feature.DeleteTableAlias) {
		t.Fatal("example disabled ordinary Bun DML aliases")
	}
	if err := createSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	invoice, err := seed(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	update := db.NewUpdate().Model(invoice).Set("paid = TRUE").WherePK().Returning("paid")
	if !strings.Contains(update.String(), " AS ") {
		t.Fatal("Bun should generate a target alias")
	}
	if _, err := update.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if !invoice.Paid {
		t.Fatal("RETURNING did not scan the updated model")
	}
	type SerialModel struct {
		bun.BaseModel `bun:"table:bun_serial_probe"`
		ID            int64 `bun:",pk,autoincrement"`
	}
	create := db.NewCreateTable().Model((*SerialModel)(nil))
	if !strings.Contains(create.String(), "BIGSERIAL") {
		t.Fatal("expected normal Bun BIGSERIAL DDL")
	}
	if _, err := create.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	serial := &SerialModel{}
	if _, err := db.NewInsert().Model(serial).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if serial.ID <= 0 {
		t.Fatal("Bun did not scan the automatically returned generated ID")
	}
}
