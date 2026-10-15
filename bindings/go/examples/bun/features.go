package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	decentdb "github.com/sphildreth/decentdb/bindings/go/decentdb-go"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

type AuditEntry struct {
	bun.BaseModel `bun:"table:bun_audit_entries,alias:entry"`
	ID            int64     `bun:",pk,autoincrement"`
	Message       string    `bun:",notnull"`
	DeletedAt     time.Time `bun:",soft_delete,nullzero"`
}

func demonstrateCollections(ctx context.Context, db *bun.DB, customerID int64, out io.Writer) error {
	invoices := []*Invoice{
		{Number: "BUN-BULK-1", CustomerID: customerID, Amount: decentdb.Decimal{Unscaled: 1, Scale: 2}, IssuedAt: issuedAt, Metadata: json.RawMessage(`{"department":"sales"}`)},
		{Number: "BUN-BULK-2", CustomerID: customerID, Amount: decentdb.Decimal{Unscaled: 1, Scale: 1}, IssuedAt: issuedAt, Metadata: json.RawMessage(`{"department":"sales"}`)},
	}
	if _, err := db.NewInsert().Model(&invoices).On("CONFLICT (number) DO UPDATE").
		Set("amount = EXCLUDED.amount").Set("paid = EXCLUDED.paid").Returning("id").Exec(ctx); err != nil {
		return fmt.Errorf("bulk insert: %w", err)
	}
	for _, invoice := range invoices {
		if invoice.ID <= 0 {
			return fmt.Errorf("bulk INSERT RETURNING did not populate ID for %s", invoice.Number)
		}
	}
	fmt.Fprintf(out, "Bulk insert/upsert + generated IDs: %d invoices\n", len(invoices))

	var customer Customer
	if err := db.NewSelect().Model(&customer).Relation("Invoices").
		Where("customer.id = ?", customerID).Scan(ctx); err != nil {
		return fmt.Errorf("has-many relation: %w", err)
	}
	if len(customer.Invoices) != 3 {
		return fmt.Errorf("expected three related invoices, got %d", len(customer.Invoices))
	}
	fmt.Fprintf(out, "Has-many relation: %s has %d invoices\n", customer.Name, len(customer.Invoices))

	if _, err := db.NewCreateIndex().Model((*Invoice)(nil)).Index("bun_invoices_amount_idx").Column("amount").IfNotExists().Exec(ctx); err != nil {
		return err
	}
	count, err := db.NewSelect().Model((*Invoice)(nil)).Where("invoice.amount = ?", decentdb.Decimal{Unscaled: 1, Scale: 2}).Count(ctx)
	if err != nil || count != 1 {
		return fmt.Errorf("indexed decimal filter: count=%d, err=%v", count, err)
	}
	var penny Invoice
	if err := db.NewSelect().Model(&penny).
		Where("invoice.amount = ?", decentdb.Decimal{Unscaled: 1, Scale: 2}).Scan(ctx); err != nil {
		return fmt.Errorf("indexed decimal model scan: %w", err)
	}
	if penny.ID != invoices[0].ID {
		return fmt.Errorf("indexed decimal model scan returned invoice %d", penny.ID)
	}
	var page []*Invoice
	if err := db.NewSelect().Model(&page).Where("invoice.customer_id = ?", customerID).
		OrderExpr("invoice.amount DESC").Limit(2).Offset(1).Scan(ctx); err != nil {
		return err
	}
	if len(page) != 2 || page[0].Amount.String() != "0.1" || page[1].Amount.String() != "0.01" {
		return fmt.Errorf("decimal ordering or pagination failed")
	}
	fmt.Fprintln(out, "Indexed decimal filter + pagination: 0.01 and 0.1 remain distinct")

	unpaid := db.NewSelect().Model((*Invoice)(nil)).Column("customer_id", "amount").Where("paid = ?", false)
	var report struct {
		Count   int64            `bun:"invoice_count"`
		Total   decentdb.Decimal `bun:"total"`
		Average decentdb.Decimal `bun:"average"`
		Largest decentdb.Decimal `bun:"largest"`
	}
	if err := db.NewSelect().With("unpaid", unpaid).TableExpr("unpaid").
		ColumnExpr("COUNT(*) AS invoice_count").ColumnExpr("SUM(amount) AS total").
		ColumnExpr("AVG(amount) AS average").ColumnExpr("MAX(amount) AS largest").Scan(ctx, &report); err != nil {
		return err
	}
	if report.Count != 2 || report.Total.String() != "0.11" || report.Average.String() != "0.055" || report.Largest.String() != "0.1" {
		return fmt.Errorf("unexpected unpaid report: %+v", report)
	}
	fmt.Fprintf(out, "CTE + exact aggregate report: %d unpaid invoices, total %s, average %s, largest %s\n", report.Count, report.Total, report.Average, report.Largest)
	return nil
}

func demonstrateSoftDelete(ctx context.Context, db *bun.DB, out io.Writer) error {
	if _, err := db.NewCreateTable().Model((*AuditEntry)(nil)).IfNotExists().Exec(ctx); err != nil {
		return err
	}
	entry := &AuditEntry{Message: "Invoice demo completed"}
	if _, err := db.NewInsert().Model(entry).Exec(ctx); err != nil {
		return err
	}
	if _, err := db.NewDelete().Model(entry).WherePK().Exec(ctx); err != nil {
		return err
	}
	active, err := db.NewSelect().Model((*AuditEntry)(nil)).Count(ctx)
	if err != nil || active != 0 {
		return fmt.Errorf("soft delete: active=%d, err=%v", active, err)
	}
	deleted, err := db.NewSelect().Model((*AuditEntry)(nil)).WhereDeleted().Count(ctx)
	if err != nil || deleted != 1 {
		return fmt.Errorf("soft delete: deleted=%d, err=%v", deleted, err)
	}
	if _, err := db.NewDelete().Model(entry).WherePK().ForceDelete().Exec(ctx); err != nil {
		return err
	}
	fmt.Fprintln(out, "Soft delete: default queries hide deleted rows; ForceDelete removes them")
	return nil
}

func demonstrateMigrations(ctx context.Context, db *bun.DB, out io.Writer) (resultErr error) {
	migrations, err := exampleMigrations()
	if err != nil {
		return err
	}
	migrator := migrate.NewMigrator(db, migrations)
	if err := migrator.Init(ctx); err != nil {
		return err
	}
	if err := migrator.Lock(ctx); err != nil {
		return err
	}
	defer func() {
		if err := migrator.Unlock(context.WithoutCancel(ctx)); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("unlock migrations: %w", err))
		}
	}()
	group, err := migrator.Migrate(ctx)
	if err != nil {
		return fmt.Errorf("apply migration: %w", err)
	}
	if group.IsZero() {
		return fmt.Errorf("expected a migration to be applied")
	}
	note := &MigrationNote{Text: "Bun migrations use the native DecentDB driver"}
	if _, err := db.NewInsert().Model(note).Exec(ctx); err != nil {
		return err
	}
	if _, err := migrator.Rollback(ctx); err != nil {
		return fmt.Errorf("rollback migration: %w", err)
	}
	fmt.Fprintln(out, "Bun migrations: schema applied, used, and rolled back under a migration lock")
	return nil
}
