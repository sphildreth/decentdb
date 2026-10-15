package main

import (
	"context"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"
)

type MigrationNote struct {
	bun.BaseModel `bun:"table:bun_migration_notes"`
	ID            int64  `bun:",pk,autoincrement"`
	Text          string `bun:",notnull"`
}

func exampleMigrations() (*migrate.Migrations, error) {
	migrations := migrate.NewMigrations()
	// Bun derives the migration's version/comment from this file's name.
	err := migrations.Register(
		func(ctx context.Context, db *bun.DB) error {
			_, err := db.NewCreateTable().Model((*MigrationNote)(nil)).Exec(ctx)
			return err
		},
		func(ctx context.Context, db *bun.DB) error {
			_, err := db.NewDropTable().Model((*MigrationNote)(nil)).Exec(ctx)
			return err
		},
	)
	return migrations, err
}
