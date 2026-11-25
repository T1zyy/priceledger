// Package testdb creates disposable databases for integration tests.
package testdb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Open(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL or run make integration")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(admin.Close)
	name := fmt.Sprintf("test_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+name); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := admin.Exec(ctx, "DROP DATABASE "+name); e != nil {
			t.Error(e)
		}
	})
	cfg := admin.Config()
	cfg.ConnConfig.Database = name
	db, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(db.Close)
	return db
}
