package db_config

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ledgerwatch/erigon/cl/persistence/sql_migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Start a transaction for testing
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	// Call ApplyMigrations with the test transaction
	err = sql_migrations.ApplyMigrations(context.Background(), tx)
	if err != nil {
		t.Fatalf("ApplyMigrations failed: %v", err)
	}
	tx.Commit()
	return db
}

func TestDBConfig(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)
	c := DatabaseConfiguration{
		FullBlocks: true,
		PruneDepth: 69,
	}
	require.NoError(t, WriteConfigurationIfNotExist(context.Background(), tx, c))
	cfg, err := ReadConfiguration(context.Background(), tx)
	require.NoError(t, err)

	require.Equal(t, cfg, c)
}
