package sql_migrations

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestApplyMigrations(t *testing.T) {
	// Open an in-memory SQLite database for testing
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction for testing
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	// Call ApplyMigrations with the test transaction
	err = ApplyMigrations(context.Background(), tx)
	if err != nil {
		t.Fatalf("ApplyMigrations failed: %v", err)
	}

	// You can add more tests here, such as verifying if the schema_version table is created and populated correctly,
	// or checking if the beacon_indices table and index are created correctly.

	// Commit the transaction (or rollback, if you're just testing)
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}
