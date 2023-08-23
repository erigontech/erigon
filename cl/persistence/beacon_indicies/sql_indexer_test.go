// beacon_indexes_test.go

package beacon_indicies

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/sql_migrations"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *sql.DB {
	// Create an in-memory SQLite DB for testing purposes
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(t, err)
	return db
}

func TestWriteBlockRoot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.Begin()

	sql_migrations.ApplyMigrations(context.Background(), tx)

	// Mock a block
	block := cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig)
	block.EncodingSizeSSZ()

	require.NoError(t, GenerateBlockIndicies(context.Background(), tx, block, false))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.HashSSZ()
	require.NoError(t, err)

	retrievedSlot, err := ReadBlockSlotByBlockRoot(context.Background(), tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, block.Slot, retrievedSlot)

}
