// beacon_indexes_test.go

package beacon_indexes

import (
	"database/sql"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
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

	indexer, err := NewSqlBeaconIndexer(db)
	require.NoError(t, err)

	// Mock a block
	block := cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig)
	block.EncodingSizeSSZ()

	err = indexer.GenerateBlockIndicies(block)
	require.NoError(t, err)

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.HashSSZ()
	require.NoError(t, err)

	retrievedSlot, err := indexer.ReadBlockSlotByBlockRoot(blockRoot)
	require.NoError(t, err)
	require.Equal(t, block.Slot, retrievedSlot)

}
