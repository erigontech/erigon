package beacon_indicies

import (
	"context"
	"database/sql"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
	defer tx.Rollback()

	sql_migrations.ApplyMigrations(context.Background(), tx)

	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.EncodingSizeSSZ()

	require.NoError(t, GenerateBlockIndicies(context.Background(), tx, block, false))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	retrievedSlot, err := ReadBlockSlotByBlockRoot(context.Background(), tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, block.Block.Slot, retrievedSlot)

	canonicalRoot, err := ReadCanonicalBlockRoot(context.Background(), tx, retrievedSlot)
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash{}, canonicalRoot)

	err = MarkRootCanonical(context.Background(), tx, retrievedSlot, blockRoot)
	require.NoError(t, err)

	canonicalRoot, err = ReadCanonicalBlockRoot(context.Background(), tx, retrievedSlot)
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(blockRoot), canonicalRoot)
}

func TestReadParentBlockRoot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.Begin()
	defer tx.Rollback()

	sql_migrations.ApplyMigrations(context.Background(), tx)

	mockParentRoot := libcommon.Hash{1}
	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.Block.ParentRoot = mockParentRoot
	block.EncodingSizeSSZ()

	require.NoError(t, GenerateBlockIndicies(context.Background(), tx, block, false))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	retrieveParentRoot, err := ReadParentBlockRoot(context.Background(), tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, mockParentRoot, retrieveParentRoot)
}

func TestTruncateCanonicalChain(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.Begin()
	defer tx.Rollback()

	sql_migrations.ApplyMigrations(context.Background(), tx)

	mockParentRoot := libcommon.Hash{1}
	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.Block.ParentRoot = mockParentRoot
	block.EncodingSizeSSZ()

	require.NoError(t, GenerateBlockIndicies(context.Background(), tx, block, true))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	canonicalRoot, err := ReadCanonicalBlockRoot(context.Background(), tx, block.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(blockRoot), canonicalRoot)

	require.NoError(t, TruncateCanonicalChain(context.Background(), tx, 0))

	canonicalRoot, err = ReadCanonicalBlockRoot(context.Background(), tx, block.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, canonicalRoot, libcommon.Hash{})
}

func TestReadBeaconBlockHeader(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.Begin()
	defer tx.Rollback()

	sql_migrations.ApplyMigrations(context.Background(), tx)

	mockParentRoot := libcommon.Hash{1}
	mockSignature := [96]byte{23}

	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.Block.ParentRoot = mockParentRoot
	block.Signature = mockSignature

	canonical := true
	block.EncodingSizeSSZ()

	require.NoError(t, GenerateBlockIndicies(context.Background(), tx, block, canonical))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	header, signature, isCanonical, err := ReadHeaderAndSignatureByBlockRoot(context.Background(), tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, isCanonical, canonical)
	require.Equal(t, signature, mockSignature)
	require.NotNil(t, header)

	headerRoot, err := header.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, headerRoot, blockRoot)

}
