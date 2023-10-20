package beacon_indicies

import (
	"context"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) kv.RwDB {
	// Create an in-memory SQLite DB for testing purposes
	db := memdb.NewTestDB(t)
	return db
}

func TestWriteBlockRoot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.EncodingSizeSSZ()

	require.NoError(t, WriteBeaconBlockHeaderAndIndicies(context.Background(), tx, block.SignedBeaconBlockHeader(), false))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	retrievedSlot, err := ReadBlockSlotByBlockRoot(tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, block.Block.Slot, *retrievedSlot)

	canonicalRoot, err := ReadCanonicalBlockRoot(tx, *retrievedSlot)
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash{}, canonicalRoot)

	err = MarkRootCanonical(context.Background(), tx, *retrievedSlot, blockRoot)
	require.NoError(t, err)

	canonicalRoot, err = ReadCanonicalBlockRoot(tx, *retrievedSlot)
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(blockRoot), canonicalRoot)
}

func TestReadParentBlockRoot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	mockParentRoot := libcommon.Hash{1}
	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.Block.ParentRoot = mockParentRoot
	block.EncodingSizeSSZ()

	require.NoError(t, WriteBeaconBlockHeaderAndIndicies(context.Background(), tx, block.SignedBeaconBlockHeader(), false))

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
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	mockParentRoot := libcommon.Hash{1}
	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.Block.ParentRoot = mockParentRoot
	block.EncodingSizeSSZ()

	require.NoError(t, WriteBeaconBlockHeaderAndIndicies(context.Background(), tx, block.SignedBeaconBlockHeader(), true))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	canonicalRoot, err := ReadCanonicalBlockRoot(tx, block.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(blockRoot), canonicalRoot)

	require.NoError(t, TruncateCanonicalChain(context.Background(), tx, 0))

	canonicalRoot, err = ReadCanonicalBlockRoot(tx, block.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, canonicalRoot, libcommon.Hash{})
}

func TestReadBeaconBlockHeader(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	mockParentRoot := libcommon.Hash{1}
	mockSignature := [96]byte{23}

	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block.Block.Slot = 56
	block.Block.ParentRoot = mockParentRoot
	block.Signature = mockSignature

	canonical := true
	block.EncodingSizeSSZ()

	require.NoError(t, WriteBeaconBlockHeaderAndIndicies(context.Background(), tx, block.SignedBeaconBlockHeader(), canonical))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	header, isCanonical, err := ReadSignedHeaderByBlockRoot(context.Background(), tx, blockRoot)
	require.NoError(t, err)
	require.Equal(t, isCanonical, canonical)
	require.NotNil(t, header)

	headerRoot, err := header.Header.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, headerRoot, blockRoot)

}
