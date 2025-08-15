// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package beacon_indicies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func setupTestDB(t *testing.T) kv.RwDB {
	// Create an in-memory SQLite DB for testing purposes
	db := memdb.NewTestDB(t, kv.ChainDB)
	return db
}

func TestWriteBlockRoot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
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
	require.Equal(t, common.Hash{}, canonicalRoot)

	err = MarkRootCanonical(context.Background(), tx, *retrievedSlot, blockRoot)
	require.NoError(t, err)

	canonicalRoot, err = ReadCanonicalBlockRoot(tx, *retrievedSlot)
	require.NoError(t, err)
	require.Equal(t, common.Hash(blockRoot), canonicalRoot)
}

func TestReadParentBlockRoot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	mockParentRoot := common.Hash{1}
	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
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

	mockParentRoot := common.Hash{1}
	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Slot = 56
	block.Block.ParentRoot = mockParentRoot
	block.EncodingSizeSSZ()

	require.NoError(t, WriteBeaconBlockHeaderAndIndicies(context.Background(), tx, block.SignedBeaconBlockHeader(), true))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	canonicalRoot, err := ReadCanonicalBlockRoot(tx, block.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, common.Hash(blockRoot), canonicalRoot)

	require.NoError(t, TruncateCanonicalChain(context.Background(), tx, 0))

	canonicalRoot, err = ReadCanonicalBlockRoot(tx, block.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, common.Hash{}, canonicalRoot)
}

func TestReadBeaconBlockHeader(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	mockParentRoot := common.Hash{1}
	mockSignature := [96]byte{23}

	// Mock a block
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
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

func TestWriteExecutionBlockNumber(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	tHash := common.HexToHash("0x2")
	require.NoError(t, WriteExecutionBlockNumber(tx, tHash, 1))
	require.NoError(t, WriteExecutionBlockNumber(tx, tHash, 2))
	require.NoError(t, WriteExecutionBlockNumber(tx, tHash, 3))

	// Try to retrieve the block's slot by its blockRoot and verify
	blockNumber, err := ReadExecutionBlockNumber(tx, tHash)
	require.NoError(t, err)
	require.Equal(t, uint64(3), *blockNumber)
}

func TestWriteExecutionBlockHash(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	tHash := common.HexToHash("0x2")
	tHash2 := common.HexToHash("0x3")
	require.NoError(t, WriteExecutionBlockHash(tx, tHash, tHash2))
	// Try to retrieve the block's slot by its blockRoot and verify
	tHash3, err := ReadExecutionBlockHash(tx, tHash)
	require.NoError(t, err)
	require.Equal(t, tHash2, tHash3)
}
