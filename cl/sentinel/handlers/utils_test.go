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

package handlers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

func setupStore(t *testing.T) (freezeblocks.BeaconSnapshotReader, kv.RwDB) {
	db := memdb.NewTestDB(t, kv.ChainDB)
	return tests.NewMockBlockReader(), db
}

func populateDatabaseWithBlocks(t *testing.T, store *tests.MockBlockReader, tx kv.RwTx, startSlot, count uint64) []*cltypes.SignedBeaconBlock {
	mockParentRoot := common.Hash{1}
	blocks := make([]*cltypes.SignedBeaconBlock, 0, count)
	for i := uint64(0); i <= count; i++ {
		slot := startSlot + i
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
		block.Block.Slot = slot
		block.Block.StateRoot = common.Hash{byte(i)}
		block.Block.ParentRoot = mockParentRoot
		block.EncodingSizeSSZ()
		bodyRoot, _ := block.Block.Body.HashSSZ()
		canonical := true

		store.U[block.Block.Slot] = block
		require.NoError(t, beacon_indicies.WriteBeaconBlock(context.Background(), tx, block))

		// Populate indiciesDB
		require.NoError(t, beacon_indicies.WriteBeaconBlockHeaderAndIndicies(
			context.Background(),
			tx,
			&cltypes.SignedBeaconBlockHeader{
				Signature: block.Signature,
				Header: &cltypes.BeaconBlockHeader{
					Slot:          block.Block.Slot,
					ParentRoot:    block.Block.ParentRoot,
					ProposerIndex: block.Block.ProposerIndex,
					Root:          block.Block.StateRoot,
					BodyRoot:      bodyRoot,
				},
			},
			canonical))
		blocks = append(blocks, block)
	}
	return blocks
}
