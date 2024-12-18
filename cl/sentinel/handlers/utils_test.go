package handlers

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/stretchr/testify/require"
)

func setupStore(t *testing.T) (freezeblocks.BeaconSnapshotReader, kv.RwDB) {
	db := memdb.NewTestDB(t)
	return tests.NewMockBlockReader(), db
}

func populateDatabaseWithBlocks(t *testing.T, store *tests.MockBlockReader, tx kv.RwTx, startSlot, count uint64) []*cltypes.SignedBeaconBlock {
	mockParentRoot := common.Hash{1}
	blocks := make([]*cltypes.SignedBeaconBlock, 0, count)
	for i := uint64(0); i <= count; i++ {
		slot := startSlot + i
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
		block.Block.Slot = slot
		block.Block.StateRoot = libcommon.Hash{byte(i)}
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
