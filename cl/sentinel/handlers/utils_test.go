package handlers

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func setupStore(t *testing.T) (persistence.RawBeaconBlockChain, kv.RwDB) {
	db := memdb.NewTestDB(t)
	af := afero.NewMemMapFs()
	rawDB := persistence.NewAferoRawBlockSaver(af, &clparams.MainnetBeaconConfig)
	return rawDB, db
}

func populateDatabaseWithBlocks(t *testing.T, store persistence.BeaconChainDatabase, tx kv.RwTx, startSlot, count uint64) []*cltypes.SignedBeaconBlock {

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

		// Populate BeaconChainDatabase
		store.WriteBlock(context.Background(), tx, block, canonical)

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
