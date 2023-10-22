package persistence

import (
	"context"
	"testing"

	_ "embed"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type mockEngine struct {
	blocks map[uint64]*types.Block
}

func newMockEngine() execution_client.ExecutionEngine {
	return &mockEngine{
		blocks: make(map[uint64]*types.Block),
	}
}

func (m *mockEngine) ForkChoiceUpdate(finalized libcommon.Hash, head libcommon.Hash) error {
	panic("unimplemented")
}

func (m *mockEngine) FrozenBlocks() uint64 {
	panic("unimplemented")
}

func (m *mockEngine) NewPayload(payload *cltypes.Eth1Block, beaconParentRoot *libcommon.Hash) (bool, error) {
	panic("unimplemented")
}

func (m *mockEngine) SupportInsertion() bool {
	return true
}

func (m *mockEngine) InsertBlocks([]*types.Block) error {
	panic("unimplemented")
}

func (m *mockEngine) IsCanonicalHash(libcommon.Hash) (bool, error) {
	panic("unimplemented")
}

func (m *mockEngine) Ready() (bool, error) {
	return true, nil
}

func (m *mockEngine) InsertBlock(b *types.Block) error {
	m.blocks[b.NumberU64()] = b
	return nil
}

func (m *mockEngine) GetBodiesByRange(start, count uint64) ([]*types.RawBody, error) {
	bds := []*types.RawBody{}
	for i := start; i < start+count; i++ {

		blk, ok := m.blocks[i]
		if !ok {
			break
		}
		bds = append(bds, blk.RawBody())
	}
	return bds, nil
}

func (m *mockEngine) GetBodiesByHashes(hashes []libcommon.Hash) ([]*types.RawBody, error) {
	panic("unimplemented")
}

//go:embed test_data/test_block.ssz_snappy
var testBlock []byte

func getTestBlock() *cltypes.SignedBeaconBlock {
	enc, err := utils.DecompressSnappy(testBlock)
	if err != nil {
		panic(err)
	}
	bcBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	if err := bcBlock.DecodeSSZ(enc, int(clparams.CapellaVersion)); err != nil {
		panic(err)
	}
	bcBlock.Block.Slot = (clparams.MainnetBeaconConfig.CapellaForkEpoch + 1) * 32
	bcBlock.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	bcBlock.Block.Body.ExecutionPayload.BlockNumber = 100
	bcBlock.Block.Body.ExecutionPayload.BlockHash = libcommon.HexToHash("0x78e6ce0d5a80c7416138af475d20c0a0a22124ae67b6dc5a0d0d0fe6f95e365d")
	return bcBlock
}

func setupStore(t *testing.T, full bool) (BeaconChainDatabase, kv.RwDB, execution_client.ExecutionEngine) {
	// Open an in-memory SQLite database for testing
	db := memdb.NewTestDB(t)
	// Create an in-memory filesystem
	fs := afero.NewMemMapFs()
	engine := newMockEngine()
	return NewBeaconChainDatabaseFilesystem(NewAferoRawBlockSaver(fs, &clparams.MainnetBeaconConfig), engine, &clparams.MainnetBeaconConfig), db, engine
}

func TestBlockSaverStoreLoadPurgeFull(t *testing.T) {
	store, db, _ := setupStore(t, true)
	defer db.Close()

	tx, _ := db.BeginRw(context.Background())
	defer tx.Rollback()

	ctx := context.Background()
	block := getTestBlock()
	require.NoError(t, store.WriteBlock(ctx, tx, block, true))

	blks, err := store.GetRange(context.Background(), tx, block.Block.Slot, 1)
	require.NoError(t, err)
	require.Equal(t, len(blks.Data), 1)

	expectedRoot, err := block.HashSSZ()
	require.NoError(t, err)

	haveRoot, err := blks.Data[0].HashSSZ()
	require.NoError(t, err)

	require.Equal(t, expectedRoot, haveRoot)

	require.NoError(t, store.PurgeRange(ctx, tx, 0, 99999999999)) // THE PUURGE

	newBlks, err := store.GetRange(context.Background(), tx, block.Block.Slot, 1)
	require.NoError(t, err)
	require.Equal(t, len(newBlks.Data), 0)
}
