package antiquary

import (
	"context"
	"testing"

	_ "embed"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/capella/blocks_0.ssz_snappy
var capella_blocks_0_ssz_snappy []byte

//go:embed test_data/capella/blocks_1.ssz_snappy
var capella_blocks_1_ssz_snappy []byte

//go:embed test_data/capella/pre.ssz_snappy
var capella_pre_state_ssz_snappy []byte

//go:embed test_data/capella/post.ssz_snappy
var capella_post_state_ssz_snappy []byte

type MockBlockReader struct {
	u map[uint64]*cltypes.SignedBeaconBlock
}

func NewMockBlockReader() *MockBlockReader {
	return &MockBlockReader{u: make(map[uint64]*cltypes.SignedBeaconBlock)}
}

func (m *MockBlockReader) ReadBlockBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	return m.u[slot], nil
}

func (m *MockBlockReader) ReadBlockByRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlock, error) {
	panic("implement me")
}
func (m *MockBlockReader) ReadHeaderByRoot(ctx context.Context, tx kv.Tx, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	panic("implement me")
}

func (m *MockBlockReader) FrozenSlots() uint64 {
	panic("implement me")
}

func LoadChain(t *testing.T, blocks []*cltypes.SignedBeaconBlock, db kv.RwDB) *MockBlockReader {
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)

	m := NewMockBlockReader()
	for _, block := range blocks {
		m.u[block.Block.Slot] = block
		h := block.SignedBeaconBlockHeader()
		require.NoError(t, beacon_indicies.WriteBeaconBlockHeaderAndIndicies(context.Background(), tx, h, true))
		require.NoError(t, beacon_indicies.WriteHighestFinalized(tx, block.Block.Slot+32))
	}

	require.NoError(t, tx.Commit())
	return m
}

func TestStateAntiquary(t *testing.T) {
	ctx := context.Background()
	block1 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
	block2 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)

	// Lets do te
	if err := utils.DecodeSSZSnappy(block1, capella_blocks_0_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		t.Fatal(err)
	}
	if err := utils.DecodeSSZSnappy(block2, capella_blocks_1_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		t.Fatal(err)
	}
	db := memdb.NewTestDB(t)
	reader := LoadChain(t, []*cltypes.SignedBeaconBlock{block1, block2}, db)
	preState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(preState, capella_pre_state_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		t.Fatal(err)
	}
	postState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(postState, capella_post_state_ssz_snappy, int(clparams.CapellaVersion)); err != nil {
		t.Fatal(err)
	}

	vt := state_accessors.NewStaticValidatorTable()
	f := afero.NewMemMapFs()
	a := NewAntiquary(ctx, preState, vt, &clparams.MainnetBeaconConfig, datadir.New("/tmp"), nil, db, nil, reader, nil, log.New(), true, f)
	require.NoError(t, a.incrementBeaconState(ctx, block2.Block.Slot+2))

	// Now lets test it against the reader
	hr := historical_states_reader.NewHistoricalStatesReader(&clparams.MainnetBeaconConfig, reader, vt, f, preState)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	s, err := hr.ReadHistoricalState(ctx, tx, block2.Block.Slot)
	require.NoError(t, err)

	postHash, err := s.HashSSZ()
	require.NoError(t, err)
	postHash2, err := postState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(postHash2), libcommon.Hash(postHash))
}
