package transition

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/block_processing/deneb_state.ssz_snappy
var denebState []byte

//go:embed test_data/block_processing/deneb_block.ssz_snappy
var denebBlock []byte

func TestBlockProcessingDeneb(t *testing.T) {
	state := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(state, denebState, int(clparams.DenebVersion)))
	block := &cltypes.SignedBeaconBlock{}
	require.NoError(t, utils.DecodeSSZSnappy(block, denebBlock, int(clparams.DenebVersion)))
	require.NoError(t, TransitionState(state, block, true)) // All checks already made in transition state
}
