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

//go:embed test_data/block_processing/capella_block.ssz_snappy
var capellaBlock []byte

//go:embed test_data/block_processing/capella_state.ssz_snappy
var capellaState []byte

func TestBlockProcessing(t *testing.T) {
	state := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(state, capellaState, int(clparams.CapellaVersion)))
	block := &cltypes.SignedBeaconBlock{}
	require.NoError(t, utils.DecodeSSZSnappy(block, capellaBlock, int(clparams.CapellaVersion)))
	require.NoError(t, TransitionState(state, block, true)) // All checks already made in transition state
}
