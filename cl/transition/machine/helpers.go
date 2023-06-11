package machine

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func executionEnabled(s *state.BeaconState, payload *cltypes.Eth1Block) bool {
	return (!state.IsMergeTransitionComplete(s.BeaconState) && payload.BlockHash != common.Hash{}) || state.IsMergeTransitionComplete(s.BeaconState)
}
