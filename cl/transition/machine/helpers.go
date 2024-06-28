package machine

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func executionEnabled(s abstract.BeaconState, blockHash common.Hash) bool {
	return (!state.IsMergeTransitionComplete(s) && blockHash != common.Hash{}) || state.IsMergeTransitionComplete(s)
}
