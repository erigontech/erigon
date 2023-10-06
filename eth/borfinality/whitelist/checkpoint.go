package whitelist

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/rawdb"
	"github.com/ledgerwatch/erigon/metrics"
)

type checkpoint struct {
	finality[*rawdb.Checkpoint]
}
type checkpointService interface {
	finalityService
}

var (
	//Metrics for collecting the whitelisted milestone number
	whitelistedCheckpointNumberMeter = metrics.GetOrCreateCounter("chain_checkpoint_latest", true)

	//Metrics for collecting the number of invalid chains received
	CheckpointChainMeter = metrics.GetOrCreateCounter("chain_checkpoint_isvalidchain")

	//Metrics for collecting the number of valid peers received
	CheckpointPeerMeter = metrics.GetOrCreateCounter("chain_checkpoint_isvalidpeer")
)

// IsValidChain checks the validity of chain by comparing it
// against the local checkpoint entry
func (w *checkpoint) IsValidChain(currentHeader uint64, chain []*types.Header) bool {
	w.finality.RLock()
	defer w.finality.RUnlock()

	res := w.finality.IsValidChain(currentHeader, chain)

	if res {
		CheckpointChainMeter.Add(1)
	} else {
		CheckpointPeerMeter.Add(-1)
	}

	return res
}

func (w *checkpoint) Process(block uint64, hash common.Hash) {
	w.finality.Lock()
	defer w.finality.Unlock()

	w.finality.Process(block, hash)

	whitelistedCheckpointNumberMeter.Set(block)
}
