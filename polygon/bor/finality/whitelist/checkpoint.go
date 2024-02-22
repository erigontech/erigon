package whitelist

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor/finality/rawdb"
)

type checkpoint struct {
	finality[*rawdb.Checkpoint]
}
type checkpointService interface {
	finalityService
}

var (
	//Metrics for collecting the whitelisted milestone number
	whitelistedCheckpointNumberMeter = metrics.GetOrCreateGauge("chain_checkpoint_latest")

	//Metrics for collecting the number of invalid chains received
	checkpointChainMeter = metrics.GetOrCreateGauge("chain_checkpoint_isvalidchain")
)

// IsValidChain checks the validity of chain by comparing it
// against the local checkpoint entry
func (w *checkpoint) IsValidChain(currentHeader uint64, chain []*types.Header) bool {
	w.finality.RLock()
	defer w.finality.RUnlock()

	res := w.finality.IsValidChain(currentHeader, chain)

	if res {
		checkpointChainMeter.Inc()
	} else {
		checkpointChainMeter.Dec()
	}

	return res
}

func (w *checkpoint) Process(block uint64, hash common.Hash) {
	w.finality.Lock()
	defer w.finality.Unlock()

	w.finality.Process(block, hash)

	whitelistedCheckpointNumberMeter.SetUint64(block)
}
