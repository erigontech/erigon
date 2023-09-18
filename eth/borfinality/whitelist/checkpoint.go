package whitelist

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/rawdb"
)

type checkpoint struct {
	finality[*rawdb.Checkpoint]
}
type checkpointService interface {
	finalityService
}

// TODO: Uncomment once metrics is added
// var (
// 	//Metrics for collecting the whitelisted milestone number
// 	whitelistedCheckpointNumberMeter = metrics.NewRegisteredGauge("chain/checkpoint/latest", nil)

// 	//Metrics for collecting the number of invalid chains received
// 	CheckpointChainMeter = metrics.NewRegisteredMeter("chain/checkpoint/isvalidchain", nil)
// )

// IsValidChain checks the validity of chain by comparing it
// against the local checkpoint entry
func (w *checkpoint) IsValidChain(currentHeader uint64, chain []*types.Header) bool {
	w.finality.RLock()
	defer w.finality.RUnlock()

	res := w.finality.IsValidChain(currentHeader, chain)

	// TODO: Uncomment once metrics is added
	// if res {
	// 	CheckpointChainMeter.Mark(int64(1))
	// } else {
	// 	CheckpointPeerMeter.Mark(int64(-1))
	// }

	return res
}

func (w *checkpoint) Process(block uint64, hash common.Hash) {
	w.finality.Lock()
	defer w.finality.Unlock()

	w.finality.Process(block, hash)

	// TODO: Uncomment once metrics is added
	// whitelistedCheckpointNumberMeter.Update(int64(block))
}
