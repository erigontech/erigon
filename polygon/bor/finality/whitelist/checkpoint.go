// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package whitelist

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor/finality/rawdb"
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
