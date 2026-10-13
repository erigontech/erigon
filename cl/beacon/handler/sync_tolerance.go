// Copyright 2026 The Erigon Authors
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

package handler

import (
	"net/http"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
)

// syncToleranceEpochs is how far blocks imported on top of the head may reach past it before the
// node treats itself as syncing. Only descendants of the head count, so neither a chain-wide gap
// nor a lighter fork trips it.
const syncToleranceEpochs = 1

// refuseLaggingHead is the 503 a validator endpoint returns while headLagsBehind holds.
func (a *ApiHandler) refuseLaggingHead() error {
	if a.headLagsBehind() {
		return beaconhttp.NewEndpointError(http.StatusServiceUnavailable, synced_data.ErrNotSynced)
	}
	return nil
}

// headLagsBehind is true without a head state, when the head is older than the justified
// checkpoint get_head starts from, and when blocks more than the tolerance beyond the head were
// imported on top of it, since get_head would have moved to them.
func (a *ApiHandler) headLagsBehind() bool {
	if a.syncedData.Syncing() {
		return true
	}
	headRoot, headSlot, ok := a.syncedData.StateHead()
	if !ok {
		return true
	}
	justified := a.forkchoiceStore.JustifiedCheckpoint()
	if headRoot != justified.Root && headSlot < justified.Epoch*a.beaconChainCfg.SlotsPerEpoch {
		return true
	}
	return a.forkchoiceStore.HasImportedDescendantAfter(headRoot, headSlot+syncToleranceEpochs*a.beaconChainCfg.SlotsPerEpoch)
}
