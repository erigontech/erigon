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

// syncToleranceEpochs is how far blocks imported on top of the head may reach past it before
// the node treats itself as syncing: it reports is_syncing and refuses blocks, attestation data,
// duties and pool attestations. Only descendants of the head count, so neither a chain-wide gap
// nor a lighter fork trips the tolerance.
const syncToleranceEpochs = 1

// refuseLaggingHead is the 503 a validator endpoint returns while headLagsBehind holds.
func (a *ApiHandler) refuseLaggingHead() error {
	if a.headLagsBehind() {
		return beaconhttp.NewEndpointError(http.StatusServiceUnavailable, synced_data.ErrNotSynced)
	}
	return nil
}

// headLagsBehind is true without a head state and also when blocks more than the tolerance
// beyond the head were imported on top of it, since get_head would have moved to them.
func (a *ApiHandler) headLagsBehind() bool {
	if a.syncedData.Syncing() {
		return true
	}
	headRoot, headSlot, ok := a.syncedData.StateHead()
	return !ok || a.forkchoiceStore.HasImportedDescendantAfter(headRoot, headSlot+syncToleranceEpochs*a.beaconChainCfg.SlotsPerEpoch)
}
