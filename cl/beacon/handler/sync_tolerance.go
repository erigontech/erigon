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

// syncToleranceEpochs is how far the head may lag behind a slot before the node treats
// itself as syncing for that slot: it reports is_syncing and refuses to propose.
const syncToleranceEpochs = 1

func headLagExceedsSyncTolerance(slot, headSlot, toleranceSlots uint64) bool {
	return slot > headSlot+toleranceSlots
}

// headLagsBehind is true without a head state and also when the head is older than the
// sync tolerance for slot, since a block built on such a head cannot become canonical.
func (a *ApiHandler) headLagsBehind(slot uint64) bool {
	if a.syncedData.Syncing() {
		return true
	}
	return headLagExceedsSyncTolerance(slot, a.syncedData.HeadSlot(), syncToleranceEpochs*a.beaconChainCfg.SlotsPerEpoch)
}
