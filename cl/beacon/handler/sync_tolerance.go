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

// syncToleranceEpochs is how far the head may trail the highest seen block before the node
// treats itself as syncing: it reports is_syncing and refuses to propose. A chain-wide gap
// leaves nothing seen beyond the head, so it never trips the tolerance.
const syncToleranceEpochs = 1

func headLagExceedsSyncTolerance(highestSeen, headSlot, toleranceSlots uint64) bool {
	return highestSeen > headSlot+toleranceSlots
}

// headLagsBehind is true without a head state and also when blocks more than the tolerance
// beyond the head have been seen, since a block built on such a head cannot become canonical.
func (a *ApiHandler) headLagsBehind() bool {
	if a.syncedData.Syncing() {
		return true
	}
	return headLagExceedsSyncTolerance(a.forkchoiceStore.HighestSeen(), a.syncedData.HeadSlot(), syncToleranceEpochs*a.beaconChainCfg.SlotsPerEpoch)
}
