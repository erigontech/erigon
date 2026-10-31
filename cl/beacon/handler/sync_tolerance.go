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
	"errors"
	"net/http"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

// errHeadBehindForkChoice tells a stuck fork-choice head apart from a node without a head state.
var errHeadBehindForkChoice = errors.New("served head is behind fork choice")

// syncToleranceEpochs is how far blocks imported on top of the head may reach past it before the
// node treats itself as syncing. Only descendants of the head count, so neither a chain-wide gap
// nor a lighter fork trips it. The head state is republished only after the stage processes a
// block, and a child of a FULL parent attaches only once that payload is applied, so a healthy head
// can trail its descendants by a few slots; an epoch absorbs that without hiding a stuck head.
const syncToleranceEpochs = 1

// refuseLaggingHead is the 503 a validator endpoint returns while the head lags.
func (a *ApiHandler) refuseLaggingHead() error {
	if err := a.headLag(); err != nil {
		return beaconhttp.NewEndpointError(http.StatusServiceUnavailable, err)
	}
	return nil
}

func (a *ApiHandler) headLagsBehind() bool {
	return a.headLag() != nil
}

// headLag reports why the served head cannot be built on: no head state; a head that does not
// descend from the justified checkpoint get_head starts from; or blocks more than the tolerance
// beyond the head imported on top of it, since get_head would have moved to them.
func (a *ApiHandler) headLag() error {
	headRoot, headSlot, ok := a.syncedData.StateHead()
	if !ok {
		return synced_data.ErrNotSynced
	}
	if !a.descendsFromJustified(headRoot, headSlot) {
		return errHeadBehindForkChoice
	}
	if a.forkchoiceStore.HasImportedDescendantAfter(headRoot, headSlot+syncToleranceEpochs*a.beaconChainCfg.SlotsPerEpoch) {
		return errHeadBehindForkChoice
	}
	return nil
}

// justifiedDescent is a resolved descendsFromJustified answer. The parent links between a served
// root and the justified block do not change, so the answer holds until either one changes.
type justifiedDescent struct {
	root      common.Hash
	justified solid.Checkpoint
	descends  bool
}

// descendsFromJustified reports whether the block root at slot descends from the justified block,
// which get_head starts from. A justified block or a path the fork graph does not hold, as after a
// checkpoint sync or a prune, counts as descending; a block below the justified block never does.
func (a *ApiHandler) descendsFromJustified(root common.Hash, slot uint64) bool {
	justified := a.forkchoiceStore.JustifiedCheckpoint()
	if known := a.justifiedDescent.Load(); known != nil && known.root == root && known.justified == justified {
		return known.descends
	}
	header, ok := a.forkchoiceStore.GetHeader(justified.Root)
	if !ok {
		return true
	}
	descends := slot >= header.Slot
	if descends {
		ancestor := a.forkchoiceStore.Ancestor(root, header.Slot).Root
		if ancestor == (common.Hash{}) {
			return true
		}
		descends = ancestor == justified.Root
	}
	a.justifiedDescent.Store(&justifiedDescent{root: root, justified: justified, descends: descends})
	return descends
}
