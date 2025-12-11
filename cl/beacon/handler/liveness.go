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

package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/db/kv"
)

type live struct {
	Index  int  `json:"index,string"`
	IsLive bool `json:"is_live"`
}

func (a *ApiHandler) liveness(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, err
	}
	maxEpoch := a.ethClock.GetCurrentEpoch()
	if epoch > maxEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("epoch %d is in the future, max epoch is %d", epoch, maxEpoch))
	}

	var idxsStr []string
	if err := json.NewDecoder(r.Body).Decode(&idxsStr); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("could not decode request body: %w. request body is required.", err))
	}
	if len(idxsStr) == 0 {
		return newBeaconResponse([]string{}), nil
	}
	idxSet := map[int]struct{}{}
	// convert the request to uint64
	idxs := make([]uint64, 0, len(idxsStr))
	for _, idxStr := range idxsStr {
		idx, err := strconv.ParseUint(idxStr, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("could not parse validator index: %w", err))
		}
		if _, ok := idxSet[int(idx)]; ok {
			continue
		}
		idxs = append(idxs, idx)
		idxSet[int(idx)] = struct{}{}
	}

	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ctx := r.Context()
	liveSet := map[uint64]*live{}
	// initialize resp.
	for _, idx := range idxs {
		liveSet[idx] = &live{Index: int(idx), IsLive: false}
	}
	var lastBlockRootProcess common.Hash
	var lastSlotProcess uint64
	// we need to obtain the relevant data:
	// Use the blocks in the epoch as heuristic
	for i := epoch * a.beaconChainCfg.SlotsPerEpoch; i < ((epoch+1)*a.beaconChainCfg.SlotsPerEpoch)-1; i++ {
		block, err := a.blockReader.ReadBlockBySlot(ctx, tx, i)
		if err != nil {
			return nil, err
		}
		if block == nil {
			continue
		}
		updateLivenessWithBlock(block, liveSet)
		lastBlockRootProcess, err = block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		lastSlotProcess = block.Block.Slot
	}
	// use the epoch participation as an additional heuristic
	currentEpochParticipation, previousEpochParticipation, err := a.obtainCurrentEpochParticipationFromEpoch(tx, epoch, lastBlockRootProcess, lastSlotProcess)
	if err != nil {
		return nil, err
	}
	if currentEpochParticipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not find participations for epoch %d, if this was an historical query, turn on --caplin.archive", epoch))
	}
	for idx, live := range liveSet {
		if live.IsLive {
			continue
		}
		if idx >= uint64(currentEpochParticipation.Length()) {
			continue
		}
		if currentEpochParticipation.Get(int(idx)) != 0 {
			live.IsLive = true
			continue
		}
		if idx >= uint64(previousEpochParticipation.Length()) {
			continue
		}
		live.IsLive = previousEpochParticipation.Get(int(idx)) != 0
	}

	resp := []*live{}
	for _, v := range liveSet {
		resp = append(resp, v)
	}
	sort.Slice(resp, func(i, j int) bool {
		return resp[i].Index < resp[j].Index
	})

	return newBeaconResponse(resp), nil
}

func (a *ApiHandler) obtainCurrentEpochParticipationFromEpoch(tx kv.Tx, epoch uint64, blockRoot common.Hash, blockSlot uint64) (*solid.ParticipationBitList, *solid.ParticipationBitList, error) {
	prevEpoch := epoch
	if epoch > 0 {
		prevEpoch--
	}
	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)

	currParticipation, ok1 := a.forkchoiceStore.Participation(epoch)
	prevParticipation, ok2 := a.forkchoiceStore.Participation(prevEpoch)
	if !ok1 || !ok2 {
		return a.stateReader.ReadParticipations(tx, stateGetter, blockSlot)
	}
	return currParticipation, prevParticipation, nil

}

func updateLivenessWithBlock(block *cltypes.SignedBeaconBlock, liveSet map[uint64]*live) {
	body := block.Block.Body
	if _, ok := liveSet[block.Block.ProposerIndex]; ok {
		liveSet[block.Block.ProposerIndex].IsLive = true
	}
	body.VoluntaryExits.Range(func(index int, value *cltypes.SignedVoluntaryExit, length int) bool {
		if _, ok := liveSet[value.VoluntaryExit.ValidatorIndex]; ok {
			liveSet[value.VoluntaryExit.ValidatorIndex].IsLive = true
		}
		return true
	})
	body.ExecutionChanges.Range(func(index int, value *cltypes.SignedBLSToExecutionChange, length int) bool {
		if _, ok := liveSet[value.Message.ValidatorIndex]; ok {
			liveSet[value.Message.ValidatorIndex].IsLive = true
		}
		return true
	})
}
