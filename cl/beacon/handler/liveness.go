package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
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
	maxEpoch := utils.GetCurrentEpoch(a.genesisCfg.GenesisTime, a.beaconChainCfg.SecondsPerSlot, a.beaconChainCfg.SlotsPerEpoch)
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
	var lastBlockRootProcess libcommon.Hash
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
	// use the epoch partecipation as an additional heuristic
	currentEpochPartecipation, previousEpochPartecipation, err := a.obtainCurrentEpochPartecipationFromEpoch(tx, epoch, lastBlockRootProcess, lastSlotProcess)
	if err != nil {
		return nil, err
	}
	if currentEpochPartecipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not find partecipations for epoch %d, if this was an historical query, turn on --caplin.archive", epoch))
	}
	for idx, live := range liveSet {
		if live.IsLive {
			continue
		}
		if idx >= uint64(currentEpochPartecipation.Length()) {
			continue
		}
		if currentEpochPartecipation.Get(int(idx)) != 0 {
			live.IsLive = true
			continue
		}
		if idx >= uint64(previousEpochPartecipation.Length()) {
			continue
		}
		live.IsLive = previousEpochPartecipation.Get(int(idx)) != 0
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

func (a *ApiHandler) obtainCurrentEpochPartecipationFromEpoch(tx kv.Tx, epoch uint64, blockRoot libcommon.Hash, blockSlot uint64) (*solid.BitList, *solid.BitList, error) {
	prevEpoch := epoch
	if epoch > 0 {
		prevEpoch--
	}

	currPartecipation, ok1 := a.forkchoiceStore.Partecipation(epoch)
	prevPartecipation, ok2 := a.forkchoiceStore.Partecipation(prevEpoch)
	if !ok1 || !ok2 {
		return a.stateReader.ReadPartecipations(tx, blockSlot)
	}
	return currPartecipation, prevPartecipation, nil

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
