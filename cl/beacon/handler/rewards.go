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
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/utils"
)

type blockRewardsResponse struct {
	ProposerIndex     uint64 `json:"proposer_index,string"`
	Attestations      uint64 `json:"attestations,string"`
	ProposerSlashings uint64 `json:"proposer_slashings,string"`
	AttesterSlashings uint64 `json:"attester_slashings,string"`
	SyncAggregate     uint64 `json:"sync_aggregate,string"`
	Total             uint64 `json:"total,string"`
}

func (a *ApiHandler) GetEthV1BeaconRewardsBlocks(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)
	blk, err := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("block not found"))
	}
	slot := blk.Header.Slot
	isFinalized := slot <= a.forkchoiceStore.FinalizedSlot()
	if slot >= a.forkchoiceStore.LowestAvailableSlot() {
		// finalized case
		blkRewards, ok := a.forkchoiceStore.BlockRewards(root)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("block not found"))
		}
		return newBeaconResponse(blockRewardsResponse{
			ProposerIndex:     blk.Header.ProposerIndex,
			Attestations:      blkRewards.Attestations,
			ProposerSlashings: blkRewards.ProposerSlashings,
			AttesterSlashings: blkRewards.AttesterSlashings,
			SyncAggregate:     blkRewards.SyncAggregate,
			Total:             blkRewards.Attestations + blkRewards.ProposerSlashings + blkRewards.AttesterSlashings + blkRewards.SyncAggregate,
		}).WithFinalized(isFinalized).WithOptimistic(isOptimistic), nil
	}
	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)
	slotData, err := state_accessors.ReadSlotData(stateGetter, slot, a.beaconChainCfg)
	if err != nil {
		return nil, err
	}
	if slotData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("could not read historical block rewards, node may not be archive or it still processing historical states"))
	}
	return newBeaconResponse(blockRewardsResponse{
		ProposerIndex:     blk.Header.ProposerIndex,
		Attestations:      slotData.AttestationsRewards,
		ProposerSlashings: slotData.ProposerSlashings,
		AttesterSlashings: slotData.AttesterSlashings,
		SyncAggregate:     slotData.SyncAggregateRewards,
		Total:             slotData.AttestationsRewards + slotData.ProposerSlashings + slotData.AttesterSlashings + slotData.SyncAggregateRewards,
	}).WithFinalized(isFinalized).WithOptimistic(isOptimistic), nil
}

type syncCommitteeReward struct {
	ValidatorIndex uint64 `json:"validator_index,string"`
	Reward         int64  `json:"reward,string"`
}

func (a *ApiHandler) PostEthV1BeaconRewardsSyncCommittees(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Retrieve all the request data -------------------------------------------
	req := []string{}
	// read the entire body
	jsonBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	// parse json body request
	if len(jsonBytes) > 0 {
		if err := json.Unmarshal(jsonBytes, &req); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
	}
	filterIndicies, err := parseQueryValidatorIndicies(a.syncedData, req)
	if err != nil {
		return nil, err
	}

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("block not found"))
	}
	slot := blk.Block.Slot
	version := a.beaconChainCfg.GetCurrentStateVersion(blk.Block.Slot / a.beaconChainCfg.SlotsPerEpoch)
	if version < clparams.AltairVersion {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("sync committee rewards not available before Altair fork"))
	}
	// retrieve the state we need -----------------------------------------------
	// We need:
	// - sync committee of the block
	// - total active balance of the block
	canonicalBlockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, blk.Block.Slot)
	if err != nil {
		return nil, err
	}

	isCanonical := canonicalBlockRoot == root

	isFinalized := blk.Block.Slot <= a.forkchoiceStore.FinalizedSlot()
	var (
		syncCommittee      *solid.SyncCommittee
		totalActiveBalance uint64
	)

	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()
	getter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)
	if slot < a.forkchoiceStore.LowestAvailableSlot() {
		if !isCanonical {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("non-canonical finalized block not found"))
		}
		epochData, err := state_accessors.ReadEpochData(getter, a.beaconChainCfg.RoundSlotToEpoch(blk.Block.Slot), a.beaconChainCfg)
		if err != nil {
			return nil, err
		}
		if epochData == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("could not read historical sync committee rewards, node may not be archive or it still processing historical states"))
		}
		totalActiveBalance = epochData.TotalActiveBalance
		syncCommittee, err = state_accessors.ReadCurrentSyncCommittee(getter, a.beaconChainCfg.RoundSlotToSyncCommitteePeriod(blk.Block.Slot))
		if err != nil {
			return nil, err
		}
		if syncCommittee == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("could not read historical sync committee, node may not be archive or it still processing historical states"))
		}
	} else {
		var ok bool
		syncCommittee, _, ok = a.forkchoiceStore.GetSyncCommittees(a.beaconChainCfg.SyncCommitteePeriod(slot))
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("non-finalized sync committee not found"))
		}
		totalActiveBalance, ok = a.forkchoiceStore.TotalActiveBalance(root)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("non-finalized total active balance not found"))
		}
	}
	committee := syncCommittee.GetCommittee()
	rewards := make([]syncCommitteeReward, 0, len(committee))

	syncAggregate := blk.Block.Body.SyncAggregate

	filterIndiciesSet := make(map[uint64]struct{})
	for _, v := range filterIndicies {
		filterIndiciesSet[v] = struct{}{}
	}
	// validator index -> accumulated rewards
	accumulatedRewards := map[uint64]int64{}
	participantReward := int64(a.syncParticipantReward(totalActiveBalance))

	for committeeIdx, v := range committee {
		idx, _, err := a.syncedData.ValidatorIndexByPublicKey(v)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("sync committee public key not found: %s", err))
		}
		if len(filterIndiciesSet) > 0 {
			if _, ok := filterIndiciesSet[idx]; !ok {
				continue
			}
		}
		if syncAggregate.IsSet(uint64(committeeIdx)) {
			accumulatedRewards[idx] += participantReward
			continue
		}
		accumulatedRewards[idx] -= participantReward
	}
	for idx, reward := range accumulatedRewards {
		rewards = append(rewards, syncCommitteeReward{
			ValidatorIndex: idx,
			Reward:         reward,
		})
	}
	sort.Slice(rewards, func(i, j int) bool {
		return rewards[i].ValidatorIndex < rewards[j].ValidatorIndex
	})
	return newBeaconResponse(rewards).WithFinalized(isFinalized).WithOptimistic(a.forkchoiceStore.IsRootOptimistic(root)), nil
}

func (a *ApiHandler) syncParticipantReward(activeBalance uint64) uint64 {
	activeBalanceSqrt := utils.IntegerSquareRoot(activeBalance)
	totalActiveIncrements := activeBalance / a.beaconChainCfg.EffectiveBalanceIncrement
	baseRewardPerInc := a.beaconChainCfg.EffectiveBalanceIncrement * a.beaconChainCfg.BaseRewardFactor / activeBalanceSqrt
	totalBaseRewards := baseRewardPerInc * totalActiveIncrements
	maxParticipantRewards := totalBaseRewards * a.beaconChainCfg.SyncRewardWeight / a.beaconChainCfg.WeightDenominator / a.beaconChainCfg.SlotsPerEpoch
	return maxParticipantRewards / a.beaconChainCfg.SyncCommitteeSize
}
