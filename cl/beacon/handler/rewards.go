package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"sort"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type blockRewardsResponse struct {
	ProposerIndex     uint64 `json:"proposer_index,string"`
	Attestations      uint64 `json:"attestations,string"`
	ProposerSlashings uint64 `json:"proposer_slashings,string"`
	AttesterSlashings uint64 `json:"attester_slashings,string"`
	SyncAggregate     uint64 `json:"sync_aggregate,string"`
	Total             uint64 `json:"total,string"`
}

func (a *ApiHandler) getBlockRewards(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()
	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := blockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	root, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}
	blk, err := a.blockReader.ReadHeaderByRoot(ctx, tx, root)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "block not found")
	}
	slot := blk.Header.Slot
	isFinalized := slot <= a.forkchoiceStore.FinalizedSlot()
	if slot >= a.forkchoiceStore.LowestAvaiableSlot() {
		// finalized case
		blkRewards, ok := a.forkchoiceStore.BlockRewards(root)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "block not found")
		}
		return newBeaconResponse(blockRewardsResponse{
			ProposerIndex:     blk.Header.ProposerIndex,
			Attestations:      blkRewards.Attestations,
			ProposerSlashings: blkRewards.ProposerSlashings,
			AttesterSlashings: blkRewards.AttesterSlashings,
			SyncAggregate:     blkRewards.SyncAggregate,
			Total:             blkRewards.Attestations + blkRewards.ProposerSlashings + blkRewards.AttesterSlashings + blkRewards.SyncAggregate,
		}).withFinalized(isFinalized), nil
	}
	slotData, err := state_accessors.ReadSlotData(tx, slot)
	if err != nil {
		return nil, err
	}
	if slotData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "could not read historical block rewards, node may not be archive or it still processing historical states")
	}
	return newBeaconResponse(blockRewardsResponse{
		ProposerIndex:     blk.Header.ProposerIndex,
		Attestations:      slotData.AttestationsRewards,
		ProposerSlashings: slotData.ProposerSlashings,
		AttesterSlashings: slotData.AttesterSlashings,
		SyncAggregate:     slotData.SyncAggregateRewards,
		Total:             slotData.AttestationsRewards + slotData.ProposerSlashings + slotData.AttesterSlashings + slotData.SyncAggregateRewards,
	}).withFinalized(isFinalized), nil
}

type syncCommitteeReward struct {
	ValidatorIndex uint64 `json:"validator_index,string"`
	Reward         int64  `json:"reward,string"`
}

func (a *ApiHandler) getSyncCommitteesRewards(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
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
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	// parse json body request
	if len(jsonBytes) > 0 {
		if err := json.Unmarshal(jsonBytes, &req); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
		}
	}
	filterIndicies, err := parseQueryValidatorIndicies(tx, req)
	if err != nil {
		return nil, err
	}

	blockId, err := blockIdFromRequest(r)
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
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "block not found")
	}
	version := a.beaconChainCfg.GetCurrentStateVersion(blk.Block.Slot / a.beaconChainCfg.SlotsPerEpoch)
	if version < clparams.AltairVersion {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "sync committee rewards not available before Altair fork")
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
	if isFinalized {
		if !isCanonical {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "non-canonical finalized block not found")
		}
		epochData, err := state_accessors.ReadEpochData(tx, blk.Block.Slot)
		if err != nil {
			return nil, err
		}
		if epochData == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "could not read historical sync committee rewards, node may not be archive or it still processing historical states")
		}
		totalActiveBalance = epochData.TotalActiveBalance
		syncCommittee, err = state_accessors.ReadCurrentSyncCommittee(tx, a.beaconChainCfg.RoundSlotToSyncCommitteePeriod(blk.Block.Slot))
		if err != nil {
			return nil, err
		}
		if syncCommittee == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "could not read historical sync committee, node may not be archive or it still processing historical states")
		}
	} else {
		var ok bool
		syncCommittee, _, ok = a.forkchoiceStore.GetSyncCommittees(root)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "non-finalized sync committee not found")
		}
		totalActiveBalance, ok = a.forkchoiceStore.TotalActiveBalance(root)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "non-finalized total active balance not found")
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
	for _, idx := range filterIndicies {
		accumulatedRewards[idx] = 0
	}
	partecipantReward := int64(a.syncPartecipantReward(totalActiveBalance))

	for committeeIdx, v := range committee {
		idx, ok, err := state_accessors.ReadValidatorIndexByPublicKey(tx, v)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "sync committee public key not found")
		}
		if len(filterIndiciesSet) > 0 {
			if _, ok := filterIndiciesSet[idx]; !ok {
				continue
			}
		}
		if syncAggregate.IsSet(uint64(committeeIdx)) {
			accumulatedRewards[idx] += partecipantReward
			continue
		}
		accumulatedRewards[idx] -= partecipantReward
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
	return newBeaconResponse(rewards).withFinalized(isFinalized), nil
}

func (a *ApiHandler) syncPartecipantReward(activeBalance uint64) uint64 {
	activeBalanceSqrt := utils.IntegerSquareRoot(activeBalance)
	totalActiveIncrements := activeBalance / a.beaconChainCfg.EffectiveBalanceIncrement
	baseRewardPerInc := a.beaconChainCfg.EffectiveBalanceIncrement * a.beaconChainCfg.BaseRewardFactor / activeBalanceSqrt
	totalBaseRewards := baseRewardPerInc * totalActiveIncrements
	maxParticipantRewards := totalBaseRewards * a.beaconChainCfg.SyncRewardWeight / a.beaconChainCfg.WeightDenominator / a.beaconChainCfg.SlotsPerEpoch
	return maxParticipantRewards / a.beaconChainCfg.SyncCommitteeSize
}
