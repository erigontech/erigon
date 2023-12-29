package handler

import (
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
)

type blockRewardsResponse struct {
	ProposerIndex     uint64 `json:"proposer_index"`
	Attestations      uint64 `json:"attestations"`
	ProposerSlashings uint64 `json:"proposer_slashings"`
	AttesterSlashings uint64 `json:"attester_slashings"`
	SyncAggregate     uint64 `json:"sync_aggregate"`
	Total             uint64 `json:"total"`
}

func (a *ApiHandler) getBlockRewards(r *http.Request) (*beaconResponse, error) {
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
	if slot >= a.forkchoiceStore.FinalizedSlot() {
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
		}).withFinalized(false), nil
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
	}).withFinalized(true), nil
}
