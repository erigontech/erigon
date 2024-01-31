package handler

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (a *ApiHandler) GetEthV1BeaconLightClientBootstrap(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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

	bootstrap, ok := a.forkchoiceStore.GetLightClientBootstrap(root)
	if !ok {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("bootstrap object evicted"))
	}
	return newBeaconResponse(bootstrap).WithVersion(bootstrap.Header.Version()), nil
}

func (a *ApiHandler) GetEthV1BeaconLightClientOptimisticUpdate(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	update := a.forkchoiceStore.NewestLightClientUpdate()
	if update == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no optimistic update loaded yet, try again later. it may take a few minutes for it to load."))
	}
	version := update.AttestedHeader.Version()
	return newBeaconResponse(&cltypes.LightClientOptimisticUpdate{
		AttestedHeader: update.AttestedHeader,
		SyncAggregate:  update.SyncAggregate,
		SignatureSlot:  update.SignatureSlot,
	}).WithVersion(version), nil
}

func (a *ApiHandler) GetEthV1BeaconLightClientFinalityUpdate(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	update := a.forkchoiceStore.NewestLightClientUpdate()
	if update == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no finility update loaded yet, try again later. it may take a few minutes for it to load."))
	}
	version := update.AttestedHeader.Version()
	return newBeaconResponse(&cltypes.LightClientFinalityUpdate{
		AttestedHeader:  update.AttestedHeader,
		FinalizedHeader: update.FinalizedHeader,
		FinalityBranch:  update.FinalityBranch,
		SyncAggregate:   update.SyncAggregate,
		SignatureSlot:   update.SignatureSlot,
	}).WithVersion(version), nil
}
