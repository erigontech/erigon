package handler

import (
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func (a *ApiHandler) GetEth1V1BuilderStatesExpectedWithdrawals(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	root, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found"))
	}
	if a.beaconChainCfg.GetCurrentStateVersion(*slot/a.beaconChainCfg.SlotsPerEpoch) < clparams.CapellaVersion {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("the specified state is not a capella state"))
	}
	headRoot, _, err := a.forkchoiceStore.GetHead()
	if err != nil {
		return nil, err
	}
	if a.syncedData.Syncing() {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("beacon node is syncing"))
	}
	if root == headRoot {
		s, cn := a.syncedData.HeadState()
		defer cn()
		return newBeaconResponse(state.ExpectedWithdrawals(s)).WithFinalized(false), nil
	}
	lookAhead := 1024
	for currSlot := *slot + 1; currSlot < *slot+uint64(lookAhead); currSlot++ {
		if currSlot > a.syncedData.HeadSlot() {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found"))
		}
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, currSlot)
		if err != nil {
			return nil, err
		}
		if blockRoot == (libcommon.Hash{}) {
			continue
		}
		blk, err := a.blockReader.ReadBlockByRoot(ctx, tx, blockRoot)
		if err != nil {
			return nil, err
		}
		return newBeaconResponse(blk.Block.Body.ExecutionPayload.Withdrawals).WithFinalized(false), nil
	}

	return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found"))
}
