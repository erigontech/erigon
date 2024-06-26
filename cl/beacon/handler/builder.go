package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
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
	isOptimistic := a.forkchoiceStore.IsRootOptimistic(root)
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
		return newBeaconResponse(state.ExpectedWithdrawals(a.syncedData.HeadState(), state.Epoch(a.syncedData.HeadState()))).WithFinalized(false), nil
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
		return newBeaconResponse(blk.Block.Body.ExecutionPayload.Withdrawals).WithFinalized(false).WithOptimistic(isOptimistic), nil
	}

	return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found"))
}

func (a *ApiHandler) PostEthV1BuilderRegisterValidator(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	registerReq := []*cltypes.ValidatorRegistration{}
	if err := json.NewDecoder(r.Body).Decode(&registerReq); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if len(registerReq) == 0 {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("empty request"))
	}
	if err := a.builderClient.RegisterValidator(r.Context(), registerReq); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	for _, v := range registerReq {
		a.logger.Debug("[Caplin] Registred new validator", "fee_recipient", v.Message.FeeRecipient)
	}
	log.Info("Registered new validator", "count", len(registerReq))
	return newBeaconResponse(nil), nil
}
