package handler

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
)

var defaultGraffitiString = "Caplin"

func (a *ApiHandler) GetEthV1ValidatorAttestationData(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	committeeIndex, err := beaconhttp.Uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if slot == nil || committeeIndex == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("slot and committee_index url params are required"))
	}
	headState := a.syncedData.HeadState()
	if headState == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("beacon node is still syncing"))
	}

	attestationData, err := a.attestationProducer.ProduceAndCacheAttestationData(headState, *slot, *committeeIndex)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	return newBeaconResponse(attestationData), nil
}

func (a *ApiHandler) GetEthV3ValidatorBlock(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	// parse request data

	targetSlotOptional, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	randaoRevealString := r.URL.Query().Get("randao_reveal")
	var randaoReveal common.Bytes96
	if err := randaoReveal.UnmarshalText([]byte(randaoRevealString)); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid randao_reveal: %v", err))
	}
	if r.URL.Query().Has("skip_randao_verification") {
		randaoReveal = common.Bytes96{0xc0} // infinity bls signature
	}
	graffiti := libcommon.HexToHash(r.URL.Query().Get("graffiti"))
	if !r.URL.Query().Has("graffiti") {
		graffiti = libcommon.HexToHash(defaultGraffitiString)
	}
	// TODO: implement mev boost.
	// _, err = beaconhttp.Uint64FromQueryParams(r, "builder_boost_factor")
	// if err != nil {
	// 	return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	// }
	if targetSlotOptional == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("slot is required"))
	}
	targetSlot := *targetSlotOptional
	_ = targetSlot
	_ = graffiti

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, err
	}
	baseBlockRoot, err := a.rootFromBlockId(ctx, tx, blockId)
	if err != nil {
		return nil, err
	}

	sourceBlock, err := a.blockReader.ReadBlockByRoot(ctx, tx, baseBlockRoot)
	if err != nil {
		return nil, err
	}
	if sourceBlock == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("block not found %x", baseBlockRoot))
	}
	baseState, err := a.forkchoiceStore.GetStateAtBlockRoot(baseBlockRoot, true) // we start the block production from this state
	if err != nil {
		return nil, err
	}
	if baseState == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found %x", baseBlockRoot))
	}
	return nil, nil
}

func (a *ApiHandler) produceBeaconBody(ctx context.Context, apiVersion int, baseBlock *cltypes.BeaconBlock, state *state.CachingBeaconState, targetSlot uint64, randaoReveal common.Bytes96, graffiti common.Hash) (*cltypes.BeaconBody, error) {
	if targetSlot <= baseBlock.Slot {
		return nil, fmt.Errorf("target slot %d must be greater than base block slot %d", targetSlot, baseBlock.Slot)
	}
	var wg sync.WaitGroup
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig)
	// Set all of this stuff to empty (fixme: Giulio2002 with more advanced staking logic)
	beaconBody.Eth1Data = &cltypes.Eth1Data{}
	beaconBody.RandaoReveal = randaoReveal
	beaconBody.Graffiti = graffiti
	beaconBody.Version = stateVersion
	beaconBody.ProposerSlashings = solid.NewStaticListSSZ[*cltypes.ProposerSlashing](int(a.beaconChainCfg.MaxProposerSlashings), 416)
	beaconBody.SyncAggregate = &cltypes.SyncAggregate{
		SyncCommiteeSignature: bls.InfiniteSignature,
	}
	beaconBody.AttesterSlashings = solid.NewDynamicListSSZ[*cltypes.AttesterSlashing](int(a.beaconChainCfg.MaxAttesterSlashings))
	beaconBody.Attestations = solid.NewDynamicListSSZ[*solid.Attestation](cltypes.MaxAttestations)
	beaconBody.Deposits = solid.NewDynamicListSSZ[*cltypes.Deposit](cltypes.MaxDeposits)
	beaconBody.VoluntaryExits = solid.NewDynamicListSSZ[*cltypes.SignedVoluntaryExit](cltypes.MaxVoluntaryExits)
	// Build execution payload
	latestExecutionPayload := state.LatestExecutionPayloadHeader()
	head := latestExecutionPayload.BlockHash
	finalizedHash := a.forkchoiceStore.GetEth1Hash(state.FinalizedCheckpoint().BlockRoot())
	if finalizedHash == (libcommon.Hash{}) {
		finalizedHash = head // probably fuck up fcu for EL but not a big deal.
	}
	proposerIndex, err := state.GetBeaconProposerIndexForSlot(targetSlot)
	if err != nil {
		return nil, err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		timeoutForBlockBuilding := 2 * time.Second // keep asking for 2 seconds for block
		retryTime := 10 * time.Millisecond
		secsDiff := (targetSlot - baseBlock.Slot) * a.beaconChainCfg.SecondsPerSlot
		feeRecipient, _ := a.validatorParams.GetFeeRecipient(proposerIndex)
		a.engine.ForkChoiceUpdate(ctx, finalizedHash, head, &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(latestExecutionPayload.Time + secsDiff),
			PrevRandao:            libcommon.Hash(randaoReveal), // fucking forgot about this shit
			SuggestedFeeRecipient: feeRecipient,
		})
	}()
	wg.Wait()

}
