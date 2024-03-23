package handler

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/log/v3"
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
	beaconBody, executionValue, err := a.produceBeaconBody(ctx, 3, sourceBlock.Block, baseState, targetSlot, randaoReveal, graffiti)
	if err != nil {
		return nil, err
	}

	proposerIndex, err := baseState.GetBeaconProposerIndexForSlot(targetSlot)
	if err != nil {
		return nil, err
	}
	rewardsCollector := &eth2.BlockRewardsCollector{}
	block := &cltypes.BeaconBlock{
		Slot:          targetSlot,
		ProposerIndex: proposerIndex,
		ParentRoot:    baseBlockRoot,
		Body:          beaconBody,
	}
	// compute the state root now
	if err := transition.TransitionState(baseState, &cltypes.SignedBeaconBlock{
		Block: block,
	}, rewardsCollector, false); err != nil {
		return nil, err
	}
	block.StateRoot, err = baseState.HashSSZ()
	if err != nil {
		return nil, err
	}
	consensusValue := rewardsCollector.Attestations + rewardsCollector.ProposerSlashings + rewardsCollector.AttesterSlashings + rewardsCollector.SyncAggregate
	isSSZBlinded := false
	return newBeaconResponse(beaconBody).
		With("execution_payload_blinded", isSSZBlinded).
		With("execution_payload_value", strconv.FormatUint(executionValue, 10)).
		With("consensus_block_value", strconv.FormatUint(consensusValue, 10)), nil
}

func (a *ApiHandler) produceBeaconBody(ctx context.Context, apiVersion int, baseBlock *cltypes.BeaconBlock, baseState *state.CachingBeaconState, targetSlot uint64, randaoReveal common.Bytes96, graffiti common.Hash) (*cltypes.BeaconBody, uint64, error) {
	if targetSlot <= baseBlock.Slot {
		return nil, 0, fmt.Errorf("target slot %d must be greater than base block slot %d", targetSlot, baseBlock.Slot)
	}
	var wg sync.WaitGroup
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(targetSlot / a.beaconChainCfg.SlotsPerEpoch)
	beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig)
	// Setup body.
	beaconBody.RandaoReveal = randaoReveal
	beaconBody.Graffiti = graffiti
	beaconBody.Version = stateVersion

	// Build execution payload
	latestExecutionPayload := baseState.LatestExecutionPayloadHeader()
	head := latestExecutionPayload.BlockHash
	finalizedHash := a.forkchoiceStore.GetEth1Hash(baseState.FinalizedCheckpoint().BlockRoot())
	if finalizedHash == (libcommon.Hash{}) {
		finalizedHash = head // probably fuck up fcu for EL but not a big deal.
	}
	proposerIndex, err := baseState.GetBeaconProposerIndexForSlot(targetSlot)
	if err != nil {
		return nil, 0, err
	}
	currEpoch := utils.GetCurrentEpoch(a.genesisCfg.GenesisTime, a.beaconChainCfg.SecondsPerSlot, a.beaconChainCfg.SlotsPerEpoch)
	random := baseState.GetRandaoMixes(currEpoch)

	var executionPayload *cltypes.Eth1Block
	var executionValue uint64

	// Process the execution data in a thread.
	wg.Add(1)
	go func() {
		defer wg.Done()
		timeoutForBlockBuilding := 2 * time.Second // keep asking for 2 seconds for block
		retryTime := 10 * time.Millisecond
		secsDiff := (targetSlot - baseBlock.Slot) * a.beaconChainCfg.SecondsPerSlot
		feeRecipient, _ := a.validatorParams.GetFeeRecipient(proposerIndex)
		var withdrawals []*types.Withdrawal
		clWithdrawals := state.ExpectedWithdrawals(baseState)
		for _, w := range clWithdrawals {
			withdrawals = append(withdrawals, &types.Withdrawal{
				Index:     w.Index,
				Amount:    w.Amount,
				Validator: w.Amount,
				Address:   w.Address,
			})
		}

		idBytes, err := a.engine.ForkChoiceUpdate(ctx, finalizedHash, head, &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(latestExecutionPayload.Time + secsDiff),
			PrevRandao:            random,
			SuggestedFeeRecipient: feeRecipient,
			Withdrawals:           withdrawals,
			ParentBeaconBlockRoot: &baseBlock.ParentRoot,
		})
		if err != nil {
			log.Error("BlockProduction: Failed to get payload id", "err", err)
			return
		}
		// Keep requesting block until it's ready
		stopTimer := time.NewTimer(timeoutForBlockBuilding)
		ticker := time.NewTicker(retryTime)
		defer stopTimer.Stop()
		defer ticker.Stop()
		for {
			select {
			case <-stopTimer.C:
				return
			case <-ticker.C:
				payload, bundles, blockValue, err := a.engine.GetAssembledBlock(ctx, idBytes)
				if err != nil {
					log.Error("BlockProduction: Failed to get payload", "err", err)
					continue
				}
				if payload == nil {
					continue
				}
				// Determine block value
				if blockValue == nil {
					executionValue = 0
				} else {
					executionValue = blockValue.Uint64()
				}

				if len(bundles.Blobs) != len(bundles.Proofs) || len(bundles.Commitments) != len(bundles.Proofs) {
					log.Error("BlockProduction: Invalid bundle")
					return
				}
				for i := range bundles.Blobs {
					if len(bundles.Commitments[i]) != length.Bytes48 {
						log.Error("BlockProduction: Invalid commitment length")
						return
					}
					if len(bundles.Proofs[i]) != length.Bytes48 {
						log.Error("BlockProduction: Invalid commitment length")
						return
					}
					if len(bundles.Blobs[i]) != int(cltypes.BYTES_PER_BLOB) {
						log.Error("BlockProduction: Invalid blob length")
						return
					}
					// add the bundle to recently produced blobs
					a.blobBundles.Add(libcommon.Bytes48(bundles.Commitments[i]), BlobBundle{
						Blob:       (*cltypes.Blob)(bundles.Blobs[i]),
						KzgProof:   libcommon.Bytes48(bundles.Proofs[i]),
						Commitment: libcommon.Bytes48(bundles.Commitments[i]),
					})
					// Assemble the KZG commitments list
					var c cltypes.KZGCommitment
					copy(c[:], bundles.Commitments[i])
					beaconBody.BlobKzgCommitments.Append((*cltypes.KZGCommitment)(&c))
				}

				executionPayload = cltypes.NewEth1BlockFromHeaderAndBody(payload.Header(), payload.RawBody(), a.beaconChainCfg)
				return
			}
		}
	}()
	wg.Wait()
	if executionPayload == nil {
		return nil, 0, fmt.Errorf("failed to produce execution payload")
	}
	beaconBody.ExecutionPayload = executionPayload
	return beaconBody, executionValue, nil
}
