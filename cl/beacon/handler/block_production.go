package handler

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/log/v3"
)

type BlockPublishingValidation string

const (
	BlockPublishingValidationGossip                   BlockPublishingValidation = "gossip"
	BlockPublishingValidationConsensus                BlockPublishingValidation = "consensus"
	BlockPublishingValidationConsensusAndEquivocation BlockPublishingValidation = "consensus_and_equivocation"
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
		graffiti = libcommon.HexToHash(hex.EncodeToString([]byte(defaultGraffitiString)))
	}

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	targetSlotStr := chi.URLParam(r, "slot")
	targetSlot, err := strconv.ParseUint(targetSlotStr, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid slot: %v", err))
	}

	s := a.syncedData.HeadState()
	if s == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("node is syncing"))
	}

	baseBlockRoot, err := s.BlockRoot()
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
	log.Info("BlockProduction: Computing HashSSZ block", "slot", targetSlot, "execution_value", executionValue, "proposerIndex", proposerIndex)

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
	a.setupHeaderReponseForBlockProduction(w, block.Version(), isSSZBlinded, executionValue, consensusValue)

	return newBeaconResponse(block).
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
	// Sync aggregate is empty for now.
	beaconBody.SyncAggregate = &cltypes.SyncAggregate{
		SyncCommiteeSignature: bls.InfiniteSignature,
	}

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

	blockRoot, err := baseBlock.HashSSZ()
	if err != nil {
		return nil, 0, err
	}
	// Process the execution data in a thread.
	wg.Add(1)
	go func() {
		defer wg.Done()
		timeoutForBlockBuilding := 2 * time.Second // keep asking for 2 seconds for block
		retryTime := 10 * time.Millisecond
		secsDiff := (targetSlot - baseBlock.Slot) * a.beaconChainCfg.SecondsPerSlot
		feeRecipient, _ := a.validatorParams.GetFeeRecipient(proposerIndex)
		var withdrawals []*types.Withdrawal
		clWithdrawals := state.ExpectedWithdrawals(baseState, targetSlot/a.beaconChainCfg.SlotsPerEpoch)
		for _, w := range clWithdrawals {
			withdrawals = append(withdrawals, &types.Withdrawal{
				Index:     w.Index,
				Amount:    w.Amount,
				Validator: w.Validator,
				Address:   w.Address,
			})
		}

		idBytes, err := a.engine.ForkChoiceUpdate(ctx, finalizedHash, head, &engine_types.PayloadAttributes{
			Timestamp:             hexutil.Uint64(latestExecutionPayload.Time + secsDiff),
			PrevRandao:            random,
			SuggestedFeeRecipient: feeRecipient,
			Withdrawals:           withdrawals,
			ParentBeaconBlockRoot: (*libcommon.Hash)(&blockRoot),
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
					beaconBody.BlobKzgCommitments.Append(&c)
				}
				executionPayload = cltypes.NewEth1Block(beaconBody.Version, a.beaconChainCfg)
				executionPayload.BlockHash = payload.BlockHash
				executionPayload.ParentHash = payload.ParentHash
				executionPayload.StateRoot = payload.StateRoot
				executionPayload.ReceiptsRoot = payload.ReceiptsRoot
				executionPayload.LogsBloom = payload.LogsBloom
				executionPayload.BlockNumber = payload.BlockNumber
				executionPayload.GasLimit = payload.GasLimit
				executionPayload.GasUsed = payload.GasUsed
				executionPayload.Time = payload.Time
				executionPayload.Extra = payload.Extra
				executionPayload.BlobGasUsed = payload.BlobGasUsed
				executionPayload.ExcessBlobGas = payload.ExcessBlobGas
				executionPayload.BaseFeePerGas = payload.BaseFeePerGas
				executionPayload.BlockHash = payload.BlockHash
				executionPayload.FeeRecipient = payload.FeeRecipient
				executionPayload.PrevRandao = payload.PrevRandao
				// Reset the limit of withdrawals
				executionPayload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(a.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
				payload.Withdrawals.Range(func(index int, value *cltypes.Withdrawal, length int) bool {
					executionPayload.Withdrawals.Append(value)
					return true
				})
				executionPayload.Transactions = payload.Transactions

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

func (a *ApiHandler) setupHeaderReponseForBlockProduction(w http.ResponseWriter, consensusVersion clparams.StateVersion, blinded bool, executionBlockValue, consensusBlockValue uint64) {
	w.Header().Set("Eth-Execution-Payload-Value", strconv.FormatUint(executionBlockValue, 10))
	w.Header().Set("Eth-Consensus-Block-Value", strconv.FormatUint(consensusBlockValue, 10))
	w.Header().Set("Eth-Consensus-Version", clparams.ClVersionToString(consensusVersion))
	w.Header().Set("Eth-Execution-Payload-Blinded", strconv.FormatBool(blinded))
}

func (a *ApiHandler) PostEthV1BeaconBlocks(w http.ResponseWriter, r *http.Request) {
	a.postBeaconBlocks(w, r, 1)
}

func (a *ApiHandler) PostEthV2BeaconBlocks(w http.ResponseWriter, r *http.Request) {
	a.postBeaconBlocks(w, r, 2)
}

func (a *ApiHandler) postBeaconBlocks(w http.ResponseWriter, r *http.Request, apiVersion int) {
	ctx := r.Context()
	version, err := a.parseEthConsensusVersion(r.Header.Get("Eth-Consensus-Version"), apiVersion)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	validation := a.parseBlockPublishingValidation(w, r, apiVersion)
	// Decode the block
	block, err := a.parseRequestBeaconBlock(version, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = validation

	if err := a.broadcastBlock(ctx, block); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)

}

func (a *ApiHandler) parseEthConsensusVersion(str string, apiVersion int) (clparams.StateVersion, error) {
	if str == "" && apiVersion == 2 {
		return 0, fmt.Errorf("Eth-Consensus-Version header is required")
	}
	if str == "" && apiVersion == 1 {
		currentEpoch := utils.GetCurrentEpoch(a.genesisCfg.GenesisTime, a.beaconChainCfg.SecondsPerSlot, a.beaconChainCfg.SlotsPerEpoch)
		return a.beaconChainCfg.GetCurrentStateVersion(currentEpoch), nil
	}
	return clparams.StringToClVersion(str)
}

func (a *ApiHandler) parseBlockPublishingValidation(w http.ResponseWriter, r *http.Request, apiVersion int) BlockPublishingValidation {
	str := r.URL.Query().Get("broadcast_validation")
	if apiVersion == 1 || str == string(BlockPublishingValidationGossip) {
		return BlockPublishingValidationGossip
	}
	// fall to consensus anyway. equivocation is not supported yet.
	return BlockPublishingValidationConsensus
}

func (a *ApiHandler) parseRequestBeaconBlock(version clparams.StateVersion, r *http.Request) (*cltypes.SignedBeaconBlock, error) {
	block := cltypes.NewSignedBeaconBlock(a.beaconChainCfg)
	block.Block.Body.Version = version
	// check content type
	if r.Header.Get("Content-Type") == "application/json" {
		return block, json.NewDecoder(r.Body).Decode(block)
	}
	octect, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if err := block.DecodeSSZ(octect, int(version)); err != nil {
		return nil, err
	}
	return block, nil
}

func (a *ApiHandler) broadcastBlock(ctx context.Context, blk *cltypes.SignedBeaconBlock) error {
	blkSSZ, err := blk.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	blobsSidecarsBytes := make([][]byte, 0, blk.Block.Body.BlobKzgCommitments.Len())
	blobsSidecars := make([]*cltypes.BlobSidecar, 0, blk.Block.Body.BlobKzgCommitments.Len())

	header := blk.SignedBeaconBlockHeader()

	if blk.Version() >= clparams.DenebVersion {
		for i := 0; i < blk.Block.Body.BlobKzgCommitments.Len(); i++ {
			blobSidecar := &cltypes.BlobSidecar{}
			commitment := blk.Block.Body.BlobKzgCommitments.Get(i)
			if commitment == nil {
				return fmt.Errorf("missing commitment %d", i)
			}
			bundle, has := a.blobBundles.Get(libcommon.Bytes48(*commitment))
			if !has {
				return fmt.Errorf("missing blob bundle for commitment %x", commitment)
			}
			// Assemble inclusion proof
			inclusionProofRaw, err := blk.Block.Body.KzgCommitmentMerkleProof(i)
			if err != nil {
				return err
			}
			blobSidecar.CommitmentInclusionProof = solid.NewHashVector(cltypes.CommitmentBranchSize)
			for i, h := range inclusionProofRaw {
				blobSidecar.CommitmentInclusionProof.Set(i, h)
			}
			blobSidecar.Index = uint64(i)
			blobSidecar.Blob = *bundle.Blob
			blobSidecar.KzgCommitment = bundle.Commitment
			blobSidecar.KzgProof = bundle.KzgProof
			blobSidecar.SignedBlockHeader = header
			blobSidecarSSZ, err := blobSidecar.EncodeSSZ(nil)
			if err != nil {
				return err
			}
			blobsSidecarsBytes = append(blobsSidecarsBytes, blobSidecarSSZ)
			blobsSidecars = append(blobsSidecars, blobSidecar)
		}
	}
	go func() {
		if err := a.storeBlockAndBlobs(context.Background(), blk, blobsSidecars); err != nil {
			log.Error("BlockPublishing: Failed to store block and blobs", "err", err)
		}
	}()

	log.Info("BlockPublishing: publishing block and blobs", "slot", blk.Block.Slot, "blobs", len(blobsSidecars))
	// Broadcast the block and its blobs
	if _, err := a.sentinel.PublishGossip(ctx, &sentinel.GossipData{
		Name: gossip.TopicNameBeaconBlock,
		Data: blkSSZ,
	}); err != nil {
		log.Error("Failed to publish block", "err", err)
		return err
	}
	for idx, blob := range blobsSidecarsBytes {
		idx64 := uint64(idx)
		if _, err := a.sentinel.PublishGossip(ctx, &sentinel.GossipData{
			Name:     gossip.TopicNamePrefixBlobSidecar,
			Data:     blob,
			SubnetId: &idx64,
		}); err != nil {
			log.Error("Failed to publish blob sidecar", "err", err)
			return err
		}
	}
	return nil
}

func (a *ApiHandler) storeBlockAndBlobs(ctx context.Context, block *cltypes.SignedBeaconBlock, sidecars []*cltypes.BlobSidecar) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	if err := a.blobStoage.WriteBlobSidecars(ctx, blockRoot, sidecars); err != nil {
		return err
	}
	if err := a.indiciesDB.Update(ctx, func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteHighestFinalized(tx, a.forkchoiceStore.FinalizedSlot()); err != nil {
			return err
		}
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
	}); err != nil {
		return err
	}

	return a.forkchoiceStore.OnBlock(ctx, block, true, false, false)
}
