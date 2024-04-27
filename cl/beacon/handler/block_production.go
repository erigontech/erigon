package handler

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

type BlockPublishingValidation string

const (
	BlockPublishingValidationGossip                   BlockPublishingValidation = "gossip"
	BlockPublishingValidationConsensus                BlockPublishingValidation = "consensus"
	BlockPublishingValidationConsensusAndEquivocation BlockPublishingValidation = "consensus_and_equivocation"
)

var defaultGraffitiString = "Caplin"

func (a *ApiHandler) GetEthV1ValidatorAttestationData(
	w http.ResponseWriter,
	r *http.Request,
) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	committeeIndex, err := beaconhttp.Uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if slot == nil || committeeIndex == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusBadRequest,
			fmt.Errorf("slot and committee_index url params are required"),
		)
	}
	headState := a.syncedData.HeadState()
	if headState == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusServiceUnavailable,
			fmt.Errorf("beacon node is still syncing"),
		)
	}

	attestationData, err := a.attestationProducer.ProduceAndCacheAttestationData(
		headState,
		*slot,
		*committeeIndex,
	)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	return newBeaconResponse(attestationData), nil
}

func (a *ApiHandler) GetEthV3ValidatorBlock(
	w http.ResponseWriter,
	r *http.Request,
) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	// parse request data

	randaoRevealString := r.URL.Query().Get("randao_reveal")
	var randaoReveal common.Bytes96
	if err := randaoReveal.UnmarshalText([]byte(randaoRevealString)); err != nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusBadRequest,
			fmt.Errorf("invalid randao_reveal: %v", err),
		)
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
		return nil, beaconhttp.NewEndpointError(
			http.StatusBadRequest,
			fmt.Errorf("invalid slot: %v", err),
		)
	}

	s := a.syncedData.HeadState()
	if s == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusServiceUnavailable,
			fmt.Errorf("node is syncing"),
		)
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
		return nil, beaconhttp.NewEndpointError(
			http.StatusNotFound,
			fmt.Errorf("block not found %x", baseBlockRoot),
		)
	}
	baseState, err := a.forkchoiceStore.GetStateAtBlockRoot(
		baseBlockRoot,
		true,
	) // we start the block production from this state
	if err != nil {
		return nil, err
	}
	if baseState == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusNotFound,
			fmt.Errorf("state not found %x", baseBlockRoot),
		)
	}
	if err := transition.DefaultMachine.ProcessSlots(baseState, targetSlot); err != nil {
		return nil, err
	}

	beaconBody, executionValue, err := a.produceBeaconBody(
		ctx,
		3,
		sourceBlock.Block,
		baseState,
		targetSlot,
		randaoReveal,
		graffiti,
	)
	if err != nil {
		return nil, err
	}

	proposerIndex, err := baseState.GetBeaconProposerIndex()
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
	log.Info(
		"BlockProduction: Computing HashSSZ block",
		"slot",
		targetSlot,
		"execution_value",
		executionValue,
		"proposerIndex",
		proposerIndex,
	)

	// compute the state root now
	if err := machine.ProcessBlock(transition.DefaultMachine, baseState, &cltypes.SignedBeaconBlock{Block: block}); err != nil {
		return nil, err
	}
	block.StateRoot, err = baseState.HashSSZ()
	if err != nil {
		return nil, err
	}
	consensusValue := rewardsCollector.Attestations + rewardsCollector.ProposerSlashings + rewardsCollector.AttesterSlashings + rewardsCollector.SyncAggregate
	isSSZBlinded := false
	a.setupHeaderReponseForBlockProduction(
		w,
		block.Version(),
		isSSZBlinded,
		executionValue,
		consensusValue,
	)

	return newBeaconResponse(block).
		With("execution_payload_blinded", isSSZBlinded).
		With("execution_payload_value", strconv.FormatUint(executionValue, 10)).
		With("consensus_block_value", strconv.FormatUint(consensusValue, 10)), nil
}

func (a *ApiHandler) produceBeaconBody(
	ctx context.Context,
	apiVersion int,
	baseBlock *cltypes.BeaconBlock,
	baseState *state.CachingBeaconState,
	targetSlot uint64,
	randaoReveal common.Bytes96,
	graffiti common.Hash,
) (*cltypes.BeaconBody, uint64, error) {
	if targetSlot <= baseBlock.Slot {
		return nil, 0, fmt.Errorf(
			"target slot %d must be greater than base block slot %d",
			targetSlot,
			baseBlock.Slot,
		)
	}
	var wg sync.WaitGroup
	stateVersion := a.beaconChainCfg.GetCurrentStateVersion(
		targetSlot / a.beaconChainCfg.SlotsPerEpoch,
	)
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
	currEpoch := a.ethClock.GetCurrentEpoch()
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
		clWithdrawals := state.ExpectedWithdrawals(
			baseState,
			targetSlot/a.beaconChainCfg.SlotsPerEpoch,
		)
		for _, w := range clWithdrawals {
			withdrawals = append(withdrawals, &types.Withdrawal{
				Index:     w.Index,
				Amount:    w.Amount,
				Validator: w.Validator,
				Address:   w.Address,
			})
		}

		idBytes, err := a.engine.ForkChoiceUpdate(
			ctx,
			finalizedHash,
			head,
			&engine_types.PayloadAttributes{
				Timestamp:             hexutil.Uint64(latestExecutionPayload.Time + secsDiff),
				PrevRandao:            random,
				SuggestedFeeRecipient: feeRecipient,
				Withdrawals:           withdrawals,
				ParentBeaconBlockRoot: (*libcommon.Hash)(&blockRoot),
			},
		)
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

				if len(bundles.Blobs) != len(bundles.Proofs) ||
					len(bundles.Commitments) != len(bundles.Proofs) {
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
				// Setup executionPayload
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
				executionPayload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](
					int(a.beaconChainCfg.MaxWithdrawalsPerPayload),
					44,
				)
				payload.Withdrawals.Range(
					func(index int, value *cltypes.Withdrawal, length int) bool {
						executionPayload.Withdrawals.Append(value)
						return true
					},
				)
				executionPayload.Transactions = payload.Transactions

				return
			}
		}
	}()
	// process the sync aggregate in parallel
	wg.Add(1)
	go func() {
		defer wg.Done()
		beaconBody.SyncAggregate, err = a.syncMessagePool.GetSyncAggregate(targetSlot-1, blockRoot)
		if err != nil {
			log.Error("BlockProduction: Failed to get sync aggregate", "err", err)
		}
	}()
	// Process operations all in parallel with each other.
	wg.Add(1)
	go func() {
		defer wg.Done()
		beaconBody.AttesterSlashings, beaconBody.ProposerSlashings, beaconBody.VoluntaryExits, beaconBody.ExecutionChanges = a.getBlockOperations(
			baseState,
			targetSlot,
		)
		beaconBody.Attestations = a.findBestAttestationsForBlockProduction(baseState)
	}()

	wg.Wait()
	if executionPayload == nil {
		return nil, 0, fmt.Errorf("failed to produce execution payload")
	}
	beaconBody.ExecutionPayload = executionPayload
	return beaconBody, executionValue, nil
}

func (a *ApiHandler) getBlockOperations(s *state.CachingBeaconState, targetSlot uint64) (
	*solid.ListSSZ[*cltypes.AttesterSlashing],
	*solid.ListSSZ[*cltypes.ProposerSlashing],
	*solid.ListSSZ[*cltypes.SignedVoluntaryExit],
	*solid.ListSSZ[*cltypes.SignedBLSToExecutionChange]) {

	attesterSlashings := solid.NewDynamicListSSZ[*cltypes.AttesterSlashing](
		int(a.beaconChainCfg.MaxAttesterSlashings),
	)
	slashedIndicies := []uint64{}
	// AttesterSlashings
AttLoop:
	for _, slashing := range a.operationsPool.AttesterSlashingsPool.Raw() {
		idxs := slashing.Attestation_1.AttestingIndices
		rawIdxs := []uint64{}
		for i := 0; i < idxs.Length(); i++ {
			currentValidatorIndex := idxs.Get(i)
			if slices.Contains(slashedIndicies, currentValidatorIndex) || slices.Contains(rawIdxs, currentValidatorIndex) {
				continue AttLoop
			}
			v := s.ValidatorSet().Get(int(currentValidatorIndex))
			if !v.IsSlashable(targetSlot / a.beaconChainCfg.SlotsPerEpoch) {
				continue AttLoop
			}
			rawIdxs = append(rawIdxs, currentValidatorIndex)
		}
		slashedIndicies = append(slashedIndicies, rawIdxs...)
		attesterSlashings.Append(slashing)
		if attesterSlashings.Len() >= int(a.beaconChainCfg.MaxAttesterSlashings) {
			break
		}
	}
	// ProposerSlashings
	proposerSlashings := solid.NewStaticListSSZ[*cltypes.ProposerSlashing](
		int(a.beaconChainCfg.MaxProposerSlashings),
		416,
	)
	for _, slashing := range a.operationsPool.ProposerSlashingsPool.Raw() {
		proposerIndex := slashing.Header1.Header.ProposerIndex
		if slices.Contains(slashedIndicies, proposerIndex) {
			continue
		}
		v := s.ValidatorSet().Get(int(proposerIndex))
		if !v.IsSlashable(targetSlot / a.beaconChainCfg.SlotsPerEpoch) {
			continue
		}
		proposerSlashings.Append(slashing)
		slashedIndicies = append(slashedIndicies, proposerIndex)
		if proposerSlashings.Len() >= int(a.beaconChainCfg.MaxProposerSlashings) {
			break
		}
	}
	// Voluntary Exits
	voluntaryExits := solid.NewStaticListSSZ[*cltypes.SignedVoluntaryExit](
		int(a.beaconChainCfg.MaxVoluntaryExits),
		112,
	)
	for _, exit := range a.operationsPool.VoluntaryExitsPool.Raw() {
		if slices.Contains(slashedIndicies, exit.VoluntaryExit.ValidatorIndex) {
			continue
		}
		if err := eth2.IsVoluntaryExitApplicable(s, exit.VoluntaryExit); err != nil {
			continue // Not applicable right now, skip.
		}
		voluntaryExits.Append(exit)
		slashedIndicies = append(slashedIndicies, exit.VoluntaryExit.ValidatorIndex)
		if voluntaryExits.Len() >= int(a.beaconChainCfg.MaxVoluntaryExits) {
			break
		}
	}
	// BLS Executions Changes
	blsToExecutionChanges := solid.NewStaticListSSZ[*cltypes.SignedBLSToExecutionChange](
		int(a.beaconChainCfg.MaxBlsToExecutionChanges),
		172,
	)
	for _, blsExecutionChange := range a.operationsPool.BLSToExecutionChangesPool.Raw() {
		if slices.Contains(slashedIndicies, blsExecutionChange.Message.ValidatorIndex) {
			continue
		}
		if blsExecutionChange.Message.ValidatorIndex >= uint64(s.ValidatorLength()) {
			continue
		}
		wc := s.ValidatorSet().
			Get(int(blsExecutionChange.Message.ValidatorIndex)).
			WithdrawalCredentials()
		// Check the validator's withdrawal credentials prefix.
		if wc[0] != byte(a.beaconChainCfg.ETH1AddressWithdrawalPrefixByte) {
			continue
		}

		// Check the validator's withdrawal credentials against the provided message.
		hashedFrom := utils.Sha256(blsExecutionChange.Message.From[:])
		if !bytes.Equal(hashedFrom[1:], wc[1:]) {
			continue
		}
		blsToExecutionChanges.Append(blsExecutionChange)
		slashedIndicies = append(slashedIndicies, blsExecutionChange.Message.ValidatorIndex)
	}
	return attesterSlashings, proposerSlashings, voluntaryExits, blsToExecutionChanges
}

func (a *ApiHandler) setupHeaderReponseForBlockProduction(
	w http.ResponseWriter,
	consensusVersion clparams.StateVersion,
	blinded bool,
	executionBlockValue, consensusBlockValue uint64,
) {
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

func (a *ApiHandler) parseEthConsensusVersion(
	str string,
	apiVersion int,
) (clparams.StateVersion, error) {
	if str == "" && apiVersion == 2 {
		return 0, fmt.Errorf("Eth-Consensus-Version header is required")
	}
	if str == "" && apiVersion == 1 {
		currentEpoch := a.ethClock.GetCurrentEpoch()
		return a.beaconChainCfg.GetCurrentStateVersion(currentEpoch), nil
	}
	return clparams.StringToClVersion(str)
}

func (a *ApiHandler) parseBlockPublishingValidation(
	w http.ResponseWriter,
	r *http.Request,
	apiVersion int,
) BlockPublishingValidation {
	str := r.URL.Query().Get("broadcast_validation")
	if apiVersion == 1 || str == string(BlockPublishingValidationGossip) {
		return BlockPublishingValidationGossip
	}
	// fall to consensus anyway. equivocation is not supported yet.
	return BlockPublishingValidationConsensus
}

func (a *ApiHandler) parseRequestBeaconBlock(
	version clparams.StateVersion,
	r *http.Request,
) (*cltypes.SignedBeaconBlock, error) {
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

	log.Info(
		"BlockPublishing: publishing block and blobs",
		"slot",
		blk.Block.Slot,
		"blobs",
		len(blobsSidecars),
	)
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

func (a *ApiHandler) storeBlockAndBlobs(
	ctx context.Context,
	block *cltypes.SignedBeaconBlock,
	sidecars []*cltypes.BlobSidecar,
) error {
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

type attestationCandidate struct {
	attestation *solid.Attestation
	reward      uint64
}

func (a *ApiHandler) findBestAttestationsForBlockProduction(
	s abstract.BeaconState,
) *solid.ListSSZ[*solid.Attestation] {

	ret := solid.NewDynamicListSSZ[*solid.Attestation](int(a.beaconChainCfg.MaxAttestations))
	attestationCandidates := []attestationCandidate{}

	for _, attestation := range a.operationsPool.AttestationsPool.Raw() {
		if err := eth2.IsAttestationApplicable(s, attestation); err != nil {
			continue // attestation not applicable skip
		}
		expectedReward, err := computeAttestationReward(s, attestation)
		if err != nil {
			log.Warn(
				"[Block Production] Could not compute expected attestation reward",
				"reason",
				err,
			)
			continue
		}
		if expectedReward == 0 {
			continue
		}
		attestationCandidates = append(attestationCandidates, attestationCandidate{
			attestation: attestation,
			reward:      expectedReward,
		})
	}
	// Rank by reward in descending order.
	sort.Slice(attestationCandidates, func(i, j int) bool {
		return attestationCandidates[i].reward > attestationCandidates[j].reward
	})
	// Some aggregates can be supersets of existing ones so let's filter out the supersets
	// this MAP is HashTreeRoot(AttestationData) => AggregationBits
	aggregationBitsByAttestationData := make(map[libcommon.Hash][]byte)
	for _, candidate := range attestationCandidates {
		// Check if it is a superset of a pre-included attestation with higher reward
		attestationDataRoot, err := candidate.attestation.AttestantionData().HashSSZ()
		if err != nil {
			log.Warn("[Block Production] Cannot compute attestation data root", "err", err)
			continue
		}
		currAggregationBits, exists := aggregationBitsByAttestationData[attestationDataRoot]
		if exists {
			if utils.IsNonStrictSupersetBitlist(
				currAggregationBits,
				candidate.attestation.AggregationBits(),
			) {
				continue
			}
			utils.MergeBitlists(currAggregationBits, candidate.attestation.AggregationBits())
		} else {
			currAggregationBits = candidate.attestation.AggregationBits()
		}
		// Update the currently built superset
		aggregationBitsByAttestationData[attestationDataRoot] = currAggregationBits

		ret.Append(candidate.attestation)
		if ret.Len() >= int(a.beaconChainCfg.MaxAttestations) {
			break
		}
	}
	return ret
}

// computeAttestationReward computes the reward for a specific attestation.
func computeAttestationReward(
	s abstract.BeaconState,
	attestation *solid.Attestation) (uint64, error) {

	baseRewardPerIncrement := s.BaseRewardPerIncrement()
	data := attestation.AttestantionData()
	currentEpoch := state.Epoch(s)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()

	participationFlagsIndicies, err := s.GetAttestationParticipationFlagIndicies(
		data,
		stateSlot-data.Slot(),
		false,
	)
	if err != nil {
		return 0, err
	}
	attestingIndicies, err := s.GetAttestingIndicies(data, attestation.AggregationBits(), true)
	if err != nil {
		return 0, err
	}
	var proposerRewardNumerator uint64

	isCurrentEpoch := data.Target().Epoch() == currentEpoch

	for _, attesterIndex := range attestingIndicies {
		val, err := s.ValidatorEffectiveBalance(int(attesterIndex))
		if err != nil {
			return 0, err
		}

		baseReward := (val / beaconConfig.EffectiveBalanceIncrement) * baseRewardPerIncrement
		for flagIndex, weight := range beaconConfig.ParticipationWeights() {
			flagParticipation := s.EpochParticipationForValidatorIndex(
				isCurrentEpoch,
				int(attesterIndex),
			)
			if !slices.Contains(participationFlagsIndicies, uint8(flagIndex)) ||
				flagParticipation.HasFlag(flagIndex) {
				continue
			}
			proposerRewardNumerator += baseReward * weight
		}
	}
	proposerRewardDenominator := (beaconConfig.WeightDenominator - beaconConfig.ProposerWeight) * beaconConfig.WeightDenominator / beaconConfig.ProposerWeight
	reward := proposerRewardNumerator / proposerRewardDenominator
	return reward, nil
}
