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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Giulio2002/bls"
	"github.com/go-chi/chi/v5"
	"golang.org/x/exp/slices"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/builder"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/engineapi/engine_types"
)

type BlockPublishingValidation string

const (
	BlockPublishingValidationGossip                   BlockPublishingValidation = "gossip"
	BlockPublishingValidationConsensus                BlockPublishingValidation = "consensus"
	BlockPublishingValidationConsensusAndEquivocation BlockPublishingValidation = "consensus_and_equivocation"
)

var (
	errBuilderNotEnabled = errors.New("builder is not enabled")
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
			errors.New("slot and committee_index url params are required"),
		)
	}
	headState := a.syncedData.HeadState()
	if headState == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusServiceUnavailable,
			errors.New("beacon node is still syncing"),
		)
	}

	attestationData, err := a.attestationProducer.ProduceAndCacheAttestationData(
		headState,
		*slot,
		*committeeIndex,
	)
	if err == attestation_producer.ErrHeadStateBehind {
		return nil, beaconhttp.NewEndpointError(
			http.StatusServiceUnavailable,
			errors.New("beacon node is still syncing"),
		)
	} else if err != nil {
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

	// builder boost factor controls block choice between local execution node or builder
	var builderBoostFactor uint64
	builderBoostFactorStr := r.URL.Query().Get("builder_boost_factor")
	if builderBoostFactorStr != "" {
		builderBoostFactor, err = strconv.ParseUint(builderBoostFactorStr, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(
				http.StatusBadRequest,
				fmt.Errorf("invalid builder_boost_factor: %v", err),
			)
		}
	}

	s := a.syncedData.HeadState()
	if s == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusServiceUnavailable,
			errors.New("node is syncing"),
		)
	}

	baseBlockRoot, err := s.BlockRoot()
	if err != nil {
		log.Warn("Failed to get block root", "err", err)
		return nil, err
	}

	sourceBlock, err := a.blockReader.ReadBlockByRoot(ctx, tx, baseBlockRoot)
	if err != nil {
		log.Warn("Failed to get source block", "err", err, "root", baseBlockRoot)
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
	block, err := a.produceBlock(ctx, builderBoostFactor, sourceBlock.Block, baseState, targetSlot, randaoReveal, graffiti)
	if err != nil {
		log.Warn("Failed to produce block", "err", err, "slot", targetSlot)
		return nil, err
	}

	// do state transition
	if err := machine.ProcessBlock(transition.DefaultMachine, baseState, block.ToGeneric()); err != nil {
		log.Warn("Failed to process execution block", "err", err, "slot", targetSlot)
		return nil, err
	}
	block.StateRoot, err = baseState.HashSSZ()
	if err != nil {
		log.Warn("Failed to get state root", "err", err)
		return nil, err
	}

	log.Info("BlockProduction: Block produced",
		"proposerIndex", block.ProposerIndex,
		"slot", targetSlot,
		"state_root", block.StateRoot,
		"execution_value", block.GetExecutionValue().Uint64(),
		"version", block.Version(),
		"blinded", block.IsBlinded(),
	)

	// todo: consensusValue
	rewardsCollector := &eth2.BlockRewardsCollector{}
	consensusValue := rewardsCollector.Attestations + rewardsCollector.ProposerSlashings + rewardsCollector.AttesterSlashings + rewardsCollector.SyncAggregate
	a.setupHeaderReponseForBlockProduction(
		w,
		block.Version(),
		block.IsBlinded(),
		block.GetExecutionValue().Uint64(),
		consensusValue,
	)

	var resp *beaconhttp.BeaconResponse
	if block.IsBlinded() {
		resp = newBeaconResponse(block.ToBlinded())
	} else {
		resp = newBeaconResponse(block.ToExecution())
	}
	return resp.WithVersion(block.Version()).With("execution_payload_blinded", block.IsBlinded()).
		With("execution_payload_value", strconv.FormatUint(block.GetExecutionValue().Uint64(), 10)).
		With("consensus_block_value", strconv.FormatUint(consensusValue, 10)), nil
}

func (a *ApiHandler) produceBlock(
	ctx context.Context,
	boostFactor uint64,
	baseBlock *cltypes.BeaconBlock,
	baseState *state.CachingBeaconState,
	targetSlot uint64,
	randaoReveal common.Bytes96,
	graffiti common.Hash,
) (*cltypes.BlindOrExecutionBeaconBlock, error) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	// produce beacon body
	var (
		beaconBody     *cltypes.BeaconBody
		localExecValue uint64
		localErr       error
		blobs          []*cltypes.Blob
		kzgProofs      []libcommon.Bytes48
	)
	go func() {
		defer wg.Done()
		beaconBody, localExecValue, localErr = a.produceBeaconBody(ctx, 3, baseBlock, baseState, targetSlot, randaoReveal, graffiti)
		// collect blobs
		if beaconBody != nil {
			for i := 0; i < beaconBody.BlobKzgCommitments.Len(); i++ {
				c := beaconBody.BlobKzgCommitments.Get(i)
				if c == nil {
					log.Warn("Nil commitment", "slot", targetSlot, "index", i)
					continue
				}
				blobBundle, ok := a.blobBundles.Get(libcommon.Bytes48(*c))
				if !ok {
					log.Warn("Blob not found", "slot", targetSlot, "commitment", c)
					continue
				}
				blobs = append(blobs, blobBundle.Blob)
				kzgProofs = append(kzgProofs, blobBundle.KzgProof)
			}
		}
	}()

	// get the builder payload
	var (
		builderHeader *builder.ExecutionHeader
		builderErr    error
	)
	go func() {
		defer wg.Done()
		if a.routerCfg.Builder && a.builderClient != nil {
			builderHeader, builderErr = a.getBuilderPayload(ctx, baseBlock, baseState, targetSlot)
			if builderErr != nil && builderErr != errBuilderNotEnabled {
				log.Warn("Failed to get builder payload", "err", builderErr)
			}
		}
	}()
	// wait for both tasks to finish
	wg.Wait()

	if localErr != nil {
		// if we failed to locally produce the beacon body, we should not proceed with the block production
		log.Error("Failed to produce beacon body", "err", localErr, "slot", targetSlot)
		return nil, localErr
	}
	// prepare basic block
	proposerIndex, err := baseState.GetBeaconProposerIndex()
	if err != nil {
		return nil, err
	}
	baseBlockRoot, err := baseBlock.HashSSZ()
	if err != nil {
		return nil, err
	}
	block := &cltypes.BlindOrExecutionBeaconBlock{
		Slot:          targetSlot,
		ProposerIndex: proposerIndex,
		ParentRoot:    baseBlockRoot,
		Cfg:           a.beaconChainCfg,
	}
	if !a.routerCfg.Builder || builderErr != nil {
		// directly return the block if:
		// 1. builder is not enabled
		// 2. failed to get builder payload
		block.BeaconBody = beaconBody
		block.Blobs = blobs
		block.KzgProofs = kzgProofs
		block.ExecutionValue = new(big.Int).SetUint64(localExecValue)
		return block, nil
	}

	// determine whether to use local execution node or builder
	// if exec_node_payload_value >= builder_boost_factor * (builder_payload_value // 100), then return a full (unblinded) block containing the execution node payload.
	// otherwise, return a blinded block containing the builder payload header.
	execValue := new(big.Int).SetUint64(localExecValue)
	builderValue := builderHeader.BlockValue()
	boostFactorBig := new(big.Int).SetUint64(boostFactor)
	useLocalExec := new(big.Int).Mul(execValue, big.NewInt(100)).Cmp(new(big.Int).Mul(builderValue, boostFactorBig)) >= 0
	log.Info("Check mev bid", "useLocalExec", useLocalExec, "execValue", execValue, "builderValue", builderValue, "boostFactor", boostFactor, "targetSlot", targetSlot)

	if useLocalExec {
		block.BeaconBody = beaconBody
		block.Blobs = blobs
		block.KzgProofs = kzgProofs
		block.ExecutionValue = execValue
	} else {
		// prepare blinded block
		blindedBody, err := beaconBody.Blinded()
		if err != nil {
			return nil, err
		}
		// cpy commitments
		cpyCommitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
		for i := 0; i < builderHeader.Data.Message.BlobKzgCommitments.Len(); i++ {
			c := builderHeader.Data.Message.BlobKzgCommitments.Get(i)
			cpy := cltypes.KZGCommitment{}
			copy(cpy[:], c[:])
			cpyCommitments.Append(&cpy)
		}
		// setup blinded block
		block.BlindedBeaconBody = blindedBody.
			SetHeader(builderHeader.Data.Message.Header).
			SetBlobKzgCommitments(cpyCommitments)
		block.ExecutionValue = builderValue
	}
	return block, nil
}

func (a *ApiHandler) getBuilderPayload(
	ctx context.Context,
	baseBlock *cltypes.BeaconBlock,
	baseState *state.CachingBeaconState,
	targetSlot uint64,
) (*builder.ExecutionHeader, error) {
	if !a.routerCfg.Builder || a.builderClient == nil {
		return nil, errBuilderNotEnabled
	}

	proposerIndex, err := baseState.GetBeaconProposerIndexForSlot(targetSlot)
	if err != nil {
		return nil, err
	}
	// pub key of the proposer
	pubKey, err := baseState.ValidatorPublicKey(int(proposerIndex))
	if err != nil {
		return nil, err
	}
	// get the parent hash of base execution block
	parentHash := baseBlock.Body.ExecutionPayload.BlockHash
	header, err := a.builderClient.GetHeader(ctx, int64(targetSlot), parentHash, pubKey)
	if err != nil {
		return nil, err
	} else if header == nil {
		return nil, errors.New("no error but nil header")
	}

	// check the version
	curVersion := baseState.Version().String()
	if !strings.EqualFold(header.Version, curVersion) {
		return nil, fmt.Errorf("invalid version %s, expected %s", header.Version, curVersion)
	}
	if ethHeader := header.Data.Message.Header; ethHeader != nil {
		ethHeader.SetVersion(baseState.Version())
	}
	// check kzg commitments
	if header != nil && baseState.Version() >= clparams.DenebVersion {
		if header.Data.Message.BlobKzgCommitments.Len() >= cltypes.MaxBlobsCommittmentsPerBlock {
			return nil, fmt.Errorf("too many blob kzg commitments: %d", header.Data.Message.BlobKzgCommitments.Len())
		}
		for i := 0; i < header.Data.Message.BlobKzgCommitments.Len(); i++ {
			c := header.Data.Message.BlobKzgCommitments.Get(i)
			if c == nil {
				return nil, errors.New("nil blob kzg commitment")
			}
			if len(c) != length.Bytes48 {
				return nil, errors.New("invalid blob kzg commitment length")
			}
		}
	}

	return header, nil
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
	finalizedHash := a.forkchoiceStore.GetEth1Hash(baseState.FinalizedCheckpoint().Root)
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
					if len(bundles.Blobs[i]) != cltypes.BYTES_PER_BLOB {
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
		return nil, 0, errors.New("failed to produce execution payload")
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

func (a *ApiHandler) PostEthV1BeaconBlocks(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	resp, err := a.postBeaconBlocks(w, r, 1)
	if err != nil {
		log.Warn("Failed to post beacon block in v1 path", "err", err)
	}
	return resp, err
}

func (a *ApiHandler) PostEthV2BeaconBlocks(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	resp, err := a.postBeaconBlocks(w, r, 2)
	if err != nil {
		log.Warn("Failed to post beacon block in v2 path", "err", err)
	}
	return resp, err
}

func (a *ApiHandler) postBeaconBlocks(w http.ResponseWriter, r *http.Request, apiVersion int) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()
	version, err := a.parseEthConsensusVersion(r.Header.Get("Eth-Consensus-Version"), apiVersion)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	validation := a.parseBlockPublishingValidation(w, r, apiVersion)
	// Decode the block
	block, err := a.parseRequestBeaconBlock(version, r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	_ = validation

	if err := a.broadcastBlock(ctx, block.SignedBlock); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	return newBeaconResponse(nil), nil
}

func (a *ApiHandler) PostEthV1BlindedBlocks(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	resp, err := a.publishBlindedBlocks(w, r, 1)
	if err != nil {
		log.Warn("Failed to publish blinded block in v1 path", "err", err)
	}
	return resp, err
}

func (a *ApiHandler) PostEthV2BlindedBlocks(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	resp, err := a.publishBlindedBlocks(w, r, 2)
	if err != nil {
		log.Warn("Failed to publish blinded block in v2 path", "err", err)
	}
	return resp, err
}

func (a *ApiHandler) publishBlindedBlocks(w http.ResponseWriter, r *http.Request, apiVersion int) (*beaconhttp.BeaconResponse, error) {
	ethVersion := r.Header.Get("Eth-Consensus-Version")
	version, err := a.parseEthConsensusVersion(ethVersion, apiVersion)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	// todo: broadcast_validation

	signedBlindedBlock := cltypes.NewSignedBlindedBeaconBlock(a.beaconChainCfg)
	signedBlindedBlock.Block.SetVersion(version)
	b, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if r.Header.Get("Content-Type") == "application/json" {
		if err := json.Unmarshal(b, signedBlindedBlock); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
	} else {
		if err := signedBlindedBlock.DecodeSSZ(b, int(version)); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
	}
	// submit and unblind the signedBlindedBlock
	blockPayload, blobsBundle, err := a.builderClient.SubmitBlindedBlocks(r.Context(), signedBlindedBlock)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	signedBlock, err := signedBlindedBlock.Unblind(blockPayload)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	// check blob bundle
	if blobsBundle != nil && blockPayload.Version() >= clparams.DenebVersion {
		err := func(b *engine_types.BlobsBundleV1) error {
			// check the length of the blobs bundle
			if len(b.Commitments) != len(b.Proofs) || len(b.Commitments) != len(b.Blobs) {
				return errors.New("commitments, proofs and blobs must have the same length")
			}
			for i := range b.Commitments {
				// check the length of each blob
				if len(b.Commitments[i]) != length.Bytes48 {
					return errors.New("commitment must be 48 bytes long")
				}
				if len(b.Proofs[i]) != length.Bytes48 {
					return errors.New("proof must be 48 bytes long")
				}
				if len(b.Blobs[i]) != 4096*32 {
					return errors.New("blob must be 4096 * 32 bytes long")
				}
			}
			return nil
		}(blobsBundle)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
		// check commitments
		blockCommitments := signedBlindedBlock.Block.Body.BlobKzgCommitments
		if len(blobsBundle.Commitments) != blockCommitments.Len() {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("commitments length mismatch"))
		}
		for i := range blobsBundle.Commitments {
			// add the bundle to recently produced blobs
			a.blobBundles.Add(libcommon.Bytes48(blobsBundle.Commitments[i]), BlobBundle{
				Blob:       (*cltypes.Blob)(blobsBundle.Blobs[i]),
				KzgProof:   libcommon.Bytes48(blobsBundle.Proofs[i]),
				Commitment: libcommon.Bytes48(blobsBundle.Commitments[i]),
			})
		}
	}

	// broadcast the block
	if err := a.broadcastBlock(r.Context(), signedBlock); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}

	log.Info("successfully publish blinded block", "block_num", signedBlock.Block.Body.ExecutionPayload.BlockNumber, "api_version", apiVersion)
	return newBeaconResponse(nil), nil
}

func (a *ApiHandler) parseEthConsensusVersion(
	str string,
	apiVersion int,
) (clparams.StateVersion, error) {
	if str == "" && apiVersion == 2 {
		return 0, errors.New("Eth-Consensus-Version header is required")
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
) (*cltypes.DenebSignedBeaconBlock, error) {
	block := cltypes.NewDenebSignedBeaconBlock(a.beaconChainCfg)
	// check content type
	switch r.Header.Get("Content-Type") {
	case "application/json":
		if err := json.NewDecoder(r.Body).Decode(block); err != nil {
			return nil, err
		}
		block.SignedBlock.Block.SetVersion(version)
		return block, nil
	case "application/octet-stream":
		octect, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if err := block.DecodeSSZ(octect, int(version)); err != nil {
			return nil, err
		}
		block.SignedBlock.Block.SetVersion(version)
		return block, nil
	}
	return nil, errors.New("invalid content type")
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

	if err := a.forkchoiceStore.OnBlock(ctx, block, true, false, false); err != nil {
		return err
	}
	finalizedBlockRoot := a.forkchoiceStore.FinalizedCheckpoint().Root
	if _, err := a.engine.ForkChoiceUpdate(ctx, a.forkchoiceStore.GetEth1Hash(finalizedBlockRoot), a.forkchoiceStore.GetEth1Hash(blockRoot), nil); err != nil {
		return err
	}
	return nil
}

type attestationCandidate struct {
	attestation *solid.Attestation
	reward      uint64
}

func (a *ApiHandler) findBestAttestationsForBlockProduction(
	s abstract.BeaconState,
) *solid.ListSSZ[*solid.Attestation] {
	// Group attestations by their data root
	hashToAtts := make(map[libcommon.Hash][]*solid.Attestation)
	for _, candidate := range a.operationsPool.AttestationsPool.Raw() {
		if err := eth2.IsAttestationApplicable(s, candidate); err != nil {
			continue // attestation not applicable skip
		}
		dataRoot, err := candidate.Data.HashSSZ()
		if err != nil {
			continue
		}

		if _, ok := hashToAtts[dataRoot]; !ok {
			hashToAtts[dataRoot] = []*solid.Attestation{}
		}

		// try to merge the attestation with the existing ones
		mergeAny := false
		candidateAggregationBits := candidate.AggregationBits.Bytes()
		for _, curAtt := range hashToAtts[dataRoot] {
			currAggregationBitsBytes := curAtt.AggregationBits.Bytes()
			if !utils.IsOverlappingBitlist(currAggregationBitsBytes, candidateAggregationBits) {
				// merge signatures
				candidateSig := candidate.Signature
				curSig := curAtt.Signature
				mergeSig, err := bls.AggregateSignatures([][]byte{candidateSig[:], curSig[:]})
				if err != nil {
					log.Warn("[Block Production] Cannot merge signatures", "err", err)
					continue
				}
				// merge aggregation bits
				mergedAggBits := solid.NewBitList(0, int(a.beaconChainCfg.MaxValidatorsPerCommittee))
				for i := 0; i < len(currAggregationBitsBytes); i++ {
					mergedAggBits.Append(currAggregationBitsBytes[i] | candidateAggregationBits[i])
				}
				var buf [96]byte
				copy(buf[:], mergeSig)
				curAtt.Signature = buf
				curAtt.AggregationBits = mergedAggBits
				mergeAny = true
			}
		}
		if !mergeAny {
			// no merge case, just append. It might be merged with other attestation later.
			hashToAtts[dataRoot] = append(hashToAtts[dataRoot], candidate)
		}
	}

	attestationCandidates := []attestationCandidate{}
	for _, atts := range hashToAtts {
		for _, att := range atts {
			expectedReward, err := computeAttestationReward(s, att)
			if err != nil {
				log.Warn("[Block Production] Could not compute expected attestation reward", "reason", err)
				continue
			}
			if expectedReward == 0 {
				continue
			}
			attestationCandidates = append(attestationCandidates, attestationCandidate{
				attestation: att,
				reward:      expectedReward,
			})
		}
	}
	sort.Slice(attestationCandidates, func(i, j int) bool {
		return attestationCandidates[i].reward > attestationCandidates[j].reward
	})
	ret := solid.NewDynamicListSSZ[*solid.Attestation](int(a.beaconChainCfg.MaxAttestations))
	for _, candidate := range attestationCandidates {
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
	data := attestation.Data
	currentEpoch := state.Epoch(s)
	stateSlot := s.Slot()
	beaconConfig := s.BeaconConfig()

	participationFlagsIndicies, err := s.GetAttestationParticipationFlagIndicies(
		data,
		stateSlot-data.Slot,
		false,
	)
	if err != nil {
		return 0, err
	}
	attestingIndicies, err := s.GetAttestingIndicies(data, attestation.AggregationBits.Bytes(), true)
	if err != nil {
		return 0, err
	}
	var proposerRewardNumerator uint64

	isCurrentEpoch := data.Target.Epoch == currentEpoch

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
