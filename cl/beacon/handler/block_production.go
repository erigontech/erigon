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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/builder"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
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

func (a *ApiHandler) waitForHeadSlot(slot uint64) {
	stopCh := time.After(time.Second)
	for {
		headSlot := a.syncedData.HeadSlot()
		if headSlot >= slot || a.slotWaitedForAttestationProduction.Contains(slot) {
			return
		}
		_, ok, err := a.attestationProducer.CachedAttestationData(slot)
		if err != nil {
			log.Warn("Failed to get attestation data", "err", err)
		}
		if ok {
			a.slotWaitedForAttestationProduction.Add(slot, struct{}{})
			return
		}

		time.Sleep(1 * time.Millisecond)
		select {
		case <-stopCh:
			a.slotWaitedForAttestationProduction.Add(slot, struct{}{})
			return
		default:
		}

	}
}

func (a *ApiHandler) GetEthV1ValidatorAttestationData(
	w http.ResponseWriter,
	r *http.Request,
) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	start := time.Now()

	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	committeeIndex, err := beaconhttp.Uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if slot == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusBadRequest,
			errors.New("slot is required"),
		)
	}
	if *slot > a.ethClock.GetCurrentSlot() {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("slot is in the future"))
	}

	a.waitForHeadSlot(*slot)

	attestationData, ok, err := a.attestationProducer.CachedAttestationData(*slot)
	if err != nil {
		log.Warn("Failed to get attestation data", "err", err)
	}

	defer func() {
		a.logger.Debug("Produced Attestation", "slot", *slot,
			"committee_index", *committeeIndex, "cached", ok, "beacon_block_root",
			attestationData.BeaconBlockRoot, "duration", time.Since(start))
	}()

	if ok {
		return newBeaconResponse(attestationData), nil
	}

	clversion := a.beaconChainCfg.GetCurrentStateVersion(*slot / a.beaconChainCfg.SlotsPerEpoch)
	if clversion.BeforeOrEqual(clparams.DenebVersion) && committeeIndex == nil {
		return nil, beaconhttp.NewEndpointError(
			http.StatusBadRequest,
			errors.New("committee_index is required for pre-Deneb versions"),
		)
	} else if clversion.AfterOrEqual(clparams.ElectraVersion) {
		// electra case
		zero := uint64(0)
		committeeIndex = &zero
	}

	if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		attestationData, err = a.attestationProducer.ProduceAndCacheAttestationData(
			tx,
			headState,
			a.syncedData.HeadRoot(),
			*slot,
		)

		if errors.Is(err, attestation_producer.ErrHeadStateBehind) {
			return beaconhttp.NewEndpointError(
				http.StatusServiceUnavailable,
				synced_data.ErrNotSynced,
			)
		} else if err != nil {
			return beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
		}
		return nil
	}); err != nil {
		return nil, err
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
	graffiti := common.HexToHash(r.URL.Query().Get("graffiti"))
	if !r.URL.Query().Has("graffiti") {
		graffiti = common.HexToHash(hex.EncodeToString([]byte(defaultGraffitiString)))
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

	log.Debug("[Beacon API] Producing block", "slot", targetSlot)
	// builder boost factor controls block choice between local execution node or builder
	builderBoostFactor := uint64(100)
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

	baseBlockRoot := a.syncedData.HeadRoot()
	if baseBlockRoot == (common.Hash{}) {
		return nil, beaconhttp.NewEndpointError(
			http.StatusServiceUnavailable,
			errors.New("node is syncing"),
		)
	}

	start := time.Now()
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

	var baseState *state.CachingBeaconState
	if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		baseState, err = headState.Copy()
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

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
	log.Info("[Beacon API] Found BeaconState object for block production", "slot", targetSlot, "duration", time.Since(start))
	block, err := a.produceBlock(ctx, builderBoostFactor, sourceBlock.Block, baseState, targetSlot, randaoReveal, graffiti)
	if err != nil {
		log.Warn("Failed to produce block", "err", err, "slot", targetSlot)
		return nil, err
	}

	startConsensusProcessing := time.Now()

	blockBuldingMachine := &eth2.Impl{}
	blockBuldingMachine.BlockRewardsCollector = &eth2.BlockRewardsCollector{}
	// do state transition
	if err := machine.ProcessBlock(blockBuldingMachine, baseState, block.ToGeneric()); err != nil {
		log.Warn("Failed to process execution block", "err", err, "slot", targetSlot)
		return nil, err
	}
	log.Info("[Beacon API] Built block consensus-state", "slot", targetSlot, "duration", time.Since(startConsensusProcessing))
	startConsensusProcessing = time.Now()
	block.StateRoot, err = baseState.HashSSZ()
	if err != nil {
		log.Warn("Failed to get state root", "err", err)
		return nil, err
	}
	log.Info("[Beacon API] Computed state root while producing slot", "slot", targetSlot, "duration", time.Since(startConsensusProcessing))

	log.Info("BlockProduction: Block produced",
		"proposerIndex", block.ProposerIndex,
		"slot", targetSlot,
		"state_root", block.StateRoot,
		"execution_value", block.GetExecutionValue().Uint64(),
		"version", block.Version(),
		"blinded", block.IsBlinded(),
		"took", time.Since(start),
	)

	// todo: consensusValue
	rewardsCollector := blockBuldingMachine.BlockRewardsCollector
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
		kzgProofs      []common.Bytes48
	)
	go func() {
		start := time.Now()
		defer func() {
			a.logger.Debug("Produced BeaconBody", "slot", targetSlot, "duration", time.Since(start))
		}()
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
				blobBundle, ok := a.blobBundles.Get(common.Bytes48(*c))
				if !ok {
					log.Warn("Blob not found", "slot", targetSlot, "commitment", c)
					continue
				}

				if len(blobBundle.KzgProofs) == 0 {
					log.Warn("Blob bundle has no KZG proofs", "slot", targetSlot, "commitment", c)
					continue
				}
				blobs = append(blobs, blobBundle.Blob)
				kzgProofs = append(kzgProofs, blobBundle.KzgProofs...)
			}
		}
	}()

	// get the builder payload
	var (
		builderHeader *builder.ExecutionHeader
		builderErr    error
	)
	go func() {
		start := time.Now()
		defer func() {
			a.logger.Debug("MevBoost", "slot", targetSlot, "duration", time.Since(start))
		}()
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
			SetBlobKzgCommitments(cpyCommitments).
			SetExecutionRequests(builderHeader.Data.Message.ExecutionRequests)
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
	if baseState.Version() >= clparams.DenebVersion && header.Data.Message.BlobKzgCommitments != nil {
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
	if baseState.Version() >= clparams.ElectraVersion && header.Data.Message.ExecutionRequests != nil {
		// check execution requests
		r := header.Data.Message.ExecutionRequests
		if r.Deposits != nil && r.Deposits.Len() > int(a.beaconChainCfg.MaxDepositRequestsPerPayload) {
			return nil, fmt.Errorf("too many deposit requests: %d", r.Deposits.Len())
		}
		if r.Withdrawals != nil && r.Withdrawals.Len() > int(a.beaconChainCfg.MaxWithdrawalRequestsPerPayload) {
			return nil, fmt.Errorf("too many withdrawal requests: %d", r.Withdrawals.Len())
		}
		if r.Consolidations != nil && r.Consolidations.Len() > int(a.beaconChainCfg.MaxConsolidationRequestsPerPayload) {
			return nil, fmt.Errorf("too many consolidation requests: %d", r.Consolidations.Len())
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
	beaconBody := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig, stateVersion)
	// Setup body.
	beaconBody.RandaoReveal = randaoReveal
	beaconBody.Graffiti = graffiti
	beaconBody.Version = stateVersion

	// Build execution payload
	latestExecutionPayload := baseState.LatestExecutionPayloadHeader()
	head := latestExecutionPayload.BlockHash
	finalizedHash := a.forkchoiceStore.GetEth1Hash(baseState.FinalizedCheckpoint().Root)
	if finalizedHash == (common.Hash{}) {
		finalizedHash = head // probably fuck up fcu for EL but not a big deal.
	}
	safeHash := a.forkchoiceStore.GetEth1Hash(baseState.CurrentJustifiedCheckpoint().Root)
	if safeHash == (common.Hash{}) {
		safeHash = head
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
		start := time.Now()
		defer func() {
			log.Info("BlockProduction: ForkChoiceUpdate&GetPayload took", "duration", time.Since(start))
		}()
		timeoutForBlockBuilding := 2 * time.Second // keep asking for 2 seconds for block
		retryTime := 10 * time.Millisecond
		secsDiff := (targetSlot - baseBlock.Slot) * a.beaconChainCfg.SecondsPerSlot
		feeRecipient, _ := a.validatorParams.GetFeeRecipient(proposerIndex)
		clWithdrawals, _ := state.ExpectedWithdrawals(
			baseState,
			targetSlot/a.beaconChainCfg.SlotsPerEpoch,
		)
		withdrawals := []*types.Withdrawal{}
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
			safeHash,
			head,
			&engine_types.PayloadAttributes{
				Timestamp:             hexutil.Uint64(latestExecutionPayload.Time + secsDiff),
				PrevRandao:            random,
				SuggestedFeeRecipient: feeRecipient,
				Withdrawals:           withdrawals,
				ParentBeaconBlockRoot: (*common.Hash)(&blockRoot),
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
				payload, bundles, requestsBundle, blockValue, err := a.engine.GetAssembledBlock(ctx, idBytes)
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

				if stateVersion.Before(clparams.FuluVersion) {
					if len(bundles.Blobs) != len(bundles.Proofs) ||
						len(bundles.Commitments) != len(bundles.Proofs) {
						log.Error("BlockProduction: Invalid bundle")
						return
					}
				} else {
					if len(bundles.Blobs) != len(bundles.Commitments) ||
						len(bundles.Proofs) != len(bundles.Blobs)*int(a.beaconChainCfg.NumberOfColumns) {
						log.Error("BlockProduction: Invalid peerdas bundle")
						return
					}
				}

				for i := range bundles.Blobs {
					if len(bundles.Commitments[i]) != length.Bytes48 {
						log.Error("BlockProduction: Invalid commitment length")
						return
					}
					if stateVersion.Before(clparams.FuluVersion) && len(bundles.Proofs[i]) != length.Bytes48 {
						log.Error("BlockProduction: Invalid commitment length")
						return
					}
					if len(bundles.Blobs[i]) != cltypes.BYTES_PER_BLOB {
						log.Error("BlockProduction: Invalid blob length")
						return
					}

					// TODO: after the hard fork, remove this legacy code
					if stateVersion.Before(clparams.FuluVersion) {
						// add the bundle to recently produced blobs
						a.blobBundles.Add(common.Bytes48(bundles.Commitments[i]), BlobBundle{
							Blob:       (*cltypes.Blob)(bundles.Blobs[i]),
							KzgProofs:  []common.Bytes48{common.Bytes48(bundles.Proofs[i])},
							Commitment: common.Bytes48(bundles.Commitments[i]),
						})
					} else {
						kzgProofs := make([]common.Bytes48, a.beaconChainCfg.NumberOfColumns)
						for j := uint64(0); j < a.beaconChainCfg.NumberOfColumns; j++ {
							kzgProofs[j] = common.Bytes48(bundles.Proofs[i*int(a.beaconChainCfg.NumberOfColumns)+int(j)])
						}
						// add the bundle to recently produced blobs
						a.blobBundles.Add(common.Bytes48(bundles.Commitments[i]), BlobBundle{
							Blob:       (*cltypes.Blob)(bundles.Blobs[i]),
							KzgProofs:  kzgProofs,
							Commitment: common.Bytes48(bundles.Commitments[i]),
						})
					}

					// Assemble the KZG commitments list
					var c cltypes.KZGCommitment
					copy(c[:], bundles.Commitments[i])
					beaconBody.BlobKzgCommitments.Append(&c)
				}

				// Add the requests bundle
				if requestsBundle != nil && requestsBundle.GetRequests() != nil {
					if len(requestsBundle.GetRequests()) > 0 {
						log.Info("BlockProduction: Received requests bundle", "len", len(requestsBundle.GetRequests()))
					}

					for _, request := range requestsBundle.GetRequests() {
						rType := request[0]
						requestData := request[1:]
						switch rType {
						case types.DepositRequestType:
							if beaconBody.ExecutionRequests.Deposits.Len() > 0 {
								log.Error("BlockProduction: Deposit request already exists")
							} else if err := beaconBody.ExecutionRequests.Deposits.DecodeSSZ(requestData, int(stateVersion)); err != nil {
								log.Error("BlockProduction: Failed to decode deposit request", "err", err)
							} else {
								log.Info("BlockProduction: Decoded deposit request", "len", beaconBody.ExecutionRequests.Deposits.Len())
							}
						case types.WithdrawalRequestType:

							if beaconBody.ExecutionRequests.Withdrawals.Len() > 0 {
								log.Error("BlockProduction: Withdrawal request already exists")
							} else if err := beaconBody.ExecutionRequests.Withdrawals.DecodeSSZ(requestData, int(stateVersion)); err != nil {
								log.Error("BlockProduction: Failed to decode withdrawal request", "err", err)
							} else {
								log.Info("BlockProduction: Decoded withdrawal request", "len", beaconBody.ExecutionRequests.Withdrawals.Len())
							}

						case types.ConsolidationRequestType:
							if beaconBody.ExecutionRequests.Consolidations.Len() > 0 {
								log.Error("BlockProduction: Consolidation request already exists")
							} else if err := beaconBody.ExecutionRequests.Consolidations.DecodeSSZ(requestData, int(stateVersion)); err != nil {
								log.Error("BlockProduction: Failed to decode consolidation request", "err", err)
							} else {
								log.Info("BlockProduction: Decoded consolidation request", "len", beaconBody.ExecutionRequests.Consolidations.Len())
							}
						}
					}
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
		start := time.Now()
		defer func() {
			log.Info("BlockProduction: GetSyncAggregate took", "duration", time.Since(start))
		}()
		beaconBody.SyncAggregate, err = a.syncMessagePool.GetSyncAggregate(targetSlot-1, blockRoot)
		if err != nil {
			log.Error("BlockProduction: Failed to get sync aggregate", "err", err)
		}
	}()
	// Process operations all in parallel with each other.
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()
		defer func() {
			log.Info("BlockProduction: GetBlockOperations&findBestAttestations took", "duration", time.Since(start))
		}()
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

	targetEpoch := targetSlot / a.beaconChainCfg.SlotsPerEpoch
	targetVersion := a.beaconChainCfg.GetCurrentStateVersion(targetEpoch)
	var maxAttesterSlashings uint64
	if targetVersion.BeforeOrEqual(clparams.DenebVersion) {
		maxAttesterSlashings = a.beaconChainCfg.MaxAttesterSlashings
	} else {
		maxAttesterSlashings = a.beaconChainCfg.MaxAttesterSlashingsElectra
	}

	attesterSlashings := solid.NewDynamicListSSZ[*cltypes.AttesterSlashing](int(maxAttesterSlashings))
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
		if attesterSlashings.Len() >= int(maxAttesterSlashings) {
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

	signedBlindedBlock := cltypes.NewSignedBlindedBeaconBlock(a.beaconChainCfg, version)
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
	blockPayload, blobsBundle, executionRequests, err := a.builderClient.SubmitBlindedBlocks(r.Context(), signedBlindedBlock)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	if signedBlindedBlock.Version().AfterOrEqual(clparams.FuluVersion) {
		log.Info("Successfully submitted blinded block", "block_num", signedBlindedBlock.Block.Body.ExecutionPayload.BlockNumber, "api_version", apiVersion)
		return newBeaconResponse(nil), nil
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

				// Finish KzGProofs and blob checks
				if blockPayload.Version() < clparams.FuluVersion {

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
			if version < clparams.FuluVersion {
				// add the bundle to recently produced blobs
				a.blobBundles.Add(common.Bytes48(blobsBundle.Commitments[i]), BlobBundle{
					Blob:       (*cltypes.Blob)(blobsBundle.Blobs[i]),
					KzgProofs:  []common.Bytes48{common.Bytes48(blobsBundle.Proofs[i])},
					Commitment: common.Bytes48(blobsBundle.Commitments[i]),
				})
			} else {
				kzgProofs := make([]common.Bytes48, a.beaconChainCfg.NumberOfColumns)
				for j := uint64(0); j < a.beaconChainCfg.NumberOfColumns; j++ {
					kzgProofs[j] = common.Bytes48(blobsBundle.Proofs[i*int(a.beaconChainCfg.NumberOfColumns)+int(j)])
				}
				// add the bundle to recently produced blobs
				a.blobBundles.Add(common.Bytes48(blobsBundle.Commitments[i]), BlobBundle{
					Blob:       (*cltypes.Blob)(blobsBundle.Blobs[i]),
					KzgProofs:  kzgProofs,
					Commitment: common.Bytes48(blobsBundle.Commitments[i]),
				})
			}
		}
	}

	if blockPayload.Version() >= clparams.ElectraVersion {
		signedBlock.Block.Body.ExecutionRequests = executionRequests
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
	block := cltypes.NewDenebSignedBeaconBlock(a.beaconChainCfg, version)
	if block == nil {
		return nil, errors.New("failed to create block")
	}
	// check content type
	switch r.Header.Get("Content-Type") {
	case "application/json":
		if err := json.NewDecoder(r.Body).Decode(block); err != nil {
			return nil, err
		}
		return block, nil
	case "application/octet-stream":
		octect, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if err := block.DecodeSSZ(octect, int(version)); err != nil {
			return nil, err
		}
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
	var columnsSidecars []*cltypes.DataColumnSidecar

	header := blk.SignedBeaconBlockHeader()

	if blk.Version() >= clparams.DenebVersion && blk.Version() < clparams.FuluVersion {
		for i := 0; i < blk.Block.Body.BlobKzgCommitments.Len(); i++ {
			blobSidecar := &cltypes.BlobSidecar{}
			commitment := blk.Block.Body.BlobKzgCommitments.Get(i)
			if commitment == nil {
				return fmt.Errorf("missing commitment %d", i)
			}
			bundle, has := a.blobBundles.Get(common.Bytes48(*commitment))
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
			blobSidecar.KzgProof = bundle.KzgProofs[0]
			blobSidecar.SignedBlockHeader = header
			blobSidecarSSZ, err := blobSidecar.EncodeSSZ(nil)
			if err != nil {
				return err
			}
			blobsSidecarsBytes = append(blobsSidecarsBytes, blobSidecarSSZ)
			blobsSidecars = append(blobsSidecars, blobSidecar)
		}
	}

	if blk.Version() >= clparams.FuluVersion && blk.Block.Body.BlobKzgCommitments.Len() > 0 {
		kzgCommitmentsCopy := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, length.Bytes48)
		for i := 0; i < blk.Block.Body.BlobKzgCommitments.Len(); i++ {
			kzgCommitmentsCopy.Append(blk.Block.Body.BlobKzgCommitments.Get(i))
		}

		// Assemble inclusion proof
		inclusionProofRaw, err := blk.Block.Body.KzgCommitmentsInclusionProof()
		if err != nil {
			return err
		}
		commitmentInclusionProof := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for i, h := range inclusionProofRaw {
			commitmentInclusionProof.Set(i, h)
		}

		cellsAndProofsPerBlob := make([]peerdasutils.CellsAndKZGProofs, 0, kzgCommitmentsCopy.Len())
		for i := 0; i < kzgCommitmentsCopy.Len(); i++ {
			commitment := kzgCommitmentsCopy.Get(i)
			bundle, has := a.blobBundles.Get(common.Bytes48(*commitment))
			if !has {
				return fmt.Errorf("missing blob bundle for commitment %x", commitment)
			}
			cells, err := das.ComputeCells(bundle.Blob)
			if err != nil {
				return err
			}

			cellsAndProof := peerdasutils.CellsAndKZGProofs{}
			for i := 0; i < len(cells); i++ {
				cellsAndProof.Blobs = append(cellsAndProof.Blobs, cells[i])
			}

			for j := 0; j < len(bundle.KzgProofs); j++ {
				cellsAndProof.Proofs = append(cellsAndProof.Proofs, cltypes.KZGProof(bundle.KzgProofs[j]))
			}
			cellsAndProofsPerBlob = append(cellsAndProofsPerBlob, cellsAndProof)
		}
		columnsSidecars, err = peerdasutils.GetDataColumnSidecars(header, kzgCommitmentsCopy, commitmentInclusionProof, cellsAndProofsPerBlob)
		if err != nil {
			return fmt.Errorf("failed to get data column sidecars: %w", err)
		}
	}

	go func() {
		if err := a.storeBlockAndBlobs(context.Background(), blk, blobsSidecars, columnsSidecars); err != nil {
			log.Error("BlockPublishing: Failed to store block and blobs", "err", err)
		}
	}()

	lenBlobs := 0
	if blk.Version() >= clparams.DenebVersion {
		lenBlobs = blk.Block.Body.BlobKzgCommitments.Len()
	}

	log.Info(
		"BlockPublishing: publishing block and blobs",
		"slot",
		blk.Block.Slot,
		"blobs",
		lenBlobs,
	)
	// Broadcast the block and its blobs
	if _, err := a.sentinel.PublishGossip(ctx, &sentinel.GossipData{
		Name: gossip.TopicNameBeaconBlock,
		Data: blkSSZ,
	}); err != nil {
		a.logger.Error("Failed to publish block", "err", err)
	}

	if blk.Version() < clparams.FuluVersion {
		for idx, blob := range blobsSidecarsBytes {
			idx64 := uint64(idx)
			if _, err := a.sentinel.PublishGossip(ctx, &sentinel.GossipData{
				Name:     gossip.TopicNamePrefixBlobSidecar,
				Data:     blob,
				SubnetId: &idx64,
			}); err != nil {
				a.logger.Error("Failed to publish blob sidecar", "err", err)
			}
		}
	}

	if blk.Version() >= clparams.FuluVersion && len(columnsSidecars) > 0 {
		for _, column := range columnsSidecars {
			columnSSZ, err := column.EncodeSSZ(nil)
			if err != nil {
				a.logger.Error("Failed to encode column sidecar", "err", err)
				continue
			}
			subnet := das.ComputeSubnetForDataColumnSidecar(column.Index)
			if _, err := a.sentinel.PublishGossip(ctx, &sentinel.GossipData{
				Name:     gossip.TopicNamePrefixDataColumnSidecar,
				Data:     columnSSZ,
				SubnetId: &subnet,
			}); err != nil {
				a.logger.Error("Failed to publish data column sidecar", "err", err)
			}
		}
	}
	return nil
}

func (a *ApiHandler) storeBlockAndBlobs(
	ctx context.Context,
	block *cltypes.SignedBeaconBlock,
	sidecars []*cltypes.BlobSidecar,
	columnSidecars []*cltypes.DataColumnSidecar,
) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	// TODO: write column sidecars if needed

	if block.Version() < clparams.FuluVersion {
		if err := a.blobStoage.WriteBlobSidecars(ctx, blockRoot, sidecars); err != nil {
			return err
		}
	}

	if err := a.indiciesDB.Update(ctx, func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteHighestFinalized(tx, a.forkchoiceStore.FinalizedSlot()); err != nil {
			return err
		}
		return beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, block, false)
	}); err != nil {
		return err
	}

	if err := a.forkchoiceStore.OnBlock(ctx, block, true, true, false); err != nil {
		return err
	}
	finalizedHash := a.forkchoiceStore.GetEth1Hash(a.forkchoiceStore.FinalizedCheckpoint().Root)
	safeHash := a.forkchoiceStore.GetEth1Hash(a.forkchoiceStore.JustifiedCheckpoint().Root)
	if _, err := a.engine.ForkChoiceUpdate(ctx, finalizedHash, safeHash, a.forkchoiceStore.GetEth1Hash(blockRoot), nil); err != nil {
		return err
	}
	headState, err := a.forkchoiceStore.GetStateAtBlockRoot(blockRoot, false)
	if err != nil {
		return err
	}
	if headState == nil {
		return errors.New("failed to get head state")
	}

	if err := a.indiciesDB.View(ctx, func(tx kv.Tx) error {
		_, err := a.attestationProducer.ProduceAndCacheAttestationData(tx, headState, blockRoot, block.Block.Slot)
		return err
	}); err != nil {
		return err
	}
	if err := a.syncedData.OnHeadState(headState); err != nil {
		return fmt.Errorf("failed to update synced data: %w", err)
	}

	return nil
}

type attestationCandidate struct {
	attestation *solid.Attestation
	reward      uint64
}

func (a *ApiHandler) electraMergedAttestationCandidates(s abstract.BeaconState) (map[common.Hash][]*solid.Attestation, error) {
	pool := map[common.Hash]map[uint64][]*solid.Attestation{} // map root -> committee -> att candidates
	// step 1: Group attestations by data root and committee index for merging
	// so after this step, pool[dataRoot][committeeIndex] will contain all the attestation candidates for that data root and committee index
	for _, candidate := range a.operationsPool.AttestationsPool.Raw() {
		if err := eth2.IsAttestationApplicable(s, candidate); err != nil {
			continue // attestation not applicable skip
		}

		attVersion := a.beaconChainCfg.GetCurrentStateVersion(candidate.Data.Slot / a.beaconChainCfg.SlotsPerEpoch)
		if attVersion.Before(clparams.ElectraVersion) {
			// Because the on chain Attestation container changes, attestations from the prior fork can’t be included
			// into post-electra blocks. Therefore the first block after the fork may have zero attestations.
			// see: https://eips.ethereum.org/EIPS/eip-7549#first-block-after-fork
			continue
		}

		dataRoot, err := candidate.Data.HashSSZ()
		if err != nil {
			log.Warn("cannot hash attestation data", "err", err)
			continue
		}
		if _, ok := pool[dataRoot]; !ok {
			pool[dataRoot] = make(map[uint64][]*solid.Attestation)
		}
		committeeBits := candidate.CommitteeBits.GetOnIndices()
		if len(committeeBits) != 1 {
			log.Warn("invalid candidate commitee bit length %v in attestation pool.", len(committeeBits))
			continue
		}
		candCommitteeBit := uint64(committeeBits[0])
		if _, ok := pool[dataRoot][candCommitteeBit]; !ok {
			pool[dataRoot][candCommitteeBit] = []*solid.Attestation{}
		}

		// try to merge the attestation with the existing ones
		var appendCandidate bool = true
		candAggrBits := candidate.AggregationBits.Bytes()
		for _, curAtt := range pool[dataRoot][candCommitteeBit] {
			currAggregationBitsBytes := curAtt.AggregationBits.Bytes()
			if utils.IsNonStrictSupersetBitlist(currAggregationBitsBytes, candAggrBits) {
				// skip the duplicate attestation
				appendCandidate = false
				continue
			}

			if !utils.IsOverlappingSSZBitlist(currAggregationBitsBytes, candAggrBits) {
				// merge signatures
				candidateSig := candidate.Signature
				curSig := curAtt.Signature
				mergeSig, err := bls.AggregateSignatures([][]byte{candidateSig[:], curSig[:]})
				if err != nil {
					log.Warn("[Block Production] Cannot merge signatures", "err", err)
					continue
				}
				// merge aggregation bits
				mergedAggBits, err := curAtt.AggregationBits.Merge(candidate.AggregationBits)
				if err != nil {
					log.Warn("[Block Production] Cannot merge aggregation bits", "err", err)
					continue
				}
				var buf [96]byte
				copy(buf[:], mergeSig)
				curAtt.Signature = buf
				curAtt.AggregationBits = mergedAggBits
				appendCandidate = false
			}
		}
		if appendCandidate {
			// no merge case, just append. It might be merged with other attestation later.
			pool[dataRoot][candCommitteeBit] = append(pool[dataRoot][candCommitteeBit], candidate.Copy())
		}
	}

	// step 2: sort each candidates list within (root, committee_bit) by number of set bits in aggregation_bits in descending order
	maxAttsPerDataRoot := map[common.Hash]int{}
	type candSort struct {
		att   *solid.Attestation
		count int
	}
	for root := range pool {
		maxAttsPerDataRoot[root] = 0
		for committee := range pool[root] {
			// Skip empty committee lists
			if len(pool[root][committee]) == 0 {
				continue
			}

			cands := make([]candSort, 0, len(pool[root][committee]))
			for _, att := range pool[root][committee] {
				if att == nil {
					continue
				}
				// sort cands by # of on bits
				cands = append(cands, candSort{
					att:   att,
					count: att.AggregationBits.Bits(),
				})
			}

			// Sort in descending order by bit count
			sort.SliceStable(cands, func(i, j int) bool {
				return cands[i].count > cands[j].count
			})

			// Create new slice with sorted attestations
			resultCands := make([]*solid.Attestation, 0, len(cands))
			for _, cand := range cands {
				resultCands = append(resultCands, cand.att)
			}
			pool[root][committee] = resultCands
			if len(resultCands) > maxAttsPerDataRoot[root] {
				maxAttsPerDataRoot[root] = len(resultCands)
			}
		}
	}

	// step 3: merge attestations from different committees within the same data root
	// Example:
	// For data root 0x123...
	// Committee 0: [Att1{bits: 1100}, Att2{bits: 0011}]
	// Committee 1: [Att3{bits: 1010}, Att4{bits: 0101}]
	// Committee 2: [Att5{bits: 1111}]
	//
	// First merge (index=0):
	// - Take Att1 from Committee 0
	// - Take Att3 from Committee 1
	// - Take Att5 from Committee 2
	// Result: Merged{
	//   committee_bits: 111 (committees 0,1,2 participated)
	//   aggregation_bits: 1100|1010|1111 (concatenated)
	//   signature: aggregate(sig1, sig3, sig5)
	// }
	//
	// Second merge (index=1):
	// - Take Att2 from Committee 0
	// - Take Att4 from Committee 1
	// - Committee 2 empty, skip
	// Result: Merged{
	//   committee_bits: 110 (committees 0,1 participated)
	//   aggregation_bits: 0011|0101
	//   signature: aggregate(sig2, sig4)
	// }

	mergeAttByCommittees := func(root common.Hash, index int) *solid.Attestation {
		signatures := [][]byte{}
		commiteeBits := solid.NewBitVector(int(a.beaconChainCfg.MaxCommitteesPerSlot))
		bitSlice := solid.NewBitSlice()
		var attData *solid.AttestationData
		for cIndex := uint64(0); cIndex < a.beaconChainCfg.MaxCommitteesPerSlot; cIndex++ {
			candidates, ok := pool[root][cIndex]
			if !ok {
				continue
			}
			if index >= len(candidates) {
				continue
			}
			att := candidates[index]
			if attData == nil {
				attData = att.Data
			}
			signatures = append(signatures, att.Signature[:])
			// set commitee bit
			commiteeBits.SetBitAt(int(cIndex), true)
			// append aggregation bits
			for i := 0; i < att.AggregationBits.Bits(); i++ {
				bitSlice.AppendBit(att.AggregationBits.GetBitAt(i))
			}
		}
		// aggregate signatures
		var buf [96]byte
		if len(signatures) == 0 {
			// no candidates to merge
			return nil
		} else if len(signatures) == 1 {
			copy(buf[:], signatures[0])
		} else {
			aggSig, err := bls.AggregateSignatures(signatures)
			if err != nil {
				log.Warn("Cannot aggregate signatures", "err", err)
				return nil
			}
			copy(buf[:], aggSig)
		}
		bitSlice.AppendBit(true) // set msb to 1
		att := &solid.Attestation{
			AggregationBits: solid.BitlistFromBytes(bitSlice.Bytes(), int(a.beaconChainCfg.MaxCommitteesPerSlot)*int(a.beaconChainCfg.MaxValidatorsPerCommittee)),
			Signature:       buf,
			Data:            attData,
			CommitteeBits:   commiteeBits,
		}
		return att
	}
	mergedCandidates := make(map[common.Hash][]*solid.Attestation)
	for root := range pool {
		mergedCandidates[root] = []*solid.Attestation{}
		maxAtts := min(maxAttsPerDataRoot[root], int(a.beaconChainCfg.MaxAttestations)) // limit the max attestations to the max attestations
		for i := 0; i < maxAtts; i++ {
			att := mergeAttByCommittees(root, i)
			if att == nil {
				// No more attestations to merge for this root at higher indices, so we can stop checking
				break
			}
			mergedCandidates[root] = append(mergedCandidates[root], att)
		}
	}

	return mergedCandidates, nil
}

func (a *ApiHandler) denebMergedAttestationCandidates(s abstract.BeaconState) (map[common.Hash][]*solid.Attestation, error) {
	// Group attestations by their data root
	hashToAtts := make(map[common.Hash][]*solid.Attestation)
	for _, candidate := range a.operationsPool.AttestationsPool.Raw() {
		if err := eth2.IsAttestationApplicable(s, candidate); err != nil {
			continue // attestation not applicable skip
		}

		attVersion := a.beaconChainCfg.GetCurrentStateVersion(candidate.Data.Slot / a.beaconChainCfg.SlotsPerEpoch)
		if attVersion >= clparams.ElectraVersion {
			continue
		}

		dataRoot, err := candidate.Data.HashSSZ()
		if err != nil {
			log.Warn("[Block Production] Cannot hash attestation data", "err", err)
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
			if !utils.IsOverlappingSSZBitlist(currAggregationBitsBytes, candidateAggregationBits) {
				// merge signatures
				candidateSig := candidate.Signature
				curSig := curAtt.Signature
				mergeSig, err := bls.AggregateSignatures([][]byte{candidateSig[:], curSig[:]})
				if err != nil {
					log.Warn("[Block Production] Cannot merge signatures", "err", err)
					continue
				}
				// merge aggregation bits
				mergedAggBits, err := curAtt.AggregationBits.Merge(candidate.AggregationBits)
				if err != nil {
					log.Warn("[Block Production] Cannot merge aggregation bits", "err", err)
					continue
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
	return hashToAtts, nil
}

func (a *ApiHandler) findBestAttestationsForBlockProduction(
	s abstract.BeaconState,
) *solid.ListSSZ[*solid.Attestation] {
	var hashToAtts map[common.Hash][]*solid.Attestation
	if s.Version() < clparams.ElectraVersion {
		var err error
		hashToAtts, err = a.denebMergedAttestationCandidates(s)
		if err != nil {
			log.Warn("[Block Production] Cannot merge deneb attestations", "err", err)
			return nil
		}
	} else {
		var err error
		hashToAtts, err = a.electraMergedAttestationCandidates(s)
		if err != nil {
			log.Warn("[Block Production] Cannot merge electra attestations", "err", err)
			return nil
		}
	}

	attestationCandidates := []attestationCandidate{}
	for _, atts := range hashToAtts {
		for _, att := range atts {
			expectedReward, err := computeAttestationReward(s, att)
			if err != nil {
				log.Debug("[Block Production] Could not compute expected attestation reward", "reason", err)
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

	// decide the max attestation length based on the version
	var maxAttLen int
	if s.Version().BeforeOrEqual(clparams.DenebVersion) {
		maxAttLen = int(a.beaconChainCfg.MaxAttestations)
	} else {
		maxAttLen = int(a.beaconChainCfg.MaxAttestationsElectra)
	}
	ret := solid.NewDynamicListSSZ[*solid.Attestation](maxAttLen)
	for _, candidate := range attestationCandidates {
		ret.Append(candidate.attestation)
		if ret.Len() >= maxAttLen {
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
	attestingIndicies, err := s.GetAttestingIndicies(attestation, true)
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
