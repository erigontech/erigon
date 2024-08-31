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

package services

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Giulio2002/bls"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
)

const (
	BatchSignatureVerificationThreshold = 100
)

type aggregateJob struct {
	aggregate    *cltypes.SignedAggregateAndProof
	creationTime time.Time
}

type AggregateVerificationData struct {
	Signatures [][]byte
	SignRoots  [][]byte
	Pks        [][]byte
	F          []func()
}

type aggregateAndProofServiceImpl struct {
	syncedDataManager *synced_data.SyncedDataManager
	forkchoiceStore   forkchoice.ForkChoiceStorage
	beaconCfg         *clparams.BeaconChainConfig
	opPool            pool.OperationsPool
	test              bool
	verifyAndExecute  chan *AggregateVerificationData

	// set of aggregates that are scheduled for later processing
	aggregatesScheduledForLaterExecution sync.Map
}

func NewAggregateAndProofService(
	ctx context.Context,
	syncedDataManager *synced_data.SyncedDataManager,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	beaconCfg *clparams.BeaconChainConfig,
	opPool pool.OperationsPool,
	test bool,
) AggregateAndProofService {
	a := &aggregateAndProofServiceImpl{
		syncedDataManager: syncedDataManager,
		forkchoiceStore:   forkchoiceStore,
		beaconCfg:         beaconCfg,
		opPool:            opPool,
		test:              test,
		verifyAndExecute:  make(chan *AggregateVerificationData, 128),
	}
	go a.loop(ctx)
	go a.startBatchSignatureVerification()
	return a
}

// When receiving SignedAggregateAndProof, we simply collect all the signature verification data and verify them together - running all the final functions afterwards
func (a *aggregateAndProofServiceImpl) startBatchSignatureVerification() {
	batchCheckInterval := 3 * time.Second
	ticker := time.NewTicker(batchCheckInterval)
	signatures, signRoots, pks, fns :=
		make([][]byte, 0, 128),
		make([][]byte, 0, 128),
		make([][]byte, 0, 128),
		make([]func(), 0, 128)
	for {
		select {
		case verification := <-a.verifyAndExecute:
			signatures, signRoots, pks, fns =
				append(signatures, verification.Signatures...),
				append(signRoots, verification.SignRoots...),
				append(pks, verification.Pks...),
				append(fns, verification.F...)

			if len(signatures) > BatchSignatureVerificationThreshold {
				if err := a.runVerificationAndExecution(signatures, signRoots, pks, fns); err != nil {
					log.Warn(err.Error())
				}
				signatures, signRoots, pks, fns =
					make([][]byte, 0, 128),
					make([][]byte, 0, 128),
					make([][]byte, 0, 128),
					make([]func(), 0, 128)
				ticker.Reset(batchCheckInterval)
			}
		case <-ticker.C:
			if len(signatures) != 0 {
				if err := a.runVerificationAndExecution(signatures, signRoots, pks, fns); err != nil {
					log.Warn(err.Error())
				}
				signatures, signRoots, pks, fns =
					make([][]byte, 0, 128),
					make([][]byte, 0, 128),
					make([][]byte, 0, 128),
					make([]func(), 0, 128)
				ticker.Reset(batchCheckInterval)
			}
		}
	}
}

func (a *aggregateAndProofServiceImpl) runVerificationAndExecution(signatures [][]byte, signRoots [][]byte, pks [][]byte, fns []func()) error {
	valid, err := bls.VerifyMultipleSignatures(signatures, signRoots, pks)
	if err != nil {
		return errors.New("aggregate_and_proof_service signature verification failed with the error: " + err.Error())
	}

	if !valid {
		return errors.New("aggregate_and_proof_service signature verification failed")
	}

	// run all the acompanying functions if those signatures were valid
	for _, f := range fns {
		f()
	}

	return nil
}

func (a *aggregateAndProofServiceImpl) ProcessMessage(
	ctx context.Context,
	subnet *uint64,
	aggregateAndProof *cltypes.SignedAggregateAndProof,
) error {
	headState := a.syncedDataManager.HeadState()
	if headState == nil {
		return ErrIgnore
	}
	selectionProof := aggregateAndProof.Message.SelectionProof
	aggregateData := aggregateAndProof.Message.Aggregate.AttestantionData()
	target := aggregateAndProof.Message.Aggregate.AttestantionData().Target()
	slot := aggregateAndProof.Message.Aggregate.AttestantionData().Slot()
	committeeIndex := aggregateAndProof.Message.Aggregate.AttestantionData().CommitteeIndex()

	if aggregateData.Slot() > headState.Slot() {
		a.scheduleAggregateForLaterProcessing(aggregateAndProof)
		return ErrIgnore
	}
	epoch := slot / a.beaconCfg.SlotsPerEpoch
	// [IGNORE] the epoch of aggregate.data.slot is either the current or previous epoch (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. compute_epoch_at_slot(aggregate.data.slot) in (get_previous_epoch(state), get_current_epoch(state))
	if state.PreviousEpoch(headState) != epoch && state.Epoch(headState) != epoch {
		return ErrIgnore
	}
	finalizedCheckpoint := a.forkchoiceStore.FinalizedCheckpoint()
	finalizedSlot := finalizedCheckpoint.Epoch() * a.beaconCfg.SlotsPerEpoch
	// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by aggregate.data.beacon_block_root -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, finalized_checkpoint.epoch) == store.finalized_checkpoint.root
	if a.forkchoiceStore.Ancestor(
		aggregateData.BeaconBlockRoot(),
		finalizedSlot,
	) != finalizedCheckpoint.BlockRoot() {
		return ErrIgnore
	}

	// [IGNORE] The block being voted for (aggregate.data.beacon_block_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue aggregates for processing once block is retrieved).
	if _, ok := a.forkchoiceStore.GetHeader(aggregateData.BeaconBlockRoot()); !ok {
		return ErrIgnore
	}

	// [REJECT] The committee index is within the expected range -- i.e. index < get_committee_count_per_slot(state, aggregate.data.target.epoch).
	committeeCountPerSlot := headState.CommitteeCount(target.Epoch())
	if aggregateData.CommitteeIndex() >= committeeCountPerSlot {
		return errors.New("invalid committee index in aggregate and proof")
	}
	// [REJECT] The aggregate attestation's epoch matches its target -- i.e. aggregate.data.target.epoch == compute_epoch_at_slot(aggregate.data.slot)
	if aggregateData.Target().Epoch() != epoch {
		return errors.New("invalid target epoch in aggregate and proof")
	}
	committee, err := headState.GetBeaconCommitee(slot, committeeIndex)
	if err != nil {
		return err
	}

	// [REJECT] The aggregator's validator index is within the committee -- i.e. aggregate_and_proof.aggregator_index in get_beacon_committee(state, aggregate.data.slot, index).
	if !slices.Contains(committee, aggregateAndProof.Message.AggregatorIndex) {
		return errors.New("committee index not in committee")
	}
	// [REJECT] The aggregate attestation's target block is an ancestor of the block named in the LMD vote -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, aggregate.data.target.epoch) == aggregate.data.target.root
	if a.forkchoiceStore.Ancestor(
		aggregateData.BeaconBlockRoot(),
		target.Epoch()*a.beaconCfg.SlotsPerEpoch,
	) != target.BlockRoot() {
		return errors.New("invalid target block")
	}
	if a.test {
		return nil
	}

	// [REJECT] aggregate_and_proof.selection_proof selects the validator as an aggregator for the slot -- i.e. is_aggregator(state, aggregate.data.slot, index, aggregate_and_proof.selection_proof) returns True.
	if !state.IsAggregator(a.beaconCfg, uint64(len(committee)), committeeIndex, selectionProof) {
		log.Warn("receveived aggregate and proof from invalid aggregator")
		return errors.New("invalid aggregate and proof")
	}
	attestingIndicies, err := headState.GetAttestingIndicies(
		aggregateAndProof.Message.Aggregate.AttestantionData(),
		aggregateAndProof.Message.Aggregate.AggregationBits(),
		true,
	)
	if err != nil {
		return err
	}
	if len(attestingIndicies) == 0 {
		return errors.New("no attesting indicies")
	}

	// aggregate signatures for later verification
	aggregateVerificationData, err := GetSignaturesOnAggregate(headState, aggregateAndProof, attestingIndicies)
	if err != nil {
		return err
	}

	// further processing will be done after async signature verification
	aggregateVerificationData.F = append(aggregateVerificationData.F, func() {
		a.opPool.AttestationsPool.Insert(
			aggregateAndProof.Message.Aggregate.Signature(),
			aggregateAndProof.Message.Aggregate,
		)
		a.forkchoiceStore.ProcessAttestingIndicies(
			aggregateAndProof.Message.Aggregate,
			attestingIndicies,
		)
	})

	// push the signatures to verify asynchronously and run final functions after that.
	a.verifyAndExecute <- aggregateVerificationData
	return nil
}

func GetSignaturesOnAggregate(
	s *state.CachingBeaconState,
	aggregateAndProof *cltypes.SignedAggregateAndProof,
	attestingIndicies []uint64,
) (*AggregateVerificationData, error) {
	// [REJECT] The aggregate_and_proof.selection_proof is a valid signature of the aggregate.data.slot by the validator with index aggregate_and_proof.aggregator_index.
	signature1, signatureRoot1, pubKey1, err := AggregateAndProofSignature(s, aggregateAndProof.Message)
	if err != nil {
		return nil, err
	}

	// [REJECT] The aggregator signature, signed_aggregate_and_proof.signature, is valid.
	signature2, signatureRoot2, pubKey2, err := AggregatorSignature(s, aggregateAndProof)
	if err != nil {
		return nil, err
	}

	// [REJECT] Verifying here the validator signatures for the aggregate
	signature3, signatureRoot3, pubKey3, err := AggregateMessageSignature(s, aggregateAndProof, attestingIndicies)
	if err != nil {
		return nil, err
	}

	return &AggregateVerificationData{
		Signatures: [][]byte{signature1, signature2, signature3},
		SignRoots:  [][]byte{signatureRoot1, signatureRoot2, signatureRoot3},
		Pks:        [][]byte{pubKey1, pubKey2, pubKey3},
		F:          make([]func(), 0, 1),
	}, nil
}

func AggregateAndProofSignature(
	state *state.CachingBeaconState,
	aggregate *cltypes.AggregateAndProof,
) ([]byte, []byte, []byte, error) {
	slot := aggregate.Aggregate.AttestantionData().Slot()
	publicKey, err := state.ValidatorPublicKey(int(aggregate.AggregatorIndex))
	if err != nil {
		return nil, nil, nil, err
	}
	domain, err := state.GetDomain(
		state.BeaconConfig().DomainSelectionProof,
		slot*state.BeaconConfig().SlotsPerEpoch,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	signingRoot := utils.Sha256(merkle_tree.Uint64Root(slot).Bytes(), domain)
	return aggregate.SelectionProof[:], signingRoot[:], publicKey[:], nil
}

func AggregatorSignature(
	state *state.CachingBeaconState,
	aggregate *cltypes.SignedAggregateAndProof,
) ([]byte, []byte, []byte, error) {
	publicKey, err := state.ValidatorPublicKey(int(aggregate.Message.AggregatorIndex))
	if err != nil {
		return nil, nil, nil, err
	}
	domain, err := state.GetDomain(state.BeaconConfig().DomainAggregateAndProof, state.Slot())
	if err != nil {
		return nil, nil, nil, err
	}
	signingRoot, err := fork.ComputeSigningRoot(aggregate.Message, domain)
	if err != nil {
		return nil, nil, nil, err
	}
	return aggregate.Signature[:], signingRoot[:], publicKey[:], nil
}

func AggregateMessageSignature(
	s *state.CachingBeaconState,
	aggregateAndProof *cltypes.SignedAggregateAndProof,
	attestingIndicies []uint64,
) ([]byte, []byte, []byte, error) {
	indexedAttestation := state.GetIndexedAttestation(
		aggregateAndProof.Message.Aggregate,
		attestingIndicies,
	)

	inds := indexedAttestation.AttestingIndices
	if inds.Length() == 0 {
		return nil, nil, nil, errors.New("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := make([][]byte, 0, inds.Length())
	if err := solid.RangeErr[uint64](inds, func(_ int, v uint64, _ int) error {
		val, err := s.ValidatorForValidatorIndex(int(v))
		if err != nil {
			return err
		}
		pk := val.PublicKeyBytes()
		pks = append(pks, pk)
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconAttester, indexedAttestation.Data.Target().Epoch())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to get the domain: %v", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(indexedAttestation.Data, domain)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to get signing root: %v", err)
	}

	pubKeys, err := bls.AggregatePublickKeys(pks)
	if err != nil {
		return nil, nil, nil, err
	}

	return indexedAttestation.Signature[:], signingRoot[:], pubKeys, nil
}

func (a *aggregateAndProofServiceImpl) scheduleAggregateForLaterProcessing(
	aggregateAndProof *cltypes.SignedAggregateAndProof,
) {
	key, err := aggregateAndProof.HashSSZ()
	if err != nil {
		panic(err)
	}

	a.aggregatesScheduledForLaterExecution.Store(key, &aggregateJob{
		aggregate:    aggregateAndProof,
		creationTime: time.Now(),
	})
}

func (a *aggregateAndProofServiceImpl) loop(ctx context.Context) {
	ticker := time.NewTicker(attestationJobsIntervalTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		headState := a.syncedDataManager.HeadState()
		if headState == nil {
			continue
		}
		a.aggregatesScheduledForLaterExecution.Range(func(key, value any) bool {
			job := value.(*aggregateJob)
			// check if it has expired
			if time.Since(job.creationTime) > attestationJobExpiry {
				a.aggregatesScheduledForLaterExecution.Delete(key.([32]byte))
				return true
			}
			aggregateData := job.aggregate.Message.Aggregate.AttestantionData()
			if aggregateData.Slot() > headState.Slot() {
				return true
			}

			if err := a.ProcessMessage(ctx, nil, job.aggregate); err != nil {
				log.Trace("blob sidecar verification failed", "err", err)
				return true
			}
			a.aggregatesScheduledForLaterExecution.Delete(key.([32]byte))
			return true
		})
	}
}
