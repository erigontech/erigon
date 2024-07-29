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
	"slices"
	"sync"
	"time"

	"github.com/Giulio2002/bls"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
)

type aggregateJob struct {
	aggregate    *cltypes.SignedAggregateAndProof
	creationTime time.Time
}

type aggregateAndProofServiceImpl struct {
	syncedDataManager *synced_data.SyncedDataManager
	forkchoiceStore   forkchoice.ForkChoiceStorage
	beaconCfg         *clparams.BeaconChainConfig
	opPool            pool.OperationsPool
	test              bool

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
	}
	go a.loop(ctx)
	return a
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
		epoch*a.beaconCfg.SlotsPerEpoch,
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
	if err := verifySignaturesOnAggregate(headState, aggregateAndProof); err != nil {
		return err
	} // Add to aggregation pool
	a.opPool.AttestationsPool.Insert(
		aggregateAndProof.Message.Aggregate.Signature(),
		aggregateAndProof.Message.Aggregate,
	)
	a.forkchoiceStore.ProcessAttestingIndicies(
		aggregateAndProof.Message.Aggregate,
		attestingIndicies,
	)
	return nil
}

func verifySignaturesOnAggregate(
	s *state.CachingBeaconState,
	aggregateAndProof *cltypes.SignedAggregateAndProof,
) error {
	aggregationBits := aggregateAndProof.Message.Aggregate.AggregationBits()
	// [REJECT] The aggregate attestation has participants -- that is, len(get_attesting_indices(state, aggregate)) >= 1.
	attestingIndicies, err := s.GetAttestingIndicies(
		aggregateAndProof.Message.Aggregate.AttestantionData(),
		aggregationBits,
		true,
	)
	if err != nil {
		return err
	}
	if len(attestingIndicies) == 0 {
		return errors.New("no attesting indicies")
	}
	// [REJECT] The aggregate_and_proof.selection_proof is a valid signature of the aggregate.data.slot by the validator with index aggregate_and_proof.aggregator_index.
	if err := verifyAggregateAndProofSignature(s, aggregateAndProof.Message); err != nil {
		return err
	}

	// [REJECT] The aggregator signature, signed_aggregate_and_proof.signature, is valid.
	if err := verifyAggregatorSignature(s, aggregateAndProof); err != nil {
		return err
	}

	return verifyAggregateMessageSignature(s, aggregateAndProof, attestingIndicies)
}

func verifyAggregateAndProofSignature(
	state *state.CachingBeaconState,
	aggregate *cltypes.AggregateAndProof,
) error {
	slot := aggregate.Aggregate.AttestantionData().Slot()
	publicKey, err := state.ValidatorPublicKey(int(aggregate.AggregatorIndex))
	if err != nil {
		return err
	}
	domain, err := state.GetDomain(
		state.BeaconConfig().DomainSelectionProof,
		slot*state.BeaconConfig().SlotsPerEpoch,
	)
	if err != nil {
		return err
	}
	signingRoot := utils.Sha256(merkle_tree.Uint64Root(slot).Bytes(), domain)
	valid, err := bls.Verify(aggregate.SelectionProof[:], signingRoot[:], publicKey[:])
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("invalid bls signature on aggregate and proof")
	}
	return nil
}

func verifyAggregatorSignature(
	state *state.CachingBeaconState,
	aggregate *cltypes.SignedAggregateAndProof,
) error {
	publicKey, err := state.ValidatorPublicKey(int(aggregate.Message.AggregatorIndex))
	if err != nil {
		return err
	}
	domain, err := state.GetDomain(state.BeaconConfig().DomainAggregateAndProof, state.Slot())
	if err != nil {
		return err
	}
	signingRoot, err := fork.ComputeSigningRoot(aggregate.Message, domain)
	if err != nil {
		return err
	}
	valid, err := bls.Verify(aggregate.Signature[:], signingRoot[:], publicKey[:])
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("invalid bls signature on aggregate and proof")
	}
	return nil
}

func verifyAggregateMessageSignature(
	s *state.CachingBeaconState,
	aggregateAndProof *cltypes.SignedAggregateAndProof,
	attestingIndicies []uint64,
) error {
	indexedAttestation := state.GetIndexedAttestation(
		aggregateAndProof.Message.Aggregate,
		attestingIndicies,
	)

	valid, err := state.IsValidIndexedAttestation(s, indexedAttestation)
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("invalid aggregate signature")
	}
	return nil
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
