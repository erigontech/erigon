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

	"github.com/erigontech/erigon/cl/utils/bls"

	"github.com/erigontech/erigon-lib/common"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
)

// SignedAggregateAndProofData is passed to SignedAggregateAndProof service. The service does the signature verification
// asynchronously. That's why we cannot wait for its ProcessMessage call to finish to check error. The service
// will do re-publishing of the gossip or banning the peer in case of invalid signature by itself.
// that's why we are passing sentinel.SentinelClient and *sentinel.GossipData to enable the service
// to do all of that by itself.
type SignedAggregateAndProofForGossip struct {
	SignedAggregateAndProof *cltypes.SignedAggregateAndProof
	Receiver                *sentinel.Peer
	ImmediateProcess        bool
}

type aggregateJob struct {
	aggregate    *SignedAggregateAndProofForGossip
	creationTime time.Time
}

type seenAggregateIndex struct {
	epoch uint64
	index uint64
}

const seenAggregateCacheSize = 10_000

type aggregateAndProofServiceImpl struct {
	syncedDataManager      *synced_data.SyncedDataManager
	forkchoiceStore        forkchoice.ForkChoiceStorage
	beaconCfg              *clparams.BeaconChainConfig
	opPool                 pool.OperationsPool
	test                   bool
	batchSignatureVerifier *BatchSignatureVerifier
	seenAggreatorIndexes   *lru.Cache[seenAggregateIndex, struct{}]

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
	batchSignatureVerifier *BatchSignatureVerifier,
) AggregateAndProofService {
	seenAggCache, err := lru.New[seenAggregateIndex, struct{}]("seenAggregate", seenAggregateCacheSize)
	if err != nil {
		panic(err)
	}
	a := &aggregateAndProofServiceImpl{
		syncedDataManager:      syncedDataManager,
		forkchoiceStore:        forkchoiceStore,
		beaconCfg:              beaconCfg,
		opPool:                 opPool,
		test:                   test,
		batchSignatureVerifier: batchSignatureVerifier,
		seenAggreatorIndexes:   seenAggCache,
	}
	go a.loop(ctx)
	return a
}

func (a *aggregateAndProofServiceImpl) ProcessMessage(
	ctx context.Context,
	subnet *uint64,
	aggregateAndProof *SignedAggregateAndProofForGossip,
) error {
	selectionProof := aggregateAndProof.SignedAggregateAndProof.Message.SelectionProof
	aggregateData := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data
	aggregate := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate
	target := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data.Target
	slot := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data.Slot
	committeeIndex := aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex

	if aggregateData.Slot > a.syncedDataManager.HeadSlot() {
		//a.scheduleAggregateForLaterProcessing(aggregateAndProof)
		return ErrIgnore
	}

	epoch := slot / a.beaconCfg.SlotsPerEpoch
	clversion := a.beaconCfg.GetCurrentStateVersion(epoch)
	if clversion.AfterOrEqual(clparams.ElectraVersion) {
		// [REJECT] len(committee_indices) == 1, where committee_indices = get_committee_indices(aggregate).
		indices := aggregate.CommitteeBits.GetOnIndices()
		if len(indices) != 1 {
			return fmt.Errorf("invalid committee_bits length in aggregate and proof: %v", len(indices))
		}
		// [REJECT] aggregate.data.index == 0
		if aggregate.Data.CommitteeIndex != 0 {
			return errors.New("invalid committee_index in aggregate and proof")
		}
		committeeIndex = uint64(indices[0])
	}

	var (
		aggregateVerificationData *AggregateVerificationData
		attestingIndices          []uint64
		seenIndex                 seenAggregateIndex
	)
	if err := a.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		// [IGNORE] the epoch of aggregate.data.slot is either the current or previous epoch (with a MAXIMUM_GOSSIP_CLOCK_DISPARITY allowance) -- i.e. compute_epoch_at_slot(aggregate.data.slot) in (get_previous_epoch(state), get_current_epoch(state))
		if state.PreviousEpoch(headState) != epoch && state.Epoch(headState) != epoch {
			return ErrIgnore
		}

		// [REJECT] The committee index is within the expected range -- i.e. index < get_committee_count_per_slot(state, aggregate.data.target.epoch).
		committeeCountPerSlot := headState.CommitteeCount(target.Epoch)
		if committeeIndex >= committeeCountPerSlot {
			return errors.New("invalid committee index in aggregate and proof")
		}
		// [REJECT] The aggregate attestation's epoch matches its target -- i.e. aggregate.data.target.epoch == compute_epoch_at_slot(aggregate.data.slot)
		if aggregateData.Target.Epoch != epoch {
			return errors.New("invalid target epoch in aggregate and proof")
		}
		finalizedCheckpoint := a.forkchoiceStore.FinalizedCheckpoint()
		finalizedSlot := finalizedCheckpoint.Epoch * a.beaconCfg.SlotsPerEpoch
		// [IGNORE] The current finalized_checkpoint is an ancestor of the block defined by aggregate.data.beacon_block_root -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, finalized_checkpoint.epoch) == store.finalized_checkpoint.root
		if a.forkchoiceStore.Ancestor(
			aggregateData.BeaconBlockRoot,
			finalizedSlot,
		) != finalizedCheckpoint.Root {
			return ErrIgnore
		}

		// [IGNORE] The block being voted for (aggregate.data.beacon_block_root) has been seen (via both gossip and non-gossip sources) (a client MAY queue aggregates for processing once block is retrieved).
		if _, ok := a.forkchoiceStore.GetHeader(aggregateData.BeaconBlockRoot); !ok {
			return ErrIgnore
		}

		// [IGNORE] The aggregate is the first valid aggregate received for the aggregator with index aggregate_and_proof.aggregator_index for the epoch aggregate.data.target.epoch
		seenIndex = seenAggregateIndex{
			epoch: target.Epoch,
			index: aggregateAndProof.SignedAggregateAndProof.Message.AggregatorIndex,
		}
		if a.seenAggreatorIndexes.Contains(seenIndex) {
			return ErrIgnore
		}

		committee, err := headState.GetBeaconCommitee(slot, committeeIndex)
		if err != nil {
			return err
		}
		// [REJECT] The attestation has participants -- that is, len(get_attesting_indices(state, aggregate)) >= 1
		attestingIndices, err = headState.GetAttestingIndicies(aggregate, false)
		if err != nil {
			return err
		}
		if len(attestingIndices) == 0 {
			return errors.New("no attesting indicies")
		}

		// [REJECT] The aggregator's validator index is within the committee -- i.e. aggregate_and_proof.aggregator_index in get_beacon_committee(state, aggregate.data.slot, index).
		if !slices.Contains(committee, aggregateAndProof.SignedAggregateAndProof.Message.AggregatorIndex) {
			return errors.New("committee index not in committee")
		}
		// [REJECT] The aggregate attestation's target block is an ancestor of the block named in the LMD vote -- i.e. get_checkpoint_block(store, aggregate.data.beacon_block_root, aggregate.data.target.epoch) == aggregate.data.target.root
		if a.forkchoiceStore.Ancestor(
			aggregateData.BeaconBlockRoot,
			target.Epoch*a.beaconCfg.SlotsPerEpoch,
		) != target.Root {
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

		// aggregate signatures for later verification
		aggregateVerificationData, err = GetSignaturesOnAggregate(headState, aggregateAndProof.SignedAggregateAndProof, attestingIndices)
		if err != nil {
			return err
		}

		monitor.ObserveNumberOfAggregateSignatures(len(attestingIndices))
		monitor.ObserveAggregateQuality(len(attestingIndices), len(committee))
		monitor.ObserveCommitteeSize(float64(len(committee)))
		return nil
	}); err != nil {
		return err
	}
	if a.test {
		return nil
	}
	// further processing will be done after async signature verification
	aggregateVerificationData.F = func() {
		a.opPool.AttestationsPool.Insert(
			aggregateAndProof.SignedAggregateAndProof.Message.Aggregate.Signature,
			aggregateAndProof.SignedAggregateAndProof.Message.Aggregate,
		)
		a.forkchoiceStore.ProcessAttestingIndicies(
			aggregateAndProof.SignedAggregateAndProof.Message.Aggregate,
			attestingIndices,
		)
		a.seenAggreatorIndexes.Add(seenIndex, struct{}{})
	}
	// for this specific request, collect data for potential peer banning or gossip publishing
	aggregateVerificationData.SendingPeer = aggregateAndProof.Receiver

	if aggregateAndProof.ImmediateProcess {
		return a.batchSignatureVerifier.ImmediateVerification(aggregateVerificationData)
	}

	// push the signatures to verify asynchronously and run final functions after that.
	a.batchSignatureVerifier.AsyncVerifyAggregateProof(aggregateVerificationData)

	return ErrIgnore

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
	}, nil
}

func AggregateAndProofSignature(
	state *state.CachingBeaconState,
	aggregate *cltypes.AggregateAndProof,
) ([]byte, []byte, []byte, error) {
	slot := aggregate.Aggregate.Data.Slot
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
		pks = append(pks, common.CopyBytes(pk))
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconAttester, indexedAttestation.Data.Target.Epoch)
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
	aggregateAndProof *SignedAggregateAndProofForGossip,
) {
	key, err := aggregateAndProof.SignedAggregateAndProof.HashSSZ()
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
	keysToDel := make([][32]byte, 0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		keysToDel = keysToDel[:0]
		a.aggregatesScheduledForLaterExecution.Range(func(key, value any) bool {
			if a.syncedDataManager.Syncing() {
				// Discard the job if we can't get the head state
				keysToDel = append(keysToDel, key.([32]byte))
				return false
			}
			job := value.(*aggregateJob)
			// check if it has expired
			if time.Since(job.creationTime) > attestationJobExpiry {
				keysToDel = append(keysToDel, key.([32]byte))
				return true
			}
			aggregateData := job.aggregate.SignedAggregateAndProof.Message.Aggregate.Data
			if aggregateData.Slot > a.syncedDataManager.HeadSlot() {
				return true
			}
			if err := a.ProcessMessage(ctx, nil, job.aggregate); err != nil {
				return true
			}

			keysToDel = append(keysToDel, key.([32]byte))
			return true
		})
		for _, key := range keysToDel {
			a.aggregatesScheduledForLaterExecution.Delete(key)
		}
	}
}
