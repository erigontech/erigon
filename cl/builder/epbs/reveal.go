// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	clutils "github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

var ErrRevealExpired = errors.New("epbs/reveal: payload reveal deadline expired")

const maxConcurrentReveals = 4

type PayloadProcessor interface {
	ProcessMessage(context.Context, *uint64, *cltypes.SignedExecutionPayloadEnvelope) error
}

type AcceptedBlockReader interface {
	GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool)
}

type PersistedEnvelopeReader interface {
	ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error)
}

type CanonicalHeadReader interface {
	GetHeadNode() (forkchoice.ForkChoiceNode, error)
}

type revealKey struct {
	beaconBlockRoot common.Hash
	signedBidRoot   common.Hash
}

type revealRequest struct {
	key      revealKey
	identity PayloadIdentity
	slot     uint64
}

type revealTracking struct {
	slot   uint64
	active bool
}

type revealRunner struct {
	beaconCfg     *clparams.BeaconChainConfig
	clock         LiveSlotClock
	signer        Signer
	coordinator   *Coordinator
	blocks        AcceptedBlockReader
	persisted     PersistedEnvelopeReader
	head          CanonicalHeadReader
	processor     PayloadProcessor
	publisher     GossipPublisher
	retryInterval time.Duration
	requests      chan revealRequest

	mu          sync.Mutex
	tracked     map[revealKey]revealTracking
	activeCount int
}

func (r *revealRunner) maxQueue() int {
	return cap(r.requests)
}

func newRevealRunner(
	beaconCfg *clparams.BeaconChainConfig,
	clock LiveSlotClock,
	signer Signer,
	coordinator *Coordinator,
	blocks AcceptedBlockReader,
	processor PayloadProcessor,
	publisher GossipPublisher,
	persisted PersistedEnvelopeReader,
	head CanonicalHeadReader,
	retryInterval time.Duration,
	maxQueued int,
) *revealRunner {
	return &revealRunner{
		beaconCfg: beaconCfg, clock: clock, signer: signer, coordinator: coordinator, blocks: blocks,
		processor: processor, publisher: publisher, persisted: persisted, head: head, retryInterval: retryInterval,
		requests: make(chan revealRequest, maxQueued), tracked: make(map[revealKey]revealTracking),
	}
}

func (r *revealRunner) Run(ctx context.Context) {
	var workers sync.WaitGroup
	for range maxConcurrentReveals {
		workers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case request := <-r.requests:
					r.runRequest(ctx, request)
				}
			}
		})
	}
	ticker := time.NewTicker(r.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			workers.Wait()
			r.releaseActiveTracking()
			return
		case <-ticker.C:
			r.reconcileCanonicalHead(ctx)
		}
	}
}

func (r *revealRunner) runRequest(ctx context.Context, request revealRequest) {
	defer r.finishRequest(request.key)
	if err := r.reveal(ctx, request); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn("Embedded builder payload reveal failed", "slot", request.slot, "blockRoot", request.key.beaconBlockRoot, "err", err)
	}
}

func (r *revealRunner) SubmitAcceptedBlock(blockRoot common.Hash) bool {
	return r.submitAcceptedBlock(blockRoot, false)
}

func (r *revealRunner) reconcileCanonicalHead(ctx context.Context) {
	if ctx.Err() != nil || isNilDependency(r.head) {
		return
	}
	head, err := r.head.GetHeadNode()
	if err != nil || head.Root == (common.Hash{}) || ctx.Err() != nil {
		return
	}
	r.submitAcceptedBlock(head.Root, true)
}

func (r *revealRunner) submitAcceptedBlock(blockRoot common.Hash, replaceInactive bool) bool {
	if r.tracksBlockRoot(blockRoot) {
		return false
	}
	r.pruneTracked(r.clock.GetCurrentSlot())
	block, ok := r.blocks.GetBlock(blockRoot)
	if !ok || block == nil || block.Block == nil || block.Block.Body == nil {
		return false
	}
	computedRoot, err := block.Block.HashSSZ()
	if err != nil || common.Hash(computedRoot) != blockRoot {
		return false
	}
	signedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if signedBid == nil || signedBid.Message == nil || signedBid.Message.Slot != block.Block.Slot || signedBid.Message.ParentBlockRoot != block.Block.ParentRoot {
		return false
	}
	signedBidRoot, err := signedBid.HashSSZ()
	if err != nil {
		return false
	}
	identity := PayloadIdentity{
		Slot: signedBid.Message.Slot, ParentBlockHash: signedBid.Message.ParentBlockHash,
		ParentBlockRoot: signedBid.Message.ParentBlockRoot, BlockHash: signedBid.Message.BlockHash,
	}
	if !r.coordinator.MatchesPayload(identity, signedBid.Message.BuilderIndex, common.Hash(signedBidRoot)) {
		return false
	}
	key := revealKey{beaconBlockRoot: blockRoot, signedBidRoot: common.Hash(signedBidRoot)}
	r.mu.Lock()
	if _, exists := r.tracked[key]; exists || r.activeCount >= cap(r.requests) {
		r.mu.Unlock()
		return false
	}
	var evictedKey revealKey
	var evictedTracking revealTracking
	var evicted bool
	if len(r.tracked) >= cap(r.requests) {
		if !replaceInactive {
			r.mu.Unlock()
			return false
		}
		evictedKey, evictedTracking, evicted = r.inactiveEvictionCandidate()
		if !evicted {
			r.mu.Unlock()
			return false
		}
		delete(r.tracked, evictedKey)
	}
	r.tracked[key] = revealTracking{slot: signedBid.Message.Slot, active: true}
	r.activeCount++
	request := revealRequest{key: key, identity: identity, slot: signedBid.Message.Slot}
	select {
	case r.requests <- request:
		r.mu.Unlock()
		return true
	default:
		delete(r.tracked, key)
		r.activeCount--
		if evicted {
			r.tracked[evictedKey] = evictedTracking
		}
		r.mu.Unlock()
		return false
	}
}

func (r *revealRunner) tracksBlockRoot(blockRoot common.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.tracked {
		if key.beaconBlockRoot == blockRoot {
			return true
		}
	}
	return false
}

func (r *revealRunner) inactiveEvictionCandidate() (revealKey, revealTracking, bool) {
	var candidateKey revealKey
	var candidateTracking revealTracking
	found := false
	for key, tracking := range r.tracked {
		if tracking.active {
			continue
		}
		if !found || tracking.slot < candidateTracking.slot ||
			(tracking.slot == candidateTracking.slot && revealKeyLess(key, candidateKey)) {
			candidateKey = key
			candidateTracking = tracking
			found = true
		}
	}
	return candidateKey, candidateTracking, found
}

func revealKeyLess(left, right revealKey) bool {
	if compared := bytes.Compare(left.beaconBlockRoot[:], right.beaconBlockRoot[:]); compared != 0 {
		return compared < 0
	}
	return bytes.Compare(left.signedBidRoot[:], right.signedBidRoot[:]) < 0
}

func (r *revealRunner) pruneTracked(currentSlot uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, tracking := range r.tracked {
		if !tracking.active && tracking.slot < currentSlot {
			delete(r.tracked, key)
		}
	}
}

func (r *revealRunner) finishRequest(key revealKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tracking, ok := r.tracked[key]
	if !ok || !tracking.active {
		return
	}
	tracking.active = false
	r.tracked[key] = tracking
	r.activeCount--
}

func (r *revealRunner) releaseActiveTracking() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, tracking := range r.tracked {
		tracking.active = false
		r.tracked[key] = tracking
	}
	r.activeCount = 0
}

func (r *revealRunner) reveal(ctx context.Context, request revealRequest) error {
	retained, ok, err := r.coordinator.Payload(request.identity)
	if err != nil || !ok {
		return err
	}
	if retained.SignedBidRoot != request.key.signedBidRoot {
		return errors.New("epbs/reveal: retained bid root mismatch")
	}
	if hasBlobData(retained) {
		return errors.New("epbs/reveal: blob payload reveal is not supported")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := payloadRevealDeadline(r.clock, r.beaconCfg, request.slot)
	if !ok {
		return errors.New("epbs/reveal: invalid payload reveal deadline")
	}
	if !time.Now().Before(deadline) {
		return ErrRevealExpired
	}
	revealCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	envelope := cltypes.NewExecutionPayloadEnvelope(r.beaconCfg)
	envelope.Payload = retained.Assembled.Eth1Block
	envelope.ExecutionRequests = retained.ExecutionRequests
	envelope.BuilderIndex = retained.BuilderIndex
	envelope.BeaconBlockRoot = request.key.beaconBlockRoot
	envelope.ParentBeaconBlockRoot = request.identity.ParentBlockRoot
	domain, err := builderDomain(r.beaconCfg, request.slot, retained.GenesisRoot)
	if err != nil {
		return err
	}
	signingRoot, err := fork.ComputeSigningRoot(envelope, domain)
	if err != nil {
		return fmt.Errorf("epbs/reveal: envelope signing root: %w", err)
	}
	signature, err := r.signer.SignEnvelope(revealCtx, common.Hash(signingRoot))
	if contextErr := revealContextError(ctx, revealCtx, deadline); contextErr != nil {
		if err != nil {
			return errors.Join(contextErr, fmt.Errorf("epbs/reveal: sign envelope: %w", err))
		}
		return contextErr
	}
	if err != nil {
		return fmt.Errorf("epbs/reveal: sign envelope: %w", err)
	}
	if signature == (common.Bytes96{}) {
		return errors.New("epbs/reveal: signer returned an empty signature")
	}
	signedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: envelope, Signature: signature}
	encoded, err := signedEnvelope.EncodeSSZ(nil)
	if err != nil {
		return fmt.Errorf("epbs/reveal: encode envelope: %w", err)
	}
	localEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(r.beaconCfg)}
	if err := localEnvelope.DecodeSSZStrict(encoded, int(clparams.GloasVersion)); err != nil {
		return fmt.Errorf("epbs/reveal: decode envelope: %w", err)
	}
	localAccepted := false
	var attemptErr error
	for {
		if err := revealContextError(ctx, revealCtx, deadline); err != nil {
			return errors.Join(err, attemptErr)
		}
		if !localAccepted {
			attemptErr = r.processor.ProcessMessage(revealCtx, nil, localEnvelope)
			localAccepted = attemptErr == nil
			if localAccepted {
				continue
			}
			if r.persistedEnvelopeMatches(request.key.beaconBlockRoot, encoded) {
				localAccepted = true
				continue
			}
		} else {
			attemptErr = r.publisher.Publish(revealCtx, gossip.TopicNameExecutionPayload, encoded)
			if attemptErr == nil {
				if err := revealContextError(ctx, revealCtx, deadline); err != nil {
					return err
				}
				return nil
			}
		}
		if err := revealContextError(ctx, revealCtx, deadline); err != nil {
			return errors.Join(err, attemptErr)
		}
		timer := time.NewTimer(min(r.retryInterval, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), attemptErr)
		case <-revealCtx.Done():
			timer.Stop()
			return errors.Join(revealContextError(ctx, revealCtx, deadline), attemptErr)
		case <-timer.C:
		}
	}
}

func revealContextError(parentCtx, revealCtx context.Context, deadline time.Time) error {
	if err := parentCtx.Err(); err != nil {
		return err
	}
	if !time.Now().Before(deadline) || errors.Is(revealCtx.Err(), context.DeadlineExceeded) {
		return ErrRevealExpired
	}
	return revealCtx.Err()
}

func (r *revealRunner) persistedEnvelopeMatches(blockRoot common.Hash, encoded []byte) bool {
	if isNilDependency(r.persisted) {
		return false
	}
	persisted, err := r.persisted.ReadEnvelopeFromDisk(blockRoot)
	if err != nil || persisted == nil || persisted.Message == nil {
		return false
	}
	persistedEncoded, err := persisted.EncodeSSZ(nil)
	return err == nil && bytes.Equal(persistedEncoded, encoded)
}

func builderDomain(beaconCfg *clparams.BeaconChainConfig, slot uint64, genesisRoot common.Hash) ([]byte, error) {
	if beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 || genesisRoot == (common.Hash{}) {
		return nil, errors.New("epbs/reveal: invalid builder domain context")
	}
	version := beaconCfg.GetCurrentStateVersion(slot / beaconCfg.SlotsPerEpoch)
	forkVersion := clutils.Uint32ToBytes4(beaconCfg.GetForkVersionByVersion(version))
	return fork.ComputeDomain(beaconCfg.DomainBeaconBuilder[:], forkVersion, genesisRoot)
}

func payloadRevealDeadline(clock LiveSlotClock, beaconCfg *clparams.BeaconChainConfig, slot uint64) (time.Time, bool) {
	if isNilDependency(clock) || beaconCfg == nil || beaconCfg.PayloadDueBps > clparams.BpsFactor ||
		beaconCfg.SecondsPerSlot > uint64(math.MaxInt64/int64(time.Second)) {
		return time.Time{}, false
	}
	slotDuration := time.Duration(beaconCfg.SecondsPerSlot) * time.Second
	slotNanos := uint64(slotDuration)
	dueBps := beaconCfg.PayloadDueBps
	dueNanos := slotNanos/clparams.BpsFactor*dueBps + slotNanos%clparams.BpsFactor*dueBps/clparams.BpsFactor
	due := time.Duration(dueNanos)
	return clock.GetSlotTime(slot).Add(due), true
}

func hasBlobData(retained *RetainedPayload) bool {
	if retained == nil || retained.Assembled == nil || retained.Assembled.BlobsBundle == nil {
		return false
	}
	bundle := retained.Assembled.BlobsBundle
	return len(bundle.Blobs) != 0 || len(bundle.Commitments) != 0 || len(bundle.Proofs) != 0
}
