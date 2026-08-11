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

package forkchoice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// errELBehind is returned by validatePayloadWithELLocked when the EL cannot process
// the payload because it hasn't caught up yet (e.g. parent block not available).
// applyEnvelope treats this as non-fatal: it proceeds with persisting the envelope
// and queues the execution block for later EL insertion.
var (
	errELBehind                = errors.New("EL behind: payload not processable yet")
	errExecutionPayloadInvalid = errors.New("execution payload envelope is invalid")
)

type pendingExecutionPayloadEnvelope struct {
	root  common.Hash
	entry *pendingExecutionPayloadEnvelopeEntry
	local bool
}

type pendingExecutionPayloadEnvelopeEntry struct {
	envelope  *cltypes.SignedExecutionPayloadEnvelope
	createdAt time.Time
}

const pendingExecutionPayloadEnvelopeExpiry = 3 * time.Minute

// validateEnvelopeAgainstBlock validates the envelope against the block and state.
// This includes:
//   - bid matching (slot, builder_index, block_hash)
//   - builder signature verification
func (f *ForkChoiceStore) validateEnvelopeAgainstBlock(
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	blockState abstract.BeaconState,
) error {
	if signedEnvelope.Message == nil {
		log.Warn("[validateEnvelopeAgainstBlock] received signed envelope with nil message")
		return errors.New("signed envelope has nil message")
	}
	envelope := signedEnvelope.Message

	// [REJECT] block.slot equals envelope.payload.slot_number (EIP-7843)
	if envelope.Payload == nil {
		return errors.New("envelope missing payload")
	}
	if block.Block.Slot != envelope.Payload.SlotNumber {
		return fmt.Errorf("block slot %d != envelope.payload.slot_number %d",
			block.Block.Slot, envelope.Payload.SlotNumber)
	}

	// Get the bid from the block
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	if bid == nil || bid.Message == nil {
		return errors.New("block missing signed_execution_payload_bid")
	}

	// Validate envelope.builder_index == bid.builder_index
	if envelope.BuilderIndex != bid.Message.BuilderIndex {
		return fmt.Errorf("envelope builder_index %d != bid builder_index %d",
			envelope.BuilderIndex, bid.Message.BuilderIndex)
	}

	// Validate payload.block_hash == bid.block_hash
	if envelope.Payload.BlockHash != bid.Message.BlockHash {
		return fmt.Errorf("payload block_hash %v != bid block_hash %v",
			envelope.Payload.BlockHash, bid.Message.BlockHash)
	}

	// Validate hash_tree_root(envelope.execution_requests) == bid.execution_requests_root
	if envelope.ExecutionRequests == nil {
		return errors.New("envelope missing execution_requests")
	}
	requestsRoot, err := envelope.ExecutionRequests.HashSSZ()
	if err != nil {
		return fmt.Errorf("failed to hash execution_requests: %w", err)
	}
	if requestsRoot != bid.Message.ExecutionRequestsRoot {
		return fmt.Errorf("execution_requests root %v != bid execution_requests_root %v",
			requestsRoot, bid.Message.ExecutionRequestsRoot)
	}

	// Validate envelope.parent_beacon_block_root == state.latest_block_header.parent_root
	if blockState != nil {
		latestBlockHeader := blockState.LatestBlockHeader()
		if envelope.ParentBeaconBlockRoot != latestBlockHeader.ParentRoot {
			return fmt.Errorf("envelope parent_beacon_block_root %v != latest_block_header parent_root %v",
				envelope.ParentBeaconBlockRoot, latestBlockHeader.ParentRoot)
		}
	}

	// Verify builder signature
	if err := f.verifyEnvelopeBuilderSignature(signedEnvelope, blockState, block.Block.Slot); err != nil {
		return fmt.Errorf("invalid builder signature: %w", err)
	}

	return nil
}

// verifyEnvelopeBuilderSignature verifies the builder's signature on the execution payload envelope.
// If builder_index is BUILDER_INDEX_SELF_BUILD, the proposer's pubkey is used; otherwise the builder's pubkey.
func (f *ForkChoiceStore) verifyEnvelopeBuilderSignature(
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	blockState abstract.BeaconState,
	blockSlot uint64,
) error {
	envelope := signedEnvelope.Message
	builderIndex := envelope.BuilderIndex

	var pk [48]byte
	if builderIndex == clparams.BuilderIndexSelfBuild {
		// Self-build: use the proposer's pubkey
		proposerIndex := blockState.LatestBlockHeader().ProposerIndex
		validator, err := blockState.ValidatorForValidatorIndex(int(proposerIndex))
		if err != nil {
			return fmt.Errorf("failed to get proposer validator: %w", err)
		}
		pk = validator.PublicKey()
	} else {
		// Builder: use the builder's pubkey
		builders := blockState.GetBuilders()
		if builders == nil {
			return errors.New("builders not found in state")
		}
		if builderIndex >= uint64(builders.Len()) {
			return fmt.Errorf("builder index %d out of range (max: %d)", builderIndex, builders.Len())
		}
		builder := builders.Get(int(builderIndex))
		if builder == nil {
			return errors.New("builder not found")
		}
		pk = builder.Pubkey
	}

	// Get domain for builder signature
	epoch := state.GetEpochAtSlot(f.beaconCfg, blockSlot)
	domain, err := blockState.GetDomain(f.beaconCfg.DomainBeaconBuilder, epoch)
	if err != nil {
		return fmt.Errorf("failed to get domain: %w", err)
	}

	// Compute signing root
	signingRoot, err := fork.ComputeSigningRoot(envelope, domain)
	if err != nil {
		return fmt.Errorf("failed to compute signing root: %w", err)
	}

	// Verify BLS signature
	valid, err := bls.Verify(signedEnvelope.Signature[:], signingRoot[:], pk[:])
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !valid {
		return errors.New("invalid signature")
	}

	return nil
}

// checkDataAvailability checks if blob data is available for the execution payload.
// For GLOAS, blob_kzg_commitments are in the committed bid, not directly in BeaconBlock.
// Returns nil if data is available, ErrEIP7594ColumnDataNotAvailable if not available yet.
func (f *ForkChoiceStore) checkDataAvailability(
	ctx context.Context,
	block *cltypes.SignedBeaconBlock,
	beaconBlockRoot common.Hash,
) error {
	// Get committed bid from the block
	committedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if committedBid == nil || committedBid.Message == nil {
		// No bid means no blobs to check
		return nil
	}

	blobCommitments := &committedBid.Message.BlobKzgCommitments
	if blobCommitments.Len() == 0 {
		// No blobs to check
		return nil
	}

	// Check PeerDAS data availability
	// Note: Unlike OnBlock, we don't skip this check even if EL has blobs,
	// because we need to ensure blobs are stored in CL's blob storage for beacon API.
	available, err := f.peerDas.IsDataAvailable(block.Block.Slot, beaconBlockRoot)
	if err != nil {
		return fmt.Errorf("checkDataAvailability: failed to check data availability: %w", err)
	}
	if !available {
		if f.syncedDataManager.Syncing() {
			// During sync, return error immediately to retry later
			return ErrEIP7594ColumnDataNotAvailable
		}
		// Not syncing - schedule deferred column data sync
		if err := f.peerDas.SyncColumnDataLater(block); err != nil {
			log.Warn("checkDataAvailability: failed to schedule deferred column data sync",
				"slot", block.Block.Slot, "beaconBlockRoot", beaconBlockRoot, "err", err)
		}
		// Return error so envelope can be queued for later processing
		return ErrEIP7594ColumnDataNotAvailable
	}

	return nil
}

// validatePayloadWithELLocked validates the payload while preserving f.mu ownership.
// Called before ProcessExecutionPayloadEnvelope verification.
func (f *ForkChoiceStore) validatePayloadWithELLocked(
	ctx context.Context,
	envelope *cltypes.ExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	beaconBlockRoot common.Hash,
) (execution_client.PayloadStatus, error) {
	if f.engine == nil {
		return execution_client.PayloadStatusNone, nil
	}
	if envelope == nil || envelope.Payload == nil || envelope.ExecutionRequests == nil {
		return execution_client.PayloadStatusNone, errors.New("validatePayloadWithEL: incomplete envelope")
	}

	// Get committed bid from the block (not from state, since state transition hasn't happened yet)
	committedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if committedBid == nil || committedBid.Message == nil {
		return execution_client.PayloadStatusNone, errors.New("validatePayloadWithEL: block missing execution payload bid")
	}

	// Calculate versioned hashes from committed bid's blob_kzg_commitments
	versionedHashes := make([]common.Hash, 0)
	blobCommitments := &committedBid.Message.BlobKzgCommitments
	if blobCommitments.Len() > 0 {
		versionedHashes = make([]common.Hash, 0, blobCommitments.Len())
		if err := solid.RangeErr[*cltypes.KZGCommitment](blobCommitments, func(_ int, k *cltypes.KZGCommitment, _ int) error {
			versionedHash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*k))
			if err != nil {
				return err
			}
			versionedHashes = append(versionedHashes, versionedHash)
			return nil
		}); err != nil {
			return execution_client.PayloadStatusNone, fmt.Errorf("validatePayloadWithEL: failed to compute versioned hashes: %w", err)
		}
	}

	// Get execution requests list
	var executionRequestsList []hexutil.Bytes
	if envelope.ExecutionRequests != nil {
		executionRequestsList = cltypes.GetExecutionRequestsList(f.beaconCfg, envelope.ExecutionRequests)
	}
	if executionRequestsList == nil {
		executionRequestsList = []hexutil.Bytes{}
	}
	validationKey, err := envelope.HashSSZ()
	if err != nil {
		return execution_client.PayloadStatusNone, fmt.Errorf("validatePayloadWithEL: failed to hash envelope: %w", err)
	}

	// Call NewPayload to validate execution payload with EL
	parentBlockRoot := block.Block.ParentRoot
	payloadStatus, err := f.newPayloadLocked(ctx, common.Hash(validationKey), envelope.Payload, &parentBlockRoot, versionedHashes, executionRequestsList)
	log.Trace("[validatePayloadWithEL] NewPayload", "status", payloadStatus, "beaconBlockRoot", beaconBlockRoot)

	// Track payload status and gas limit by execution block hash for parent payload validation
	executionBlockHash := envelope.Payload.BlockHash
	f.executionPayloadStatus.Add(executionBlockHash, payloadStatus)
	f.executionPayloadGasLimit.Add(executionBlockHash, envelope.Payload.GasLimit)
	if payloadStatus == execution_client.PayloadStatusInvalidated {
		log.Warn("validatePayloadWithEL: payload is invalid", "beaconBlockRoot", beaconBlockRoot, "err", err)
		f.markPayloadInvalidLocked(beaconBlockRoot, executionBlockHash)
		if err != nil {
			return payloadStatus, fmt.Errorf("execution payload is invalid: %w", err)
		}
		return payloadStatus, errors.New("execution payload is invalid")
	}
	if err != nil {
		return payloadStatus, fmt.Errorf("%w: %w", ErrELPayloadValidationUnavailable, err)
	}

	switch payloadStatus {
	case execution_client.PayloadStatusNone:
		// EL could not process the block (e.g. parent not yet available because
		// EL is still catching up after forward sync).  Return errELBehind so that
		// applyEnvelope can persist the envelope and queue the execution block
		// for later insertion into EL.
		log.Warn("validatePayloadWithEL: EL could not process payload (EL behind)",
			"beaconBlockRoot", beaconBlockRoot, "blockHash", executionBlockHash, "err", err)
		if optErr := f.optimisticStore.AddOptimisticCandidate(beaconBlockRoot, block.Block); optErr != nil {
			return payloadStatus, fmt.Errorf("failed to add block to optimistic store: %v", optErr)
		}
		return payloadStatus, errELBehind
	case execution_client.PayloadStatusNotValidated:
		log.Trace("validatePayloadWithEL: payload is not validated yet", "beaconBlockRoot", beaconBlockRoot)
		// optimistic block candidate
		if err := f.optimisticStore.AddOptimisticCandidate(beaconBlockRoot, block.Block); err != nil {
			return payloadStatus, fmt.Errorf("failed to add block to optimistic store: %v", err)
		}
	case execution_client.PayloadStatusValidated:
		log.Trace("validatePayloadWithEL: payload is validated", "beaconBlockRoot", beaconBlockRoot)
	}

	return payloadStatus, nil
}

// newPayloadLocked requires f.mu and reacquires it after the engine call.
func (f *ForkChoiceStore) newPayloadLocked(
	ctx context.Context,
	beaconBlockRoot common.Hash,
	payload *cltypes.Eth1Block,
	parentBlockRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (execution_client.PayloadStatus, error) {
	if f.payloadValidator == nil {
		f.payloadValidator = execution_client.NewPayloadValidationCoordinator(f.engine)
	}
	payloadValidator := f.payloadValidator
	f.mu.Unlock()
	defer f.mu.Lock()
	return payloadValidator.NewPayload(ctx, beaconBlockRoot, payload, parentBlockRoot, versionedHashes, executionRequestsList)
}

func (f *ForkChoiceStore) lockEnvelopeOwner(blockRoot common.Hash) func() {
	unlock, err := f.lockEnvelopeOwnerContext(context.Background(), blockRoot)
	if err != nil {
		panic(err)
	}
	return unlock
}

func (f *ForkChoiceStore) lockEnvelopeOwnerContext(ctx context.Context, blockRoot common.Hash) (func(), error) {
	f.envelopeOwnersMu.Lock()
	if f.envelopeOwners == nil {
		f.envelopeOwners = make(map[common.Hash]*envelopeOwner)
	}
	owner := f.envelopeOwners[blockRoot]
	if owner == nil {
		owner = &envelopeOwner{slot: make(chan struct{}, 1)}
		f.envelopeOwners[blockRoot] = owner
	}
	owner.refs++
	f.envelopeOwnersMu.Unlock()

	select {
	case owner.slot <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-owner.slot
			f.releaseEnvelopeOwnerReference(blockRoot, owner)
			return nil, err
		}
	case <-ctx.Done():
		f.releaseEnvelopeOwnerReference(blockRoot, owner)
		return nil, ctx.Err()
	}
	return func() {
		<-owner.slot
		f.releaseEnvelopeOwnerReference(blockRoot, owner)
	}, nil
}

func (f *ForkChoiceStore) releaseEnvelopeOwnerReference(blockRoot common.Hash, owner *envelopeOwner) {
	f.envelopeOwnersMu.Lock()
	owner.refs--
	if owner.refs == 0 && f.envelopeOwners[blockRoot] == owner {
		delete(f.envelopeOwners, blockRoot)
	}
	f.envelopeOwnersMu.Unlock()
}

func (f *ForkChoiceStore) acquirePendingEnvelopeRetry(ctx context.Context) (func(), error) {
	f.pendingEnvelopeRetryOnce.Do(func() {
		f.pendingEnvelopeRetrySlot = make(chan struct{}, 1)
	})
	select {
	case f.pendingEnvelopeRetrySlot <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-f.pendingEnvelopeRetrySlot
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return func() { <-f.pendingEnvelopeRetrySlot }, nil
}

// RetryPendingExecutionPayloadEnvelopes retries a bounded, fair batch of envelopes retained by fork choice.
func (f *ForkChoiceStore) RetryPendingExecutionPayloadEnvelopes(ctx context.Context, limit int) int {
	if limit <= 0 || ctx.Err() != nil {
		return 0
	}

	releaseRetry, err := f.acquirePendingEnvelopeRetry(ctx)
	if err != nil {
		return 0
	}
	defer releaseRetry()

	candidates := f.pendingExecutionPayloadEnvelopeCandidates(ctx, limit)
	attempted := 0
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			break
		}
		attempted++
		f.pendingEnvelopeRetryLocal = !candidate.local
		var err error
		if candidate.local {
			err = f.ApplyLocalSelfBuildEnvelope(ctx, candidate.entry.envelope)
		} else {
			err = f.OnExecutionPayload(ctx, candidate.entry.envelope, true, true)
		}
		if err != nil && !errors.Is(err, errExecutionPayloadInvalid) {
			if err := f.rotatePendingExecutionPayloadEnvelope(ctx, candidate); err != nil {
				break
			}
			log.Debug("pending execution payload envelope retry deferred", "blockRoot", candidate.root, "local", candidate.local, "err", err)
			continue
		}
		if err := f.removePendingExecutionPayloadEnvelope(ctx, candidate); err != nil {
			break
		}
	}
	return attempted
}

func (f *ForkChoiceStore) pendingExecutionPayloadEnvelopeCandidates(ctx context.Context, limit int) []pendingExecutionPayloadEnvelope {
	var gossipRoots, localRoots []common.Hash
	if f.pendingEnvelopes != nil {
		gossipRoots = f.pendingEnvelopes.Keys()
	}
	if f.pendingLocalSelfBuildEnvelopes != nil {
		localRoots = f.pendingLocalSelfBuildEnvelopes.Keys()
	}
	candidates := make([]pendingExecutionPayloadEnvelope, 0, min(limit, len(gossipRoots)+len(localRoots)))
	gossipIndex, localIndex := 0, 0
	preferLocal := f.pendingEnvelopeRetryLocal
	for len(candidates) < limit && (gossipIndex < len(gossipRoots) || localIndex < len(localRoots)) {
		local := preferLocal
		if local && localIndex >= len(localRoots) {
			local = false
		} else if !local && gossipIndex >= len(gossipRoots) {
			local = true
		}

		var root common.Hash
		var entry *pendingExecutionPayloadEnvelopeEntry
		var ok bool
		if local {
			root = localRoots[localIndex]
			localIndex++
			entry, ok = f.pendingLocalSelfBuildEnvelopes.Peek(root)
		} else {
			root = gossipRoots[gossipIndex]
			gossipIndex++
			entry, ok = f.pendingEnvelopes.Peek(root)
		}
		if !ok {
			continue
		}
		if f.pendingExecutionPayloadEnvelopeExpired(root, entry) {
			if err := f.removePendingExecutionPayloadEnvelope(ctx, pendingExecutionPayloadEnvelope{root: root, entry: entry, local: local}); err != nil {
				return candidates
			}
			continue
		}
		candidates = append(candidates, pendingExecutionPayloadEnvelope{root: root, entry: entry, local: local})
		preferLocal = !local
	}
	return candidates
}

func (f *ForkChoiceStore) pendingExecutionPayloadEnvelopeExpired(root common.Hash, entry *pendingExecutionPayloadEnvelopeEntry) bool {
	if entry == nil || time.Since(entry.createdAt) > pendingExecutionPayloadEnvelopeExpiry {
		return true
	}
	checkpoint := f.finalizedCheckpoint.Load()
	if checkpoint == nil || f.beaconCfg == nil || f.beaconCfg.SlotsPerEpoch == 0 || f.forkGraph == nil {
		return false
	}
	block, ok := f.GetBlock(root)
	finalized := checkpoint.(solid.Checkpoint)
	finalizedSlot := f.computeStartSlotAtEpoch(finalized.Epoch)
	return ok && block != nil && block.Block != nil && block.Block.Slot <= finalizedSlot
}

func (f *ForkChoiceStore) removePendingExecutionPayloadEnvelope(ctx context.Context, candidate pendingExecutionPayloadEnvelope) error {
	unlockOwner, err := f.lockEnvelopeOwnerContext(ctx, candidate.root)
	if err != nil {
		return err
	}
	defer unlockOwner()

	cache := f.pendingEnvelopes
	if candidate.local {
		cache = f.pendingLocalSelfBuildEnvelopes
	}
	if cache == nil {
		return nil
	}
	current, ok := cache.Peek(candidate.root)
	if ok && current == candidate.entry {
		cache.Remove(candidate.root)
	}
	return nil
}

func (f *ForkChoiceStore) rotatePendingExecutionPayloadEnvelope(ctx context.Context, candidate pendingExecutionPayloadEnvelope) error {
	unlockOwner, err := f.lockEnvelopeOwnerContext(ctx, candidate.root)
	if err != nil {
		return err
	}
	defer unlockOwner()

	cache := f.pendingEnvelopes
	if candidate.local {
		cache = f.pendingLocalSelfBuildEnvelopes
	}
	if cache == nil {
		return nil
	}
	current, ok := cache.Peek(candidate.root)
	if ok && current == candidate.entry {
		cache.Get(candidate.root)
	}
	return nil
}

func (f *ForkChoiceStore) retainPendingExecutionPayloadEnvelope(signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, local bool) {
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return
	}
	cache := f.pendingEnvelopes
	if local {
		cache = f.pendingLocalSelfBuildEnvelopes
	}
	if cache != nil {
		root := signedEnvelope.Message.BeaconBlockRoot
		createdAt := time.Now()
		if current, ok := cache.Peek(root); ok && current != nil {
			createdAt = current.createdAt
		}
		cache.Add(root, &pendingExecutionPayloadEnvelopeEntry{envelope: signedEnvelope, createdAt: createdAt})
	}
}

func (f *ForkChoiceStore) writeEnvelopeIndices(ctx context.Context, blockRoot common.Hash, envelope *cltypes.ExecutionPayloadEnvelope, retryEnvelope *cltypes.SignedExecutionPayloadEnvelope, local bool) error {
	if f.db == nil {
		return nil
	}
	if err := f.persistEnvelopeIndices(ctx, blockRoot, envelope); err != nil {
		f.retainPendingExecutionPayloadEnvelope(retryEnvelope, local)
		return err
	}
	return nil
}

func (f *ForkChoiceStore) persistEnvelopeIndices(ctx context.Context, blockRoot common.Hash, envelope *cltypes.ExecutionPayloadEnvelope) error {
	if err := f.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, envelope)
	}); err != nil {
		return err
	}
	persistence, err := f.envelopePersistenceStore()
	if err != nil {
		return err
	}
	return persistence.MarkEnvelopeIndicesCommitted(blockRoot)
}

func (f *ForkChoiceStore) reconcilePendingEnvelopeIndices(ctx context.Context) error {
	if f.db == nil {
		return nil
	}
	persistence, err := f.envelopePersistenceStore()
	if err != nil {
		return err
	}
	roots, err := persistence.PendingEnvelopeIndexRoots()
	if err != nil {
		return err
	}
	for _, root := range roots {
		envelope, err := f.forkGraph.ReadEnvelopeFromDisk(root)
		if os.IsNotExist(err) {
			if err := persistence.MarkEnvelopeIndicesCommitted(root); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if envelope == nil || envelope.Message == nil || envelope.Message.Payload == nil || envelope.Message.BeaconBlockRoot != root {
			return fmt.Errorf("invalid pending envelope for block %x", root)
		}
		if err := f.persistEnvelopeIndices(ctx, root, envelope.Message); err != nil {
			return err
		}
	}
	return nil
}

func (f *ForkChoiceStore) prepareEnvelopeWithoutForkChoiceLock(blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) (func() error, error) {
	f.mu.Unlock()
	defer f.mu.Lock()
	persistence, err := f.envelopePersistenceStore()
	if err != nil {
		return nil, err
	}
	return persistence.PrepareEnvelopeOnDisk(blockRoot, signedEnvelope, true)
}

func (f *ForkChoiceStore) envelopePersistenceStore() (fork_graph.EnvelopePersistence, error) {
	if f.envelopePersistence != nil {
		return f.envelopePersistence, nil
	}
	persistence, ok := f.forkGraph.(fork_graph.EnvelopePersistence)
	if !ok {
		return nil, errors.New("fork graph does not support envelope persistence")
	}
	return persistence, nil
}

// applyEnvelope processes the envelope under f.mu: validates, verifies with CL and EL,
// and persists the envelope to disk. No CL state transition is performed — the
// execution effects are deferred to the next block's ProcessParentExecutionPayload.
// Returns (true, nil) if the envelope was applied,
// (false, nil) if it was skipped (already processed or block not yet known),
// or (false, err) on failure.
func (f *ForkChoiceStore) applyEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) (bool, error) {
	if signedEnvelope.Message == nil {
		log.Warn("[applyEnvelope] received signed envelope with nil message")
		return false, errors.New("signed envelope has nil message")
	}
	unlockOwner, err := f.lockEnvelopeOwnerContext(ctx, signedEnvelope.Message.BeaconBlockRoot)
	if err != nil {
		return false, err
	}
	defer unlockOwner()
	return f.applyEnvelopeOwned(ctx, signedEnvelope, checkBlobData, validatePayload)
}

func (f *ForkChoiceStore) applyEnvelopeOwned(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.applyEnvelopeLocked(ctx, signedEnvelope, checkBlobData, validatePayload, false)
}

func (f *ForkChoiceStore) validatePersistedEnvelopeOwned(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.applyEnvelopeLocked(ctx, signedEnvelope, checkBlobData, true, true)
	return err
}

// applyEnvelopeLocked is the lock-held implementation of applyEnvelope.
// The caller MUST hold f.mu before calling this method.
// Returns (true, nil) if the envelope was applied,
// (false, nil) if it was skipped (already processed or block not yet known),
// or (false, err) on failure.
func (f *ForkChoiceStore) applyEnvelopeLocked(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload, alreadyPersisted bool) (bool, error) {
	if signedEnvelope.Message == nil {
		log.Warn("[applyEnvelopeLocked] received signed envelope with nil message")
		return false, fmt.Errorf("%w: signed envelope has nil message", errExecutionPayloadInvalid)
	}
	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	// Skip if envelope already processed and persisted
	if !alreadyPersisted && f.forkGraph.HasEnvelope(beaconBlockRoot) {
		return false, nil
	}

	// Envelope verification only reads the state (the consume-once
	// PreviousStateRoot it takes is restored), so the shared reference is safe
	// and avoids a full state copy per envelope under the write lock.
	blockState, err := f.forkGraph.GetState(beaconBlockRoot, false)
	if err != nil {
		return false, fmt.Errorf("OnExecutionPayload: failed to get block state: %w", err)
	}
	if blockState == nil {
		// Block hasn't arrived yet, queue envelope for later processing.
		// Per spec: assert envelope.beacon_block_root in store.block_states
		// Return an error so callers can distinguish "queued" from "applied".
		f.retainPendingExecutionPayloadEnvelope(signedEnvelope, false)
		log.Trace("OnExecutionPayload: block not found, queuing envelope for later", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block state not found for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Get the block to verify it exists
	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		f.retainPendingExecutionPayloadEnvelope(signedEnvelope, false)
		log.Trace("OnExecutionPayload: block not found in fork graph, queuing envelope", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block not found in fork graph for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Validate envelope against block (bid matching + signature verification)
	if validatePayload {
		if err := f.validateEnvelopeAgainstBlock(signedEnvelope, block, blockState); err != nil {
			return false, fmt.Errorf("%w: OnExecutionPayload: envelope validation failed: %w", errExecutionPayloadInvalid, err)
		}
	}

	// Check blob data availability
	if checkBlobData {
		if err := f.checkDataAvailability(ctx, block, common.Hash(beaconBlockRoot)); err != nil {
			return false, err
		}
	}

	// Validate payload with EL
	var elBehind bool
	var payloadValidated bool
	if validatePayload {
		payloadStatus, validationErr := f.validatePayloadWithELLocked(ctx, envelope, block, common.Hash(beaconBlockRoot))
		payloadValidated = payloadStatus == execution_client.PayloadStatusValidated
		if validationErr != nil {
			switch {
			case errors.Is(validationErr, errELBehind):
				// EL is behind (e.g. parent block not yet available after forward sync).
				// Proceed with persisting the envelope so HasEnvelope() returns true.
				// The execution block will be fed to EL via blockCollector on the next Flush().
				elBehind = true
			case payloadStatus == execution_client.PayloadStatusInvalidated:
				return false, fmt.Errorf("%w: %w", errExecutionPayloadInvalid, validationErr)
			default:
				return false, validationErr
			}
		}
		if !alreadyPersisted && f.forkGraph.HasEnvelope(beaconBlockRoot) {
			return false, nil
		}
		blockState, err = f.forkGraph.GetState(beaconBlockRoot, false)
		if err != nil {
			return false, fmt.Errorf("OnExecutionPayload: failed to refresh block state: %w", err)
		}
		block, ok = f.forkGraph.GetBlock(beaconBlockRoot)
		if blockState == nil || !ok || block == nil {
			return false, fmt.Errorf("%w: block disappeared during payload validation for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
		}
	}

	// Ensure the correct state root is available for the beacon_block_root check
	// inside ProcessExecutionPayloadEnvelope. PreviousStateRoot() is consumptive
	// (cleared on read) and may have already been consumed by transitionSlot during
	// TransitionState, or by a replay in GetState. Re-setting it from the block's
	// known-correct StateRoot guarantees ProcessExecutionPayloadEnvelope can
	// reconstruct the block header root without relying on the incremental hash cache.
	blockState.SetPreviousStateRoot(block.Block.StateRoot)

	// Run ProcessExecutionPayloadEnvelope for CL-level verification (no state mutation).
	// Always use ValidatingMachine so that signature verification and all spec checks run,
	// regardless of whether the EL-level validatePayload flag is set.
	if err := transition.ValidatingMachine.ProcessExecutionPayloadEnvelope(blockState, signedEnvelope); err != nil {
		return false, fmt.Errorf("%w: OnExecutionPayload: failed to verify execution payload: %w", errExecutionPayloadInvalid, err)
	}

	// Persist envelope to disk — this marks the root as "has payload" in store.payloads
	if !alreadyPersisted {
		publish, err := f.prepareEnvelopeWithoutForkChoiceLock(beaconBlockRoot, signedEnvelope)
		if err != nil {
			return false, fmt.Errorf("OnExecutionPayload: failed to dump envelope: %w", err)
		}
		if err := publish(); err != nil {
			return false, fmt.Errorf("OnExecutionPayload: failed to publish envelope: %w", err)
		}
	}
	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}
	if payloadValidated {
		f.markPayloadVerifiedLocked(beaconBlockRoot, envelope.Payload.BlockHash)
	}

	// Invalidate head cache — payload status may have changed from PENDING to FULL.
	// This forces GetHead to recompute on next call so GetHeadPayloadStatus is fresh.
	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending

	// If EL was behind, queue the block+envelope for later EL insertion.
	if elBehind {
		f.addPendingELPayload(block, signedEnvelope)
	}

	return true, nil
}

// StoreAnchorEnvelope persists an envelope to disk and updates eth2Roots without
// running the CL state transition. Used during checkpoint sync where the finalized
// state already includes the envelope's effects but forward sync needs the envelope
// on disk to resolve parent execution payloads for subsequent blocks.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) StoreAnchorEnvelope(blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return errors.New("StoreAnchorEnvelope: nil envelope")
	}
	envelope := signedEnvelope.Message
	if envelope.BeaconBlockRoot != blockRoot {
		return fmt.Errorf("StoreAnchorEnvelope: envelope root %v does not match block root %v", envelope.BeaconBlockRoot, blockRoot)
	}
	unlockOwner := f.lockEnvelopeOwner(blockRoot)
	defer unlockOwner()

	acceptedEnvelope := envelope
	var publish func() error
	if f.forkGraph.HasEnvelope(blockRoot) {
		persistedEnvelope, err := f.forkGraph.ReadEnvelopeFromDisk(blockRoot)
		if err != nil {
			return fmt.Errorf("StoreAnchorEnvelope: failed to read persisted envelope: %w", err)
		}
		if persistedEnvelope == nil || persistedEnvelope.Message == nil || persistedEnvelope.Message.Payload == nil || persistedEnvelope.Message.BeaconBlockRoot != blockRoot {
			return errors.New("StoreAnchorEnvelope: invalid persisted envelope")
		}
		acceptedEnvelope = persistedEnvelope.Message
	} else {
		var err error
		persistence, persistenceErr := f.envelopePersistenceStore()
		if persistenceErr != nil {
			return persistenceErr
		}
		publish, err = persistence.PrepareEnvelopeOnDisk(blockRoot, signedEnvelope, false)
		if err != nil {
			return fmt.Errorf("StoreAnchorEnvelope: failed to dump envelope: %w", err)
		}
	}

	f.mu.Lock()
	if publish != nil {
		if err := publish(); err != nil {
			f.mu.Unlock()
			return fmt.Errorf("StoreAnchorEnvelope: failed to publish envelope: %w", err)
		}
	}
	f.eth2Roots.Add(blockRoot, acceptedEnvelope.Payload.BlockHash)
	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending
	f.mu.Unlock()
	if f.db != nil {
		if err := f.persistEnvelopeIndices(context.Background(), blockRoot, acceptedEnvelope); err != nil {
			return fmt.Errorf("StoreAnchorEnvelope: failed to write indices: %w", err)
		}
	}

	return nil
}

// OnExecutionPayload processes an incoming execution payload envelope.
// Run upon receiving a new execution payload from the builder.
// If the corresponding block hasn't arrived yet, the envelope is queued and processed
// when the block is received via OnBlock.
//
// Parameters:
//   - checkBlobData: if true, verify blob data availability via PeerDAS before processing
//   - validatePayload: if true, call engine.NewPayload() to validate with EL before state transition
func (f *ForkChoiceStore) OnExecutionPayload(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) error {
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return fmt.Errorf("%w: nil execution payload envelope", errExecutionPayloadInvalid)
	}
	if signedEnvelope.Message.Payload == nil || signedEnvelope.Message.ExecutionRequests == nil {
		return fmt.Errorf("%w: incomplete execution payload envelope", errExecutionPayloadInvalid)
	}
	if signedEnvelope.Signature == common.Bytes96(bls.InfiniteSignature) {
		return fmt.Errorf("%w: unauthenticated execution payload envelope", errExecutionPayloadInvalid)
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot
	unlockOwner, err := f.lockEnvelopeOwnerContext(ctx, beaconBlockRoot)
	if err != nil {
		return err
	}
	defer unlockOwner()

	// Process envelope under f.mu; DB index write happens after unlock to avoid
	// deadlock with postForkchoiceOperations (which holds MDBX tx then needs f.mu.RLock).
	applied, err := f.applyEnvelopeOwned(ctx, signedEnvelope, checkBlobData, validatePayload)
	if err != nil {
		if !errors.Is(err, errExecutionPayloadInvalid) {
			f.retainPendingExecutionPayloadEnvelope(signedEnvelope, false)
		}
		return err
	}
	acceptedEnvelope := envelope
	if !applied {
		if !f.forkGraph.HasEnvelope(beaconBlockRoot) {
			return nil
		}
		if !validatePayload && f.db == nil {
			return nil
		}
		persistedEnvelope, readErr := f.forkGraph.ReadEnvelopeFromDisk(beaconBlockRoot)
		if readErr != nil {
			return fmt.Errorf("OnExecutionPayload: failed to read persisted envelope: %w", readErr)
		}
		if persistedEnvelope == nil || persistedEnvelope.Message == nil || persistedEnvelope.Message.Payload == nil || persistedEnvelope.Message.BeaconBlockRoot != beaconBlockRoot {
			return fmt.Errorf("%w: OnExecutionPayload: invalid persisted envelope", errExecutionPayloadInvalid)
		}
		callerIdentity, identityErr := signedEnvelope.Message.HashSSZ()
		if identityErr != nil {
			return fmt.Errorf("%w: OnExecutionPayload: failed to hash caller envelope message: %w", errExecutionPayloadInvalid, identityErr)
		}
		persistedIdentity, identityErr := persistedEnvelope.Message.HashSSZ()
		if identityErr != nil {
			return fmt.Errorf("%w: OnExecutionPayload: failed to hash persisted envelope message: %w", errExecutionPayloadInvalid, identityErr)
		}
		if callerIdentity != persistedIdentity {
			return fmt.Errorf("%w: OnExecutionPayload: caller does not match persisted envelope message", errExecutionPayloadInvalid)
		}
		callerWrapperIdentity, identityErr := signedEnvelope.HashSSZ()
		if identityErr != nil {
			return fmt.Errorf("%w: OnExecutionPayload: failed to hash caller envelope: %w", errExecutionPayloadInvalid, identityErr)
		}
		persistedWrapperIdentity, identityErr := persistedEnvelope.HashSSZ()
		if identityErr != nil {
			return fmt.Errorf("%w: OnExecutionPayload: failed to hash persisted envelope: %w", errExecutionPayloadInvalid, identityErr)
		}
		wrapperChanged := callerWrapperIdentity != persistedWrapperIdentity
		if wrapperChanged && !validatePayload {
			return fmt.Errorf("%w: OnExecutionPayload: signature replacement requires validation", errExecutionPayloadInvalid)
		}
		if validatePayload {
			if err := f.validatePersistedEnvelopeOwned(ctx, signedEnvelope, checkBlobData); err != nil {
				return err
			}
		}
		if wrapperChanged {
			persistence, persistenceErr := f.envelopePersistenceStore()
			if persistenceErr != nil {
				return persistenceErr
			}
			publishReplacement, err := persistence.PrepareEnvelopeOnDisk(beaconBlockRoot, signedEnvelope, true)
			if err != nil {
				f.retainPendingExecutionPayloadEnvelope(signedEnvelope, false)
				return fmt.Errorf("OnExecutionPayload: failed to prepare authenticated envelope replacement: %w", err)
			}
			if err := publishReplacement(); err != nil {
				f.retainPendingExecutionPayloadEnvelope(signedEnvelope, false)
				return fmt.Errorf("OnExecutionPayload: failed to publish authenticated envelope replacement: %w", err)
			}
		}
		acceptedEnvelope = signedEnvelope.Message
	}

	// Write execution block indices outside f.mu.
	if err := f.writeEnvelopeIndices(ctx, common.Hash(beaconBlockRoot), acceptedEnvelope, signedEnvelope, false); err != nil {
		return fmt.Errorf("OnExecutionPayload: failed to write execution payload indices: %w", err)
	}

	return nil
}

// ApplyLocalSelfBuildEnvelope processes a locally-produced self-build envelope
// that carries InfiniteSignature. The CL node constructs these when the VC does
// not provide a pre-signed envelope; the private key lives in the VC and is not
// available here.
//
// Unlike OnExecutionPayload, this method skips BLS signature verification
// (both the forkchoice-level check and the CL state-transition check) since
// we produced the envelope ourselves. EL validation via NewPayload still runs.
//
// This method MUST only be called from the local block production path.
// Gossip-received envelopes MUST go through OnExecutionPayload which always
// verifies BLS signatures.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) ApplyLocalSelfBuildEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return fmt.Errorf("%w: nil execution payload envelope", errExecutionPayloadInvalid)
	}
	if signedEnvelope.Message.Payload == nil || signedEnvelope.Message.ExecutionRequests == nil {
		return fmt.Errorf("%w: incomplete execution payload envelope", errExecutionPayloadInvalid)
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot
	unlockOwner, err := f.lockEnvelopeOwnerContext(ctx, beaconBlockRoot)
	if err != nil {
		return err
	}
	defer unlockOwner()

	applied, err := f.applyLocalSelfBuildEnvelopeOwned(ctx, signedEnvelope)
	if err != nil {
		if !errors.Is(err, errExecutionPayloadInvalid) {
			f.retainPendingExecutionPayloadEnvelope(signedEnvelope, true)
		}
		return err
	}
	acceptedEnvelope := envelope
	if !applied {
		if !f.forkGraph.HasEnvelope(beaconBlockRoot) || f.db == nil {
			return nil
		}
		persistedEnvelope, readErr := f.forkGraph.ReadEnvelopeFromDisk(beaconBlockRoot)
		if readErr != nil {
			return fmt.Errorf("ApplyLocalSelfBuildEnvelope: failed to read persisted envelope: %w", readErr)
		}
		if persistedEnvelope == nil || persistedEnvelope.Message == nil || persistedEnvelope.Message.Payload == nil || persistedEnvelope.Message.BeaconBlockRoot != beaconBlockRoot {
			return fmt.Errorf("%w: ApplyLocalSelfBuildEnvelope: invalid persisted envelope", errExecutionPayloadInvalid)
		}
		callerIdentity, identityErr := signedEnvelope.HashSSZ()
		if identityErr != nil {
			return fmt.Errorf("%w: ApplyLocalSelfBuildEnvelope: failed to hash caller envelope: %w", errExecutionPayloadInvalid, identityErr)
		}
		persistedIdentity, identityErr := persistedEnvelope.HashSSZ()
		if identityErr != nil {
			return fmt.Errorf("%w: ApplyLocalSelfBuildEnvelope: failed to hash persisted envelope: %w", errExecutionPayloadInvalid, identityErr)
		}
		if callerIdentity != persistedIdentity {
			return fmt.Errorf("%w: ApplyLocalSelfBuildEnvelope: caller does not match persisted envelope", errExecutionPayloadInvalid)
		}
		acceptedEnvelope = persistedEnvelope.Message
	}

	if err := f.writeEnvelopeIndices(ctx, common.Hash(beaconBlockRoot), acceptedEnvelope, signedEnvelope, true); err != nil {
		return fmt.Errorf("ApplyLocalSelfBuildEnvelope: failed to write execution payload indices: %w", err)
	}

	return nil
}

// applyLocalSelfBuildEnvelope acquires f.mu and delegates to the lock-held implementation.
func (f *ForkChoiceStore) applyLocalSelfBuildEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
	if signedEnvelope.Message == nil {
		return false, errors.New("signed envelope has nil message")
	}
	unlockOwner, err := f.lockEnvelopeOwnerContext(ctx, signedEnvelope.Message.BeaconBlockRoot)
	if err != nil {
		return false, err
	}
	defer unlockOwner()
	return f.applyLocalSelfBuildEnvelopeOwned(ctx, signedEnvelope)
}

func (f *ForkChoiceStore) applyLocalSelfBuildEnvelopeOwned(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.applyLocalSelfBuildEnvelopeLocked(ctx, signedEnvelope)
}

// applyLocalSelfBuildEnvelopeLocked is the lock-held implementation for local self-build envelopes.
// It mirrors applyEnvelopeLocked but skips signature verification by:
//   - Not calling validateEnvelopeAgainstBlock
//   - Using transition.DefaultMachine (FullValidation=false) instead of ValidatingMachine
//
// The caller MUST hold f.mu before calling this method.
// EL validation via NewPayload still runs.
func (f *ForkChoiceStore) applyLocalSelfBuildEnvelopeLocked(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
	if signedEnvelope.Message == nil {
		return false, errors.New("signed envelope has nil message")
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	if f.forkGraph.HasEnvelope(beaconBlockRoot) {
		return false, nil
	}

	blockState, err := f.forkGraph.GetState(beaconBlockRoot, false)
	if err != nil {
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeLocked: failed to get block state: %w", err)
	}
	if blockState == nil {
		f.retainPendingExecutionPayloadEnvelope(signedEnvelope, true)
		log.Trace("applyLocalSelfBuildEnvelopeLocked: block not found, queuing envelope for later", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block state not found for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		f.retainPendingExecutionPayloadEnvelope(signedEnvelope, true)
		log.Trace("applyLocalSelfBuildEnvelopeLocked: block not found in fork graph, queuing envelope", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block not found in fork graph for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Skip validateEnvelopeAgainstBlock — we produced this envelope locally.

	// Validate payload with EL (NewPayload).
	var elBehind bool
	payloadStatus, validationErr := f.validatePayloadWithELLocked(ctx, envelope, block, common.Hash(beaconBlockRoot))
	if validationErr != nil {
		switch {
		case errors.Is(validationErr, errELBehind):
			elBehind = true
		case payloadStatus == execution_client.PayloadStatusInvalidated:
			return false, fmt.Errorf("%w: %w", errExecutionPayloadInvalid, validationErr)
		default:
			return false, validationErr
		}
	}
	if f.forkGraph.HasEnvelope(beaconBlockRoot) {
		return false, nil
	}
	blockState, err = f.forkGraph.GetState(beaconBlockRoot, false)
	if err != nil {
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeLocked: failed to refresh block state: %w", err)
	}
	block, ok = f.forkGraph.GetBlock(beaconBlockRoot)
	if blockState == nil || !ok || block == nil {
		return false, fmt.Errorf("%w: block disappeared during payload validation for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	blockState.SetPreviousStateRoot(block.Block.StateRoot)

	// Use DefaultMachine (FullValidation=false) to skip BLS signature verification
	// in ProcessExecutionPayloadEnvelope while still running all other spec checks.
	if err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(blockState, signedEnvelope); err != nil {
		return false, fmt.Errorf("%w: applyLocalSelfBuildEnvelopeLocked: failed to verify execution payload: %w", errExecutionPayloadInvalid, err)
	}

	publish, err := f.prepareEnvelopeWithoutForkChoiceLock(beaconBlockRoot, signedEnvelope)
	if err != nil {
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeLocked: failed to dump envelope: %w", err)
	}
	if err := publish(); err != nil {
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeLocked: failed to publish envelope: %w", err)
	}
	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}
	if payloadStatus == execution_client.PayloadStatusValidated {
		f.markPayloadVerifiedLocked(beaconBlockRoot, envelope.Payload.BlockHash)
	}

	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending

	if elBehind {
		f.addPendingELPayload(block, signedEnvelope)
	}

	return true, nil
}
