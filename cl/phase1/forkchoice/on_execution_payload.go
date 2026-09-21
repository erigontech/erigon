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
	"time"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// errELBehind is returned by validatePayloadWithEL when the EL cannot process
// the payload because it hasn't caught up yet (e.g. parent block not available).
// applyEnvelope treats this as non-fatal: it proceeds with persisting the envelope
// and queues the execution block for later EL insertion.
var errELBehind = errors.New("EL behind: payload not processable yet")

var errPayloadValidationAdmission = errors.New("payload validation admission canceled")

// ErrExecutionPayloadEnvelopeIndicesPending reports a persisted envelope whose database indices are queued for retry.
var ErrExecutionPayloadEnvelopeIndicesPending = errors.New("execution payload envelope indices pending")

// ErrExecutionPayloadEnvelopePersistenceFailed reports a validated envelope that could not be persisted.
var ErrExecutionPayloadEnvelopePersistenceFailed = errors.New("execution payload envelope persistence failed")

var (
	ErrInvalidExecutionPayloadEnvelope = errors.New("invalid execution payload envelope")
	errPendingEnvelopeAgeBounded       = errors.New("pending execution payload envelope is age bounded")
)

func (f *ForkChoiceStore) claimEnvelopeIndexRepair(root common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied bool) (envelopeIndexRepairToken, bool, error) {
	return f.claimEnvelopeIndexRepairWith(root, signedEnvelope, applied, f.envelopeIndexRepairs.claim)
}

func (f *ForkChoiceStore) claimAnchorEnvelopeIndexRepair(root common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied bool) (envelopeIndexRepairToken, bool, error) {
	return f.claimEnvelopeIndexRepairWith(root, signedEnvelope, applied, f.envelopeIndexRepairs.claimAnchor)
}

func (f *ForkChoiceStore) claimEnvelopeIndexRepairWith(
	root common.Hash,
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	applied bool,
	claim func(common.Hash) (envelopeIndexRepairToken, bool),
) (envelopeIndexRepairToken, bool, error) {
	if f.db == nil || (!applied && !f.forkGraph.HasEnvelope(root)) {
		return envelopeIndexRepairToken{}, false, nil
	}
	token, ok := claim(root)
	if !ok {
		return envelopeIndexRepairToken{}, false, nil
	}
	if !token.valuesKnown {
		if !applied {
			var err error
			signedEnvelope, err = f.forkGraph.ReadEnvelopeFromDisk(root)
			if err != nil {
				if !f.forkGraph.HasEnvelope(root) {
					f.envelopeIndexRepairs.complete(token)
				}
				return token, true, err
			}
		}
		token = f.captureEnvelopeIndexRepairValues(token, signedEnvelope)
		if token.generation == 0 {
			return envelopeIndexRepairToken{}, false, nil
		}
		if !token.valuesKnown {
			if !f.forkGraph.HasEnvelope(root) {
				f.envelopeIndexRepairs.complete(token)
			}
			return token, true, errors.New("persisted execution payload envelope is incomplete")
		}
	}
	if applied {
		token = f.envelopeIndexRepairs.markNotify(token)
	}
	return token, true, nil
}

func (f *ForkChoiceStore) captureEnvelopeIndexRepairValues(token envelopeIndexRepairToken, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) envelopeIndexRepairToken {
	if signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return token
	}
	return f.envelopeIndexRepairs.setValues(token, signedEnvelope.Message.Payload.BlockNumber, signedEnvelope.Message.Payload.BlockHash)
}

func envelopeForIndexRepair(token envelopeIndexRepairToken) *cltypes.SignedExecutionPayloadEnvelope {
	return &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload: &cltypes.Eth1Block{BlockNumber: token.blockNumber, BlockHash: token.blockHash},
	}}
}

func (f *ForkChoiceStore) ensureClaimedEnvelopeIndexRepair(
	ctx context.Context,
	blockRoot common.Hash,
	token envelopeIndexRepairToken,
	tracked bool,
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	applied bool,
) (*cltypes.SignedExecutionPayloadEnvelope, bool, error) {
	if tracked && token.valuesKnown {
		envelope, indexed, err := f.ensureKnownExecutionPayloadEnvelopeIndices(ctx, token.root, envelopeForIndexRepair(token), false)
		return envelope, token.notify || indexed, err
	}
	return f.ensureExecutionPayloadEnvelopeIndices(ctx, blockRoot, signedEnvelope, applied)
}

// validateEnvelopeAgainstBlock validates the envelope against the block and state.
// This includes:
//   - bid matching (slot, builder_index, block_hash)
//   - builder signature verification
func (f *ForkChoiceStore) validateEnvelopeAgainstBlock(
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	blockState abstract.BeaconState,
) error {
	return f.validateEnvelopeAgainstBlockInternal(signedEnvelope, block, blockState, false, true)
}

func (f *ForkChoiceStore) validateEnvelopeAgainstBlockAfterCommitments(
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	blockState abstract.BeaconState,
) error {
	return f.validateEnvelopeAgainstBlockInternal(signedEnvelope, block, blockState, true, true)
}

func (f *ForkChoiceStore) validateEnvelopeAgainstBlockForGossip(
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	blockState abstract.BeaconState,
) error {
	return f.validateEnvelopeAgainstBlockInternal(signedEnvelope, block, blockState, true, false)
}

func (f *ForkChoiceStore) validateEnvelopeAgainstBlockInternal(
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	blockState abstract.BeaconState,
	commitmentsValidated bool,
	validateParentBeaconBlockRoot bool,
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
	if !commitmentsValidated {
		if err := cltypes.ValidateExecutionPayloadEnvelopeCommitments(f.beaconCfg, block, signedEnvelope); err != nil {
			return fmt.Errorf("invalid execution payload envelope commitments: %w", err)
		}
	}

	// Validate envelope.parent_beacon_block_root == state.latest_block_header.parent_root
	if validateParentBeaconBlockRoot && blockState != nil {
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

// validatePayloadWithEL validates the execution payload with the execution layer engine.
// Called before ProcessExecutionPayloadEnvelope verification.
func (f *ForkChoiceStore) validatePayloadWithEL(
	ctx context.Context,
	envelope *cltypes.ExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	beaconBlockRoot common.Hash,
) (execution_client.PayloadStatus, error) {
	if f.engine == nil {
		return execution_client.PayloadStatusNone, nil
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

	// Call NewPayload to validate execution payload with EL
	parentBlockRoot := block.Block.ParentRoot
	payloadStatus, err := f.newPayloadWhileYieldingForkChoiceLock(ctx, envelope.Payload, &parentBlockRoot, versionedHashes, executionRequestsList)
	log.Trace("[validatePayloadWithEL] NewPayload", "status", payloadStatus, "beaconBlockRoot", beaconBlockRoot)
	return payloadStatus, err
}

func (f *ForkChoiceStore) newPayloadWhileYieldingForkChoiceLock(
	ctx context.Context,
	payload *cltypes.Eth1Block,
	parentBlockRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (execution_client.PayloadStatus, error) {
	f.mu.Unlock()
	defer f.mu.Lock()
	return f.withPayloadValidationAdmission(ctx, func() (execution_client.PayloadStatus, error) {
		return f.engine.NewPayload(ctx, payload, parentBlockRoot, versionedHashes, executionRequestsList)
	})
}

// executionHashMarkedInvalid reports whether this execution payload is already known bad.
func (f *ForkChoiceStore) executionHashMarkedInvalid(executionBlockHash common.Hash) bool {
	// invalidatedExecutionPayloads outlives the bounded status caches, so it has to be
	// consulted too or the verdict is lost once the entry is evicted.
	if f.invalidatedExecutionPayloads != nil {
		if _, invalidated := f.invalidatedExecutionPayloads.Load(executionBlockHash); invalidated {
			return true
		}
	}
	if f.inFlightInvalidPayloads != nil {
		if _, invalidated := f.inFlightInvalidPayloads.Load(executionBlockHash); invalidated {
			return true
		}
	}
	if f.executionPayloadStatus == nil {
		return false
	}
	status, ok := f.executionPayloadStatus.Get(executionBlockHash)
	return ok && status == execution_client.PayloadStatusInvalidated
}

// rootMarkedInvalid reports whether the payload for this beacon root is already known bad.
func (f *ForkChoiceStore) rootMarkedInvalid(blockRoot common.Hash) bool {
	if f.payloadStatusByRoot == nil {
		return false
	}
	status, ok := f.payloadStatusByRoot.Get(blockRoot)
	return ok && status == execution_client.PayloadStatusInvalidated
}

// newPayloadForBlockWhileYieldingForkChoiceLock validates a pre-Gloas block's payload with
// the EL without holding f.mu. stillAdmissible is best effort: it runs under a read lock
// when that lock is free and is skipped when it is not, so the admission token is never
// held waiting on f.mu. Verdicts it accepts reach the status caches before the token is
// released, so a queued caller can short-circuit instead of re-asking the EL.
func (f *ForkChoiceStore) newPayloadForBlockWhileYieldingForkChoiceLock(
	ctx context.Context,
	blockRoot common.Hash,
	stillAdmissible func() error,
	derivedExecutionHash func() (common.Hash, bool),
	payload *cltypes.Eth1Block,
	parentBlockRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (execution_client.PayloadStatus, common.Hash, error) {
	var publishedInvalidHash common.Hash
	f.mu.Unlock()
	defer f.mu.Lock()
	status, err := f.withPayloadValidationAdmission(ctx, func() (execution_client.PayloadStatus, error) {
		// The wait for the token can be long enough for the block to go stale. The check
		// needs f.mu, but this owns the global token, so never wait for it: whoever holds
		// the lock may be in a slow EL call of its own and every payload would queue
		// behind that. Skipping costs an EL round trip plus the status and optimistic
		// entries the caller records before its own re-check rejects the block.
		if f.mu.TryRLock() {
			err := stillAdmissible()
			f.mu.RUnlock()
			if err != nil {
				return execution_client.PayloadStatusNone, err
			}
		}
		// Invalid is terminal and outranks a validated marker, matching markPayloadStatus.
		// The claimed hash is safe to read: only derived hashes are ever written, so a hit
		// means this payload really is the one the EL rejected.
		if f.rootMarkedInvalid(blockRoot) || f.executionHashMarkedInvalid(payload.BlockHash) {
			return execution_client.PayloadStatusInvalidated, nil
		}
		if f.verifiedExecutionPayload != nil && f.verifiedExecutionPayload.Contains(blockRoot) {
			return execution_client.PayloadStatusValidated, nil
		}
		status, err := f.engine.NewPayload(ctx, payload, parentBlockRoot, versionedHashes, executionRequestsList)
		switch status {
		case execution_client.PayloadStatusValidated:
			// A VALID status alongside an error is contradictory; the caller rejects it.
			// A root invalidated during the call stays invalid, so do not revive it.
			if err == nil && f.verifiedExecutionPayload != nil && !f.rootMarkedInvalid(blockRoot) {
				f.verifiedExecutionPayload.Add(blockRoot, struct{}{})
			}
		case execution_client.PayloadStatusInvalidated:
			// Cache nothing unless the request named its own payload. A mismatched hash is
			// rejected for naming the wrong payload, which says nothing about the content
			// of either one, and a root verdict cached here is later promoted to the
			// claimed hash by the caller's early invalid branch.
			executionHash, derived := derivedExecutionHash()
			if !derived || executionHash != payload.BlockHash {
				break
			}
			// Both clients report INVALID with the reason attached, so this cannot be
			// gated on err.
			if f.payloadStatusByRoot != nil {
				f.payloadStatusByRoot.Add(blockRoot, status)
			}
			if f.executionPayloadStatus != nil {
				f.executionPayloadStatus.Add(executionHash, status)
			}
			if f.inFlightInvalidPayloads != nil {
				f.inFlightInvalidPayloads.Store(executionHash, struct{}{})
				publishedInvalidHash = executionHash
			}
		}
		return status, err
	})
	return status, publishedInvalidHash, err
}

func (f *ForkChoiceStore) validateEnvelopePersistenceCommitmentsWhileYieldingForkChoiceLock(
	block *cltypes.SignedBeaconBlock,
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	validatePayload bool,
) error {
	return f.withForkChoiceLockYielded(func() error {
		return f.validateEnvelopePersistenceCommitments(block, signedEnvelope, validatePayload)
	})
}

func (f *ForkChoiceStore) withForkChoiceLockYielded(run func() error) error {
	f.mu.Unlock()
	defer f.mu.Lock()
	return run()
}

func (f *ForkChoiceStore) validatePayloadHashFallbackLocked(blockRoot, executionBlockHash common.Hash, validate func() error) error {
	if err := f.withForkChoiceLockYielded(validate); err != nil {
		return err
	}
	if f.payloadInvalidatedLocked(blockRoot, executionBlockHash) {
		f.markPayloadStatusIfRetainedLocked(blockRoot, executionBlockHash, execution_client.PayloadStatusInvalidated)
		return fmt.Errorf("%w: execution payload was invalidated during local payload hash validation", ErrInvalidExecutionPayloadEnvelope)
	}
	return nil
}

func (f *ForkChoiceStore) authenticatePayloadHashBeforeStatusProjectionLocked(
	payloadStatus execution_client.PayloadStatus,
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
) (*cltypes.SignedBeaconBlock, error) {
	envelope := signedEnvelope.Message
	blockRoot := envelope.BeaconBlockRoot
	executionBlockHash := envelope.Payload.BlockHash
	needsAuthentication := payloadStatus == execution_client.PayloadStatusNone || payloadStatus == execution_client.PayloadStatusInvalidated
	if !needsAuthentication && payloadStatus != execution_client.PayloadStatusValidated {
		needsAuthentication = f.payloadValidatedLocked(blockRoot, executionBlockHash)
	}
	if !needsAuthentication {
		return block, nil
	}
	if err := f.validatePayloadHashFallbackLocked(blockRoot, executionBlockHash, func() error {
		return cltypes.ValidateExecutionPayloadEnvelopeCommitments(f.beaconCfg, block, signedEnvelope)
	}); err != nil {
		if errors.Is(err, ErrInvalidExecutionPayloadEnvelope) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: EL did not authenticate payload hash and local validation failed: %w", ErrInvalidExecutionPayloadEnvelope, err)
	}
	block, err := f.refreshEnvelopeBlockLocked(blockRoot)
	return block, err
}

func (f *ForkChoiceStore) withPayloadValidationAdmission(ctx context.Context, validate func() (execution_client.PayloadStatus, error)) (execution_client.PayloadStatus, error) {

	f.payloadValidationOnce.Do(func() {
		f.payloadValidationAdmission = make(chan struct{}, 1)
	})
	select {
	case f.payloadValidationAdmission <- struct{}{}:
		defer func() { <-f.payloadValidationAdmission }()
	case <-ctx.Done():
		return execution_client.PayloadStatusNone, fmt.Errorf("%w: %w", errPayloadValidationAdmission, ctx.Err())
	}
	timeStartExec := time.Now()
	defer monitor.ObserveNewPayloadTime(timeStartExec)
	return validate()
}

// NewPayloadWithAdmission serializes EL payload validation across fork-choice and stage retries.
func (f *ForkChoiceStore) NewPayloadWithAdmission(
	ctx context.Context,
	payload *cltypes.Eth1Block,
	parentBlockRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (execution_client.PayloadStatus, error) {
	if f.engine == nil {
		return execution_client.PayloadStatusNone, errors.New("execution client is not configured")
	}
	return f.withPayloadValidationAdmission(ctx, func() (execution_client.PayloadStatus, error) {
		return f.engine.NewPayload(ctx, payload, parentBlockRoot, versionedHashes, executionRequestsList)
	})
}

func (f *ForkChoiceStore) applyPayloadValidationResultLocked(
	payloadStatus execution_client.PayloadStatus,
	validationErr error,
	envelope *cltypes.ExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	beaconBlockRoot common.Hash,
) error {
	// Track payload status and gas limit by execution block hash for parent payload validation
	executionBlockHash := envelope.Payload.BlockHash
	if err := validatePayloadValidationResult(payloadStatus, validationErr); err != nil {
		return err
	}
	if err := f.rejectKnownInvalidPayloadStatusLocked(payloadStatus, beaconBlockRoot, executionBlockHash); err != nil {
		return err
	}
	if payloadStatus != execution_client.PayloadStatusValidated && payloadStatus != execution_client.PayloadStatusInvalidated && f.payloadValidatedLocked(beaconBlockRoot, executionBlockHash) {
		f.markPayloadStatusIfRetainedLocked(beaconBlockRoot, executionBlockHash, execution_client.PayloadStatusValidated)
		return nil
	}
	if guard, ok := f.forkGraph.(retainedBlockGuard); ok {
		retained := guard.WithRetainedBlock(beaconBlockRoot, func() {
			payloadStatus = f.markPayloadStatusRetainedLocked(beaconBlockRoot, executionBlockHash, payloadStatus)
		})
		if !retained {
			return fmt.Errorf("%w: block disappeared during payload validation for beacon_block_root %v", ErrIgnore, beaconBlockRoot)
		}
	} else {
		payloadStatus = f.markPayloadStatusLocked(beaconBlockRoot, executionBlockHash, payloadStatus)
	}
	f.executionPayloadGasLimit.Add(executionBlockHash, envelope.Payload.GasLimit)

	switch payloadStatus {
	case execution_client.PayloadStatusNone:
		// EL could not process the block (e.g. parent not yet available because
		// EL is still catching up after forward sync).  Return errELBehind so that
		// applyEnvelope can persist the envelope and queue the execution block
		// for later insertion into EL.
		log.Warn("validatePayloadWithEL: EL could not process payload (EL behind)",
			"beaconBlockRoot", beaconBlockRoot, "blockHash", executionBlockHash, "err", validationErr)
		if optErr := f.optimisticStore.AddOptimisticCandidate(beaconBlockRoot, block.Block); optErr != nil {
			return fmt.Errorf("failed to add block to optimistic store: %w", optErr)
		}
		return errELBehind
	case execution_client.PayloadStatusNotValidated:
		log.Trace("validatePayloadWithEL: payload is not validated yet", "beaconBlockRoot", beaconBlockRoot)
		// optimistic block candidate
		if err := f.optimisticStore.AddOptimisticCandidate(beaconBlockRoot, block.Block); err != nil {
			return fmt.Errorf("failed to add block to optimistic store: %w", err)
		}
	case execution_client.PayloadStatusInvalidated:
		log.Warn("validatePayloadWithEL: payload is invalid", "beaconBlockRoot", beaconBlockRoot, "err", validationErr)
		return fmt.Errorf("%w: execution payload is invalid", ErrInvalidExecutionPayloadEnvelope)
	case execution_client.PayloadStatusValidated:
		log.Trace("validatePayloadWithEL: payload is validated", "beaconBlockRoot", beaconBlockRoot)
	}

	return nil
}

func validatePayloadValidationResult(payloadStatus execution_client.PayloadStatus, validationErr error) error {
	if validationErr != nil && payloadStatus != execution_client.PayloadStatusNone && payloadStatus != execution_client.PayloadStatusInvalidated {
		return fmt.Errorf("validatePayloadWithEL: newPayload failed: %w", validationErr)
	}
	if payloadStatus < execution_client.PayloadStatusNone || payloadStatus > execution_client.PayloadStatusValidated {
		return fmt.Errorf("validatePayloadWithEL: unexpected payload status %d", payloadStatus)
	}
	return nil
}

func (f *ForkChoiceStore) rejectKnownInvalidPayloadStatusLocked(payloadStatus execution_client.PayloadStatus, blockRoot, executionBlockHash common.Hash) error {
	if payloadStatus == execution_client.PayloadStatusInvalidated || !f.payloadInvalidatedLocked(blockRoot, executionBlockHash) {
		return nil
	}
	f.markPayloadStatusIfRetainedLocked(blockRoot, executionBlockHash, execution_client.PayloadStatusInvalidated)
	return fmt.Errorf("%w: execution payload was invalidated while validation was in progress", ErrInvalidExecutionPayloadEnvelope)
}

func (f *ForkChoiceStore) payloadInvalidatedLocked(blockRoot, executionBlockHash common.Hash) bool {
	return f.rootMarkedInvalid(blockRoot) || f.executionHashMarkedInvalid(executionBlockHash)
}

func (f *ForkChoiceStore) payloadValidatedLocked(blockRoot, executionBlockHash common.Hash) bool {
	if f.IsPayloadVerified(blockRoot) {
		return true
	}
	if f.payloadStatusByRoot != nil {
		if status, ok := f.payloadStatusByRoot.Get(blockRoot); ok && status == execution_client.PayloadStatusValidated {
			return true
		}
	}
	if f.executionPayloadStatus != nil {
		if status, ok := f.executionPayloadStatus.Get(executionBlockHash); ok && status == execution_client.PayloadStatusValidated {
			return true
		}
	}
	return false
}

func (f *ForkChoiceStore) applyTerminalPayloadValidationResultLocked(
	payloadStatus execution_client.PayloadStatus,
	validationErr error,
	envelope *cltypes.ExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	beaconBlockRoot common.Hash,
) (bool, error) {
	if err := validatePayloadValidationResult(payloadStatus, validationErr); err != nil {
		return true, err
	}
	if payloadStatus == execution_client.PayloadStatusInvalidated {
		return true, f.applyPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, beaconBlockRoot)
	}
	if f.payloadInvalidatedLocked(beaconBlockRoot, envelope.Payload.BlockHash) {
		f.markPayloadStatusIfRetainedLocked(beaconBlockRoot, envelope.Payload.BlockHash, execution_client.PayloadStatusInvalidated)
		return true, fmt.Errorf("%w: execution payload was invalidated while validation was in progress", ErrInvalidExecutionPayloadEnvelope)
	}
	if payloadStatus == execution_client.PayloadStatusValidated {
		return true, f.applyPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, beaconBlockRoot)
	}
	if f.payloadValidatedLocked(beaconBlockRoot, envelope.Payload.BlockHash) {
		f.markPayloadStatusIfRetainedLocked(beaconBlockRoot, envelope.Payload.BlockHash, execution_client.PayloadStatusValidated)
		return true, nil
	}
	return false, nil
}

func (f *ForkChoiceStore) refreshEnvelopeBlockLocked(beaconBlockRoot common.Hash) (*cltypes.SignedBeaconBlock, error) {
	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		return nil, fmt.Errorf("%w: block disappeared during payload validation for beacon_block_root %v", ErrIgnore, beaconBlockRoot)
	}
	return block, nil
}

type missingEnvelopeMode bool

const (
	retryQueuedEnvelope  missingEnvelopeMode = false
	queueMissingEnvelope missingEnvelopeMode = true
)

// applyEnvelope processes the envelope under f.mu except while waiting for EL validation.
// Returns (true, nil) if the envelope was applied,
// (false, nil) if it was skipped (already processed or block not yet known),
// or (false, err) on failure.
func (f *ForkChoiceStore) applyEnvelope(
	ctx context.Context,
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	checkBlobData bool,
	validatePayload bool,
	commitmentsValidated bool,
	missingMode missingEnvelopeMode,
	receivedAt time.Time,
) (bool, error) {
	if signedEnvelope.Message == nil {
		log.Warn("[applyEnvelope] received signed envelope with nil message")
		return false, fmt.Errorf("%w: signed envelope has nil message", ErrInvalidExecutionPayloadEnvelope)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.applyEnvelopeCoordinated(ctx, signedEnvelope, checkBlobData, validatePayload, commitmentsValidated, missingMode, receivedAt)
}

func (f *ForkChoiceStore) ValidateExecutionPayloadEnvelopeForGossip(signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if err := f.validateExecutionPayloadEnvelopeInput(signedEnvelope); err != nil {
		return err
	}

	releaseValidation, err := f.acquireExecutionPayloadValidationRead(context.Background())
	if err != nil {
		return err
	}
	validationHeld := true
	lockHeld := true
	defer func() {
		if lockHeld {
			f.mu.RUnlock()
		}
		if validationHeld {
			releaseValidation()
		}
	}()
	root := signedEnvelope.Message.BeaconBlockRoot
	blockState, err := f.forkGraph.GetState(root, true)
	if err != nil || blockState == nil {
		return fmt.Errorf("%w: beacon block state %v is unavailable", ErrIgnore, root)
	}
	block, ok := f.forkGraph.GetBlock(root)
	finalizedSlot := f.computeStartSlotAtEpoch(f.FinalizedCheckpoint().Epoch)
	f.mu.RUnlock()
	lockHeld = false
	if !ok || block == nil || block.Block == nil {
		return fmt.Errorf("%w: beacon block %v is unavailable", ErrIgnore, root)
	}
	if signedEnvelope.Message.Payload.SlotNumber < finalizedSlot {
		return fmt.Errorf("%w: envelope slot %d is before finalized slot %d", ErrIgnore, signedEnvelope.Message.Payload.SlotNumber, finalizedSlot)
	}
	if err := f.validateEnvelopeAgainstBlockForGossip(signedEnvelope, block, blockState); err != nil {
		releaseValidation()
		validationHeld = false
		return fmt.Errorf("execution payload envelope failed gossip validation: %w", err)
	}
	releaseValidation()
	validationHeld = false
	f.mu.RLock()
	lockHeld = true
	finalizedSlot = f.computeStartSlotAtEpoch(f.FinalizedCheckpoint().Epoch)
	f.mu.RUnlock()
	lockHeld = false
	if signedEnvelope.Message.Payload.SlotNumber < finalizedSlot {
		return fmt.Errorf("%w: envelope slot %d is before finalized slot %d", ErrIgnore, signedEnvelope.Message.Payload.SlotNumber, finalizedSlot)
	}
	return nil
}

// acquireExecutionPayloadValidationRead returns with f.mu held and never waits for it while holding validation capacity.
func (f *ForkChoiceStore) acquireExecutionPayloadValidationRead(ctx context.Context) (func(), error) {
	f.executionPayloadValidationOnce.Do(func() {
		f.executionPayloadValidation = make(chan struct{}, 1)
	})
	for {
		select {
		case f.executionPayloadValidation <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if f.mu.TryRLock() {
			return func() { <-f.executionPayloadValidation }, nil
		}
		<-f.executionPayloadValidation
		f.mu.RLock()
		f.mu.RUnlock() //nolint:gocritic,staticcheck // This pair waits for a queued writer before retrying.
	}
}

func (f *ForkChoiceStore) ClaimExecutionPayloadEnvelopeForGossip(
	ctx context.Context,
	beaconBlockRoot common.Hash,
	builderIndex uint64,
) (ExecutionPayloadEnvelopeAdmissionToken, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionPayloadEnvelopeAdmissionToken{}, err
	}
	token, err := f.envelopeGossipAdmissions.Claim(ctx, beaconBlockRoot, builderIndex)
	if err != nil {
		return ExecutionPayloadEnvelopeAdmissionToken{}, err
	}
	if f.forkGraph.HasEnvelope(beaconBlockRoot) {
		f.envelopeGossipAdmissions.Finish(token, true)
		return ExecutionPayloadEnvelopeAdmissionToken{}, ErrExecutionPayloadEnvelopeLookupRequired
	}
	if err := ctx.Err(); err != nil {
		f.envelopeGossipAdmissions.Finish(token, false)
		return ExecutionPayloadEnvelopeAdmissionToken{}, err
	}
	return token, nil
}

func (f *ForkChoiceStore) TryClaimExecutionPayloadEnvelopeForGossip(
	beaconBlockRoot common.Hash,
	builderIndex uint64,
) (ExecutionPayloadEnvelopeAdmissionToken, error) {
	if f.forkGraph.HasEnvelope(beaconBlockRoot) {
		return ExecutionPayloadEnvelopeAdmissionToken{}, ErrExecutionPayloadEnvelopeLookupRequired
	}
	token, err := f.envelopeGossipAdmissions.TryClaim(beaconBlockRoot, builderIndex)
	if err != nil {
		return ExecutionPayloadEnvelopeAdmissionToken{}, err
	}
	if f.forkGraph.HasEnvelope(beaconBlockRoot) {
		f.envelopeGossipAdmissions.Finish(token, true)
		return ExecutionPayloadEnvelopeAdmissionToken{}, ErrExecutionPayloadEnvelopeLookupRequired
	}
	return token, nil
}

func (f *ForkChoiceStore) FinishExecutionPayloadEnvelopeForGossip(token ExecutionPayloadEnvelopeAdmissionToken, seen bool) {
	f.envelopeGossipAdmissions.Finish(token, seen)
}

func (f *ForkChoiceStore) ForgetExecutionPayloadEnvelopeForGossip(beaconBlockRoot common.Hash, builderIndex uint64) {
	f.envelopeGossipAdmissions.ForgetSeen(beaconBlockRoot, builderIndex)
}

func (f *ForkChoiceStore) ValidateExecutionPayloadEnvelopeForConsensus(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if err := f.validateExecutionPayloadEnvelopeInput(signedEnvelope); err != nil {
		return err
	}

	f.mu.RLock()
	root := signedEnvelope.Message.BeaconBlockRoot
	blockState, err := f.forkGraph.GetState(root, true)
	if err != nil || blockState == nil {
		f.mu.RUnlock()
		return fmt.Errorf("beacon block state %v is unavailable", root)
	}
	block, ok := f.forkGraph.GetBlock(root)
	finalizedSlot := f.computeStartSlotAtEpoch(f.FinalizedCheckpoint().Epoch)
	f.mu.RUnlock()
	if !ok || block == nil || block.Block == nil {
		return fmt.Errorf("beacon block %v is unavailable", root)
	}
	if signedEnvelope.Message.Payload.SlotNumber < finalizedSlot {
		return fmt.Errorf("envelope slot %d is before finalized slot %d", signedEnvelope.Message.Payload.SlotNumber, finalizedSlot)
	}
	if err := f.validateEnvelopeAgainstBlock(signedEnvelope, block, blockState); err != nil {
		return fmt.Errorf("execution payload envelope failed gossip validation: %w", err)
	}
	if err := f.checkDataAvailability(ctx, block, root); err != nil {
		return err
	}
	blockState.SetPreviousStateRoot(block.Block.StateRoot)
	if err := transition.ValidatingMachine.ProcessExecutionPayloadEnvelope(blockState, signedEnvelope); err != nil {
		return fmt.Errorf("%w: execution payload envelope consensus validation failed: %w", ErrInvalidExecutionPayloadEnvelope, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	payloadStatus, err := f.validatePayloadWithEL(ctx, signedEnvelope.Message, block, root)
	if err != nil {
		return fmt.Errorf("execution payload envelope EL validation failed: %w", err)
	}
	if payloadStatus != execution_client.PayloadStatusValidated {
		return fmt.Errorf("execution payload envelope is not fully validated by EL: status %d", payloadStatus)
	}
	if f.payloadInvalidatedLocked(root, signedEnvelope.Message.Payload.BlockHash) {
		return fmt.Errorf("%w: execution payload was invalidated during consensus validation", ErrInvalidExecutionPayloadEnvelope)
	}
	finalizedSlot = f.computeStartSlotAtEpoch(f.FinalizedCheckpoint().Epoch)
	if signedEnvelope.Message.Payload.SlotNumber < finalizedSlot {
		return fmt.Errorf("envelope slot %d is before finalized slot %d", signedEnvelope.Message.Payload.SlotNumber, finalizedSlot)
	}
	_, err = f.refreshEnvelopeBlockLocked(root)
	return err
}

func (f *ForkChoiceStore) validateExecutionPayloadEnvelopeInput(signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if signedEnvelope == nil {
		return errors.New("nil execution payload envelope")
	}
	if err := signedEnvelope.ValidateForConfig(f.beaconCfg); err != nil {
		return fmt.Errorf("invalid execution payload envelope: %w", err)
	}
	if err := signedEnvelope.ValidateForPersistence(f.beaconCfg); err != nil {
		return fmt.Errorf("unpersistable execution payload envelope: %w", err)
	}
	return nil
}

func (f *ForkChoiceStore) validateEnvelopePersistenceCommitments(block *cltypes.SignedBeaconBlock, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, validatePayload bool) error {
	if validatePayload && f.engine != nil {
		return cltypes.ValidateExecutionPayloadEnvelopeBidCommitments(f.beaconCfg, block, signedEnvelope)
	}
	return cltypes.ValidateExecutionPayloadEnvelopeCommitments(f.beaconCfg, block, signedEnvelope)
}

func (f *ForkChoiceStore) validateKnownEnvelopeCommitments(signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, validatePayload bool) (bool, error) {
	root := signedEnvelope.Message.BeaconBlockRoot
	f.mu.RLock()
	block, ok := f.forkGraph.GetBlock(root)
	f.mu.RUnlock()
	if !ok || block == nil {
		return false, nil
	}
	if err := f.validateEnvelopePersistenceCommitments(block, signedEnvelope, validatePayload); err != nil {
		return false, err
	}
	return true, nil
}

func (f *ForkChoiceStore) validatePendingEnvelopeCommitments(signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, validatePayload bool) (bool, error) {
	root := signedEnvelope.Message.BeaconBlockRoot
	f.mu.RLock()
	blockState, stateErr := f.forkGraph.GetState(root, false)
	if stateErr != nil || blockState == nil {
		f.mu.RUnlock()
		return false, nil
	}
	block, ok := f.forkGraph.GetBlock(root)
	f.mu.RUnlock()
	if !ok || block == nil {
		return false, nil
	}
	if err := f.validateEnvelopePersistenceCommitments(block, signedEnvelope, validatePayload); err != nil {
		return false, err
	}
	return true, nil
}

// applyEnvelopeCoordinated temporarily yields the caller-held fork-choice lock during EL validation.
// Returns (true, nil) if the envelope was applied,
// (false, nil) if it was skipped (already processed or block not yet known),
// or (false, err) on failure.
func (f *ForkChoiceStore) applyEnvelopeCoordinated(
	ctx context.Context,
	signedEnvelope *cltypes.SignedExecutionPayloadEnvelope,
	checkBlobData bool,
	validatePayload bool,
	commitmentsValidated bool,
	missingMode missingEnvelopeMode,
	receivedAt time.Time,
) (bool, error) {
	if signedEnvelope.Message == nil {
		log.Warn("[applyEnvelopeCoordinated] received signed envelope with nil message")
		return false, fmt.Errorf("%w: signed envelope has nil message", ErrInvalidExecutionPayloadEnvelope)
	}
	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	// Skip if envelope already processed and persisted
	if f.forkGraph.HasEnvelope(beaconBlockRoot) {
		return false, nil
	}

	// Envelope verification only reads the state (the consume-once
	// PreviousStateRoot it takes is restored), so the shared reference is safe
	// and avoids a full state copy per envelope under the write lock.
	blockState, err := f.forkGraph.GetState(beaconBlockRoot, false)
	if err != nil {
		return false, fmt.Errorf("%w: OnExecutionPayload: failed to get block state: %w", errPendingEnvelopeAgeBounded, err)
	}
	if blockState == nil {
		if missingMode == queueMissingEnvelope {
			f.rememberPendingEnvelopeArrival(signedEnvelope, receivedAt)
			f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		}
		log.Trace("OnExecutionPayload: block state not found", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block state not found for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Get the block to verify it exists
	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		if missingMode == queueMissingEnvelope {
			f.rememberPendingEnvelopeArrival(signedEnvelope, receivedAt)
			f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		}
		log.Trace("OnExecutionPayload: block not found in fork graph", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block not found in fork graph for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}
	if !commitmentsValidated {
		if err := f.validateEnvelopePersistenceCommitmentsWhileYieldingForkChoiceLock(block, signedEnvelope, validatePayload); err != nil {
			return false, fmt.Errorf("%w: OnExecutionPayload: invalid execution payload envelope commitments: %w", ErrInvalidExecutionPayloadEnvelope, err)
		}
		block, err = f.refreshEnvelopeBlockLocked(beaconBlockRoot)
		if err != nil {
			if missingMode == queueMissingEnvelope {
				f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, fmt.Errorf("OnExecutionPayload: failed to refresh block after commitment validation: %w", err)
		}
		blockState, err = f.forkGraph.GetState(beaconBlockRoot, false)
		if err != nil {
			return false, fmt.Errorf("%w: OnExecutionPayload: failed to refresh block state: %w", errPendingEnvelopeAgeBounded, err)
		}
		if blockState == nil {
			if missingMode == queueMissingEnvelope {
				f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, fmt.Errorf("%w: block state disappeared during commitment validation for beacon_block_root %v", ErrIgnore, beaconBlockRoot)
		}
	}

	// Validate envelope against block (bid matching + signature verification)
	if validatePayload {
		if err := f.validateEnvelopeAgainstBlockAfterCommitments(signedEnvelope, block, blockState); err != nil {
			return false, fmt.Errorf("%w: OnExecutionPayload: envelope validation failed: %w", ErrInvalidExecutionPayloadEnvelope, err)
		}
		f.recordExecutionPayloadArrival(beaconBlockRoot, block.Block.Slot, receivedAt)
	}

	// Check blob data availability
	if checkBlobData {
		if err := f.checkDataAvailability(ctx, block, common.Hash(beaconBlockRoot)); err != nil {
			if missingMode == queueMissingEnvelope && errors.Is(err, ErrEIP7594ColumnDataNotAvailable) {
				f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, err
		}
	}
	blockState.SetPreviousStateRoot(block.Block.StateRoot)
	if err := transition.ValidatingMachine.ProcessExecutionPayloadEnvelope(blockState, signedEnvelope); err != nil {
		return false, fmt.Errorf("%w: OnExecutionPayload: failed to verify execution payload: %w", ErrInvalidExecutionPayloadEnvelope, err)
	}

	// Validate payload with EL
	var elBehind bool
	if validatePayload && f.engine != nil {
		payloadStatus, validationErr := f.validatePayloadWithEL(ctx, envelope, block, common.Hash(beaconBlockRoot))
		if errors.Is(validationErr, errPayloadValidationAdmission) {
			if missingMode == queueMissingEnvelope && !f.forkGraph.HasEnvelope(beaconBlockRoot) {
				f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, validationErr
		}
		if err := validatePayloadValidationResult(payloadStatus, validationErr); err != nil {
			return false, err
		}
		block, err := f.authenticatePayloadHashBeforeStatusProjectionLocked(payloadStatus, signedEnvelope, block)
		if err != nil {
			if missingMode == queueMissingEnvelope && errors.Is(err, ErrIgnore) {
				f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, fmt.Errorf("OnExecutionPayload: payload hash authentication failed: %w", err)
		}
		validationApplied, err := f.applyTerminalPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, common.Hash(beaconBlockRoot))
		if err != nil {
			return false, err
		}
		if f.forkGraph.HasEnvelope(beaconBlockRoot) {
			if validationErr != nil && payloadStatus != execution_client.PayloadStatusInvalidated {
				return false, nil
			}
			block, err = f.refreshEnvelopeBlockLocked(beaconBlockRoot)
			if err != nil {
				return false, fmt.Errorf("OnExecutionPayload: failed to refresh completed block: %w", err)
			}
			if err := f.applyPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, beaconBlockRoot); err != nil && !errors.Is(err, errELBehind) {
				return false, err
			}
			return false, nil
		}
		block, err = f.refreshEnvelopeBlockLocked(beaconBlockRoot)
		if err != nil {
			return false, fmt.Errorf("OnExecutionPayload: failed to refresh block: %w", err)
		}
		if !validationApplied {
			err = f.applyPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, common.Hash(beaconBlockRoot))
		}
		if err != nil {
			if errors.Is(err, errELBehind) {
				// EL is behind (e.g. parent block not yet available after forward sync).
				// Proceed with persisting the envelope so HasEnvelope() returns true.
				// The execution block will be fed to EL via blockCollector on the next Flush().
				elBehind = true
			} else {
				return false, err
			}
		}
	}

	// Persist envelope to disk — this marks the root as "has payload" in store.payloads
	if err := f.forkGraph.DumpEnvelopeOnDisk(beaconBlockRoot, signedEnvelope); err != nil {
		return false, fmt.Errorf("%w: OnExecutionPayload: failed to dump envelope: %w", ErrExecutionPayloadEnvelopePersistenceFailed, err)
	}
	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}
	if f.engine == nil && envelope.Payload != nil {
		if _, retained := f.markPayloadStatusIfRetainedLocked(beaconBlockRoot, envelope.Payload.BlockHash, execution_client.PayloadStatusNotValidated); !retained {
			return false, fmt.Errorf("%w: block disappeared while storing payload status for beacon_block_root %v", ErrIgnore, beaconBlockRoot)
		}
	}

	// Payload status participates in Gloas head selection, so a change invalidates the cached head.
	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending

	// If EL was behind, queue the block+envelope for later EL insertion.
	if elBehind {
		f.addPendingELPayload(block, signedEnvelope)
	}

	return true, nil
}

func (f *ForkChoiceStore) ExecutionPayloadReceivedBefore(blockRoot common.Hash, deadline time.Time) bool {
	f.executionPayloadFirstSeenMu.Lock()
	defer f.executionPayloadFirstSeenMu.Unlock()
	arrival, ok := f.executionPayloadFirstSeen[blockRoot]
	return ok && arrival.receivedAt.Before(deadline)
}

func (f *ForkChoiceStore) recordExecutionPayloadArrival(blockRoot common.Hash, slot uint64, receivedAt time.Time) {
	f.executionPayloadFirstSeenMu.Lock()
	defer f.executionPayloadFirstSeenMu.Unlock()
	if f.executionPayloadFirstSeen == nil {
		f.executionPayloadFirstSeen = make(map[common.Hash]executionPayloadArrival)
	}
	if _, ok := f.executionPayloadFirstSeen[blockRoot]; !ok {
		f.executionPayloadFirstSeen[blockRoot] = executionPayloadArrival{receivedAt: receivedAt, slot: slot}
	}
}

func (f *ForkChoiceStore) pruneExecutionPayloadFirstSeen(pruneSlot uint64) {
	f.executionPayloadFirstSeenMu.Lock()
	defer f.executionPayloadFirstSeenMu.Unlock()
	for root, arrival := range f.executionPayloadFirstSeen {
		if arrival.slot < pruneSlot {
			delete(f.executionPayloadFirstSeen, root)
		}
	}
}

func (f *ForkChoiceStore) rememberPendingEnvelopeArrival(envelope *cltypes.SignedExecutionPayloadEnvelope, receivedAt time.Time) {
	key, ok := pendingEnvelopeArrivalIdentity(envelope)
	if !ok {
		return
	}
	f.initPendingEnvelopeArrival()
	f.pendingEnvelopeArrivalMu.Lock()
	defer f.pendingEnvelopeArrivalMu.Unlock()
	if !f.pendingEnvelopeArrival.Contains(key) {
		f.pendingEnvelopeArrival.Add(key, receivedAt)
	}
}

func (f *ForkChoiceStore) pendingEnvelopeReceivedAt(envelope *cltypes.SignedExecutionPayloadEnvelope, fallback time.Time) time.Time {
	key, ok := pendingEnvelopeArrivalIdentity(envelope)
	if !ok {
		return fallback
	}
	f.initPendingEnvelopeArrival()
	f.pendingEnvelopeArrivalMu.Lock()
	defer f.pendingEnvelopeArrivalMu.Unlock()
	receivedAt, ok := f.pendingEnvelopeArrival.Peek(key)
	if !ok {
		return fallback
	}
	return receivedAt
}

func (f *ForkChoiceStore) forgetPendingEnvelopeArrival(envelope *cltypes.SignedExecutionPayloadEnvelope) {
	key, ok := pendingEnvelopeArrivalIdentity(envelope)
	if !ok {
		return
	}
	f.initPendingEnvelopeArrival()
	f.pendingEnvelopeArrivalMu.Lock()
	defer f.pendingEnvelopeArrivalMu.Unlock()
	f.pendingEnvelopeArrival.Remove(key)
}

func (f *ForkChoiceStore) initPendingEnvelopeArrival() {
	f.pendingEnvelopeArrivalOnce.Do(func() {
		var err error
		f.pendingEnvelopeArrival, err = lru.New[pendingEnvelopeArrivalKey, time.Time]("pending_execution_payload_arrival", queueCacheSize)
		if err != nil {
			panic(err)
		}
	})
}

func pendingEnvelopeArrivalIdentity(envelope *cltypes.SignedExecutionPayloadEnvelope) (key pendingEnvelopeArrivalKey, ok bool) {
	if envelope == nil {
		return key, false
	}
	key.envelope = envelope
	defer func() {
		if recover() != nil {
			key.root = common.Hash{}
			key.envelope = envelope
		}
	}()
	root, err := envelope.HashSSZ()
	if err == nil {
		key.root = root
		key.envelope = nil
	}
	return key, true
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

	f.mu.Lock()
	applied := true
	if f.forkGraph.HasEnvelope(blockRoot) {
		persisted, readErr := f.forkGraph.ReadEnvelopeFromDisk(blockRoot)
		if readErr == nil && persisted != nil && persisted.Message != nil && persisted.Message.Payload != nil {
			signedEnvelope = persisted
			envelope = persisted.Message
			applied = false
		}
	}
	if applied {
		if err := f.forkGraph.DumpEnvelopeOnDisk(blockRoot, signedEnvelope); err != nil {
			f.mu.Unlock()
			return fmt.Errorf("%w: StoreAnchorEnvelope failed to dump envelope: %w", ErrExecutionPayloadEnvelopePersistenceFailed, err)
		}
	}
	if f.engine == nil {
		if _, retained := f.markPayloadStatusIfRetainedLocked(blockRoot, envelope.Payload.BlockHash, execution_client.PayloadStatusNotValidated); !retained {
			f.mu.Unlock()
			return fmt.Errorf("%w: block disappeared while storing anchor payload status for beacon_block_root %v", ErrIgnore, blockRoot)
		}
	}
	f.eth2Roots.Add(blockRoot, envelope.Payload.BlockHash)
	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending
	f.mu.Unlock()

	token, tracked, err := f.claimAnchorEnvelopeIndexRepair(blockRoot, signedEnvelope, applied)
	if err != nil {
		if tracked {
			return fmt.Errorf("%w: StoreAnchorEnvelope failed to load persisted index values: %w", ErrExecutionPayloadEnvelopeIndicesPending, err)
		}
		return fmt.Errorf("StoreAnchorEnvelope: %w", err)
	}
	indexEnvelope, _, err := f.ensureClaimedEnvelopeIndexRepair(context.Background(), blockRoot, token, tracked, signedEnvelope, applied)
	if tracked {
		token = f.captureEnvelopeIndexRepairValues(token, indexEnvelope)
	}
	if err != nil {
		if !tracked && f.pendingEnvelopes != nil {
			f.pendingEnvelopes.Add(blockRoot, indexEnvelope)
		}
		return fmt.Errorf("%w: StoreAnchorEnvelope failed to write indices: %w", ErrExecutionPayloadEnvelopeIndicesPending, err)
	}
	if tracked {
		f.envelopeIndexRepairs.complete(token)
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
	return f.OnExecutionPayloadAt(ctx, signedEnvelope, checkBlobData, validatePayload, time.Now())
}

func (*ForkChoiceStore) EmitsExecutionPayloadIntegrationEvents() bool {
	return true
}

func (f *ForkChoiceStore) OnExecutionPayloadAt(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool, receivedAt time.Time) error {
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return errors.New("nil execution payload envelope")
	}
	if err := signedEnvelope.ValidateForConfig(f.beaconCfg); err != nil {
		return fmt.Errorf("invalid execution payload envelope: %w", err)
	}
	if err := signedEnvelope.ValidateForPersistence(f.beaconCfg); err != nil {
		return fmt.Errorf("unpersistable execution payload envelope: %w", err)
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot
	commitmentsValidated, err := f.validateKnownEnvelopeCommitments(signedEnvelope, validatePayload)
	if err != nil {
		return fmt.Errorf("%w: OnExecutionPayload: invalid execution payload envelope commitments: %w", ErrInvalidExecutionPayloadEnvelope, err)
	}

	// Process envelope under f.mu; DB index write happens after unlock to avoid
	// deadlock with postForkchoiceOperations (which holds MDBX tx then needs f.mu.RLock).
	applied, err := f.applyEnvelope(ctx, signedEnvelope, checkBlobData, validatePayload, commitmentsValidated, queueMissingEnvelope, receivedAt)
	if err != nil {
		if errors.Is(err, ErrExecutionPayloadEnvelopePersistenceFailed) {
			f.pendingEnvelopes.Add(common.Hash(beaconBlockRoot), signedEnvelope)
		}
		return err
	}
	token, tracked, err := f.claimEnvelopeIndexRepair(common.Hash(beaconBlockRoot), signedEnvelope, applied)
	if err != nil {
		if tracked {
			return fmt.Errorf("%w: OnExecutionPayload failed to load persisted index values: %w", ErrExecutionPayloadEnvelopeIndicesPending, err)
		}
		return err
	}
	indexEnvelope, notify, err := f.ensureClaimedEnvelopeIndexRepair(ctx, common.Hash(beaconBlockRoot), token, tracked, signedEnvelope, applied)
	if tracked {
		token = f.captureEnvelopeIndexRepairValues(token, indexEnvelope)
	}
	if err != nil {
		if !tracked {
			f.pendingEnvelopes.Add(common.Hash(beaconBlockRoot), indexEnvelope)
		}
		return fmt.Errorf("%w: OnExecutionPayload: failed to write execution payload indices: %w", ErrExecutionPayloadEnvelopeIndicesPending, err)
	}
	if tracked {
		f.envelopeIndexRepairs.complete(token)
	}
	if notify {
		f.emitExecutionPayloadIntegrationEvents(common.Hash(beaconBlockRoot), indexEnvelope)
	}
	if !applied {
		return fmt.Errorf("%w: execution payload envelope already processed", ErrIgnore)
	}

	return nil
}

func (f *ForkChoiceStore) ValidateExecutionPayloadEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return errors.New("nil execution payload envelope")
	}
	releaseValidation, err := f.acquireExecutionPayloadValidationRead(ctx)
	if err != nil {
		return err
	}
	defer func() {
		f.mu.RUnlock()
		releaseValidation()
	}()
	blockRoot := common.Hash(signedEnvelope.Message.BeaconBlockRoot)
	block, ok := f.forkGraph.GetBlock(blockRoot)
	if !ok || block == nil {
		return fmt.Errorf("block not found for beacon_block_root %v", blockRoot)
	}
	blockState, err := f.forkGraph.GetState(blockRoot, false)
	if err != nil {
		return fmt.Errorf("failed to get block state: %w", err)
	}
	if blockState == nil {
		return fmt.Errorf("block state not found for beacon_block_root %v", blockRoot)
	}
	return f.validateEnvelopeAgainstBlock(signedEnvelope, block, blockState)
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
	if signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return errors.New("execution payload envelope has nil payload")
	}
	if err := signedEnvelope.ValidateForConfig(f.beaconCfg); err != nil {
		return fmt.Errorf("invalid execution payload envelope: %w", err)
	}
	if err := signedEnvelope.ValidateForPersistence(f.beaconCfg); err != nil {
		return fmt.Errorf("unpersistable execution payload envelope: %w", err)
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	applied, err := f.applyLocalSelfBuildEnvelope(ctx, signedEnvelope, queueMissingEnvelope)
	if err != nil {
		if errors.Is(err, ErrExecutionPayloadEnvelopePersistenceFailed) {
			f.pendingLocalSelfBuildEnvelopes.Add(common.Hash(beaconBlockRoot), signedEnvelope)
		}
		return err
	}
	token, tracked, err := f.claimEnvelopeIndexRepair(common.Hash(beaconBlockRoot), signedEnvelope, applied)
	if err != nil {
		if tracked {
			return fmt.Errorf("%w: ApplyLocalSelfBuildEnvelope failed to load persisted index values: %w", ErrExecutionPayloadEnvelopeIndicesPending, err)
		}
		return err
	}
	indexEnvelope, notify, err := f.ensureClaimedEnvelopeIndexRepair(ctx, common.Hash(beaconBlockRoot), token, tracked, signedEnvelope, applied)
	if tracked {
		token = f.captureEnvelopeIndexRepairValues(token, indexEnvelope)
	}
	if err != nil {
		if !tracked {
			f.pendingLocalSelfBuildEnvelopes.Add(common.Hash(beaconBlockRoot), indexEnvelope)
		}
		return fmt.Errorf("%w: ApplyLocalSelfBuildEnvelope failed to write execution payload indices: %w", ErrExecutionPayloadEnvelopeIndicesPending, err)
	}
	if tracked {
		f.envelopeIndexRepairs.complete(token)
	}
	if notify {
		f.emitExecutionPayloadIntegrationEvents(common.Hash(beaconBlockRoot), indexEnvelope)
	}

	return nil
}

func (f *ForkChoiceStore) emitExecutionPayloadIntegrationEvents(blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) {
	if f.emitters == nil || signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return
	}
	envelope := signedEnvelope.Message
	f.emitters.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{
		Slot: envelope.Payload.SlotNumber, BuilderIndex: envelope.BuilderIndex, BlockHash: envelope.Payload.BlockHash, BlockRoot: blockRoot,
		ExecutionOptimistic: f.IsRootOptimistic(blockRoot),
	})
	f.emitters.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{
		Slot: envelope.Payload.SlotNumber, BlockRoot: blockRoot,
	})
	if f.beaconCfg == nil {
		return
	}
	block, ok := f.GetBlock(blockRoot)
	if !ok || block == nil || block.Block == nil {
		return
	}
	f.mu.RLock()
	headCached := f.headHash != (common.Hash{})
	f.mu.RUnlock()
	if !headCached && f.justifiedCheckpoint.Load() == nil {
		return
	}
	head, headSlot, headErr := f.GetHeadNode()
	if headErr != nil || head.Root != blockRoot || f.beaconCfg.SlotsPerEpoch == 0 {
		return
	}
	var headEvent *beaconevents.HeadV2Data
	if err := f.ViewStateAtBlockRoot(blockRoot, func(headState *state.CachingBeaconState) error {
		var err error
		headEvent, err = beaconevents.BuildHeadV2Data(
			f.beaconCfg,
			headState,
			headSlot,
			head.Root,
			block.Block.StateRoot,
			"full",
			f.IsRootOptimistic(blockRoot),
		)
		return err
	}); err != nil || headEvent == nil {
		return
	}
	f.emitters.WithHeadEventLock(func() {
		currentHead, currentHeadSlot, err := f.GetHeadNode()
		if err != nil || currentHead.Root != head.Root || currentHeadSlot != headSlot ||
			beaconevents.PayloadStatusName(currentHead.PayloadStatus) != headEvent.Data.PayloadStatus ||
			f.IsRootOptimistic(currentHead.Root) != headEvent.Data.ExecutionOptimistic {
			return
		}
		f.emitters.State().SendHeadV2(headEvent)
	})
}
func (f *ForkChoiceStore) ensureExecutionPayloadEnvelopeIndices(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied bool) (*cltypes.SignedExecutionPayloadEnvelope, bool, error) {
	return f.ensureExecutionPayloadEnvelopeIndicesWithTrust(ctx, blockRoot, signedEnvelope, applied, false)
}

func (f *ForkChoiceStore) ensureKnownExecutionPayloadEnvelopeIndices(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied bool) (*cltypes.SignedExecutionPayloadEnvelope, bool, error) {
	return f.ensureExecutionPayloadEnvelopeIndicesWithTrust(ctx, blockRoot, signedEnvelope, applied, true)
}

func (f *ForkChoiceStore) ensureExecutionPayloadEnvelopeIndicesWithTrust(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied, knownPersisted bool) (*cltypes.SignedExecutionPayloadEnvelope, bool, error) {
	if f.db == nil || (!applied && !f.forkGraph.HasEnvelope(blockRoot)) {
		return signedEnvelope, applied, nil
	}
	retried := false
	for {
		write := &envelopeIndexWrite{done: make(chan struct{})}
		existing, loaded := f.envelopeIndexWrites.LoadOrStore(blockRoot, write)
		if loaded {
			current := existing.(*envelopeIndexWrite)
			select {
			case <-current.done:
				if !retried && ctx.Err() == nil && (errors.Is(current.err, context.Canceled) || errors.Is(current.err, context.DeadlineExceeded) || errors.Is(current.err, errExecutionPayloadIndexWritePanicked)) {
					retried = true
					continue
				}
				return current.envelope, false, current.err
			case <-ctx.Done():
				return signedEnvelope, false, ctx.Err()
			}
		}
		return f.runExecutionPayloadEnvelopeIndexWrite(ctx, blockRoot, signedEnvelope, applied, knownPersisted, write)
	}
}

var errExecutionPayloadIndexWritePanicked = errors.New("execution payload index write panicked")

func (f *ForkChoiceStore) runExecutionPayloadEnvelopeIndexWrite(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied, knownPersisted bool, write *envelopeIndexWrite) (envelope *cltypes.SignedExecutionPayloadEnvelope, indexed bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			write.envelope = signedEnvelope
			write.err = fmt.Errorf("%w: %v", errExecutionPayloadIndexWritePanicked, recovered)
			f.envelopeIndexWrites.CompareAndDelete(blockRoot, write)
			close(write.done)
			panic(recovered)
		}
		write.envelope, write.err = envelope, err
		f.envelopeIndexWrites.CompareAndDelete(blockRoot, write)
		close(write.done)
	}()
	envelope, indexed, err = f.writeExecutionPayloadEnvelopeIndices(ctx, blockRoot, signedEnvelope, applied, knownPersisted)
	return envelope, (applied || indexed) && err == nil, err
}

func (f *ForkChoiceStore) writeExecutionPayloadEnvelopeIndices(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied, knownPersisted bool) (*cltypes.SignedExecutionPayloadEnvelope, bool, error) {
	if !applied && !knownPersisted {
		indexed, err := f.executionPayloadEnvelopeIndicesAreWellFormed(ctx, blockRoot)
		if err != nil {
			return nil, false, err
		}
		if indexed {
			return signedEnvelope, false, nil
		}
		signedEnvelope, err = f.forkGraph.ReadEnvelopeFromDisk(blockRoot)
		if err != nil {
			return signedEnvelope, false, err
		}
	}
	if signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return signedEnvelope, false, errors.New("persisted execution payload envelope is incomplete")
	}
	indexed := false
	err := f.db.View(ctx, func(tx kv.Tx) error {
		blockNumberBytes, err := tx.GetOne(kv.BlockRootToBlockNumber, blockRoot[:])
		if err != nil {
			return err
		}
		blockHashBytes, err := tx.GetOne(kv.BlockRootToBlockHash, blockRoot[:])
		if err != nil {
			return err
		}
		if len(blockNumberBytes) != 4 || len(blockHashBytes) != len(common.Hash{}) {
			return nil
		}
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		if err != nil {
			return err
		}
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		if err != nil {
			return err
		}
		indexed = blockNumber != nil && *blockNumber == signedEnvelope.Message.Payload.BlockNumber && blockHash == signedEnvelope.Message.Payload.BlockHash
		return nil
	})
	if err != nil || indexed {
		return signedEnvelope, false, err
	}
	err = f.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, signedEnvelope.Message)
	})
	return signedEnvelope, err == nil, err
}

func (f *ForkChoiceStore) executionPayloadEnvelopeIndicesAreWellFormed(ctx context.Context, blockRoot common.Hash) (bool, error) {
	indexed := false
	err := f.db.View(ctx, func(tx kv.Tx) error {
		blockNumber, err := tx.GetOne(kv.BlockRootToBlockNumber, blockRoot[:])
		if err != nil {
			return err
		}
		blockHash, err := tx.GetOne(kv.BlockRootToBlockHash, blockRoot[:])
		if err != nil {
			return err
		}
		indexed = len(blockNumber) == 4 && len(blockHash) == len(common.Hash{}) && common.BytesToHash(blockHash) != (common.Hash{})
		return nil
	})
	return indexed, err
}

// applyLocalSelfBuildEnvelope coordinates fork-choice ownership around local envelope processing.
func (f *ForkChoiceStore) applyLocalSelfBuildEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, missingMode missingEnvelopeMode) (bool, error) {
	if signedEnvelope.Message == nil {
		return false, fmt.Errorf("%w: signed envelope has nil message", ErrInvalidExecutionPayloadEnvelope)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.applyLocalSelfBuildEnvelopeCoordinated(ctx, signedEnvelope, missingMode)
}

// applyLocalSelfBuildEnvelopeCoordinated skips only BLS verification for locally produced envelopes.
func (f *ForkChoiceStore) applyLocalSelfBuildEnvelopeCoordinated(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, missingMode missingEnvelopeMode) (bool, error) {
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
		return false, fmt.Errorf("%w: applyLocalSelfBuildEnvelopeCoordinated: failed to get block state: %w", errPendingEnvelopeAgeBounded, err)
	}
	if blockState == nil {
		if missingMode == queueMissingEnvelope {
			f.pendingLocalSelfBuildEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		}
		log.Trace("applyLocalSelfBuildEnvelopeCoordinated: block state not found", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block state not found for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		if missingMode == queueMissingEnvelope {
			f.pendingLocalSelfBuildEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		}
		log.Trace("applyLocalSelfBuildEnvelopeCoordinated: block not found in fork graph", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block not found in fork graph for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Skip validateEnvelopeAgainstBlock — we produced this envelope locally.
	blockState.SetPreviousStateRoot(block.Block.StateRoot)
	if err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(blockState, signedEnvelope); err != nil {
		return false, fmt.Errorf("%w: applyLocalSelfBuildEnvelopeCoordinated: failed to verify execution payload: %w", ErrInvalidExecutionPayloadEnvelope, err)
	}

	// Validate payload with EL (NewPayload).
	var elBehind bool
	if f.engine != nil {
		payloadStatus, validationErr := f.validatePayloadWithEL(ctx, envelope, block, common.Hash(beaconBlockRoot))
		if errors.Is(validationErr, errPayloadValidationAdmission) {
			if missingMode == queueMissingEnvelope && !f.forkGraph.HasEnvelope(beaconBlockRoot) {
				f.pendingLocalSelfBuildEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, validationErr
		}
		if err := validatePayloadValidationResult(payloadStatus, validationErr); err != nil {
			return false, err
		}
		block, err := f.authenticatePayloadHashBeforeStatusProjectionLocked(payloadStatus, signedEnvelope, block)
		if err != nil {
			if missingMode == queueMissingEnvelope && errors.Is(err, ErrIgnore) {
				f.pendingLocalSelfBuildEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, fmt.Errorf("applyLocalSelfBuildEnvelopeCoordinated: payload hash authentication failed: %w", err)
		}
		validationApplied, err := f.applyTerminalPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, common.Hash(beaconBlockRoot))
		if err != nil {
			return false, err
		}
		if f.forkGraph.HasEnvelope(beaconBlockRoot) {
			return false, nil
		}
		block, err = f.refreshEnvelopeBlockLocked(beaconBlockRoot)
		if err != nil {
			return false, fmt.Errorf("applyLocalSelfBuildEnvelopeCoordinated: failed to refresh block: %w", err)
		}
		if !validationApplied {
			err = f.applyPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, common.Hash(beaconBlockRoot))
		}
		if err != nil {
			if errors.Is(err, errELBehind) {
				elBehind = true
			} else {
				return false, err
			}
		}
	} else {
		if err := f.validatePayloadHashFallbackLocked(beaconBlockRoot, envelope.Payload.BlockHash, func() error {
			return cltypes.ValidateExecutionPayloadEnvelopeCommitments(f.beaconCfg, block, signedEnvelope)
		}); err != nil {
			if errors.Is(err, ErrInvalidExecutionPayloadEnvelope) {
				return false, err
			}
			return false, fmt.Errorf("%w: applyLocalSelfBuildEnvelopeCoordinated: local payload hash validation failed: %w", ErrInvalidExecutionPayloadEnvelope, err)
		}
		if f.forkGraph.HasEnvelope(beaconBlockRoot) {
			return false, nil
		}
		block, err = f.refreshEnvelopeBlockLocked(beaconBlockRoot)
		if err != nil {
			if missingMode == queueMissingEnvelope {
				f.pendingLocalSelfBuildEnvelopes.Add(beaconBlockRoot, signedEnvelope)
			}
			return false, fmt.Errorf("applyLocalSelfBuildEnvelopeCoordinated: failed to refresh block after local payload hash validation: %w", err)
		}
	}

	if err := f.forkGraph.DumpEnvelopeOnDisk(beaconBlockRoot, signedEnvelope); err != nil {
		return false, fmt.Errorf("%w: applyLocalSelfBuildEnvelopeCoordinated: failed to dump envelope: %w", ErrExecutionPayloadEnvelopePersistenceFailed, err)
	}
	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}

	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending

	if elBehind {
		f.addPendingELPayload(block, signedEnvelope)
	}

	return true, nil
}
