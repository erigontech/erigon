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
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
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

// validatePayloadWithEL validates the execution payload with the execution layer engine.
// Called before ProcessExecutionPayloadEnvelope verification.
func (f *ForkChoiceStore) validatePayloadWithEL(
	ctx context.Context,
	envelope *cltypes.ExecutionPayloadEnvelope,
	block *cltypes.SignedBeaconBlock,
	beaconBlockRoot common.Hash,
) error {
	if f.engine == nil {
		return nil
	}

	// Get committed bid from the block (not from state, since state transition hasn't happened yet)
	committedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if committedBid == nil || committedBid.Message == nil {
		return errors.New("validatePayloadWithEL: block missing execution payload bid")
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
			return fmt.Errorf("validatePayloadWithEL: failed to compute versioned hashes: %w", err)
		}
	}

	// Get execution requests list
	var executionRequestsList []hexutil.Bytes
	if envelope.ExecutionRequests != nil {
		executionRequestsList = cltypes.GetExecutionRequestsList(f.beaconCfg, envelope.ExecutionRequests)
	}

	// Call NewPayload to validate execution payload with EL
	timeStartExec := time.Now()
	parentBlockRoot := block.Block.ParentRoot
	payloadStatus, err := f.engine.NewPayload(ctx, envelope.Payload, &parentBlockRoot, versionedHashes, executionRequestsList)
	monitor.ObserveNewPayloadTime(timeStartExec)
	log.Trace("[validatePayloadWithEL] NewPayload", "status", payloadStatus, "beaconBlockRoot", beaconBlockRoot)

	// Track payload status and gas limit by execution block hash for parent payload validation
	executionBlockHash := envelope.Payload.BlockHash
	f.executionPayloadStatus.Add(executionBlockHash, payloadStatus)
	f.executionPayloadGasLimit.Add(executionBlockHash, envelope.Payload.GasLimit)

	switch payloadStatus {
	case execution_client.PayloadStatusNone:
		// EL could not process the block (e.g. parent not yet available because
		// EL is still catching up after forward sync).  Return errELBehind so that
		// applyEnvelope can persist the envelope and queue the execution block
		// for later insertion into EL.
		log.Warn("validatePayloadWithEL: EL could not process payload (EL behind)",
			"beaconBlockRoot", beaconBlockRoot, "blockHash", executionBlockHash, "err", err)
		if optErr := f.optimisticStore.AddOptimisticCandidate(beaconBlockRoot, block.Block); optErr != nil {
			return fmt.Errorf("failed to add block to optimistic store: %v", optErr)
		}
		return errELBehind
	case execution_client.PayloadStatusNotValidated:
		log.Trace("validatePayloadWithEL: payload is not validated yet", "beaconBlockRoot", beaconBlockRoot)
		// optimistic block candidate
		if err := f.optimisticStore.AddOptimisticCandidate(beaconBlockRoot, block.Block); err != nil {
			return fmt.Errorf("failed to add block to optimistic store: %v", err)
		}
	case execution_client.PayloadStatusInvalidated:
		log.Warn("validatePayloadWithEL: payload is invalid", "beaconBlockRoot", beaconBlockRoot, "err", err)
		f.forkGraph.MarkHeaderAsInvalid(beaconBlockRoot)
		// remove from optimistic candidate
		if err := f.optimisticStore.InvalidateBlock(beaconBlockRoot, block.Block); err != nil {
			return fmt.Errorf("failed to remove block from optimistic store: %v", err)
		}
		return errors.New("execution payload is invalid")
	case execution_client.PayloadStatusValidated:
		log.Trace("validatePayloadWithEL: payload is validated", "beaconBlockRoot", beaconBlockRoot)
		// remove from optimistic candidate
		if err := f.optimisticStore.ValidateBlock(beaconBlockRoot, block.Block); err != nil {
			return fmt.Errorf("failed to validate block in optimistic store: %v", err)
		}
		f.verifiedExecutionPayload.Add(beaconBlockRoot, struct{}{})
	}

	if err != nil {
		return fmt.Errorf("validatePayloadWithEL: newPayload failed: %v", err)
	}

	return nil
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

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.applyEnvelopeLocked(ctx, signedEnvelope, checkBlobData, validatePayload)
}

// applyEnvelopeLocked is the lock-held implementation of applyEnvelope.
// The caller MUST hold f.mu before calling this method.
// Returns (true, nil) if the envelope was applied,
// (false, nil) if it was skipped (already processed or block not yet known),
// or (false, err) on failure.
func (f *ForkChoiceStore) applyEnvelopeLocked(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) (bool, error) {
	if signedEnvelope.Message == nil {
		log.Warn("[applyEnvelopeLocked] received signed envelope with nil message")
		return false, errors.New("signed envelope has nil message")
	}
	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	// Skip if envelope already processed and persisted
	if f.forkGraph.HasEnvelope(beaconBlockRoot) {
		return false, nil
	}

	// Get block state for verification.
	// ProcessExecutionPayloadEnvelope backfills the header root as part of verification,
	// but we don't want that to affect the canonical block state.
	blockState, err := f.forkGraph.GetState(beaconBlockRoot, false)
	if err != nil {
		return false, fmt.Errorf("OnExecutionPayload: failed to get block state: %w", err)
	}
	if blockState == nil {
		// Block hasn't arrived yet, queue envelope for later processing.
		// Per spec: assert envelope.beacon_block_root in store.block_states
		// Return an error so callers can distinguish "queued" from "applied".
		f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		log.Trace("OnExecutionPayload: block not found, queuing envelope for later", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block state not found for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Get the block to verify it exists
	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		log.Trace("OnExecutionPayload: block not found in fork graph, queuing envelope", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block not found in fork graph for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Validate envelope against block (bid matching + signature verification)
	if validatePayload {
		if err := f.validateEnvelopeAgainstBlock(signedEnvelope, block, blockState); err != nil {
			return false, fmt.Errorf("OnExecutionPayload: envelope validation failed: %w", err)
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
	if validatePayload {
		if err := f.validatePayloadWithEL(ctx, envelope, block, common.Hash(beaconBlockRoot)); err != nil {
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
		return false, fmt.Errorf("OnExecutionPayload: failed to verify execution payload: %w", err)
	}

	// Update eth2Roots mapping for FCU
	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}

	// Persist envelope to disk — this marks the root as "has payload" in store.payloads
	if err := f.forkGraph.DumpEnvelopeOnDisk(beaconBlockRoot, signedEnvelope); err != nil {
		return false, fmt.Errorf("OnExecutionPayload: failed to dump envelope: %w", err)
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
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return errors.New("StoreAnchorEnvelope: nil envelope")
	}
	envelope := signedEnvelope.Message

	f.mu.Lock()
	// Update eth2Roots mapping so FCU can resolve the EL block hash
	if envelope.Payload != nil {
		f.eth2Roots.Add(blockRoot, envelope.Payload.BlockHash)
	}
	// Persist to disk so HasEnvelope() returns true and forward sync can find it
	if err := f.forkGraph.DumpEnvelopeOnDisk(blockRoot, signedEnvelope); err != nil {
		f.mu.Unlock()
		return fmt.Errorf("StoreAnchorEnvelope: failed to dump envelope: %w", err)
	}
	f.mu.Unlock()

	// Write DB indices outside the lock
	if f.db != nil {
		ctx := context.Background()
		if err := f.db.Update(ctx, func(tx kv.RwTx) error {
			return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, envelope)
		}); err != nil {
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
		return errors.New("nil execution payload envelope")
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	// Process envelope under f.mu; DB index write happens after unlock to avoid
	// deadlock with postForkchoiceOperations (which holds MDBX tx then needs f.mu.RLock).
	applied, err := f.applyEnvelope(ctx, signedEnvelope, checkBlobData, validatePayload)
	if err != nil || !applied {
		return err
	}

	// Write execution block indices outside f.mu.
	if f.db != nil {
		if err := f.db.Update(ctx, func(tx kv.RwTx) error {
			return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, common.Hash(beaconBlockRoot), envelope)
		}); err != nil {
			return fmt.Errorf("OnExecutionPayload: failed to write execution payload indices: %w", err)
		}
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
		return errors.New("nil execution payload envelope")
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	applied, err := f.applyLocalSelfBuildEnvelope(ctx, signedEnvelope)
	if err != nil || !applied {
		return err
	}

	if f.db != nil {
		if err := f.db.Update(ctx, func(tx kv.RwTx) error {
			return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, common.Hash(beaconBlockRoot), envelope)
		}); err != nil {
			return fmt.Errorf("ApplyLocalSelfBuildEnvelope: failed to write execution payload indices: %w", err)
		}
	}

	return nil
}

// applyLocalSelfBuildEnvelope acquires f.mu and delegates to the lock-held implementation.
func (f *ForkChoiceStore) applyLocalSelfBuildEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
	if signedEnvelope.Message == nil {
		return false, errors.New("signed envelope has nil message")
	}

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
		f.pendingLocalSelfBuildEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		log.Trace("applyLocalSelfBuildEnvelopeLocked: block not found, queuing envelope for later", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block state not found for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		f.pendingLocalSelfBuildEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		log.Trace("applyLocalSelfBuildEnvelopeLocked: block not found in fork graph, queuing envelope", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block not found in fork graph for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Skip validateEnvelopeAgainstBlock — we produced this envelope locally.

	// Validate payload with EL (NewPayload).
	var elBehind bool
	if err := f.validatePayloadWithEL(ctx, envelope, block, common.Hash(beaconBlockRoot)); err != nil {
		if errors.Is(err, errELBehind) {
			elBehind = true
		} else {
			return false, err
		}
	}

	blockState.SetPreviousStateRoot(block.Block.StateRoot)

	// Use DefaultMachine (FullValidation=false) to skip BLS signature verification
	// in ProcessExecutionPayloadEnvelope while still running all other spec checks.
	if err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(blockState, signedEnvelope); err != nil {
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeLocked: failed to verify execution payload: %w", err)
	}

	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}

	if err := f.forkGraph.DumpEnvelopeOnDisk(beaconBlockRoot, signedEnvelope); err != nil {
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeLocked: failed to dump envelope: %w", err)
	}

	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending

	if elBehind {
		f.addPendingELPayload(block, signedEnvelope)
	}

	return true, nil
}
