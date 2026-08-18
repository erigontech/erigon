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

var errPayloadValidationAdmission = errors.New("payload validation admission canceled")

var errInvalidExecutionPayloadEnvelope = errors.New("invalid execution payload envelope")

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
	payloadStatus, err := f.newPayloadWhileYieldingForkChoiceLock(ctx, beaconBlockRoot, envelope.Payload, &parentBlockRoot, versionedHashes, executionRequestsList)
	log.Trace("[validatePayloadWithEL] NewPayload", "status", payloadStatus, "beaconBlockRoot", beaconBlockRoot)
	return payloadStatus, err
}

func (f *ForkChoiceStore) newPayloadWhileYieldingForkChoiceLock(
	ctx context.Context,
	beaconBlockRoot common.Hash,
	payload *cltypes.Eth1Block,
	parentBlockRoot *common.Hash,
	versionedHashes []common.Hash,
	executionRequestsList []hexutil.Bytes,
) (execution_client.PayloadStatus, error) {
	f.mu.Unlock()
	defer f.mu.Lock()
	return f.withPayloadValidationAdmission(ctx, func() (execution_client.PayloadStatus, error) {
		if f.forkGraph.HasEnvelope(beaconBlockRoot) {
			return execution_client.PayloadStatusValidated, nil
		}
		return f.engine.NewPayload(ctx, payload, parentBlockRoot, versionedHashes, executionRequestsList)
	})
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
	f.executionPayloadStatus.Add(executionBlockHash, payloadStatus)
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
		f.markPayloadInvalidLocked(beaconBlockRoot, executionBlockHash)
		return fmt.Errorf("%w: execution payload is invalid", errInvalidExecutionPayloadEnvelope)
	case execution_client.PayloadStatusValidated:
		log.Trace("validatePayloadWithEL: payload is validated", "beaconBlockRoot", beaconBlockRoot)
		f.markPayloadVerifiedLocked(beaconBlockRoot, executionBlockHash)
	}

	if validationErr != nil {
		return fmt.Errorf("validatePayloadWithEL: newPayload failed: %w", validationErr)
	}

	return nil
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
func (f *ForkChoiceStore) applyEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool, missingMode missingEnvelopeMode) (bool, error) {
	if signedEnvelope.Message == nil {
		log.Warn("[applyEnvelope] received signed envelope with nil message")
		return false, fmt.Errorf("%w: signed envelope has nil message", errInvalidExecutionPayloadEnvelope)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.applyEnvelopeCoordinated(ctx, signedEnvelope, checkBlobData, validatePayload, missingMode)
}

// applyEnvelopeCoordinated temporarily yields the caller-held fork-choice lock during EL validation.
// Returns (true, nil) if the envelope was applied,
// (false, nil) if it was skipped (already processed or block not yet known),
// or (false, err) on failure.
func (f *ForkChoiceStore) applyEnvelopeCoordinated(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool, missingMode missingEnvelopeMode) (bool, error) {
	if signedEnvelope.Message == nil {
		log.Warn("[applyEnvelopeCoordinated] received signed envelope with nil message")
		return false, fmt.Errorf("%w: signed envelope has nil message", errInvalidExecutionPayloadEnvelope)
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
		return false, fmt.Errorf("OnExecutionPayload: failed to get block state: %w", err)
	}
	if blockState == nil {
		if missingMode == queueMissingEnvelope {
			f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		}
		log.Trace("OnExecutionPayload: block state not found", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block state not found for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Get the block to verify it exists
	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		if missingMode == queueMissingEnvelope {
			f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		}
		log.Trace("OnExecutionPayload: block not found in fork graph", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return false, fmt.Errorf("%w: block not found in fork graph for beacon_block_root %v", ErrIgnore, common.Hash(beaconBlockRoot))
	}

	// Validate envelope against block (bid matching + signature verification)
	if validatePayload {
		if err := f.validateEnvelopeAgainstBlock(signedEnvelope, block, blockState); err != nil {
			return false, fmt.Errorf("%w: OnExecutionPayload: envelope validation failed: %w", errInvalidExecutionPayloadEnvelope, err)
		}
	}

	// Check blob data availability
	if checkBlobData {
		if err := f.checkDataAvailability(ctx, block, common.Hash(beaconBlockRoot)); err != nil {
			return false, err
		}
	}
	blockState.SetPreviousStateRoot(block.Block.StateRoot)
	if err := transition.ValidatingMachine.ProcessExecutionPayloadEnvelope(blockState, signedEnvelope); err != nil {
		return false, fmt.Errorf("%w: OnExecutionPayload: failed to verify execution payload: %w", errInvalidExecutionPayloadEnvelope, err)
	}

	// Validate payload with EL
	var elBehind bool
	if validatePayload && f.engine != nil {
		payloadStatus, validationErr := f.validatePayloadWithEL(ctx, envelope, block, common.Hash(beaconBlockRoot))
		if errors.Is(validationErr, errPayloadValidationAdmission) {
			return false, validationErr
		}
		if f.forkGraph.HasEnvelope(beaconBlockRoot) {
			return false, nil
		}
		block, err = f.refreshEnvelopeBlockLocked(beaconBlockRoot)
		if err != nil {
			return false, fmt.Errorf("OnExecutionPayload: failed to refresh block: %w", err)
		}
		if err := f.applyPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, common.Hash(beaconBlockRoot)); err != nil {
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
	if signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return errors.New("StoreAnchorEnvelope: nil envelope")
	}
	envelope := signedEnvelope.Message
	if envelope.BeaconBlockRoot != blockRoot {
		return fmt.Errorf("StoreAnchorEnvelope: envelope root %v does not match block root %v", envelope.BeaconBlockRoot, blockRoot)
	}

	f.mu.Lock()
	if err := f.forkGraph.DumpEnvelopeOnDisk(blockRoot, signedEnvelope); err != nil {
		f.mu.Unlock()
		return fmt.Errorf("StoreAnchorEnvelope: failed to dump envelope: %w", err)
	}
	f.eth2Roots.Add(blockRoot, envelope.Payload.BlockHash)
	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending
	f.mu.Unlock()

	if f.db != nil {
		ctx := context.Background()
		if err := f.db.Update(ctx, func(tx kv.RwTx) error {
			return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, envelope)
		}); err != nil {
			f.pendingEnvelopes.Add(blockRoot, signedEnvelope)
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
	applied, err := f.applyEnvelope(ctx, signedEnvelope, checkBlobData, validatePayload, queueMissingEnvelope)
	if err != nil {
		return err
	}
	indexEnvelope, err := f.ensureExecutionPayloadEnvelopeIndices(ctx, common.Hash(beaconBlockRoot), signedEnvelope, applied)
	if err != nil {
		f.pendingEnvelopes.Add(common.Hash(beaconBlockRoot), indexEnvelope)
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
	if signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return errors.New("execution payload envelope has nil payload")
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	applied, err := f.applyLocalSelfBuildEnvelope(ctx, signedEnvelope, queueMissingEnvelope)
	if err != nil {
		return err
	}
	indexEnvelope, err := f.ensureExecutionPayloadEnvelopeIndices(ctx, common.Hash(beaconBlockRoot), signedEnvelope, applied)
	if err != nil {
		f.pendingLocalSelfBuildEnvelopes.Add(common.Hash(beaconBlockRoot), indexEnvelope)
		return fmt.Errorf("ApplyLocalSelfBuildEnvelope: failed to write execution payload indices: %w", err)
	}

	return nil
}

func (f *ForkChoiceStore) ensureExecutionPayloadEnvelopeIndices(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied bool) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if f.db == nil || (!applied && !f.forkGraph.HasEnvelope(blockRoot)) {
		return signedEnvelope, nil
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
				return current.envelope, current.err
			case <-ctx.Done():
				return signedEnvelope, ctx.Err()
			}
		}
		return f.runExecutionPayloadEnvelopeIndexWrite(ctx, blockRoot, signedEnvelope, applied, write)
	}
}

var errExecutionPayloadIndexWritePanicked = errors.New("execution payload index write panicked")

func (f *ForkChoiceStore) runExecutionPayloadEnvelopeIndexWrite(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied bool, write *envelopeIndexWrite) (envelope *cltypes.SignedExecutionPayloadEnvelope, err error) {
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
	return f.writeExecutionPayloadEnvelopeIndices(ctx, blockRoot, signedEnvelope, applied)
}

func (f *ForkChoiceStore) writeExecutionPayloadEnvelopeIndices(ctx context.Context, blockRoot common.Hash, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, applied bool) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if !applied {
		persisted, err := f.forkGraph.ReadEnvelopeFromDisk(blockRoot)
		if err != nil {
			return nil, err
		}
		signedEnvelope = persisted
	}
	if signedEnvelope == nil || signedEnvelope.Message == nil || signedEnvelope.Message.Payload == nil {
		return signedEnvelope, errors.New("persisted execution payload envelope is incomplete")
	}
	indexed := false
	err := f.db.View(ctx, func(tx kv.Tx) error {
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
		return signedEnvelope, err
	}
	err = f.db.Update(ctx, func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, signedEnvelope.Message)
	})
	return signedEnvelope, err
}

// applyLocalSelfBuildEnvelope coordinates fork-choice ownership around local envelope processing.
func (f *ForkChoiceStore) applyLocalSelfBuildEnvelope(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, missingMode missingEnvelopeMode) (bool, error) {
	if signedEnvelope.Message == nil {
		return false, fmt.Errorf("%w: signed envelope has nil message", errInvalidExecutionPayloadEnvelope)
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
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeCoordinated: failed to get block state: %w", err)
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
		return false, fmt.Errorf("%w: applyLocalSelfBuildEnvelopeCoordinated: failed to verify execution payload: %w", errInvalidExecutionPayloadEnvelope, err)
	}

	// Validate payload with EL (NewPayload).
	var elBehind bool
	if f.engine != nil {
		payloadStatus, validationErr := f.validatePayloadWithEL(ctx, envelope, block, common.Hash(beaconBlockRoot))
		if errors.Is(validationErr, errPayloadValidationAdmission) {
			return false, validationErr
		}
		if f.forkGraph.HasEnvelope(beaconBlockRoot) {
			return false, nil
		}
		block, err = f.refreshEnvelopeBlockLocked(beaconBlockRoot)
		if err != nil {
			return false, fmt.Errorf("applyLocalSelfBuildEnvelopeCoordinated: failed to refresh block: %w", err)
		}
		if err := f.applyPayloadValidationResultLocked(payloadStatus, validationErr, envelope, block, common.Hash(beaconBlockRoot)); err != nil {
			if errors.Is(err, errELBehind) {
				elBehind = true
			} else {
				return false, err
			}
		}
	}

	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}

	if err := f.forkGraph.DumpEnvelopeOnDisk(beaconBlockRoot, signedEnvelope); err != nil {
		return false, fmt.Errorf("applyLocalSelfBuildEnvelopeCoordinated: failed to dump envelope: %w", err)
	}

	f.headHash = common.Hash{}
	f.headPayloadStatus = cltypes.PayloadStatusPending

	if elBehind {
		f.addPendingELPayload(block, signedEnvelope)
	}

	return true, nil
}
