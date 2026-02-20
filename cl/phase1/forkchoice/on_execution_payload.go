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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

// checkDataAvailability checks if blob data is available for the execution payload.
// For GLOAS, blob_kzg_commitments are in the committed bid, not directly in BeaconBlock.
// Returns nil if data is available, ErrEIP7594ColumnDataNotAvailable if not available yet.
// [New in Gloas:EIP7732]
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
// This is called BEFORE ProcessExecutionPayloadEnvelope to match Pre-GLOAS flow where
// NewPayload is called before state transition (AddChainSegment).
// [New in Gloas:EIP7732]
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
	var versionedHashes []common.Hash
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
	log.Debug("[validatePayloadWithEL] NewPayload", "status", payloadStatus, "beaconBlockRoot", beaconBlockRoot)

	switch payloadStatus {
	case execution_client.PayloadStatusNotValidated:
		log.Debug("validatePayloadWithEL: payload is not validated yet", "beaconBlockRoot", beaconBlockRoot)
		// TODO: Add optimistic candidate handling for GLOAS if needed
	case execution_client.PayloadStatusInvalidated:
		log.Warn("validatePayloadWithEL: payload is invalid", "beaconBlockRoot", beaconBlockRoot, "err", err)
		return errors.New("execution payload is invalid")
	case execution_client.PayloadStatusValidated:
		log.Trace("validatePayloadWithEL: payload is validated", "beaconBlockRoot", beaconBlockRoot)
	}

	if err != nil {
		return fmt.Errorf("validatePayloadWithEL: newPayload failed: %v", err)
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
//
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) OnExecutionPayload(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) error {
	if signedEnvelope == nil || signedEnvelope.Message == nil {
		return errors.New("nil execution payload envelope")
	}

	envelope := signedEnvelope.Message
	beaconBlockRoot := envelope.BeaconBlockRoot

	f.mu.Lock()
	defer f.mu.Unlock()

	// The corresponding beacon block root needs to be known
	blockStateCopy, err := f.forkGraph.GetState(beaconBlockRoot, true)
	if err != nil {
		return fmt.Errorf("OnExecutionPayload: failed to get block state: %w", err)
	}
	if blockStateCopy == nil {
		// Block hasn't arrived yet, queue envelope for later processing
		f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		log.Debug("OnExecutionPayload: block not found, queuing envelope for later", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return nil
	}

	// Get the block to verify it exists
	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		// Block state exists but block itself not found (shouldn't happen normally)
		f.pendingEnvelopes.Add(beaconBlockRoot, signedEnvelope)
		log.Debug("OnExecutionPayload: block not found in fork graph, queuing envelope", "beaconBlockRoot", common.Hash(beaconBlockRoot))
		return nil
	}

	// Check if blob data is available (skip during forward sync when data comes from snapshots)
	// If not available, the envelope may be queued and processed when data becomes available
	if checkBlobData {
		if err := f.checkDataAvailability(ctx, block, common.Hash(beaconBlockRoot)); err != nil {
			return err
		}
	}

	// Validate payload with EL BEFORE state transition (matches Pre-GLOAS flow)
	// Skip during forward sync when we trust the chain already validated by EL
	if validatePayload {
		if err := f.validatePayloadWithEL(ctx, envelope, block, common.Hash(beaconBlockRoot)); err != nil {
			return err
		}
	}

	// Process the execution payload for state transition
	if err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(blockStateCopy, signedEnvelope); err != nil {
		return fmt.Errorf("OnExecutionPayload: failed to process execution payload: %w", err)
	}

	// Update eth2Roots mapping for FCU (beaconBlockRoot -> executionBlockHash)
	if envelope.Payload != nil {
		f.eth2Roots.Add(beaconBlockRoot, envelope.Payload.BlockHash)
	}

	// Persist envelope to disk for recovery after restart.
	// The full state can be reconstructed via GetExecutionPayloadState() which replays the envelope.
	// HasEnvelope() checks disk for existence, replacing in-memory tracking.
	if err := f.forkGraph.DumpEnvelopeOnDisk(beaconBlockRoot, signedEnvelope); err != nil {
		return fmt.Errorf("OnExecutionPayload: failed to dump envelope: %w", err)
	}

	return nil
}
