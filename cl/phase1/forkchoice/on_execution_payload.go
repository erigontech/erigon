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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/transition"
)

// OnExecutionPayload processes an incoming execution payload envelope.
// Run upon receiving a new execution payload from the builder.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) OnExecutionPayload(ctx context.Context, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope) error {
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
		return errors.New("OnExecutionPayload: beacon block root not found in block_states")
	}

	// Get the block to verify it exists
	block, ok := f.forkGraph.GetBlock(beaconBlockRoot)
	if !ok || block == nil {
		return errors.New("OnExecutionPayload: beacon block not found")
	}

	// Check if blob data is available
	// If not, this payload MAY be queued and subsequently considered when blob data becomes available
	// TODO: Implement column data availability check for GLOAS

	// Process the execution payload for validation
	if err := transition.DefaultMachine.ProcessExecutionPayloadEnvelope(blockStateCopy, signedEnvelope); err != nil {
		return fmt.Errorf("OnExecutionPayload: failed to process execution payload: %w", err)
	}
	// Persist envelope to disk for recovery after restart.
	// The full state can be reconstructed via GetExecutionPayloadState() which replays the envelope.
	// HasEnvelope() checks disk for existence, replacing in-memory tracking.
	if err := f.forkGraph.DumpEnvelopeOnDisk(beaconBlockRoot, signedEnvelope); err != nil {
		return fmt.Errorf("OnExecutionPayload: failed to dump envelope: %w", err)
	}

	return nil
}
