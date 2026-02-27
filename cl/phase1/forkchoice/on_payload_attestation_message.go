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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

// OnPayloadAttestationMessage processes a payload attestation message and updates
// the PTC vote tracking in the store.
// Run upon receiving a new ptc_message from either within a block or directly on the wire.
// Returns ErrIgnore for IGNORE conditions, other errors for REJECT conditions.
// Caller should handle errors appropriately based on isFromBlock context.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) OnPayloadAttestationMessage(
	msg *cltypes.PayloadAttestationMessage,
	isFromBlock bool,
) error {
	if msg.Data == nil {
		return errors.New("nil payload attestation data")
	}

	data := msg.Data
	blockRoot := data.BeaconBlockRoot

	// PTC attestation must be for a known block
	blockState, err := f.forkGraph.GetState(blockRoot, false)
	if err != nil {
		return err
	}
	if blockState == nil {
		return fmt.Errorf("%w: block state not found for root %v", ErrIgnore, blockRoot)
	}

	// Get the PTC for the attestation slot
	ptc, err := blockState.GetPTC(data.Slot)
	if err != nil {
		return err
	}

	// PTC votes can only change the vote for their assigned beacon block
	if data.Slot != blockState.Slot() {
		return fmt.Errorf("%w: attestation slot %d does not match block slot %d", ErrIgnore, data.Slot, blockState.Slot())
	}

	// [REJECT] Check that the attester is from the PTC
	ptcIndex := -1
	for i, idx := range ptc {
		if idx == msg.ValidatorIndex {
			ptcIndex = i
			break
		}
	}
	if ptcIndex == -1 {
		return fmt.Errorf("validator %d is not in PTC for slot %d", msg.ValidatorIndex, data.Slot)
	}

	// Verify the signature and check that it's for the current slot if coming from wire
	if !isFromBlock {
		// [IGNORE] Check that the attestation is for the current slot
		if data.Slot != f.Slot() {
			return fmt.Errorf("%w: attestation slot %d is not current slot %d", ErrIgnore, data.Slot, f.Slot())
		}
		// [REJECT] Verify the signature
		indexedAttestation := &cltypes.IndexedPayloadAttestation{
			AttestingIndices: solid.NewRawUint64List(1, []uint64{msg.ValidatorIndex}),
			Data:             data,
			Signature:        msg.Signature,
		}
		valid, err := state.IsValidIndexedPayloadAttestation(blockState, indexedAttestation)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("invalid payload attestation signature")
		}
	}

	// Get or initialize the payload timeliness vote array for this block root
	var timelinessVotes [clparams.PtcSize]bool
	if existing, ok := f.payloadTimelinessVote.Load(blockRoot); ok {
		timelinessVotes = existing.([clparams.PtcSize]bool)
	}

	// Update the payload timeliness vote for the block
	timelinessVotes[ptcIndex] = data.PayloadPresent
	f.payloadTimelinessVote.Store(blockRoot, timelinessVotes)

	// Get or initialize the data availability vote array for this block root
	var dataAvailabilityVotes [clparams.PtcSize]bool
	if existing, ok := f.payloadDataAvailabilityVote.Load(blockRoot); ok {
		dataAvailabilityVotes = existing.([clparams.PtcSize]bool)
	}

	// Update the data availability vote for the block
	dataAvailabilityVotes[ptcIndex] = data.BlobDataAvailable
	f.payloadDataAvailabilityVote.Store(blockRoot, dataAvailabilityVotes)

	return nil
}
