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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// OnPayloadAttestationMessage processes a payload attestation message and updates
// the PTC vote tracking in the store.
// Run upon receiving a new ptc_message from either within a block or directly on the wire.
// Returns ErrIgnore for IGNORE conditions, other errors for REJECT conditions.
// Caller should handle errors appropriately based on isFromBlock context.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) OnPayloadAttestationMessage(
	ctx context.Context,
	msg *cltypes.PayloadAttestationMessage,
	isFromBlock bool,
) error {
	if msg == nil || msg.Data == nil {
		return errors.New("nil payload attestation data")
	}

	data := msg.Data
	blockRoot := data.BeaconBlockRoot

	if !isFromBlock {
		// Wall-clock time is authoritative for gossip because store time can lag OnTick.
		currentSlot := f.ethClock.GetCurrentSlot()
		if data.Slot != currentSlot {
			return fmt.Errorf("%w: attestation slot %d is not current slot %d", ErrIgnore, data.Slot, currentSlot)
		}
	}

	validationContext, err := f.payloadAttestationValidationContext(ctx, blockRoot, data.Slot)
	if err != nil {
		return err
	}
	ptcIndices, err := validationContext.ptcPositions(msg)
	if err != nil {
		return err
	}

	// Verify the signature if coming from wire.
	if !isFromBlock {
		if err := validationContext.validateSignature(msg); err != nil {
			return err
		}
	}

	return f.applyValidatedPayloadAttestation(msg.ValidatorIndex, ptcIndices, data, blockRoot, isFromBlock)
}

func (f *ForkChoiceStore) applyValidatedPayloadAttestation(
	validatorIndex uint64,
	ptcIndices []int,
	data *cltypes.PayloadAttestationData,
	blockRoot common.Hash,
	isFromBlock bool,
) error {
	f.ptcVoteMu.Lock()
	defer f.ptcVoteMu.Unlock()

	var timelinessVotes [clparams.PtcSize]int8
	if existing, ok := f.payloadTimelinessVote.Load(blockRoot); ok {
		timelinessVotes = existing.([clparams.PtcSize]int8)
	}
	var dataAvailabilityVotes [clparams.PtcSize]int8
	if existing, ok := f.payloadDataAvailabilityVote.Load(blockRoot); ok {
		dataAvailabilityVotes = existing.([clparams.PtcSize]int8)
	}
	if !isFromBlock {
		if f.payloadAttestationSeen != nil && data.Slot < f.payloadAttestationSeenSlot {
			return fmt.Errorf("%w: payload attestation validation completed after a newer slot", ErrIgnore)
		}
		if f.payloadAttestationSeen == nil || f.payloadAttestationSeenSlot < data.Slot {
			f.payloadAttestationSeenSlot = data.Slot
			f.payloadAttestationSeen = make(map[uint64]struct{}, clparams.PtcSize)
		}
		if _, seen := f.payloadAttestationSeen[validatorIndex]; seen {
			return fmt.Errorf("%w: already processed a valid payload attestation", ErrIgnore)
		}
		f.payloadAttestationSeen[validatorIndex] = struct{}{}
	}
	for _, idx := range ptcIndices {
		timelinessVotes[idx] = boolToVote(data.PayloadPresent)
		dataAvailabilityVotes[idx] = boolToVote(data.BlobDataAvailable)
	}
	f.payloadTimelinessVote.Store(blockRoot, timelinessVotes)
	f.payloadDataAvailabilityVote.Store(blockRoot, dataAvailabilityVotes)
	return nil
}
