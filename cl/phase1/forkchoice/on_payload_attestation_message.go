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

// ErrPayloadAttestationNotCurrentSlot marks a validly signed wire message that fork
// choice does not count because its slot is before the store's current slot.
var ErrPayloadAttestationNotCurrentSlot = errors.New("payload attestation is not for the current slot")

// PayloadAttestationSignatureVerifier being nil means signatures are verified immediately with bls.VerifyAggregate.
type PayloadAttestationSignatureVerifier func(ctx context.Context, signature, signingRoot, publicKey []byte) error

// OnPayloadAttestationMessage implements on_payload_attestation_message for a wire message.
// Returns ErrIgnore for IGNORE conditions, other errors for REJECT conditions.
func (f *ForkChoiceStore) OnPayloadAttestationMessage(
	ctx context.Context,
	msg *cltypes.PayloadAttestationMessage,
) error {
	return f.onPayloadAttestationMessage(ctx, msg, false, nil)
}

// OnGossipPayloadAttestationMessage is OnPayloadAttestationMessage for a gossip message,
// which must also be the first valid message from its validator in the slot.
func (f *ForkChoiceStore) OnGossipPayloadAttestationMessage(
	ctx context.Context,
	msg *cltypes.PayloadAttestationMessage,
	verifySignature PayloadAttestationSignatureVerifier,
) error {
	return f.onPayloadAttestationMessage(ctx, msg, true, verifySignature)
}

func (f *ForkChoiceStore) onPayloadAttestationMessage(
	ctx context.Context,
	msg *cltypes.PayloadAttestationMessage,
	firstValidOnly bool,
	verifySignature PayloadAttestationSignatureVerifier,
) error {
	if msg == nil || msg.Data == nil {
		return errors.New("nil payload attestation data")
	}

	data := msg.Data
	blockRoot := data.BeaconBlockRoot

	validationContext, err := f.payloadAttestationValidationContext(ctx, blockRoot, data.Slot)
	var ptcIndices []int
	if err == nil {
		ptcIndices, err = validationContext.ptcPositions(msg)
	}
	if !firstValidOnly && errors.Is(err, errPayloadAttestationSlotMismatch) {
		return nil
	}
	if err != nil {
		return err
	}

	if err := validationContext.validateSignature(ctx, msg, verifySignature); err != nil {
		return err
	}
	return f.applyWirePayloadAttestation(msg.ValidatorIndex, ptcIndices, data, blockRoot, firstValidOnly)
}

// applyWirePayloadAttestation counts a wire message only if it is for the store's current slot.
// The slot is read under f.mu: OnTick resets the cached head under f.mu after advancing the time,
// so a vote counted here is always ordered before the next slot's head decision.
func (f *ForkChoiceStore) applyWirePayloadAttestation(
	validatorIndex uint64,
	ptcIndices []int,
	data *cltypes.PayloadAttestationData,
	blockRoot common.Hash,
	firstValidOnly bool,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	currentSlot := f.Slot()
	if data.Slot > currentSlot {
		return fmt.Errorf("%w: payload attestation slot %d is after the current slot %d", ErrIgnore, data.Slot, currentSlot)
	}
	if data.Slot == currentSlot {
		return f.recordPayloadAttestationVotes(validatorIndex, ptcIndices, data, blockRoot, firstValidOnly)
	}
	if firstValidOnly {
		f.ptcVoteMu.Lock()
		err := f.claimFirstValidPayloadAttestation(validatorIndex, data.Slot, currentSlot)
		f.ptcVoteMu.Unlock()
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: slot %d, current slot %d", ErrPayloadAttestationNotCurrentSlot, data.Slot, currentSlot)
}

func (f *ForkChoiceStore) recordPayloadAttestationVotes(
	validatorIndex uint64,
	ptcIndices []int,
	data *cltypes.PayloadAttestationData,
	blockRoot common.Hash,
	firstValidOnly bool,
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
	if firstValidOnly {
		if err := f.claimFirstValidPayloadAttestation(validatorIndex, data.Slot, data.Slot); err != nil {
			return err
		}
	}
	for _, idx := range ptcIndices {
		timelinessVotes[idx] = boolToVote(data.PayloadPresent)
		dataAvailabilityVotes[idx] = boolToVote(data.BlobDataAvailable)
	}
	f.payloadTimelinessVote.Store(blockRoot, timelinessVotes)
	f.payloadDataAvailabilityVote.Store(blockRoot, dataAvailabilityVotes)
	return nil
}

type payloadAttestationSeenKey struct {
	slot           uint64
	validatorIndex uint64
}

// claimFirstValidPayloadAttestation records the first valid gossip message from a validator
// in the store's current or previous slot. The caller must hold ptcVoteMu so that concurrent
// candidates have one winner.
func (f *ForkChoiceStore) claimFirstValidPayloadAttestation(validatorIndex, slot, currentSlot uint64) error {
	if currentSlot > slot && currentSlot-slot > 1 {
		return fmt.Errorf("%w: payload attestation slot %d is before the previous store slot", ErrIgnore, slot)
	}
	if f.payloadAttestationSeen == nil {
		f.payloadAttestationSeen = make(map[payloadAttestationSeenKey]struct{}, 2*clparams.PtcSize)
	}
	if currentSlot > f.payloadAttestationPrunedSlot {
		for key := range f.payloadAttestationSeen {
			if currentSlot > key.slot && currentSlot-key.slot > 1 {
				delete(f.payloadAttestationSeen, key)
			}
		}
		f.payloadAttestationPrunedSlot = currentSlot
	}
	key := payloadAttestationSeenKey{slot: slot, validatorIndex: validatorIndex}
	if _, seen := f.payloadAttestationSeen[key]; seen {
		return fmt.Errorf("%w: already processed a valid payload attestation", ErrIgnore)
	}
	f.payloadAttestationSeen[key] = struct{}{}
	return nil
}
