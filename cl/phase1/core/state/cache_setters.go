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

package state

import "github.com/erigontech/erigon/cl/cltypes/solid"

// Below are setters.

func (b *CachingBeaconState) SetSlot(slot uint64) error {
	if err := b.BeaconState.SetSlot(slot); err != nil {
		return err
	}
	b.proposerIndex = nil
	if slot%b.BeaconConfig().SlotsPerEpoch == 0 {
		b.totalActiveBalanceCache = nil
	}
	return nil
}

func (b *CachingBeaconState) AddValidator(validator solid.Validator, balance uint64) error {
	if err := b.BeaconState.AddValidator(validator, balance); err != nil {
		return err
	}
	b.publicKeyIndices.add(validator.PublicKey(), uint64(b.ValidatorLength())-1)
	// change in validator set means cache purging
	b.totalActiveBalanceCache = nil
	return nil
}

func (b *CachingBeaconState) AppendValidator(validator solid.Validator) {
	b.BeaconState.AppendValidator(validator)
	b.publicKeyIndices.add(validator.PublicKey(), uint64(b.ValidatorLength())-1)
}

func (b *CachingBeaconState) SetValidators(validators *solid.ValidatorSet) {
	b.BeaconState.SetValidators(validators)
	b.rebuildPubkeyIndex()
}

func (b *CachingBeaconState) SetValidatorSet(validators *solid.ValidatorSet) {
	b.BeaconState.SetValidatorSet(validators)
	b.rebuildPubkeyIndex()
}

func (b *CachingBeaconState) SetValidatorAtIndex(index int, validator solid.Validator) {
	b.BeaconState.SetValidatorAtIndex(index, validator)
	b.rebuildPubkeyIndex()
}
