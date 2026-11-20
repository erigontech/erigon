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

package validator_params

import (
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common"
)

type ValidatorParams struct {
	feeRecipients      sync.Map
	attachedValidators sync.Map
	generation         atomic.Uint64
	// defaultFeeRecipient is the chain-wide coinbase for a proposer that has registered none, so its fees do not go to address(0).
	defaultFeeRecipient common.Address
}

func NewValidatorParams() *ValidatorParams {
	return &ValidatorParams{}
}

// SetDefaultFeeRecipient sets the chain-level fallback. Chain-level rather than per-node on
// purpose: every client must derive the SAME coinbase or their executions differ.
func (vp *ValidatorParams) SetDefaultFeeRecipient(addr common.Address) {
	vp.defaultFeeRecipient = addr
}

func (vp *ValidatorParams) SetFeeRecipient(validatorIndex uint64, feeRecipient common.Address) {
	// Validator clients re-register unchanged on a schedule, so only a real change counts: a
	// generation that moved on every call would be useless to anyone caching a lookup.
	if previous, loaded := vp.feeRecipients.Swap(validatorIndex, feeRecipient); !loaded || previous.(common.Address) != feeRecipient {
		vp.generation.Add(1)
	}
	vp.AddAttachedValidator(validatorIndex)
}

func (vp *ValidatorParams) AddAttachedValidator(validatorIndex uint64) {
	vp.attachedValidators.Store(validatorIndex, struct{}{})
}

// Generation changes whenever a registration is added or its fee recipient changes, and is zero
// until the first one arrives. A consumer caching a lookup can compare it to find out whether a
// later registration could have changed the answer.
func (vp *ValidatorParams) Generation() uint64 {
	return vp.generation.Load()
}

// GetFeeRecipient returns the proposer's registered recipient, falling back to the chain default.
// The bool reports whether a recipient was found AT ALL — registered or default — so a caller can
// still tell "nobody has said where these fees go" from "they go here".
func (vp *ValidatorParams) GetFeeRecipient(validatorIndex uint64) (common.Address, bool) {
	val, ok := vp.feeRecipients.Load(validatorIndex)
	if !ok {
		if vp.defaultFeeRecipient == (common.Address{}) {
			return common.Address{}, false
		}
		return vp.defaultFeeRecipient, true
	}
	return val.(common.Address), true
}

func (vp *ValidatorParams) HasAttachedValidator(validatorIndex uint64) bool {
	_, ok := vp.attachedValidators.Load(validatorIndex)
	return ok
}

func (vp *ValidatorParams) GetValidators() []uint64 {
	validators := []uint64{}
	vp.attachedValidators.Range(func(key, value any) bool {
		validators = append(validators, key.(uint64))
		return true
	})
	return validators
}
