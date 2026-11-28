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
	"time"

	"github.com/erigontech/erigon/common"
)

// attachmentTTL is how long a validator stays attached after its last request. Validator clients repeat
// their proposer preparation and committee subscriptions every epoch, so only a validator that moved to
// another beacon node or stopped goes unrefreshed for this long.
const attachmentTTL = time.Hour

type ValidatorParams struct {
	feeRecipients      sync.Map
	attachedValidators sync.Map // validator index -> time.Time of the last request that attached it
	generation         atomic.Uint64
	now                func() time.Time
}

func NewValidatorParams() *ValidatorParams {
	return &ValidatorParams{now: time.Now}
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
	vp.attachedValidators.Store(validatorIndex, vp.now())
}

func isAttached(attachedAt any, now time.Time) bool {
	return now.Sub(attachedAt.(time.Time)) < attachmentTTL
}

// Generation changes whenever a registration is added or its fee recipient changes, and is zero
// until the first one arrives. A consumer caching a lookup can compare it to find out whether a
// later registration could have changed the answer.
func (vp *ValidatorParams) Generation() uint64 {
	return vp.generation.Load()
}

func (vp *ValidatorParams) GetFeeRecipient(validatorIndex uint64) (common.Address, bool) {
	val, ok := vp.feeRecipients.Load(validatorIndex)
	if !ok {
		return common.Address{}, false
	}
	return val.(common.Address), true
}

func (vp *ValidatorParams) HasAttachedValidator(validatorIndex uint64) bool {
	attachedAt, ok := vp.attachedValidators.Load(validatorIndex)
	return ok && isAttached(attachedAt, vp.now())
}

// GetValidators returns the attached validators and drops the expired ones.
func (vp *ValidatorParams) GetValidators() []uint64 {
	validators := []uint64{}
	now := vp.now()
	vp.attachedValidators.Range(func(key, attachedAt any) bool {
		if isAttached(attachedAt, now) {
			validators = append(validators, key.(uint64))
		} else {
			// A request that refreshed the entry meanwhile keeps it.
			vp.attachedValidators.CompareAndDelete(key, attachedAt)
		}
		return true
	})
	return validators
}
