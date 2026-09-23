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
	feeRecipients sync.Map
	generation    atomic.Uint64
}

func NewValidatorParams() *ValidatorParams {
	return &ValidatorParams{}
}

func (vp *ValidatorParams) SetFeeRecipient(validatorIndex uint64, feeRecipient common.Address) {
	// Validator clients re-register unchanged on a schedule, so only a real change counts: a
	// generation that moved on every call would be useless to anyone caching a lookup.
	if previous, loaded := vp.feeRecipients.Swap(validatorIndex, feeRecipient); !loaded || previous.(common.Address) != feeRecipient {
		vp.generation.Add(1)
	}
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

func (vp *ValidatorParams) GetValidators() []uint64 {
	validators := []uint64{}
	vp.feeRecipients.Range(func(key, value any) bool {
		validators = append(validators, key.(uint64))
		return true
	})
	return validators
}
