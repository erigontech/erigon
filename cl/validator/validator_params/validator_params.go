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

	"github.com/erigontech/erigon-lib/common"
)

type ValidatorParams struct {
	feeRecipients sync.Map
}

func NewValidatorParams() *ValidatorParams {
	return &ValidatorParams{}
}

func (vp *ValidatorParams) SetFeeRecipient(validatorIndex uint64, feeRecipient common.Address) {
	vp.feeRecipients.Store(validatorIndex, feeRecipient)
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
	vp.feeRecipients.Range(func(key, value interface{}) bool {
		validators = append(validators, key.(uint64))
		return true
	})
	return validators
}
