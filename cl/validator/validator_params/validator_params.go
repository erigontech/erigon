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

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
)

type ValidatorParams struct {
	feeRecipients sync.Map
}

func NewValidatorParams() *ValidatorParams {
	return &ValidatorParams{}
}

func (vp *ValidatorParams) SetFeeRecipient(validatorIndex uint64, feeRecipient libcommon.Address) {
	vp.feeRecipients.Store(validatorIndex, feeRecipient)
}

func (vp *ValidatorParams) GetFeeRecipient(validatorIndex uint64) (libcommon.Address, bool) {
	val, ok := vp.feeRecipients.Load(validatorIndex)
	if !ok {
		return libcommon.Address{}, false
	}
	return val.(libcommon.Address), true
}
