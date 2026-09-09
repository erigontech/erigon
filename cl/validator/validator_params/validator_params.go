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

	"github.com/erigontech/erigon/common"
)

type ValidatorParams struct {
	feeRecipients sync.Map
	// defaultFeeRecipient is the chain's coinbase for any proposer that has not registered one of
	// its own. It exists because the alternative is address(0): coinbase is execution-affecting
	// (fee credit and the COINBASE opcode), so an unregistered proposer does not merely forgo its
	// fees — it burns every block's fees to the zero address, and a follower that disagrees about
	// the address diverges outright.
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
	vp.feeRecipients.Store(validatorIndex, feeRecipient)
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

func (vp *ValidatorParams) GetValidators() []uint64 {
	validators := []uint64{}
	vp.feeRecipients.Range(func(key, value any) bool {
		validators = append(validators, key.(uint64))
		return true
	})
	return validators
}
