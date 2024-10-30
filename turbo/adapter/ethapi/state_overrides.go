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

package ethapi

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/v3/core/state"
	"github.com/erigontech/erigon/v3/core/tracing"
)

type StateOverrides map[libcommon.Address]Account

func (overrides *StateOverrides) Override(state *state.IntraBlockState) error {

	for addr, account := range *overrides {
		// Override account nonce.
		if account.Nonce != nil {
			state.SetNonce(addr, uint64(*account.Nonce))
		}
		// Override account(contract) code.
		if account.Code != nil {
			state.SetCode(addr, *account.Code)
		}
		// Override account balance.
		if account.Balance != nil {
			balance, overflow := uint256.FromBig((*big.Int)(*account.Balance))
			if overflow {
				return errors.New("account.Balance higher than 2^256-1")
			}
			state.SetBalance(addr, balance, tracing.BalanceChangeUnspecified)
		}
		if account.State != nil && account.StateDiff != nil {
			return fmt.Errorf("account %s has both 'state' and 'stateDiff'", addr.Hex())
		}
		// Replace entire state if caller requires.
		if account.State != nil {
			intState := map[libcommon.Hash]uint256.Int{}
			for key, value := range *account.State {
				intValue := new(uint256.Int).SetBytes32(value.Bytes())
				intState[key] = *intValue
			}
			state.SetStorage(addr, intState)
		}
		// Apply state diff into specified accounts.
		if account.StateDiff != nil {
			for key, value := range *account.StateDiff {
				key := key
				intValue := new(uint256.Int).SetBytes32(value.Bytes())
				state.SetState(addr, &key, *intValue)
			}
		}
	}

	return nil
}
