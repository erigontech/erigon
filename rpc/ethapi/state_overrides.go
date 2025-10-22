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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm"
)

type StateOverrides map[common.Address]Account

func (so *StateOverrides) Override(state *state.IntraBlockState) error {
	for addr, account := range *so {
		// Override account nonce.
		if account.Nonce != nil {
			if err := state.SetNonce(addr, uint64(*account.Nonce)); err != nil {
				return err
			}
		}
		// Override account (contract) code.
		if account.Code != nil {
			if err := state.SetCode(addr, *account.Code); err != nil {
				return err
			}
		}
		// Override account balance.
		if account.Balance != nil {
			balance, overflow := uint256.FromBig((*big.Int)(*account.Balance))
			if overflow {
				return errors.New("account.Balance higher than 2^256-1")
			}
			if err := state.SetBalance(addr, *balance, tracing.BalanceChangeUnspecified); err != nil {
				return err
			}
		}
		if account.State != nil && account.StateDiff != nil {
			return fmt.Errorf("account %s has both 'state' and 'stateDiff'", addr.Hex())
		}
		// Replace entire state if caller requires.
		if account.State != nil {
			intState := map[common.Hash]uint256.Int{}
			for key, value := range *account.State {
				intValue := new(uint256.Int).SetBytes32(value.Bytes())
				intState[key] = *intValue
			}
			if err := state.SetStorage(addr, intState); err != nil {
				return err
			}
		}
		// Apply state diff into specified accounts.
		if account.StateDiff != nil {
			for key, value := range *account.StateDiff {
				intValue := new(uint256.Int).SetBytes32(value.Bytes())
				if err := state.SetState(addr, key, *intValue); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (so *StateOverrides) OverrideWithPrecompiles(state *state.IntraBlockState, precompiles vm.PrecompiledContracts) error {
	err := so.Override(state)
	if err != nil {
		return err
	}
	// Tracks destinations of precompiles that were moved.
	dirtyAddresses := make(map[common.Address]struct{})
	for addr, account := range *so {
		// If a precompile was moved to this address already, it can't be overridden.
		if _, ok := dirtyAddresses[addr]; ok {
			return fmt.Errorf("account %s has already been overridden by a precompile", addr.Hex())
		}
		p, isPrecompile := precompiles[addr]
		// The MovePrecompileTo feature makes it possible to move a precompile code to another address. If the target address
		// is another precompile, the code for the latter is lost for this session. Note the destination account is not cleared upon move.
		if account.MovePrecompileTo != nil {
			if !isPrecompile {
				return fmt.Errorf("account %s is not a precompile", addr.Hex())
			}
			// Refuse to move a precompile to an address that has been or will be overridden.
			if _, ok := (*so)[*account.MovePrecompileTo]; ok {
				return fmt.Errorf("account %s is already overridden", account.MovePrecompileTo.Hex())
			}
			precompiles[*account.MovePrecompileTo] = p
			dirtyAddresses[*account.MovePrecompileTo] = struct{}{}
		}
		if isPrecompile {
			delete(precompiles, addr)
		}
	}
	return nil
}
