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

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type StateOverrides map[accounts.Address]Account

func (so *StateOverrides) override(ibs *state.IntraBlockState) error {
	for addr, account := range *so {
		// Override account nonce.
		if account.Nonce != nil {
			if err := ibs.SetNonce(addr, uint64(*account.Nonce)); err != nil {
				return err
			}
		}
		// Override account (contract) code.
		if account.Code != nil {
			if err := ibs.SetCode(addr, *account.Code); err != nil {
				return err
			}
		}
		// Override account balance.
		if account.Balance != nil {
			balance, overflow := uint256.FromBig((*big.Int)(*account.Balance))
			if overflow {
				return errors.New("account.Balance higher than 2^256-1")
			}
			if err := ibs.SetBalance(addr, *balance, tracing.BalanceChangeUnspecified); err != nil {
				return err
			}
		}
		if account.State != nil && account.StateDiff != nil {
			return fmt.Errorf("account %s has both 'state' and 'stateDiff'", addr)
		}
		// Replace entire state if caller requires.
		if account.State != nil {
			intState := map[accounts.StorageKey]uint256.Int{}
			for key, value := range *account.State {
				intValue := *new(uint256.Int).SetBytes32(value.Bytes())
				intState[accounts.InternKey(key)] = intValue
			}
			if err := ibs.SetStorage(addr, intState); err != nil {
				return err
			}
		}
		// Apply state diff into specified accounts.
		if account.StateDiff != nil {
			for key, value := range *account.StateDiff {
				intValue := *new(uint256.Int).SetBytes32(value.Bytes())
				if err := ibs.SetState(addr, accounts.InternKey(key), intValue); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (so *StateOverrides) Override(ibs *state.IntraBlockState, precompiles vm.PrecompiledContracts, rules *chain.Rules) error {
	err := so.override(ibs)
	if err != nil {
		return err
	}
	// Tracks destinations of precompiles that were moved.
	dirtyAddresses := make(map[accounts.Address]struct{})
	for addr, account := range *so {
		// If a precompile was moved to this address already, it can't be overridden.
		if _, ok := dirtyAddresses[addr]; ok {
			return fmt.Errorf("account %s has already been overridden by a precompile", addr)
		}
		p, isPrecompile := precompiles[addr]
		// The MovePrecompileTo feature makes it possible to move a precompile code to another address. If the target address
		// is another precompile, the code for the latter is lost for this session. Note the destination account is not cleared upon move.
		if account.MovePrecompileTo != nil {
			if !isPrecompile {
				return fmt.Errorf("account %s is not a precompile", addr)
			}
			// Refuse to move a precompile to an address that has been or will be overridden.
			precompileMoveTo := accounts.InternAddress(*account.MovePrecompileTo)
			if _, ok := (*so)[precompileMoveTo]; ok {
				return fmt.Errorf("account %s is already overridden", account.MovePrecompileTo)
			}
			precompiles[precompileMoveTo] = p
			dirtyAddresses[precompileMoveTo] = struct{}{}
		}
		if isPrecompile {
			delete(precompiles, addr)
		}
	}

	// Disable EIP-161 empty-account removal when finalizing state overrides.
	// FinalizeTx with a NoopWriter commits dirty storage into originStorage
	// (needed for correct SSTORE gas), but EIP-161 would also mark any account
	// that becomes empty (nonce=0, code=0x, balance=0) as deleted in the IBS —
	// even though the deletion is never written to the DB.  That spurious
	// deleted=true flag causes IntraBlockState.HasStorage to short-circuit to
	// false before reaching the state reader, breaking EIP-7610 collision
	// detection in multi-block eth_simulateV1 when a prior simulated block
	// deployed a contract at the overridden address.
	// State overrides are simulation-only mutations and must not trigger
	// consensus rules like EIP-161.
	noEIP161Rules := *rules
	noEIP161Rules.IsSpuriousDragon = false
	return ibs.FinalizeTx(&noEIP161Rules, state.NewNoopWriter())
}
