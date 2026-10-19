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
	"bytes"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type StateOverrides map[accounts.Address]Account

func (so *StateOverrides) override(ibs *state.IntraBlockState, addrs []accounts.Address) error {
	for _, addr := range addrs {
		account := (*so)[addr]
		// Override account nonce.
		if account.Nonce != nil {
			if err := ibs.SetNonce(addr, uint64(*account.Nonce), tracing.NonceChangeUnspecified); err != nil {
				return err
			}
		}
		// Override account (contract) code.
		if account.Code != nil {
			if err := ibs.SetCode(addr, *account.Code, tracing.CodeChangeUnspecified); err != nil {
				return err
			}
		}
		// Override account balance.
		if account.Balance != nil {
			if err := ibs.SetBalance(addr, uint256.Int(**account.Balance), tracing.BalanceChangeUnspecified); err != nil {
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
				intValue := *new(uint256.Int).SetBytes32(value[:])
				intState[accounts.InternKey(key)] = intValue
			}
			if err := ibs.SetStorage(addr, intState); err != nil {
				return err
			}
		}
		// Apply state diff into specified accounts.
		if account.StateDiff != nil {
			// The versioned SetState does not create the absent account the materializing one creates.
			if ibs.IsVersioned() && len(*account.StateDiff) > 0 {
				exists, err := ibs.Exist(addr)
				if err != nil {
					return err
				}
				if !exists {
					if err := ibs.CreateAccount(addr, false); err != nil {
						return err
					}
				}
			}
			for key, value := range *account.StateDiff {
				intValue := *new(uint256.Int).SetBytes32(value[:])
				if err := ibs.SetState(addr, accounts.InternKey(key), intValue); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (so *StateOverrides) Override(ibs *state.IntraBlockState, precompiles vm.PrecompiledContracts, rules *chain.Rules) error {
	if precompiles == nil {
		precompiles = make(vm.PrecompiledContracts)
	}
	// Sort addresses for deterministic iteration order across both loops (map iteration is random in Go).
	addrs := make([]accounts.Address, 0, len(*so))
	for addr := range *so {
		addrs = append(addrs, addr)
	}
	slices.SortFunc(addrs, func(a, b accounts.Address) int {
		av, bv := a.Value(), b.Value()
		return bytes.Compare(av[:], bv[:])
	})

	err := so.override(ibs, addrs)
	if err != nil {
		return err
	}

	// Tracks destinations of precompiles that were moved.
	dirtyAddresses := make(map[accounts.Address]struct{})
	for _, addr := range addrs {
		account := (*so)[addr]
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

	// State overrides are synthetic pre-state and must not trigger EIP-161 account deletion.
	overrideRules := *rules
	overrideRules.DisabledEIPs = append(slices.Clone(rules.DisabledEIPs), 161)
	if err := FinalizeCallTx(ibs, &overrideRules); err != nil {
		return err
	}
	if ibs.IsVersioned() {
		ibs.SetTxContext(ibs.BlockNumber(), ibs.TxnIndex()+1)
	}
	return nil
}

// FinalizeCallTx ends one transaction of a request that runs several on the same state.
// On the versioned path the writes go to the version map, where the next transaction index
// reads them; on the materializing path FinalizeTx commits them to the state objects.
func FinalizeCallTx(ibs *state.IntraBlockState, rules *chain.Rules) error {
	if !ibs.IsVersioned() {
		return ibs.FinalizeTx(rules, state.NewNoopWriter())
	}
	writes := ibs.FinalizedWrites(rules)
	ibs.FlushWritesToVersionMap(writes)
	// A touched empty account has no write of its own: its EIP-161 removal is derived and published.
	normalized, err := writes.Normalize(ibs.VersionMap(), ibs.TxnIndex(), 0, ibs.StateReader(), nil, rules.IsEIP161Enabled(), rules.IsAura, rules.IsAmsterdam)
	if err != nil {
		return err
	}
	for addr, removal := range normalized.SelfDestructs() {
		if raw, ok := writes.GetSelfDestruct(addr); removal.Val && (!ok || !raw.Val) {
			// The transaction's own cells of the account would read as its revival.
			ibs.VersionMap().DeleteAll(addr, ibs.TxnIndex())
			ibs.VersionMap().WriteSelfDestruct(addr, removal.Version, true, true)
		}
	}
	ibs.SoftFinalise()
	ibs.ResetVersionedIO()
	return nil
}
