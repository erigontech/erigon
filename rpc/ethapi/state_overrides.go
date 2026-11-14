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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type StateOverrides map[accounts.Address]Account

// UnmarshalJSON rejects account override keys that are not defined.
// encoding/json alone ignores unknown keys.
func (so *StateOverrides) UnmarshalJSON(input []byte) error {
	var raw map[common.Address]json.RawMessage
	if err := json.Unmarshal(input, &raw); err != nil {
		return err
	}
	if raw == nil {
		*so = nil
		return nil
	}
	overrides := make(StateOverrides, len(raw))
	for _, addr := range slices.SortedFunc(maps.Keys(raw), common.Address.Cmp) {
		if err := CheckOverrideKeys(raw[addr], reflect.TypeFor[Account](), "state override for "+addr.Hex()); err != nil {
			return err
		}
		var account Account
		if err := json.Unmarshal(raw[addr], &account); err != nil {
			return err
		}
		overrides[accounts.InternAddress(addr)] = account
	}
	*so = overrides
	return nil
}

// CheckOverrideKeys returns an error for a key of the JSON object that does not
// name a field of t, or that names a field another key already names. Names
// match case-insensitively, as in encoding/json. Repeated identical keys cannot
// be seen here; encoding/json keeps the last one.
func CheckOverrideKeys(input json.RawMessage, t reflect.Type, what string) error {
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(input, &keys); err != nil || keys == nil {
		return nil
	}
	seen := make(map[int]string)
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		field := -1
		for i := range t.NumField() {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			if strings.EqualFold(key, name) {
				field = i
				break
			}
		}
		if field < 0 {
			return fmt.Errorf("unknown field %q in %s", key, what)
		}
		if prev, ok := seen[field]; ok {
			return fmt.Errorf("keys %q and %q in %s name the same field", prev, key, what)
		}
		seen[field] = key
	}
	return nil
}

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
			// SetState does not create an absent account.
			if len(*account.StateDiff) > 0 {
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

// sortedAddrs gives a deterministic iteration order (map iteration is random in Go).
func (so *StateOverrides) sortedAddrs() []accounts.Address {
	addrs := make([]accounts.Address, 0, len(*so))
	for addr := range *so {
		addrs = append(addrs, addr)
	}
	slices.SortFunc(addrs, func(a, b accounts.Address) int {
		av, bv := a.Value(), b.Value()
		return bytes.Compare(av[:], bv[:])
	})
	return addrs
}

func (so *StateOverrides) Override(ibs *state.IntraBlockState, precompiles vm.PrecompiledContracts, rules *chain.Rules) error {
	return so.OverrideTo(ibs, precompiles, rules, nil)
}

// OverrideTo is Override that also hands the overridden accounts to stateWriter.
func (so *StateOverrides) OverrideTo(ibs *state.IntraBlockState, precompiles vm.PrecompiledContracts, rules *chain.Rules, stateWriter state.StateWriter) error {
	if precompiles == nil {
		precompiles = make(vm.PrecompiledContracts)
	}
	addrs := so.sortedAddrs()
	err := so.override(ibs, addrs)
	if err != nil {
		return err
	}
	if err := so.overridePrecompiles(precompiles, addrs); err != nil {
		return err
	}

	// State overrides are synthetic pre-state and must not trigger EIP-161 account deletion.
	overrideRules := *rules
	overrideRules.DisabledEIPs = append(slices.Clone(rules.DisabledEIPs), 161)
	return EndCallTx(ibs, &overrideRules, stateWriter)
}

// ActivePrecompiles returns the precompile set of rules with the code overrides and precompile
// moves applied, for a call that runs under other rules than the ones Override saw. It returns
// nil, the chain's own set, when there are no overrides.
func (so *StateOverrides) ActivePrecompiles(rules *chain.Rules) (vm.PrecompiledContracts, error) {
	if so == nil {
		return nil, nil
	}
	precompiles := vm.ActivePrecompiledContracts(rules)
	if err := so.overridePrecompiles(precompiles, so.sortedAddrs()); err != nil {
		return nil, err
	}
	return precompiles, nil
}

func (so *StateOverrides) overridePrecompiles(precompiles vm.PrecompiledContracts, addrs []accounts.Address) error {
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
	return nil
}

// FinalizeCallTx ends one transaction of a request that runs several on the same state.
// The writes go to the version map, where the next transaction index reads them.
func FinalizeCallTx(ibs *state.IntraBlockState, rules *chain.Rules) error {
	return FinalizeCallTxTo(ibs, rules, nil)
}

// FinalizeCallTxTo is FinalizeCallTx that also hands the tx's state changes to stateWriter;
// a nil stateWriter takes none.
func FinalizeCallTxTo(ibs *state.IntraBlockState, rules *chain.Rules, stateWriter state.StateWriter) error {
	if !ibs.IsVersioned() {
		return errors.New("call state has no version map")
	}
	writes, err := ibs.FinalizedWrites(rules)
	if err != nil {
		return err
	}
	ibs.FlushWritesToVersionMap(writes)
	if stateWriter != nil {
		if err := ibs.WriteFlushedTo(writes, stateWriter); err != nil {
			return err
		}
	}
	ibs.SoftFinalise()
	ibs.ResetVersionedIO()
	return nil
}

// EndCallTx is FinalizeCallTxTo that also moves the state to the next tx index,
// where the finished tx's writes read back.
func EndCallTx(ibs *state.IntraBlockState, rules *chain.Rules, stateWriter state.StateWriter) error {
	if err := FinalizeCallTxTo(ibs, rules, stateWriter); err != nil {
		return err
	}
	ibs.SetTxContext(ibs.BlockNumber(), ibs.TxnIndex()+1)
	return nil
}
