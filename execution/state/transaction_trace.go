// Copyright 2026 The Erigon Authors
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

package state

import (
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type BalanceChange struct {
	Address       accounts.Address
	Before, After uint256.Int
}

// TransactionTrace exposes surviving transaction changes to ASSERT, including
// payment escrow and nonce consumption, before final fee settlement.
type TransactionTrace struct {
	Balances []BalanceChange
	Slots    []StorageChange
	Codes    []CodeChange
	Nonces   []NonceChange
	Events   []*types.Log
}

type StorageChange struct {
	Address       accounts.Address
	Key           accounts.StorageKey
	Before, After uint256.Int
}

type CodeChange struct {
	Address       accounts.Address
	Before, After accounts.CodeHash
}

type NonceChange struct {
	Address       accounts.Address
	Before, After uint64
}

// TransactionTrace builds the view once for the read-only ASSERT suffix. A failed
// assertion ends the suffix, so the cache is never reused after body rollback.
// It relies on EIP-8253 preventing creation over non-empty storage, so the first
// journaled slot value is its transaction prestate: https://eips.ethereum.org/EIPS/eip-8253
func (ctx *FrameContext) TransactionTrace(ibs *IntraBlockState) (*TransactionTrace, error) {
	if ctx.trace != nil {
		return ctx.trace, nil
	}
	trace := &TransactionTrace{}
	balances := make(map[accounts.Address]uint256.Int)
	nonces := make(map[accounts.Address]uint64)
	slots := make(map[frameStorageSlot]uint256.Int)
	codes := make(map[accounts.Address]accounts.CodeHash)
	for _, entry := range ibs.journal.entries[ctx.journalStart:] {
		switch entry.kind {
		case kindBalance, kindSelfdestruct:
			if _, ok := balances[entry.account]; !ok {
				balances[entry.account] = entry.value
			}
		case kindNonce:
			if _, ok := nonces[entry.account]; !ok {
				nonces[entry.account] = entry.aux
			}
		case kindStorage, kindFakeStorage:
			slot := frameStorageSlot{entry.account, entry.key}
			if _, ok := slots[slot]; !ok {
				slots[slot] = entry.value
			}
		case kindCode:
			if _, ok := codes[entry.account]; !ok {
				hash := accounts.EmptyCodeHash
				if entry.extra != nil && !entry.extra.prevhash.IsEmpty() {
					hash = entry.extra.prevhash
				}
				codes[entry.account] = hash
			}
		}
	}
	for address, before := range balances {
		after, err := ibs.GetBalance(address)
		if err != nil {
			return nil, err
		}
		if before != after {
			trace.Balances = append(trace.Balances, BalanceChange{address, before, after})
		}
	}
	for address, before := range nonces {
		// Parallel nonce writes can journal an older base-account value. Resolve
		// the transaction prestate through the versioned reader and retain its dependency.
		if ibs.versionMap != nil {
			var err error
			before, _, _, err = readNonce(ibs, address, true)
			if err != nil {
				return nil, err
			}
		}
		after, err := ibs.GetNonce(address)
		if err != nil {
			return nil, err
		}
		if before != after {
			trace.Nonces = append(trace.Nonces, NonceChange{address, before, after})
		}
	}
	for slot, before := range slots {
		after, err := ibs.GetState(slot.address, slot.key)
		if err != nil {
			return nil, err
		}
		if before != after {
			trace.Slots = append(trace.Slots, StorageChange{slot.address, slot.key, before, after})
		}
	}
	for address, before := range codes {
		after, err := ibs.GetCodeHash(address)
		if err != nil {
			return nil, err
		}
		if after.IsEmpty() {
			after = accounts.EmptyCodeHash
		}
		if before != after {
			trace.Codes = append(trace.Codes, CodeChange{address, before, after})
		}
	}
	slices.SortFunc(trace.Balances, func(a, b BalanceChange) int { return a.Address.Cmp(b.Address) })
	slices.SortFunc(trace.Slots, func(a, b StorageChange) int {
		if order := a.Address.Cmp(b.Address); order != 0 {
			return order
		}
		return a.Key.Cmp(b.Key)
	})
	slices.SortFunc(trace.Codes, func(a, b CodeChange) int { return a.Address.Cmp(b.Address) })
	slices.SortFunc(trace.Nonces, func(a, b NonceChange) int { return a.Address.Cmp(b.Address) })
	trace.Events = ibs.LogsSince(ctx.logStart)
	ctx.trace = trace
	return trace, nil
}
