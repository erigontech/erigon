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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	accountNonceChanged = 1 << iota
	accountBalanceChanged
	accountStorageChanged
	accountCodeChanged
)

type BalanceChange struct {
	Address       accounts.Address
	Before, After uint256.Int
}

// TransactionTrace exposes surviving transaction changes to POST_TX, including
// payment escrow and nonce consumption, before final fee settlement.
type TransactionTrace struct {
	Balances              []BalanceChange
	Slots                 []StorageChange
	Deployed              []CodeChange
	Events                []*types.Log
	AccountFlags          map[accounts.Address]uint64
	SlotIndicesByAddress  map[accounts.Address][]int
	EventIndicesByAddress map[accounts.Address][]int
	EventIndicesByTopic   map[common.Hash][]int
	balanceBefore         map[accounts.Address]uint256.Int
	storageBefore         map[frameStorageSlot]uint256.Int
	codeBefore            map[accounts.Address]accounts.CodeHash
}

type StorageChange struct {
	Address       accounts.Address
	Key           accounts.StorageKey
	Before, After uint256.Int
}

type CodeChange struct {
	Address accounts.Address
	After   accounts.CodeHash
}

// TransactionTrace builds the view once for the read-only POST_TX suffix. A failed
// assertion ends the suffix, so the cache is never reused after body rollback.
// It relies on EIP-8253 preventing creation over non-empty storage, so the first
// journaled slot value is its transaction prestate: https://eips.ethereum.org/EIPS/eip-8253
func (ctx *FrameContext) TransactionTrace(ibs *IntraBlockState) (*TransactionTrace, error) {
	if ctx.trace != nil {
		return ctx.trace, nil
	}
	trace := &TransactionTrace{
		AccountFlags:          make(map[accounts.Address]uint64),
		SlotIndicesByAddress:  make(map[accounts.Address][]int),
		EventIndicesByAddress: make(map[accounts.Address][]int),
		EventIndicesByTopic:   make(map[common.Hash][]int),
		balanceBefore:         make(map[accounts.Address]uint256.Int),
		storageBefore:         make(map[frameStorageSlot]uint256.Int),
		codeBefore:            make(map[accounts.Address]accounts.CodeHash),
	}
	nonces := make(map[accounts.Address]uint64)
	for _, entry := range ibs.journal.entries[ctx.journalStart:] {
		switch entry.kind {
		case kindBalance, kindSelfdestruct:
			if _, ok := trace.balanceBefore[entry.account]; !ok {
				trace.balanceBefore[entry.account] = entry.value
			}
		case kindNonce:
			if _, ok := nonces[entry.account]; !ok {
				nonces[entry.account] = entry.aux
			}
		case kindStorage, kindFakeStorage:
			slot := frameStorageSlot{entry.account, entry.key}
			if _, ok := trace.storageBefore[slot]; !ok {
				trace.storageBefore[slot] = entry.value
			}
		case kindCode:
			if _, ok := trace.codeBefore[entry.account]; !ok {
				hash := accounts.EmptyCodeHash
				if entry.extra != nil && !entry.extra.prevhash.IsEmpty() {
					hash = entry.extra.prevhash
				}
				trace.codeBefore[entry.account] = hash
			}
		}
	}
	for address, value := range trace.balanceBefore {
		after, err := ibs.GetBalance(address)
		if err != nil {
			return nil, err
		}
		if value != after {
			trace.Balances = append(trace.Balances, BalanceChange{address, value, after})
			trace.AccountFlags[address] |= accountBalanceChanged
		}
	}
	for address, value := range nonces {
		after, err := ibs.GetNonce(address)
		if err != nil {
			return nil, err
		}
		if value != after {
			trace.AccountFlags[address] |= accountNonceChanged
		}
	}
	for slot, value := range trace.storageBefore {
		after, err := ibs.GetState(slot.address, slot.key)
		if err != nil {
			return nil, err
		}
		if value != after {
			trace.Slots = append(trace.Slots, StorageChange{slot.address, slot.key, value, after})
			trace.AccountFlags[slot.address] |= accountStorageChanged
		}
	}
	for address, value := range trace.codeBefore {
		after, err := trace.CodeHash(ibs, address, false)
		if err != nil {
			return nil, err
		}
		if value != after {
			trace.AccountFlags[address] |= accountCodeChanged
			if value.IsEmpty() && !after.IsEmpty() {
				code, err := ibs.GetCode(address)
				if err != nil {
					return nil, err
				}
				if _, delegation := types.ParseDelegation(code); !delegation {
					trace.Deployed = append(trace.Deployed, CodeChange{address, after})
				}
			}
		}
	}
	slices.SortFunc(trace.Balances, func(a, b BalanceChange) int {
		return a.Address.Cmp(b.Address)
	})
	slices.SortFunc(trace.Slots, func(a, b StorageChange) int {
		if order := a.Address.Cmp(b.Address); order != 0 {
			return order
		}
		return a.Key.Cmp(b.Key)
	})
	slices.SortFunc(trace.Deployed, func(a, b CodeChange) int {
		return a.Address.Cmp(b.Address)
	})
	for i, slot := range trace.Slots {
		trace.SlotIndicesByAddress[slot.Address] = append(trace.SlotIndicesByAddress[slot.Address], i)
	}
	trace.Events = ibs.LogsSince(ctx.logStart)
	for i, event := range trace.Events {
		address := accounts.InternAddress(event.Address)
		trace.EventIndicesByAddress[address] = append(trace.EventIndicesByAddress[address], i)
		for j := 1; j < len(event.Topics); j++ {
			topic := event.Topics[j]
			if !slices.Contains(event.Topics[1:j], topic) {
				trace.EventIndicesByTopic[topic] = append(trace.EventIndicesByTopic[topic], i)
			}
		}
	}
	ctx.trace = trace
	return trace, nil
}

func (trace *TransactionTrace) Balance(ibs *IntraBlockState, address accounts.Address, before bool) (uint256.Int, error) {
	if value, ok := trace.balanceBefore[address]; before && ok {
		return value, nil
	}
	return ibs.GetBalance(address)
}

func (trace *TransactionTrace) Storage(ibs *IntraBlockState, address accounts.Address, key accounts.StorageKey, before bool) (uint256.Int, error) {
	// Read even when the before-value is in the diff: TXDIFF records a
	// storage access in the BAL, unlike TXTRACE's enumeration.
	value, err := ibs.GetState(address, key)
	if err != nil || !before {
		return value, err
	}
	if original, ok := trace.storageBefore[frameStorageSlot{address, key}]; ok {
		return original, nil
	}
	return value, nil
}

func (trace *TransactionTrace) CodeHash(ibs *IntraBlockState, address accounts.Address, before bool) (accounts.CodeHash, error) {
	if hash, ok := trace.codeBefore[address]; before && ok {
		return hash, nil
	}
	hash, err := ibs.GetCodeHash(address)
	if hash.IsEmpty() {
		hash = accounts.EmptyCodeHash
	}
	return hash, err
}
