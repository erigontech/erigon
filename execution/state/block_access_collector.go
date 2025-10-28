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

package state

import (
	"bytes"
	"math/big"
	"slices"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type blockAccessTouch struct {
	address bool
	slots   map[common.Hash]struct{}
}

type blockAccessAccountSnapshot struct {
	touched       bool
	storageWrites map[common.Hash]common.Hash
	storageReads  map[common.Hash]struct{}
	balance       *big.Int
	balanceSet    bool
	nonce         uint64
	nonceSet      bool
	code          []byte
	codeSet       bool
}

type blockAccessSnapshot struct {
	index    uint16
	accounts map[common.Address]*blockAccessAccountSnapshot
}

type blockAccessAggregateAccount struct {
	slotChanges  map[common.Hash]*types.SlotChanges
	storageReads map[common.Hash]struct{}
	balances     map[uint16]*types.BalanceChange
	nonces       map[uint16]*types.NonceChange
	codes        map[uint16]*types.CodeChange
	touched      bool
}

func (snap *blockAccessSnapshot) account(addr common.Address) *blockAccessAccountSnapshot {
	if snap.accounts == nil {
		snap.accounts = make(map[common.Address]*blockAccessAccountSnapshot)
	}
	acc := snap.accounts[addr]
	if acc == nil {
		acc = &blockAccessAccountSnapshot{}
		snap.accounts[addr] = acc
	}
	return acc
}

func (acc *blockAccessAccountSnapshot) ensureStorageWrites() map[common.Hash]common.Hash {
	if acc.storageWrites == nil {
		acc.storageWrites = make(map[common.Hash]common.Hash)
	}
	return acc.storageWrites
}

func (acc *blockAccessAccountSnapshot) ensureStorageReads() map[common.Hash]struct{} {
	if acc.storageReads == nil {
		acc.storageReads = make(map[common.Hash]struct{})
	}
	return acc.storageReads
}

func (acc *blockAccessAccountSnapshot) setBalance(v *big.Int) {
	if v == nil {
		acc.balance = new(big.Int)
		acc.balanceSet = true
		return
	}
	if acc.balance == nil {
		acc.balance = new(big.Int)
	}
	acc.balance.Set(v)
	acc.balanceSet = true
}

func (acc *blockAccessAccountSnapshot) setNonce(v uint64) {
	acc.nonce = v
	acc.nonceSet = true
}

func (acc *blockAccessAccountSnapshot) setCode(data []byte) {
	if data == nil {
		acc.code = nil
		acc.codeSet = true
		return
	}
	acc.code = slices.Clone(data)
	acc.codeSet = true
}

func (a *blockAccessAggregateAccount) ensureSlot(slot common.Hash) *types.SlotChanges {
	if a.slotChanges == nil {
		a.slotChanges = make(map[common.Hash]*types.SlotChanges)
	}
	slotChanges := a.slotChanges[slot]
	if slotChanges == nil {
		slotChanges = &types.SlotChanges{Slot: slot}
		a.slotChanges[slot] = slotChanges
	}
	return slotChanges
}

func (a *blockAccessAggregateAccount) ensureBalance(index uint16) *types.BalanceChange {
	if a.balances == nil {
		a.balances = make(map[uint16]*types.BalanceChange)
	}
	change := a.balances[index]
	if change == nil {
		change = &types.BalanceChange{Index: index}
		a.balances[index] = change
	}
	return change
}

func (a *blockAccessAggregateAccount) ensureNonce(index uint16) *types.NonceChange {
	if a.nonces == nil {
		a.nonces = make(map[uint16]*types.NonceChange)
	}
	change := a.nonces[index]
	if change == nil {
		change = &types.NonceChange{Index: index}
		a.nonces[index] = change
	}
	return change
}

func (a *blockAccessAggregateAccount) ensureCode(index uint16) *types.CodeChange {
	if a.codes == nil {
		a.codes = make(map[uint16]*types.CodeChange)
	}
	change := a.codes[index]
	if change == nil {
		change = &types.CodeChange{Index: index}
		a.codes[index] = change
	}
	return change
}

func uint256ToHash(v *uint256.Int) common.Hash {
	if v == nil {
		return common.Hash{}
	}
	return common.Hash(v.Bytes32())
}

func (sdb *IntraBlockState) EnableBlockAccessList(rules *chain.Rules) {
	enabled := rules != nil && rules.IsGlamsterdam
	sdb.blockAccessEnabled = enabled
	sdb.blockAccessIgnorePreload = false
	if !enabled {
		sdb.blockAccessTxTouches = nil
		sdb.blockAccessSnapshots = nil
		return
	}
	sdb.blockAccessTxTouches = make(map[common.Address]*blockAccessTouch)
	sdb.blockAccessSnapshots = sdb.blockAccessSnapshots[:0]
}

func (sdb *IntraBlockState) ResetTxTracking() {
	if !sdb.blockAccessEnabled {
		return
	}
	sdb.resetBlockAccessTxTouches()
}

func (sdb *IntraBlockState) SnapshotTxAccess(index uint16) error {
	return sdb.captureBlockAccessSnapshot(index)
}

func (sdb *IntraBlockState) SnapshotSystemAccess(index uint16) error {
	return sdb.captureBlockAccessSnapshot(index)
}

func (sdb *IntraBlockState) captureBlockAccessSnapshot(index uint16) error {
	if !sdb.blockAccessEnabled {
		return nil
	}

	snapshot := &blockAccessSnapshot{index: index}
	writtenSlots := make(map[common.Address]map[common.Hash]struct{})

	for _, entry := range sdb.journal.entries {
		if err := sdb.applyJournalEntry(snapshot, writtenSlots, entry); err != nil {
			return err
		}
	}

	sdb.applyTouches(snapshot, writtenSlots)

	if len(snapshot.accounts) > 0 {
		sdb.blockAccessSnapshots = append(sdb.blockAccessSnapshots, snapshot)
	}

	sdb.resetBlockAccessTxTouches()

	return nil
}

func (sdb *IntraBlockState) BuildBlockAccessList() (types.BlockAccessList, error) {
	if !sdb.blockAccessEnabled {
		return nil, nil
	}

	aggregated := make(map[common.Address]*blockAccessAggregateAccount)

	for _, snapshot := range sdb.blockAccessSnapshots {
		for addr, account := range snapshot.accounts {
			agg := aggregated[addr]
			if agg == nil {
				agg = &blockAccessAggregateAccount{}
				aggregated[addr] = agg
			}

			if account.touched {
				agg.touched = true
			}

			for slot, value := range account.storageWrites {
				slotChanges := agg.ensureSlot(slot)
				slotChanges.Changes = append(slotChanges.Changes, &types.StorageChange{
					Index: snapshot.index,
					Value: value,
				})
			}

			for slot := range account.storageReads {
				if agg.storageReads == nil {
					agg.storageReads = make(map[common.Hash]struct{})
				}
				agg.storageReads[slot] = struct{}{}
			}

			if account.balanceSet {
				change := agg.ensureBalance(snapshot.index)
				if account.balance != nil {
					change.Value = new(big.Int).Set(account.balance)
				} else {
					change.Value = new(big.Int)
				}
			}

			if account.nonceSet {
				change := agg.ensureNonce(snapshot.index)
				change.Value = account.nonce
			}

			if account.codeSet {
				change := agg.ensureCode(snapshot.index)
				if account.code != nil {
					change.Data = slices.Clone(account.code)
				} else {
					change.Data = nil
				}
			}
		}
	}

	for addr := range sdb.stateObjectsDirty {
		if _, ok := aggregated[addr]; !ok {
			aggregated[addr] = &blockAccessAggregateAccount{touched: true}
		}
	}

	if len(aggregated) == 0 {
		return make(types.BlockAccessList, 0), nil
	}

	addresses := make([]common.Address, 0, len(aggregated))
	for addr := range aggregated {
		addresses = append(addresses, addr)
	}
	sort.Slice(addresses, func(i, j int) bool {
		return bytes.Compare(addresses[i][:], addresses[j][:]) < 0
	})

	result := make(types.BlockAccessList, 0, len(addresses))

	for _, addr := range addresses {
		agg := aggregated[addr]
		account := &types.AccountChanges{Address: addr}

		if len(agg.slotChanges) > 0 {
			slotKeys := make([]common.Hash, 0, len(agg.slotChanges))
			for slot := range agg.slotChanges {
				slotKeys = append(slotKeys, slot)
			}
			sort.Slice(slotKeys, func(i, j int) bool {
				return bytes.Compare(slotKeys[i][:], slotKeys[j][:]) < 0
			})

			account.StorageChanges = make([]*types.SlotChanges, 0, len(slotKeys))
			for _, slot := range slotKeys {
				slotChanges := agg.slotChanges[slot]
				sort.Slice(slotChanges.Changes, func(i, j int) bool {
					return slotChanges.Changes[i].Index < slotChanges.Changes[j].Index
				})
				account.StorageChanges = append(account.StorageChanges, slotChanges)
			}
		}

		if len(agg.storageReads) > 0 {
			readSlots := make([]common.Hash, 0, len(agg.storageReads))
			for slot := range agg.storageReads {
				readSlots = append(readSlots, slot)
			}
			sort.Slice(readSlots, func(i, j int) bool {
				return bytes.Compare(readSlots[i][:], readSlots[j][:]) < 0
			})
			account.StorageReads = readSlots
		}

		if len(agg.balances) > 0 {
			indices := make([]uint16, 0, len(agg.balances))
			for idx := range agg.balances {
				indices = append(indices, idx)
			}
			sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
			account.BalanceChanges = make([]*types.BalanceChange, 0, len(indices))
			for _, idx := range indices {
				change := agg.balances[idx]
				if change.Value == nil {
					change.Value = new(big.Int)
				} else {
					change.Value = new(big.Int).Set(change.Value)
				}
				account.BalanceChanges = append(account.BalanceChanges, change)
			}
		}

		if len(agg.nonces) > 0 {
			indices := make([]uint16, 0, len(agg.nonces))
			for idx := range agg.nonces {
				indices = append(indices, idx)
			}
			sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
			account.NonceChanges = make([]*types.NonceChange, 0, len(indices))
			for _, idx := range indices {
				account.NonceChanges = append(account.NonceChanges, agg.nonces[idx])
			}
		}

		if len(agg.codes) > 0 {
			indices := make([]uint16, 0, len(agg.codes))
			for idx := range agg.codes {
				indices = append(indices, idx)
			}
			sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
			account.CodeChanges = make([]*types.CodeChange, 0, len(indices))
			for _, idx := range indices {
				change := agg.codes[idx]
				if change.Data != nil {
					change.Data = slices.Clone(change.Data)
				}
				account.CodeChanges = append(account.CodeChanges, change)
			}
		}

		if agg.touched || len(account.StorageChanges) > 0 || len(account.StorageReads) > 0 || len(account.BalanceChanges) > 0 || len(account.NonceChanges) > 0 || len(account.CodeChanges) > 0 {
			result = append(result, account)
		}
	}

	return result, nil
}

func (sdb *IntraBlockState) resetBlockAccessTxTouches() {
	if !sdb.blockAccessEnabled {
		sdb.blockAccessTxTouches = nil
		return
	}
	sdb.blockAccessTxTouches = make(map[common.Address]*blockAccessTouch)
}

func (sdb *IntraBlockState) ensureBlockAccessTouch(addr common.Address) *blockAccessTouch {
	if sdb.blockAccessTxTouches == nil {
		sdb.blockAccessTxTouches = make(map[common.Address]*blockAccessTouch)
	}
	t := sdb.blockAccessTxTouches[addr]
	if t == nil {
		t = &blockAccessTouch{}
		sdb.blockAccessTxTouches[addr] = t
	}
	return t
}

func (sdb *IntraBlockState) applyJournalEntry(snapshot *blockAccessSnapshot, writtenSlots map[common.Address]map[common.Hash]struct{}, entry journalEntry) error {
	switch e := entry.(type) {
	case createObjectChange:
		addr := e.account
		acc := snapshot.account(addr)
		acc.touched = true
	case resetObjectChange:
		addr := e.account
		acc := snapshot.account(addr)
		acc.touched = true
	case storageChange:
		if e.account == nil {
			return nil
		}
		addr := *e.account
		acc := snapshot.account(addr)
		acc.touched = true

		so, err := sdb.getStateObject(addr)
		if err != nil {
			return err
		}
		var value common.Hash
		if so != nil && !so.deleted {
			var current uint256.Int
			so.GetState(e.key, &current)
			value = uint256ToHash(&current)
		}
		acc.ensureStorageWrites()[e.key] = value

		slotSet := writtenSlots[addr]
		if slotSet == nil {
			slotSet = make(map[common.Hash]struct{})
			writtenSlots[addr] = slotSet
		}
		slotSet[e.key] = struct{}{}
	case balanceChange:
		if e.account == nil {
			return nil
		}
		addr := *e.account
		acc := snapshot.account(addr)
		acc.touched = true

		so, err := sdb.getStateObject(addr)
		if err != nil {
			return err
		}
		if so != nil && !so.deleted {
			balance := so.Balance()
			acc.setBalance(balance.ToBig())
		} else {
			acc.setBalance(new(big.Int))
		}
	case nonceChange:
		if e.account == nil {
			return nil
		}
		addr := *e.account
		acc := snapshot.account(addr)
		acc.touched = true

		so, err := sdb.getStateObject(addr)
		if err != nil {
			return err
		}
		if so != nil && !so.deleted {
			acc.setNonce(so.Nonce())
		} else {
			acc.setNonce(0)
		}
	case codeChange:
		if e.account == nil {
			return nil
		}
		addr := *e.account
		acc := snapshot.account(addr)
		acc.touched = true

		so, err := sdb.getStateObject(addr)
		if err != nil {
			return err
		}
		if so != nil && !so.deleted {
			code, err := so.Code()
			if err != nil {
				return err
			}
			acc.setCode(code)
		} else {
			acc.setCode(nil)
		}
	case selfdestructChange:
		if e.account == nil {
			return nil
		}
		addr := *e.account
		acc := snapshot.account(addr)
		acc.touched = true

		so, err := sdb.getStateObject(addr)
		if err != nil {
			return err
		}
		if so != nil && !so.deleted {
			balance := so.Balance()
			acc.setBalance(balance.ToBig())
			acc.setCode(nil)
		} else {
			acc.setBalance(new(big.Int))
			acc.setCode(nil)
		}
	case touchChange:
		addr := e.account
		acc := snapshot.account(addr)
		acc.touched = true
	case balanceIncrease:
		if e.account == nil {
			return nil
		}
		addr := *e.account
		acc := snapshot.account(addr)
		acc.touched = true
	}
	return nil
}

func (sdb *IntraBlockState) applyTouches(snapshot *blockAccessSnapshot, writtenSlots map[common.Address]map[common.Hash]struct{}) {
	if len(sdb.blockAccessTxTouches) == 0 {
		return
	}
	for addr, touch := range sdb.blockAccessTxTouches {
		if touch == nil {
			continue
		}
		acc := snapshot.account(addr)
		if touch.address {
			acc.touched = true
		}
		if len(touch.slots) == 0 {
			continue
		}
		acc.touched = true

		written := writtenSlots[addr]
		for slot := range touch.slots {
			if written != nil {
				if _, ok := written[slot]; ok {
					continue
				}
			}
			acc.ensureStorageReads()[slot] = struct{}{}
		}
	}
}
