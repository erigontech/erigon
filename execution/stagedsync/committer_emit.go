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

package stagedsync

import (
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// The GetAccount commitment path folds a block's updates straight from the version
// map instead of accumulating per-field values in calcState. accumulateTouched
// collects the block's key-only touched union (account keys, storage slots, and the
// self-destruct subtree-reset set); emitTouched then reads each final value from the
// version map via GetAccount/GetStorage and writes one Update per key. The version map
// already carries the EIP-161 / self-destruct existence verdict, so this path has no
// EIP knowledge and no re-derivation.

func (cc *commitmentCalculator) ensureTouched() {
	if cc.touchedAccounts == nil {
		cc.touchedAccounts = map[accounts.Address]struct{}{}
		cc.touchedStorage = map[accounts.Address]map[accounts.StorageKey]struct{}{}
		cc.destructed = map[accounts.Address]struct{}{}
	}
}

func (cc *commitmentCalculator) clearTouched() {
	clear(cc.touchedAccounts)
	clear(cc.touchedStorage)
	clear(cc.destructed)
}

// accumulateTouched folds one write-set view's key-set into the touched union. An
// account is marked touched by any field write and by any storage write (so its
// storageRoot refolds); a slot is marked only when it actually changed
// (StoragesChanged drops net-zero writes); a true SelfDestruct marks the subtree reset.
func (cc *commitmentCalculator) accumulateTouched(writes state.WriteSetView) {
	if writes == nil || writes.IsEmpty() {
		return
	}
	cc.ensureTouched()
	for a := range writes.Balances() {
		cc.touchedAccounts[a] = struct{}{}
	}
	for a := range writes.Nonces() {
		cc.touchedAccounts[a] = struct{}{}
	}
	for a := range writes.Incarnations() {
		cc.touchedAccounts[a] = struct{}{}
	}
	for a := range writes.CodeHashes() {
		cc.touchedAccounts[a] = struct{}{}
	}
	for a := range writes.Codes() {
		cc.touchedAccounts[a] = struct{}{}
	}
	for a := range writes.CreateContracts() {
		cc.touchedAccounts[a] = struct{}{}
	}
	for a, vw := range writes.SelfDestructs() {
		cc.touchedAccounts[a] = struct{}{}
		if vw.Val {
			cc.destructed[a] = struct{}{}
		}
	}
	for a := range writes.Storages() {
		cc.touchedAccounts[a] = struct{}{}
	}
	for a, slots := range writes.StoragesChanged() {
		m := cc.touchedStorage[a]
		if m == nil {
			m = map[accounts.StorageKey]struct{}{}
			cc.touchedStorage[a] = m
		}
		for k := range slots {
			m[k] = struct{}{}
		}
	}
}

// emitTouched writes one commitment Update per touched key, reading each final value
// from vm at the fold txNum T over base (the prevBlocks-aware read-through reader): an
// account absent at T removes its leaf (and subtree), a present one writes its full
// account; a storage slot cleared to zero removes its leaf, a non-zero one writes it.
func (cc *commitmentCalculator) emitTouched(vm *state.VersionMap, T uint64, base state.StateReader, updates *commitment.Updates) {
	for addr := range cc.touchedAccounts {
		address := addr.Value()
		var u commitment.Update
		if acc, ok := vm.GetAccount(addr, T, base); ok {
			u = commitment.Update{
				Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
				Balance:  acc.Balance,
				Nonce:    acc.Nonce,
				CodeHash: acc.CodeHash.Value(),
			}
		} else {
			u = commitment.Update{Flags: commitment.DeleteUpdate, CodeHash: empty.CodeHash}
			u.DeleteStorageSubtree = true
		}
		if _, destructed := cc.destructed[addr]; destructed {
			u.DeleteStorageSubtree = true
		}
		updates.TouchPlainKeyDirect(string(address[:]), &u)
	}
	for addr, slots := range cc.touchedStorage {
		address := addr.Value()
		for slot := range slots {
			slotVal := slot.Value()
			composite := make([]byte, 20+32)
			copy(composite, address[:])
			copy(composite[20:], slotVal[:])
			var u commitment.Update
			if val, ok := vm.GetStorage(addr, slot, T); ok && !val.IsZero() {
				vBytes := val.Bytes()
				u.Flags = commitment.StorageUpdate
				u.StorageLen = int8(len(vBytes))
				copy(u.Storage[:], vBytes)
			} else {
				u.Flags = commitment.DeleteUpdate
			}
			updates.TouchPlainKeyDirect(string(composite), &u)
		}
	}
}
