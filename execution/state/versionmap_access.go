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
	"cmp"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// SetBaseTxNum records the block's first global txNum (the block-start system tx at
// TxIndex -1), so a global txNum maps to the block-relative TxIndex:
// txNum = baseTxNum + TxIndex + 1.
func (vm *VersionMap) SetBaseTxNum(baseTxNum uint64) { vm.baseTxNum.Store(baseTxNum) }
func (vm *VersionMap) BaseTxNum() uint64             { return vm.baseTxNum.Load() }

// VersionMapView is a post-seal, data-only read over the version map pinned at a
// global txNum. It composes each account/slot from the map cells, falling back to base
// for fields no cell covers, and carries no lifecycle or EIP reasoning — the add/remove
// verdict is read straight off the sealed state. It is a pure access class: construct
// one for a txNum, then read by address/key. Execution does not use it; exec reads the
// live, lifecycle-aware map mid-block.
type VersionMapView struct {
	vm    *VersionMap
	txNum uint64
	base  StateReader
	err   error
}

// Err reports the first base read error of any read through the view; a read that
// hit one reports absent, so the caller must check Err before trusting the results.
func (view *VersionMapView) Err() error { return view.err }

// View pins a read of vm at the given global txNum. base is the cascade tail —
// predecessor version maps then the shared domain — supplying fields no cell covers.
func (vm *VersionMap) View(txNum uint64, base StateReader) *VersionMapView {
	return &VersionMapView{vm: vm, txNum: txNum, base: base}
}

func (view *VersionMapView) txIdx() int {
	return int(view.txNum - view.vm.baseTxNum.Load())
}

// GetAccount composes addr's final account at the view's txNum from the version map
// cells over base and reports whether it belongs in the trie. The add/remove verdict is
// read straight off the state: a ValueDeleted balance cell is a removal — exec already
// resolved a self-destruct burn or an EIP-161 empty into it, the same value it flushes
// to the base domain as a delete — and an account with no live cell here that the base
// also reports absent does not exist (in a cross-block batch the touched union carries
// accounts removed in an earlier block; the base resolves them as absent). Otherwise the
// composed account is present. No self-destruct / EIP / lifecycle reasoning crosses into
// the read; it is the why-free add/remove projection.
func (view *VersionMapView) GetAccount(addr accounts.Address) (accounts.Account, bool) {
	txIdx := view.txIdx()
	if _, balRes, balOk := view.vm.ReadBalance(addr, txIdx); balOk && balRes.ValStatus() == ValueDeleted {
		return accounts.Account{}, false
	}
	av := NewVersionedAccountView(addr, txIdx, view.vm, view.base)
	exists := av.recordExists()
	if err := av.Err(); err != nil {
		view.err = cmp.Or(view.err, err)
		return accounts.Account{}, false
	}
	if !exists {
		return accounts.Account{}, false
	}
	return *av.compose(), true
}

// GetStorage returns addr's slot value at the view's txNum, in comma-ok form: a true ok
// with a zero value is a slot cleared to zero (a leaf-removing delete), false ok means no
// value resolves. A slot with no cell in this map falls back to base — a cross-block
// batch's touched union spans earlier blocks whose storage writes are not in this map.
func (view *VersionMapView) GetStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool) {
	if val, res, ok := liveStorageValue(view.vm, addr, key, view.txIdx()); ok && res.Status() != MVReadResultNone {
		return val, true
	}
	if view.base != nil {
		val, ok, err := view.base.ReadAccountStorage(addr, key)
		if err != nil {
			view.err = cmp.Or(view.err, err)
			return uint256.Int{}, false
		}
		if ok {
			return val, true
		}
	}
	return uint256.Int{}, false
}

// WasDestroyed reports whether addr was destroyed or removed (self-destruct or EIP-161
// touched-empty removal) at or below the view's txNum — the lifecycle verdict the
// commitment uses to delete the account's whole storage subtree.
func (view *VersionMapView) WasDestroyed(addr accounts.Address) bool {
	state, _, _, _ := view.vm.AccountLifecycleAt(addr, view.txIdx())
	return state != LifecycleLive
}

func (vm *VersionMap) SeedEmptyRemovals(eip161Enabled, isAura bool, base StateReader) error {
	var stamps []*WriteSet
	var candidates []uint32
	for i := range vm.bal {
		ac := &vm.bal[i]
		addr := accounts.InternAddress(ac.Address)
		if !EIP161EmptyRemoval(eip161Enabled, isAura, addr) {
			continue
		}
		var maxIdx uint32
		balChangeIndexes(ac, func(idx uint32) { maxIdx = max(maxIdx, idx) })
		if maxIdx == 0 {
			continue // only reads, no field change — not a removal candidate
		}
		candidates = candidates[:0]
		for _, c := range ac.BalanceChanges {
			if c.Value.IsZero() && c.Index != maxIdx {
				candidates = append(candidates, c.Index)
			}
		}
		candidates = append(candidates, maxIdx)
		for _, idx := range candidates {
			if balProvesNonEmptyAt(ac, idx) {
				continue
			}
			av := NewVersionedAccountView(addr, int(idx), vm, base)
			acc := av.Account()
			if err := av.Err(); err != nil {
				return err
			}
			if acc == nil || !acc.Balance.IsZero() || acc.Nonce != 0 || !acc.CodeHash.IsEmpty() {
				continue
			}
			rm := &WriteSet{}
			rm.SetEmptyRemoval(addr, Version{TxIndex: int(idx) - 1})
			stamps = append(stamps, rm)
			if idx == maxIdx {
				continue
			}
			next := balNextChangeIndex(ac, idx)
			if !slices.ContainsFunc(ac.BalanceChanges, func(c *types.BalanceChange) bool { return c.Index == next }) {
				live := &WriteSet{}
				live.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: int(next) - 1}}})
				stamps = append(stamps, live)
			}
		}
	}
	for _, ws := range stamps {
		vm.FlushVersionedWrites(ws, true)
	}
	return nil
}

// SeedAddresses marks each BAL account as existing at its creating tx, so the address
// existence read resolves present without a first-access base lookup in exec. The BAL
// writes only field cells (not an AddressPath), so a block-created account would
// otherwise read absent — the EIP-161 withhold path then stamps a spurious removal. The
// existence is taken straight from the BAL (the account's first change index), with no
// base read: the AddressPath is stamped ValueUnknown, so field reads still fall back to
// the committed base for a base-present account's untouched fields.
func (vm *VersionMap) SeedAddresses() {
	for i := range vm.bal {
		minIdx := balNextChangeIndex(&vm.bal[i], 0)
		if minIdx == 0 {
			continue
		}
		addr := accounts.InternAddress(vm.bal[i].Address)
		if acc := NewVersionedAccountView(addr, int(minIdx), vm, nil).Account(); acc != nil {
			vm.WriteAddressExists(addr, Version{TxIndex: int(minIdx) - 1}, acc)
		}
	}
}

// balProvesNonEmptyAt reports whether the account's last balance, nonce or code change at or
// below idx already makes it non-empty, so no committed-base read is needed.
func balProvesNonEmptyAt(ac *types.AccountChanges, idx uint32) bool {
	if c := lastChangeAt(ac.BalanceChanges, idx, func(c *types.BalanceChange) uint32 { return c.Index }); c != nil && !c.Value.IsZero() {
		return true
	}
	if c := lastChangeAt(ac.NonceChanges, idx, func(c *types.NonceChange) uint32 { return c.Index }); c != nil && c.Value != 0 {
		return true
	}
	c := lastChangeAt(ac.CodeChanges, idx, func(c *types.CodeChange) uint32 { return c.Index })
	return c != nil && len(c.Bytecode) > 0
}

func lastChangeAt[T any](changes []*T, idx uint32, index func(*T) uint32) *T {
	var last *T
	for _, c := range changes {
		if i := index(c); i <= idx && (last == nil || i > index(last)) {
			last = c
		}
	}
	return last
}

func balChangeIndexes(ac *types.AccountChanges, fn func(uint32)) {
	for _, c := range ac.BalanceChanges {
		fn(c.Index)
	}
	for _, c := range ac.NonceChanges {
		fn(c.Index)
	}
	for _, c := range ac.CodeChanges {
		fn(c.Index)
	}
	for _, sc := range ac.StorageChanges {
		for _, c := range sc.Changes {
			fn(c.Index)
		}
	}
}

func balNextChangeIndex(ac *types.AccountChanges, after uint32) uint32 {
	var m uint32
	balChangeIndexes(ac, func(idx uint32) {
		if idx > after && (m == 0 || idx < m) {
			m = idx
		}
	})
	return m
}
