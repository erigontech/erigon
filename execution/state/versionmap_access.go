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
// composed account is present. wasDeleted reports a self-destruct or EIP-161 removal at
// or below the view's txNum, the commitment's signal to drop the whole storage subtree.
func (view *VersionMapView) GetAccount(addr accounts.Address) (acc accounts.Account, ok, wasDeleted bool) {
	f := view.vm.accountFloorsAt(addr, view.txIdx())
	if f.balanceDeleted {
		return accounts.Account{}, false, f.wasDeleted
	}
	base, err := view.baseAccount(addr, &f)
	if err != nil {
		view.err = cmp.Or(view.err, err)
		return accounts.Account{}, false, f.wasDeleted
	}
	if base == nil && !f.hasBalance && !f.hasNonce && !f.hasIncarnation && !f.hasCodeHash {
		return accounts.Account{}, false, f.wasDeleted
	}
	acc = accounts.NewAccount()
	if base != nil {
		acc = *base
	}
	if f.hasBalance {
		acc.Balance = f.balance
	}
	if f.hasNonce {
		acc.Nonce = f.nonce
	}
	if f.hasIncarnation {
		acc.Incarnation = f.incarnation
	}
	if f.hasCodeHash {
		acc.CodeHash = f.codeHash
	}
	return acc, true, f.wasDeleted
}

// baseAccount is the record the field cells overlay: a non-BAL address cell, else the
// seeded origin when no address cell exists, else the base reader.
func (view *VersionMapView) baseAccount(addr accounts.Address, f *accountFloors) (*accounts.Account, error) {
	switch {
	case f.hasAddress && !f.addressFromBAL && f.address != nil:
		return f.address, nil
	case !f.hasAddress:
		if oacc, seeded := view.vm.LoadOriginAccount(addr); seeded && oacc != nil {
			return oacc, nil
		}
	}
	if view.base == nil {
		return nil, nil
	}
	return view.base.ReadAccountData(addr)
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

// GetCode returns addr's code written in this map at or below the view's txNum; false ok means
// no tx of the map wrote it, and the caller reads its base.
func (view *VersionMapView) GetCode(addr accounts.Address) ([]byte, bool) {
	code, _, ok := view.vm.ReadCode(addr, view.txIdx())
	return code.Bytes, ok
}

// DestroyedAt reports whether addr was destroyed or removed (self-destruct or EIP-161
// touched-empty removal) at or below the view's txNum, and the tx index of that destruct —
// the lifecycle verdict the commitment uses to delete the account's whole storage subtree.
func (view *VersionMapView) DestroyedAt(addr accounts.Address) (txIdx int, destroyed bool) {
	state, _, destroyedAt, _ := view.vm.AccountLifecycleAt(addr, view.txIdx())
	return destroyedAt, state != LifecycleLive
}

// HasEmptyBaseAccount reports whether the BAL lists an account that is EIP-161 empty in base. A
// 0-value touch removes such an account, and the BAL does not show the removal.
func (vm *VersionMap) HasEmptyBaseAccount(eip161Enabled, isAura bool, base StateReader) (bool, error) {
	for i := range vm.bal {
		addr := accounts.InternAddress(vm.bal[i].Address)
		if !EIP161EmptyRemoval(eip161Enabled, isAura, addr) {
			continue
		}
		acc, err := base.ReadAccountData(addr)
		if err != nil {
			return false, err
		}
		if acc != nil && acc.Empty() {
			return true, nil
		}
	}
	return false, nil
}

func (vm *VersionMap) SeedEmptyRemovals(eip161Enabled, isAura bool, base StateReader) error {
	var stamps []*WriteSet
	var candidates []uint32
	for i := range vm.bal {
		ac := &vm.bal[i]
		var maxIdx uint32
		balChangeIndexes(ac, func(idx uint32) { maxIdx = max(maxIdx, idx) })
		if maxIdx == 0 {
			continue // only reads, no field change — not a removal candidate
		}
		addr := accounts.InternAddress(ac.Address)
		if !EIP161EmptyRemoval(eip161Enabled, isAura, addr) {
			continue
		}
		if len(ac.BalanceChanges) == 0 && len(ac.NonceChanges) == 0 && len(ac.CodeChanges) == 0 {
			continue
		}
		candidates = candidates[:0]
		for _, c := range ac.BalanceChanges {
			if c.Value.IsZero() && c.Index != maxIdx {
				candidates = append(candidates, c.Index)
			}
		}
		candidates = append(candidates, maxIdx)
		for _, idx := range candidates {
			if b, ok := versionedUpdateBalance(vm, addr, int(idx)); ok && !b.IsZero() {
				continue
			}
			if n, ok := versionedUpdateNonce(vm, addr, int(idx)); ok && n != 0 {
				continue
			}
			if ch, ok := versionedUpdateCodeHash(vm, addr, int(idx)); ok && !ch.IsEmpty() {
				continue
			}
			av := VersionedAccountView{addr: addr, txIdx: int(idx), vm: vm, base: base}
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
// existence read resolves present. The BAL
// writes only field cells (not an AddressPath), so a block-created account would
// otherwise read absent — the EIP-161 withhold path then stamps a spurious removal. The
// existence is taken straight from the BAL (the account's first change index), with no
// base read: the AddressPath is marked AccountFromBAL, so field reads still fall back to
// the committed base for a base-present account's untouched fields.
func (vm *VersionMap) SeedAddresses() {
	forEachBALChunk(len(vm.bal), func(int) int { return 1 }, func(lo, hi int) {
		for i := lo; i < hi; i++ {
			minIdx := balNextChangeIndex(&vm.bal[i], 0)
			if minIdx == 0 {
				continue
			}
			addr := accounts.InternAddress(vm.bal[i].Address)
			av := VersionedAccountView{addr: addr, txIdx: int(minIdx), vm: vm}
			if acc := av.Account(); acc != nil {
				vm.WriteAddressExists(addr, Version{TxIndex: int(minIdx) - 1}, acc)
			}
		}
	})
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
