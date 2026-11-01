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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// SetBaseTxNum records the block's first global txNum (the block-start system tx at
// TxIndex -1), so a global txNum maps to the block-relative TxIndex:
// txNum = baseTxNum + TxIndex + 1.
func (vm *VersionMap) SetBaseTxNum(baseTxNum uint64) { vm.baseTxNum.Store(baseTxNum) }
func (vm *VersionMap) BaseTxNum() uint64             { return vm.baseTxNum.Load() }

// blockTxIndex maps a global txNum to the block-relative TxIndex, using the map's base.
func (vm *VersionMap) blockTxIndex(txNum uint64) int {
	return int(txNum-vm.baseTxNum.Load()) - 1
}

// GetAccount returns addr's account composed at the given global txNum — including
// the writes of the tx at that txNum — and whether it exists there, in comma-ok form.
// It is the post-seal projection accessor commitment uses instead of folding per-field
// cells and re-deriving existence: a false ok means the account is deleted/absent.
// Reading one past the tx's TxIndex (txNum - baseTxNum, since txNum = baseTxNum +
// TxIndex + 1) includes that tx's own writes.
func (vm *VersionMap) GetAccount(addr accounts.Address, txNum uint64, base AccountBaseReader) (accounts.Account, bool) {
	txIdx := int(txNum - vm.baseTxNum.Load())
	return NewVersionedAccountView(addr, txIdx, vm, base).GetAccount()
}

// VersionMapView is a post-seal, data-only read over the version map pinned at a
// global txNum. It composes each account/slot from the map cells, falling back to base
// for fields no cell covers, and carries no lifecycle or EIP reasoning — the add/remove
// verdict is read straight off the sealed state. It is a pure access class: construct
// one for a txNum, then read by address/key. Execution does not use it; exec reads the
// live, lifecycle-aware map mid-block.
type VersionMapView struct {
	vm    *VersionMap
	txNum uint64
	base  AccountBaseReader
	err   error
}

// Err reports the first base read error of any read through the view; a read that
// hit one reports absent, so the caller must check Err before trusting the results.
func (view *VersionMapView) Err() error { return view.err }

// View pins a read of vm at the given global txNum. base is the cascade tail —
// predecessor version maps then the shared domain — supplying fields no cell covers.
func (vm *VersionMap) View(txNum uint64, base AccountBaseReader) *VersionMapView {
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
	acc := av.AccountForCommitment()
	if err := av.Err(); err != nil {
		view.err = cmp.Or(view.err, err)
		return accounts.Account{}, false
	}
	if acc == nil {
		return accounts.Account{}, false
	}
	return *acc, true
}

// GetStorage returns addr's slot value at the view's txNum, in comma-ok form: a true ok
// with a zero value is a slot cleared to zero (a leaf-removing delete), false ok means no
// value resolves. A slot with no cell in this map falls back to base — a cross-block
// batch's touched union spans earlier blocks whose storage writes are not in this map.
func (view *VersionMapView) GetStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool) {
	if val, res, ok := liveStorageValue(view.vm, addr, key, view.txIdx()); ok && res.Status() != MVReadResultNone {
		return val, true
	}
	if sr, ok := view.base.(StorageBaseReader); ok && sr != nil {
		val, ok, err := sr.ReadStorage(addr, key)
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

func (vm *VersionMap) SeedEmptyRemovals(bal types.BlockAccessList, eip161Enabled, isAura bool, base AccountBaseReader) error {
	ws := &WriteSet{}
	var interior []*WriteSet
	var candidates []uint32
	for i := range bal {
		ac := &bal[i]
		addr := accounts.InternAddress(ac.Address)
		if !EIP161EmptyRemoval(eip161Enabled, isAura, addr) {
			continue
		}
		maxIdx := balMaxChangeIndex(ac)
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
			av := NewVersionedAccountView(addr, int(idx), vm, base)
			acc, ok := av.GetAccount()
			if err := av.Err(); err != nil {
				return err
			}
			if !ok || !isEmptyForEIP161(acc.Balance, acc.Nonce, acc.CodeHash) {
				continue
			}
			if idx == maxIdx {
				ws.SetEmptyRemoval(addr, Version{TxIndex: int(idx) - 1})
				continue
			}
			rm := &WriteSet{}
			rm.SetEmptyRemoval(addr, Version{TxIndex: int(idx) - 1})
			interior = append(interior, rm)
		}
	}
	for _, rm := range interior {
		vm.FlushVersionedWrites(rm, true)
	}
	if !ws.IsEmpty() {
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
func (vm *VersionMap) SeedAddresses(bal types.BlockAccessList) {
	for i := range bal {
		vm.seedOneAddress(&bal[i])
	}
}

func (vm *VersionMap) seedOneAddress(ac *types.AccountChanges) {
	minIdx := balMinChangeIndex(ac)
	if minIdx == 0 {
		return // only reads, nothing created
	}
	addr := accounts.InternAddress(ac.Address)
	// Compose the existence marker from the BAL field cells alone (nil base → no DB
	// read); a touched-empty account composes to nil and is left for the withhold.
	acc, ok := NewVersionedAccountView(addr, int(minIdx), vm, nil).GetAccount()
	if !ok {
		return
	}
	vm.WriteAddressExists(addr, Version{TxIndex: int(minIdx) - 1}, &acc)
}

// balMinChangeIndex is the lowest 1-based change index across an account's BAL entries,
// i.e. the index at which it first appears this block, or 0 if it only has reads.
func balMinChangeIndex(ac *types.AccountChanges) uint32 {
	var m uint32
	upd := func(idx uint32) {
		if idx != 0 && (m == 0 || idx < m) {
			m = idx
		}
	}
	for _, c := range ac.BalanceChanges {
		upd(c.Index)
	}
	for _, c := range ac.NonceChanges {
		upd(c.Index)
	}
	for _, c := range ac.CodeChanges {
		upd(c.Index)
	}
	for _, sc := range ac.StorageChanges {
		for _, c := range sc.Changes {
			upd(c.Index)
		}
	}
	return m
}

// balMaxChangeIndex is the highest 1-based change index across an account's BAL
// entries, i.e. the index of its last touch this block.
func balMaxChangeIndex(ac *types.AccountChanges) uint32 {
	var m uint32
	for _, c := range ac.BalanceChanges {
		m = max(m, c.Index)
	}
	for _, c := range ac.NonceChanges {
		m = max(m, c.Index)
	}
	for _, c := range ac.CodeChanges {
		m = max(m, c.Index)
	}
	for _, sc := range ac.StorageChanges {
		for _, c := range sc.Changes {
			m = max(m, c.Index)
		}
	}
	return m
}

// isEmptyForEIP161 reports whether an account is empty per EIP-161: zero balance,
// zero nonce, and empty (or nil) code hash.
func isEmptyForEIP161(balance uint256.Int, nonce uint64, codeHash accounts.CodeHash) bool {
	return balance.IsZero() && nonce == 0 && (codeHash == accounts.EmptyCodeHash || codeHash == accounts.CodeHash{})
}
