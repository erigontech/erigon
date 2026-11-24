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
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
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
	// preferOrigin serves a BAL-marked account from its seeded in-memory origin rather
	// than a base read — a validation-time speed-up for the exec/apply readers. Commit's
	// View leaves it false and reads the committed base ("the marker hides the origin").
	preferOrigin bool
}

// Err reports the first base read error of any read through the view; a read that
// hit one reports absent, so the caller must check Err before trusting the results.
func (view *VersionMapView) Err() error { return view.err }

// View pins a read of vm at the given global txNum. base is the cascade tail —
// predecessor version maps then the shared domain — supplying fields no cell covers.
func (vm *VersionMap) View(txNum uint64, base StateReader) *VersionMapView {
	return &VersionMapView{vm: vm, txNum: txNum, base: base}
}

// ViewAtTxIndex pins the finalized view at a block-relative TxIndex rather than a global
// txNum. The modular arithmetic round-trips negative (system-tx) indices back through
// txIdx(). This is the reader≡commit view — no apply-only arbitration.
func (vm *VersionMap) ViewAtTxIndex(txIndex int, base StateReader) *VersionMapView {
	return &VersionMapView{vm: vm, txNum: vm.baseTxNum.Load() + uint64(txIndex), base: base}
}

// ApplyView is the apply arbiter's view at a block-relative TxIndex: identical to the
// finalized view except it arbitrates the base toward the in-memory seeded origin
// (the mid-block vm-vs-DB cascade) rather than reading the committed DB, which the
// finalized reader/commit use. Only apply sees this difference.
func (vm *VersionMap) ApplyView(txIndex int, base StateReader) *VersionMapView {
	v := vm.ViewAtTxIndex(txIndex, base)
	v.preferOrigin = true
	return v
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
	acc, base, composed := view.composeAccount(addr, &f)
	if !composed {
		return accounts.Account{}, false, f.wasDeleted
	}
	// A self-destruct with no revival that composes empty is gone — the one existence
	// rule, keyed on the lifecycle, not on the fork. (A balance-preserving destruct is
	// not empty and stays as a balance-only EOA.) The EIP-161 removal of a LIVE
	// touched-empty account is a separate fork-specific write-side decision, stamped as
	// a ValueDeleted balance (handled above), so a 0-value touch still reads present.
	if f.absent && acc.Empty() {
		return accounts.Account{}, false, f.wasDeleted
	}
	// No committed base and no field cell: nothing to compose, so no record. A non-BAL
	// address cell becomes the base above, so it is already present; a bare BAL marker
	// does not (it defers to base/origin).
	if base == nil && !f.hasBalance && !f.hasNonce && !f.hasIncarnation && !f.hasCodeHash {
		return accounts.Account{}, false, f.wasDeleted
	}
	return acc, true, f.wasDeleted
}

// composeAccount layers the version-map field cells over base — the single field
// interpretation shared by every existence verdict. It returns the composed account,
// the resolved base (nil if none), and false if a base read failed. The add/remove
// decision is the caller's: GetAccount applies the trie (EIP-161) verdict,
// accountByPresence the seeding verdict.
func (view *VersionMapView) composeAccount(addr accounts.Address, f *accountFloors) (accounts.Account, *accounts.Account, bool) {
	base, err := view.baseAccount(addr, f)
	if err != nil {
		view.err = cmp.Or(view.err, err)
		return accounts.Account{}, nil, false
	}
	acc := accounts.NewAccount()
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
	return acc, base, true
}

// baseAccount is the record the field cells overlay: a non-BAL address cell, else the
// seeded origin when no address cell exists, else the base reader.
func (view *VersionMapView) baseAccount(addr accounts.Address, f *accountFloors) (*accounts.Account, error) {
	switch {
	case f.hasAddress && !f.addressFromBAL && f.address != nil:
		return f.address, nil
	case !f.hasAddress || (view.preferOrigin && f.addressFromBAL):
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
	if val, res, ok, _ := liveStorageValue(view.vm, addr, key, view.txIdx()); ok && res.Status() != MVReadResultNone {
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

// VersionMapView is the common read interface over the versioned cells plus base: it
// satisfies StateReader so apply, commit and external readers share one reader instead
// of the separate VersionedAccountView / versionedStateReader cores. The StateReader
// methods delegate to the GetAccount/GetStorage/GetCode read path; a per-call base-read
// error surfaces via the view's accumulated err.
var _ StateReader = (*VersionMapView)(nil)

func (view *VersionMapView) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	before := view.err
	acc, ok, _ := view.GetAccount(addr)
	if before == nil && view.err != nil {
		return nil, view.err
	}
	if !ok {
		return nil, nil
	}
	return &acc, nil
}

func (view *VersionMapView) ReadAccountByPlainAddr(address common.Address) (*accounts.Account, error) {
	return view.ReadAccountData(accounts.InternAddress(address))
}

func (view *VersionMapView) HasAccount(address common.Address) (bool, error) {
	acc, err := view.ReadAccountByPlainAddr(address)
	return acc != nil, err
}

func (view *VersionMapView) ReadAccountDataForDebug(addr accounts.Address) (*accounts.Account, error) {
	return view.ReadAccountData(addr)
}

func (view *VersionMapView) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	before := view.err
	val, ok := view.GetStorage(addr, key)
	if before == nil && view.err != nil {
		return uint256.Int{}, false, view.err
	}
	return val, ok, nil
}

func (view *VersionMapView) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	code, res, ok, life := view.vm.ReadCodeLife(addr, view.txIdx())
	if life.state != LifecycleLive {
		// Destroyed or removed: the bytecode is gone, consistent with the SD-wiped
		// code hash; only a code write above the destruct (a revival) survives.
		if ok && res.resolved() && res.DepIdx() > life.destroyedAt {
			return code.Bytes, nil
		}
		return nil, nil
	}
	if ok && res.Status() != MVReadResultNone {
		return code.Bytes, nil
	}
	if view.base != nil {
		return view.base.ReadAccountCode(addr)
	}
	return nil, nil
}

func (view *VersionMapView) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	code, err := view.ReadAccountCode(addr)
	return len(code), err
}

func (view *VersionMapView) ReadAccountIncarnation(addr accounts.Address) (uint64, error) {
	before := view.err
	acc, ok, _ := view.GetAccount(addr)
	if before == nil && view.err != nil {
		return 0, view.err
	}
	if !ok {
		return 0, nil
	}
	return acc.Incarnation, nil
}

func (view *VersionMapView) SetTrace(trace bool, tracePrefix string) {
	if view.base != nil {
		view.base.SetTrace(trace, tracePrefix)
	}
}

func (view *VersionMapView) Trace() bool {
	if view.base != nil {
		return view.base.Trace()
	}
	return false
}

func (view *VersionMapView) TracePrefix() string {
	if view.base != nil {
		return view.base.TracePrefix()
	}
	return ""
}

// HasAmbiguousBaseAccount reports whether the BAL lists an account whose change the BAL may not
// show. A 0-value touch removes an account that is EIP-161 empty in base. A create replaces the
// storage of an account with no nonce and no code, and such an account can hold storage only with
// a contract incarnation.
func (vm *VersionMap) HasAmbiguousBaseAccount(eip161Enabled, isAura bool, base StateReader) (bool, error) {
	for i := range vm.bal {
		addr := accounts.InternAddress(vm.bal[i].Address)
		acc, err := base.ReadAccountData(addr)
		if err != nil {
			return false, err
		}
		if acc == nil {
			continue
		}
		if acc.Empty() && EIP161EmptyRemoval(eip161Enabled, isAura, addr) {
			return true, nil
		}
		if acc.Nonce == 0 && acc.IsEmptyCodeHash() && acc.Incarnation > 0 {
			return true, nil
		}
	}
	return false, nil
}

// SeedEmptyRemovals considers only accounts with a balance, nonce, or code write
// and at least one write after block start. It checks for EIP-161 removal at
// zero-balance writes and at the account's final write, which may be to storage.
func (vm *VersionMap) SeedEmptyRemovals(eip161Enabled, isAura bool, base StateReader) error {
	var stamps []*WriteSet
	var removals []balRemoval
	for i := range vm.bal {
		ac := &vm.bal[i]
		maxIdx := max(
			balLastChangeIndex(ac.BalanceChanges),
			balLastChangeIndex(ac.NonceChanges),
			balLastChangeIndex(ac.CodeChanges),
		)
		for _, slot := range ac.StorageChanges {
			if changes := slot.Changes; len(changes) > 0 {
				maxIdx = max(maxIdx, changes[len(changes)-1].Index)
			}
		}
		if maxIdx == 0 {
			continue // The account is read-only or all its writes are at block start.
		}
		addr := accounts.InternAddress(ac.Address)
		if !EIP161EmptyRemoval(eip161Enabled, isAura, addr) {
			continue
		}
		if len(ac.BalanceChanges) == 0 && len(ac.NonceChanges) == 0 && len(ac.CodeChanges) == 0 {
			continue
		}
		removals = removals[:0]
		for j, c := range ac.BalanceChanges {
			if !c.Value.IsZero() || c.Index == maxIdx {
				continue
			}
			stamp, err := vm.deriveEmptyRemoval(addr, int(c.Index), base)
			if err != nil {
				return err
			}
			if stamp == nil {
				continue
			}
			stamps = append(stamps, stamp)
			removal := balRemoval{index: c.Index}
			if j+1 < len(ac.BalanceChanges) {
				removal.nextWriteIndex = ac.BalanceChanges[j+1].Index
			}
			removals = append(removals, removal)
		}
		if len(removals) > 0 {
			recordBALRevivals(removals, ac.NonceChanges)
			recordBALRevivals(removals, ac.CodeChanges)
			for _, slot := range ac.StorageChanges {
				recordBALRevivals(removals, slot.Changes)
			}
			for _, removal := range removals {
				if !removal.seedZeroBalance {
					continue
				}
				revival := &WriteSet{}
				revival.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{
					Address: addr, Path: BalancePath, Version: Version{TxIndex: int(removal.nextWriteIndex) - 1},
				}})
				stamps = append(stamps, revival)
			}
		}
		// Stage the final removal last so it overwrites a revival at the same index.
		stamp, err := vm.deriveEmptyRemoval(addr, int(maxIdx), base)
		if err != nil {
			return err
		}
		if stamp != nil {
			stamps = append(stamps, stamp)
		}
	}
	for _, ws := range stamps {
		vm.FlushVersionedWrites(ws, true)
	}
	return nil
}

func (vm *VersionMap) deriveEmptyRemoval(addr accounts.Address, balIndex int, base StateReader) (*WriteSet, error) {
	// One known field can rule out removal and save a base read.
	if b, ok := versionedUpdateBalance(vm, addr, balIndex); ok && !b.IsZero() {
		return nil, nil
	}
	if n, ok := versionedUpdateNonce(vm, addr, balIndex); ok && n != 0 {
		return nil, nil
	}
	if ch, ok := versionedUpdateCodeHash(vm, addr, balIndex); ok && !ch.IsEmpty() {
		return nil, nil
	}
	view := VersionedAccountView{addr: addr, txIdx: balIndex, vm: vm, base: base}
	acc := view.Account()
	if err := view.Err(); err != nil {
		return nil, err
	}
	if acc == nil || !acc.Balance.IsZero() || acc.Nonce != 0 || !acc.CodeHash.IsEmpty() {
		return nil, nil
	}
	txIdx := balIndex - 1
	stamp := &WriteSet{}
	stamp.SetEmptyRemoval(addr, Version{TxIndex: txIdx})
	vm.recordDerivedRemoval(addr, txIdx)
	return stamp, nil
}

func (vm *VersionMap) recordDerivedRemoval(addr accounts.Address, txIdx int) {
	if vm.derivedRemovals == nil {
		vm.derivedRemovals = map[derivedRemovalKey]struct{}{}
	}
	vm.derivedRemovals[derivedRemovalKey{addr, txIdx}] = struct{}{}
}

type derivedRemovalKey struct {
	addr  accounts.Address
	txIdx int
}

// SeedAddresses marks each BAL account as existing at its creating tx, so the address
// existence read resolves present. The BAL
// writes only field cells (not an AddressPath), so a block-created account would
// otherwise read absent — the EIP-161 withhold path then stamps a spurious removal. The
// existence is taken straight from the BAL (the account's first change index), with no
// base read: the AddressPath is marked AccountFromBAL, so field reads still fall back to
// the committed base for a base-present account's untouched fields.
func (vm *VersionMap) SeedAddresses() {
	for i := range vm.bal {
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

func balLastChangeIndex[T interface{ GetIndex() uint32 }](changes []T) uint32 {
	if len(changes) == 0 {
		return 0
	}
	return changes[len(changes)-1].GetIndex()
}

type balRemoval struct {
	index           uint32
	nextWriteIndex  uint32
	seedZeroBalance bool
}

// recordBALRevivals checks the sorted writes to nonce, code or one storage slot.
// A removed account needs a zero balance at the first later write unless the BAL
// supplies an explicit balance at that same index.
func recordBALRevivals[T interface{ GetIndex() uint32 }](removals []balRemoval, changes []T) {
	for len(removals) > 0 && len(changes) > 0 {
		idx := changes[0].GetIndex()
		changes = changes[1:]
		// Each removal here occurs at a balance write. An earlier removal's next
		// balance write is at or before the following removal's index. Once this
		// sorted list reaches the last removal's next known write, we can stop.
		if limit := removals[len(removals)-1].nextWriteIndex; limit != 0 && idx >= limit {
			return
		}
		after := removals[0].index
		if idx <= after {
			first := sort.Search(len(changes), func(i int) bool { return changes[i].GetIndex() > after })
			changes = changes[first:]
			continue
		}
		nBeforeWrite := sort.Search(len(removals), func(i int) bool { return removals[i].index >= idx })
		removal := &removals[nBeforeWrite-1]
		if removal.nextWriteIndex == 0 || idx < removal.nextWriteIndex {
			removal.nextWriteIndex = idx
			removal.seedZeroBalance = true
		}
		removals = removals[nBeforeWrite:]
	}
}
