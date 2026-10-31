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
	"iter"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// pathBits packs the non-storage AccountPaths a tx wrote into one word per
// address, so a txAccessList holds one map entry per touched account rather than
// one per (account, path).
type pathBits uint16

const (
	balanceBit pathBits = 1 << iota
	nonceBit
	incarnationBit
	codeHashBit
	codeBit
	selfDestructBit
	createContractBit
)

// txAccessList is the frozen set of cells one tx wrote, owned by the version map
// and keyed by TxIndex. It records only which cells were written — the values are
// read back from the version map — so it is the enumerable access list the domain
// apply and commitment range over in place of a retained write-set. Built once when
// the tx publishes and never mutated afterwards, so its enumeration needs no lock.
// storage retains each written slot's valStatus (the one byte decided at write
// time from the tx's own lifecycle-aware prior read): a BAL block never materializes
// a net-zero cell and StorageChanges filters before reading one, so the status
// cannot be recovered from the map. Non-storage paths read their status from the
// cell, which always carries it.
type txAccessList struct {
	paths   map[accounts.Address]pathBits
	storage map[accounts.Address]map[accounts.StorageKey]valueStatus
	count   int
}

func yieldPath[T any](paths map[accounts.Address]pathBits, bit pathBits, path AccountPath) iter.Seq2[accounts.Address, *VersionedWrite[T]] {
	return func(yield func(accounts.Address, *VersionedWrite[T]) bool) {
		for addr, bits := range paths {
			if bits&bit == 0 {
				continue
			}
			if !yield(addr, &VersionedWrite[T]{WriteHeader: WriteHeader{Address: addr, Path: path}}) {
				return
			}
		}
	}
}

func (t *txAccessList) pathMap() map[accounts.Address]pathBits {
	if t == nil {
		return nil
	}
	return t.paths
}

func (t *txAccessList) Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]] {
	return yieldPath[uint256.Int](t.pathMap(), balanceBit, BalancePath)
}

func (t *txAccessList) Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return yieldPath[uint64](t.pathMap(), nonceBit, NoncePath)
}

func (t *txAccessList) Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return yieldPath[uint64](t.pathMap(), incarnationBit, IncarnationPath)
}

func (t *txAccessList) CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]] {
	return yieldPath[accounts.CodeHash](t.pathMap(), codeHashBit, CodeHashPath)
}

func (t *txAccessList) Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]] {
	return yieldPath[accounts.Code](t.pathMap(), codeBit, CodePath)
}

func (t *txAccessList) SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	return yieldPath[bool](t.pathMap(), selfDestructBit, SelfDestructPath)
}

func (t *txAccessList) CreateContracts() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	return yieldPath[bool](t.pathMap(), createContractBit, CreateContractPath)
}

func (t *txAccessList) storageSeq() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return func(yield func(accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]) bool) {
		if t == nil {
			return
		}
		for addr, inner := range t.storage {
			out := make(map[accounts.StorageKey]*VersionedWrite[uint256.Int], len(inner))
			for k, vs := range inner {
				out[k] = &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: k, valStatus: vs}}
			}
			if !yield(addr, out) {
				return
			}
		}
	}
}

func (t *txAccessList) Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return t.storageSeq()
}

// StorageChanges carries each slot's valStatus; the net-zero filtering is applied by
// the versionMapWriteView that wraps the access list before the domain/commitment read.
func (t *txAccessList) StorageChanges() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return t.storageSeq()
}

func (t *txAccessList) AccountChanges() iter.Seq[accounts.Address] { return accountChangeAddrs(t) }

func (t *txAccessList) IsEmpty() bool { return t == nil || t.count == 0 }

func (t *txAccessList) Count() int {
	if t == nil {
		return 0
	}
	return t.count
}

var _ WriteSetView = (*txAccessList)(nil)

// SetBaseTxNum records the block's first global txNum (the block-start system tx at
// TxIndex -1), so a global txNum maps to the block-relative TxIndex the access list
// is keyed by: txNum = baseTxNum + TxIndex + 1.
func (vm *VersionMap) SetBaseTxNum(baseTxNum uint64) { vm.baseTxNum.Store(baseTxNum) }
func (vm *VersionMap) BaseTxNum() uint64             { return vm.baseTxNum.Load() }

// blockTxIndex maps a global txNum to the block-relative TxIndex the access list is
// keyed by, using the map's base.
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
}

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
	acc := NewVersionedAccountView(addr, txIdx, view.vm, view.base).AccountForCommitment()
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
	if val, res, ok := view.vm.ReadStorage(addr, key, view.txIdx()); ok && res.Status() != MVReadResultNone {
		return val, true
	}
	if sr, ok := view.base.(StorageBaseReader); ok && sr != nil {
		if val, ok, err := sr.ReadStorage(addr, key); err == nil && ok {
			return val, true
		}
	}
	return uint256.Int{}, false
}

// SeedEmptyRemovals encodes the EIP-161 empty-account removal as a write-side data
// artifact in the BAL-seeded map, so GetAccount reads the deletion deterministically
// with no read-side rules. The BAL carries no removal field (EIP-7928), so for each
// touched account whose block-end state — its seeded cells composed with the committed
// base for untouched fields — is empty and removable, it stamps a self-destruct removal
// cell at the account's last-changed index. Mirrors exec's encodeExistingEmptyRemovals.
func (vm *VersionMap) SeedEmptyRemovals(bal types.BlockAccessList, eip161Enabled, isAura bool, base AccountBaseReader) {
	ws := &WriteSet{}
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
		acc, ok := NewVersionedAccountView(addr, int(maxIdx), vm, base).GetAccount()
		if !ok || !isEmptyForEIP161(acc.Balance, acc.Nonce, acc.CodeHash) {
			continue
		}
		ws.SetEmptyRemoval(addr, Version{TxIndex: int(maxIdx) - 1})
	}
	if !ws.IsEmpty() {
		vm.FlushVersionedWrites(ws, true)
	}
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

// txAccessKey addresses the per-tx access list. The block-end task's execution
// writes and its engine.Finalize writes share one txNum but are consumed as two
// separate apply units, so finalize distinguishes their slots.
type txAccessKey struct {
	txIdx    int
	finalize bool
}

// RecordWriteKeysAtTxNum freezes the key-set a tx wrote into the version map's per-tx
// access list, keyed by the tx's global txNum (and whether it is the finalize phase).
// The values are not copied — apply and commitment read them back from the map.
func (vm *VersionMap) RecordWriteKeysAtTxNum(txNum uint64, finalize bool, ws WriteSetView) {
	vm.RecordWriteKeysAtTxIndex(vm.blockTxIndex(txNum), finalize, ws)
}

// RecordWriteKeysAtTxIndex is RecordWriteKeysAtTxNum keyed directly by the block-relative
// TxIndex (init phase = -1), for the serial block-assembly paths that think in TxIndex.
func (vm *VersionMap) RecordWriteKeysAtTxIndex(txIdx int, finalize bool, ws WriteSetView) {
	if ws == nil {
		return
	}
	key := txAccessKey{txIdx, finalize}
	tal := &txAccessList{paths: map[accounts.Address]pathBits{}}
	set := func(addr accounts.Address, bit pathBits) {
		if tal.paths[addr]&bit == 0 {
			tal.count++
		}
		tal.paths[addr] |= bit
	}
	for addr := range ws.Balances() {
		set(addr, balanceBit)
	}
	for addr := range ws.Nonces() {
		set(addr, nonceBit)
	}
	for addr := range ws.Incarnations() {
		set(addr, incarnationBit)
	}
	for addr := range ws.CodeHashes() {
		set(addr, codeHashBit)
	}
	for addr := range ws.Codes() {
		set(addr, codeBit)
	}
	for addr := range ws.SelfDestructs() {
		set(addr, selfDestructBit)
	}
	for addr := range ws.CreateContracts() {
		set(addr, createContractBit)
	}
	for addr, inner := range ws.Storages() {
		if len(inner) == 0 {
			continue
		}
		if tal.storage == nil {
			tal.storage = map[accounts.Address]map[accounts.StorageKey]valueStatus{}
		}
		m := tal.storage[addr]
		if m == nil {
			m = make(map[accounts.StorageKey]valueStatus, len(inner))
			tal.storage[addr] = m
		}
		for k, vw := range inner {
			if _, ok := m[k]; !ok {
				tal.count++
			}
			m[k] = vw.valStatus
		}
	}
	vm.writeKeys.Store(key, tal)
}

// WritesAtTxNum returns a lock-free view of the writes a tx made at the given global
// txNum (finalize selects the block-end task's engine.Finalize phase), its key-set
// enumerated from the frozen per-tx access list and each value read back from the
// version map. A tx with no recorded access list yields an empty view.
func (vm *VersionMap) WritesAtTxNum(txNum uint64, finalize bool) WriteSetView {
	return vm.WritesAtTxIndex(vm.blockTxIndex(txNum), finalize)
}

// WritesAtTxIndex is WritesAtTxNum keyed directly by the block-relative TxIndex (init
// phase = -1), for the serial block-assembly paths.
func (vm *VersionMap) WritesAtTxIndex(txIdx int, finalize bool) WriteSetView {
	v, ok := vm.writeKeys.Load(txAccessKey{txIdx, finalize})
	if !ok {
		return NewVersionMapWriteView((*txAccessList)(nil), vm, txIdx)
	}
	return NewVersionMapWriteView(v.(*txAccessList), vm, txIdx)
}
