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
type txAccessList struct {
	paths   map[accounts.Address]pathBits
	storage map[accounts.Address]map[accounts.StorageKey]struct{}
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
			for k := range inner {
				out[k] = &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: k}}
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

// StoragesChanged is key-only on the access list — the net-zero filtering happens in
// the versionMapWriteView that wraps it and reads the cells' valStatus from the map.
func (t *txAccessList) StoragesChanged() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return t.storageSeq()
}

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

// blockTxIndex maps a global txNum to the block-relative TxIndex the access list is
// keyed by, using the map's base.
func (vm *VersionMap) blockTxIndex(txNum uint64) int {
	return int(txNum-vm.baseTxNum.Load()) - 1
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
			tal.storage = map[accounts.Address]map[accounts.StorageKey]struct{}{}
		}
		m := tal.storage[addr]
		if m == nil {
			m = make(map[accounts.StorageKey]struct{}, len(inner))
			tal.storage[addr] = m
		}
		for k := range inner {
			if _, ok := m[k]; !ok {
				m[k] = struct{}{}
				tal.count++
			}
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
