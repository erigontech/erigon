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
	"maps"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// addrIO is one address's state in the running tx: its versioned reads, its
// EIP-7928 access mark and the journal's count of its unreverted changes, so one
// lookup finds all of them. Each field keeps its own read header: validation and
// the BAL get the same per-field entries as before.
type addrIO struct {
	addr     accounts.Address
	has      uint16 // 1<<path for each recorded non-storage read
	dirty    int32
	accessed bool
	access   accessOptions

	address        VersionedRead[AccountView]
	balance        VersionedRead[uint256.Int]
	nonce          VersionedRead[uint64]
	incarnation    VersionedRead[uint64]
	selfDestruct   VersionedRead[bool]
	createContract VersionedRead[bool]
	code           VersionedRead[accounts.Code]
	codeHash       VersionedRead[accounts.CodeHash]
	codeSize       VersionedRead[int]
	storage        map[accounts.StorageKey]VersionedRead[uint256.Int]
}

// ioTable holds an IntraBlockState's addrIO records in a slice reused across
// txs. A record pointer is valid only until the next record is added.
type ioTable struct {
	idx  map[accounts.Address]int32
	recs []addrIO
	last int32
}

func pathBit(p AccountPath) uint16 { return 1 << p }

func (t *ioTable) find(addr accounts.Address) *addrIO {
	if int(t.last) < len(t.recs) && t.recs[t.last].addr == addr {
		return &t.recs[t.last]
	}
	i, ok := t.idx[addr]
	if !ok {
		return nil
	}
	t.last = i
	return &t.recs[i]
}

func (t *ioTable) get(addr accounts.Address) *addrIO {
	if r := t.find(addr); r != nil {
		return r
	}
	if t.idx == nil {
		t.idx = make(map[accounts.Address]int32)
	}
	i := len(t.recs)
	if i < cap(t.recs) {
		t.recs = t.recs[:i+1]
	} else {
		t.recs = append(t.recs, addrIO{})
	}
	r := &t.recs[i]
	*r = addrIO{addr: addr, storage: r.storage} // a reused slot keeps its cleared slot map
	t.idx[addr] = int32(i)
	t.last = int32(i)
	return r
}

// maxReusedSlots bounds the slot map a record keeps across txs: a cleared map
// keeps the buckets it grew.
const maxReusedSlots = 256

func (r *addrIO) clearReads() {
	r.has = 0
	r.accessed, r.access = false, accessOptions{}
	// The reads hand out values that must not be pinned by a reused table.
	r.address, r.code, r.codeHash = VersionedRead[AccountView]{}, VersionedRead[accounts.Code]{}, VersionedRead[accounts.CodeHash]{}
	if len(r.storage) > maxReusedSlots {
		r.storage = nil
	} else {
		clear(r.storage)
	}
}

// clearReads drops every read and access mark. Records with unreverted journal
// changes stay, so their dirty counts survive.
func (t *ioTable) clearReads() {
	n := 0
	for i := range t.recs {
		t.recs[i].clearReads()
		if t.recs[i].dirty > 0 {
			t.recs[n], t.recs[i] = t.recs[i], t.recs[n]
			n++
		}
	}
	for i := n; i < len(t.recs); i++ {
		t.recs[i] = addrIO{storage: t.recs[i].storage}
	}
	if cap(t.recs) > maxPooledEntries && n < maxPooledEntries {
		t.recs = append([]addrIO(nil), t.recs[:n]...)
	}
	t.recs = t.recs[:n]
	clear(t.idx)
	for i := range t.recs {
		t.idx[t.recs[i].addr] = int32(i)
	}
	t.last = 0
}

// entries counts records and their recorded slots, for the pool size bound.
func (t *ioTable) entries() int {
	n := len(t.recs)
	for i := range t.recs {
		n += len(t.recs[i].storage)
	}
	return n
}

func (t *ioTable) isDirty(addr accounts.Address) bool {
	r := t.find(addr)
	return r != nil && r.dirty > 0
}

func (t *ioTable) clearDirty() {
	for i := range t.recs {
		t.recs[i].dirty = 0
	}
}

func (t *ioTable) markAccess(addr accounts.Address, revertable bool) {
	r := t.get(addr)
	if !r.accessed {
		r.accessed, r.access = true, accessOptions{revertable: revertable}
	} else if !revertable {
		r.access.revertable = false
	}
}

func (t *ioTable) accessed(addr accounts.Address) bool {
	r := t.find(addr)
	return r != nil && r.accessed
}

func (t *ioTable) clearAccess() {
	for i := range t.recs {
		t.recs[i].accessed, t.recs[i].access = false, accessOptions{}
	}
}

func getRead[T any](t *ioTable, addr accounts.Address, path AccountPath, pick func(*addrIO) *VersionedRead[T]) (VersionedRead[T], bool) {
	if r := t.find(addr); r != nil && r.has&pathBit(path) != 0 {
		return *pick(r), true
	}
	return VersionedRead[T]{}, false
}

func setRead[T any](t *ioTable, addr accounts.Address, path AccountPath, pick func(*addrIO) *VersionedRead[T], tr VersionedRead[T]) {
	r := t.get(addr)
	*pick(r) = tr
	r.has |= pathBit(path)
}

func pickAddress(r *addrIO) *VersionedRead[AccountView]        { return &r.address }
func pickBalance(r *addrIO) *VersionedRead[uint256.Int]        { return &r.balance }
func pickNonce(r *addrIO) *VersionedRead[uint64]               { return &r.nonce }
func pickIncarnation(r *addrIO) *VersionedRead[uint64]         { return &r.incarnation }
func pickSelfDestruct(r *addrIO) *VersionedRead[bool]          { return &r.selfDestruct }
func pickCreateContract(r *addrIO) *VersionedRead[bool]        { return &r.createContract }
func pickCode(r *addrIO) *VersionedRead[accounts.Code]         { return &r.code }
func pickCodeHash(r *addrIO) *VersionedRead[accounts.CodeHash] { return &r.codeHash }
func pickCodeSize(r *addrIO) *VersionedRead[int]               { return &r.codeSize }

func (t *ioTable) GetAddress(addr accounts.Address) (VersionedRead[AccountView], bool) {
	return getRead(t, addr, AddressPath, pickAddress)
}

func (t *ioTable) GetBalance(addr accounts.Address) (VersionedRead[uint256.Int], bool) {
	return getRead(t, addr, BalancePath, pickBalance)
}

func (t *ioTable) GetNonce(addr accounts.Address) (VersionedRead[uint64], bool) {
	return getRead(t, addr, NoncePath, pickNonce)
}

func (t *ioTable) GetIncarnation(addr accounts.Address) (VersionedRead[uint64], bool) {
	return getRead(t, addr, IncarnationPath, pickIncarnation)
}

func (t *ioTable) GetSelfDestruct(addr accounts.Address) (VersionedRead[bool], bool) {
	return getRead(t, addr, SelfDestructPath, pickSelfDestruct)
}

func (t *ioTable) GetCreateContract(addr accounts.Address) (VersionedRead[bool], bool) {
	return getRead(t, addr, CreateContractPath, pickCreateContract)
}

func (t *ioTable) GetCode(addr accounts.Address) (VersionedRead[accounts.Code], bool) {
	return getRead(t, addr, CodePath, pickCode)
}

func (t *ioTable) GetCodeHash(addr accounts.Address) (VersionedRead[accounts.CodeHash], bool) {
	return getRead(t, addr, CodeHashPath, pickCodeHash)
}

func (t *ioTable) GetCodeSize(addr accounts.Address) (VersionedRead[int], bool) {
	return getRead(t, addr, CodeSizePath, pickCodeSize)
}

func (t *ioTable) GetStorage(addr accounts.Address, key accounts.StorageKey) (VersionedRead[uint256.Int], bool) {
	if r := t.find(addr); r != nil {
		tr, ok := r.storage[key]
		return tr, ok
	}
	return VersionedRead[uint256.Int]{}, false
}

func (t *ioTable) SetAddress(addr accounts.Address, tr VersionedRead[AccountView]) {
	setRead(t, addr, AddressPath, pickAddress, tr)
}

func (t *ioTable) SetBalance(addr accounts.Address, tr VersionedRead[uint256.Int]) {
	setRead(t, addr, BalancePath, pickBalance, tr)
}

func (t *ioTable) SetNonce(addr accounts.Address, tr VersionedRead[uint64]) {
	setRead(t, addr, NoncePath, pickNonce, tr)
}

func (t *ioTable) SetIncarnation(addr accounts.Address, tr VersionedRead[uint64]) {
	setRead(t, addr, IncarnationPath, pickIncarnation, tr)
}

func (t *ioTable) SetSelfDestruct(addr accounts.Address, tr VersionedRead[bool]) {
	setRead(t, addr, SelfDestructPath, pickSelfDestruct, tr)
}

func (t *ioTable) SetCreateContract(addr accounts.Address, tr VersionedRead[bool]) {
	setRead(t, addr, CreateContractPath, pickCreateContract, tr)
}

func (t *ioTable) SetCode(addr accounts.Address, tr VersionedRead[accounts.Code]) {
	setRead(t, addr, CodePath, pickCode, tr)
}

func (t *ioTable) SetCodeHash(addr accounts.Address, tr VersionedRead[accounts.CodeHash]) {
	setRead(t, addr, CodeHashPath, pickCodeHash, tr)
}

func (t *ioTable) SetCodeSize(addr accounts.Address, tr VersionedRead[int]) {
	setRead(t, addr, CodeSizePath, pickCodeSize, tr)
}

func (t *ioTable) SetStorage(addr accounts.Address, key accounts.StorageKey, tr VersionedRead[uint256.Int]) {
	r := t.get(addr)
	if r.storage == nil {
		r.storage = make(map[accounts.StorageKey]VersionedRead[uint256.Int])
	}
	r.storage[key] = tr
}

func (t *ioTable) getHeader(addr accounts.Address, path AccountPath, key accounts.StorageKey) (ReadHeader, bool) {
	r := t.find(addr)
	if r == nil {
		return ReadHeader{}, false
	}
	if path == StoragePath {
		tr, ok := r.storage[key]
		return tr.ReadHeader, ok
	}
	if r.has&pathBit(path) == 0 {
		return ReadHeader{}, false
	}
	switch path {
	case AddressPath:
		return r.address.ReadHeader, true
	case BalancePath:
		return r.balance.ReadHeader, true
	case NoncePath:
		return r.nonce.ReadHeader, true
	case IncarnationPath:
		return r.incarnation.ReadHeader, true
	case SelfDestructPath:
		return r.selfDestruct.ReadHeader, true
	case CreateContractPath:
		return r.createContract.ReadHeader, true
	case CodePath:
		return r.code.ReadHeader, true
	case CodeHashPath:
		return r.codeHash.ReadHeader, true
	case CodeSizePath:
		return r.codeSize.ReadHeader, true
	}
	return ReadHeader{}, false
}

// SetHeader records a header-only read (zero value), as ReadSet.SetHeader.
func (t *ioTable) SetHeader(addr accounts.Address, path AccountPath, key accounts.StorageKey, hdr ReadHeader) {
	switch path {
	case AddressPath:
		t.SetAddress(addr, VersionedRead[AccountView]{hdr, nil})
	case BalancePath:
		t.SetBalance(addr, VersionedRead[uint256.Int]{ReadHeader: hdr})
	case NoncePath:
		t.SetNonce(addr, VersionedRead[uint64]{ReadHeader: hdr})
	case IncarnationPath:
		t.SetIncarnation(addr, VersionedRead[uint64]{ReadHeader: hdr})
	case SelfDestructPath:
		t.SetSelfDestruct(addr, VersionedRead[bool]{ReadHeader: hdr})
	case CreateContractPath:
		t.SetCreateContract(addr, VersionedRead[bool]{ReadHeader: hdr})
	case CodePath:
		t.SetCode(addr, VersionedRead[accounts.Code]{ReadHeader: hdr})
	case CodeHashPath:
		t.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{ReadHeader: hdr})
	case CodeSizePath:
		t.SetCodeSize(addr, VersionedRead[int]{ReadHeader: hdr})
	case StoragePath:
		t.SetStorage(addr, key, VersionedRead[uint256.Int]{ReadHeader: hdr})
	}
}

// ScanAddr visits every read of addr with a mutable header, as ReadSet.ScanAddr.
func (t *ioTable) ScanAddr(addr accounts.Address, fn func(path AccountPath, key accounts.StorageKey, hdr *ReadHeader)) int {
	r := t.find(addr)
	if r == nil {
		return 0
	}
	n := 0
	visit := func(path AccountPath, hdr *ReadHeader) {
		if r.has&pathBit(path) != 0 {
			fn(path, accounts.NilKey, hdr)
			n++
		}
	}
	visit(AddressPath, &r.address.ReadHeader)
	visit(BalancePath, &r.balance.ReadHeader)
	visit(NoncePath, &r.nonce.ReadHeader)
	visit(IncarnationPath, &r.incarnation.ReadHeader)
	visit(SelfDestructPath, &r.selfDestruct.ReadHeader)
	visit(CreateContractPath, &r.createContract.ReadHeader)
	visit(CodePath, &r.code.ReadHeader)
	visit(CodeHashPath, &r.codeHash.ReadHeader)
	visit(CodeSizePath, &r.codeSize.ReadHeader)
	for k, tr := range r.storage {
		fn(StoragePath, k, &tr.ReadHeader)
		r.storage[k] = tr
		n++
	}
	return n
}

// toReadSet copies the reads and access marks into a ReadSet the caller owns.
func (t *ioTable) toReadSet() ReadSet {
	var s ReadSet
	for i := range t.recs {
		r := &t.recs[i]
		a := r.addr
		if r.has != 0 {
			if r.has&pathBit(AddressPath) != 0 {
				readSetPut(&s.address, a, r.address)
			}
			if r.has&pathBit(BalancePath) != 0 {
				readSetPut(&s.balance, a, r.balance)
			}
			if r.has&pathBit(NoncePath) != 0 {
				readSetPut(&s.nonce, a, r.nonce)
			}
			if r.has&pathBit(IncarnationPath) != 0 {
				readSetPut(&s.incarnation, a, r.incarnation)
			}
			if r.has&pathBit(SelfDestructPath) != 0 {
				readSetPut(&s.selfDestruct, a, r.selfDestruct)
			}
			if r.has&pathBit(CreateContractPath) != 0 {
				readSetPut(&s.createContract, a, r.createContract)
			}
			if r.has&pathBit(CodePath) != 0 {
				readSetPut(&s.code, a, r.code)
			}
			if r.has&pathBit(CodeHashPath) != 0 {
				readSetPut(&s.codeHash, a, r.codeHash)
			}
			if r.has&pathBit(CodeSizePath) != 0 {
				readSetPut(&s.codeSize, a, r.codeSize)
			}
		}
		if len(r.storage) > 0 {
			if s.storage == nil {
				s.storage = make(map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int])
			}
			s.storage[a] = maps.Clone(r.storage)
		}
		if r.accessed {
			if s.access == nil {
				s.access = make(AccessSet)
			}
			s.access[a] = r.access
		}
	}
	return s
}
