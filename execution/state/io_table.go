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

// addrIO is one address's state in the running tx: its versioned reads and
// writes, its EIP-7928 access mark and the journal's count of its unreverted
// changes, so one lookup finds all of them. Each field keeps its own header:
// validation and the BAL get the same per-field entries as before.
type addrIO struct {
	addr     accounts.Address
	has      uint16 // 1<<path for each recorded non-storage read
	dirty    int32
	accessed bool
	access   accessOptions

	ri      [CreateContractPath + 1]int32 // per path: index of this record's read in the table's readSlots
	storage map[accounts.StorageKey]VersionedRead[uint256.Int]

	wAddress        *VersionedWrite[*accounts.Account]
	wBalance        *VersionedWrite[uint256.Int]
	wNonce          *VersionedWrite[uint64]
	wIncarnation    *VersionedWrite[uint64]
	wSelfDestruct   *VersionedWrite[bool]
	wCreateContract *VersionedWrite[bool]
	wCode           *VersionedWrite[accounts.Code]
	wCodeHash       *VersionedWrite[accounts.CodeHash]
	wCodeSize       *VersionedWrite[int]
	wStorage        map[accounts.StorageKey]*VersionedWrite[uint256.Int]
}

// ioTable holds an IntraBlockState's addrIO records in a slice reused across
// txs. A record pointer is valid only until the next record is added.
type ioTable struct {
	idx   map[accounts.Address]int32
	recs  []addrIO
	last  int32
	reads readSlots
}

// readSlots holds the non-storage reads of all records, one dense slice per
// path, so a record pays only for the fields its tx read.
type readSlots struct {
	address        []VersionedRead[AccountView]
	balance        []VersionedRead[uint256.Int]
	nonce          []VersionedRead[uint64]
	incarnation    []VersionedRead[uint64]
	selfDestruct   []VersionedRead[bool]
	createContract []VersionedRead[bool]
	code           []VersionedRead[accounts.Code]
	codeHash       []VersionedRead[accounts.CodeHash]
	codeSize       []VersionedRead[int]
}

func resetSlots[T any](s *[]VersionedRead[T]) {
	if cap(*s) > maxPooledEntries {
		*s = nil
		return
	}
	clear(*s) // the values must not be pinned by a reused table
	*s = (*s)[:0]
}

func (rs *readSlots) reset() {
	resetSlots(&rs.address)
	resetSlots(&rs.balance)
	resetSlots(&rs.nonce)
	resetSlots(&rs.incarnation)
	resetSlots(&rs.selfDestruct)
	resetSlots(&rs.createContract)
	resetSlots(&rs.code)
	resetSlots(&rs.codeHash)
	resetSlots(&rs.codeSize)
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
	*r = addrIO{addr: addr, storage: r.storage, wStorage: r.wStorage} // a reused slot keeps its cleared slot maps
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
	if len(r.storage) > maxReusedSlots {
		r.storage = nil
	} else {
		clear(r.storage)
	}
}

func (r *addrIO) clearWrites() {
	r.wAddress, r.wBalance, r.wNonce, r.wIncarnation, r.wSelfDestruct = nil, nil, nil, nil, nil
	r.wCreateContract, r.wCode, r.wCodeHash, r.wCodeSize = nil, nil, nil, nil
	if len(r.wStorage) > maxReusedSlots {
		r.wStorage = nil
	} else {
		clear(r.wStorage)
	}
}

func (r *addrIO) hasWrites() bool {
	return r.wAddress != nil || r.wBalance != nil || r.wNonce != nil || r.wIncarnation != nil ||
		r.wSelfDestruct != nil || r.wCreateContract != nil || r.wCode != nil || r.wCodeHash != nil ||
		r.wCodeSize != nil || len(r.wStorage) > 0
}

// clearReads drops every read and access mark. Records with writes or with
// unreverted journal changes stay, so their dirty counts survive.
func (t *ioTable) clearReads() {
	n := 0
	t.reads.reset()
	for i := range t.recs {
		t.recs[i].clearReads()
		if t.recs[i].dirty > 0 || t.recs[i].hasWrites() {
			t.recs[n], t.recs[i] = t.recs[i], t.recs[n]
			n++
		}
	}
	for i := n; i < len(t.recs); i++ {
		t.recs[i] = addrIO{storage: t.recs[i].storage, wStorage: t.recs[i].wStorage}
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
		n += len(t.recs[i].storage) + len(t.recs[i].wStorage)
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

func getRead[T any](t *ioTable, addr accounts.Address, path AccountPath, pick func(*ioTable) *[]VersionedRead[T]) (VersionedRead[T], bool) {
	if r := t.find(addr); r != nil && r.has&pathBit(path) != 0 {
		return (*pick(t))[r.ri[path]], true
	}
	return VersionedRead[T]{}, false
}

func setRead[T any](t *ioTable, addr accounts.Address, path AccountPath, pick func(*ioTable) *[]VersionedRead[T], tr VersionedRead[T]) {
	r, slots := t.get(addr), pick(t)
	if r.has&pathBit(path) != 0 {
		(*slots)[r.ri[path]] = tr
		return
	}
	r.ri[path] = int32(len(*slots))
	*slots = append(*slots, tr)
	r.has |= pathBit(path)
}

func pickAddress(t *ioTable) *[]VersionedRead[AccountView]        { return &t.reads.address }
func pickBalance(t *ioTable) *[]VersionedRead[uint256.Int]        { return &t.reads.balance }
func pickNonce(t *ioTable) *[]VersionedRead[uint64]               { return &t.reads.nonce }
func pickIncarnation(t *ioTable) *[]VersionedRead[uint64]         { return &t.reads.incarnation }
func pickSelfDestruct(t *ioTable) *[]VersionedRead[bool]          { return &t.reads.selfDestruct }
func pickCreateContract(t *ioTable) *[]VersionedRead[bool]        { return &t.reads.createContract }
func pickCode(t *ioTable) *[]VersionedRead[accounts.Code]         { return &t.reads.code }
func pickCodeHash(t *ioTable) *[]VersionedRead[accounts.CodeHash] { return &t.reads.codeHash }
func pickCodeSize(t *ioTable) *[]VersionedRead[int]               { return &t.reads.codeSize }

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
		return t.reads.address[r.ri[AddressPath]].ReadHeader, true
	case BalancePath:
		return t.reads.balance[r.ri[BalancePath]].ReadHeader, true
	case NoncePath:
		return t.reads.nonce[r.ri[NoncePath]].ReadHeader, true
	case IncarnationPath:
		return t.reads.incarnation[r.ri[IncarnationPath]].ReadHeader, true
	case SelfDestructPath:
		return t.reads.selfDestruct[r.ri[SelfDestructPath]].ReadHeader, true
	case CreateContractPath:
		return t.reads.createContract[r.ri[CreateContractPath]].ReadHeader, true
	case CodePath:
		return t.reads.code[r.ri[CodePath]].ReadHeader, true
	case CodeHashPath:
		return t.reads.codeHash[r.ri[CodeHashPath]].ReadHeader, true
	case CodeSizePath:
		return t.reads.codeSize[r.ri[CodeSizePath]].ReadHeader, true
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
	visit := func(path AccountPath, hdr func(int32) *ReadHeader) {
		if r.has&pathBit(path) != 0 {
			fn(path, accounts.NilKey, hdr(r.ri[path]))
			n++
		}
	}
	visit(AddressPath, func(i int32) *ReadHeader { return &t.reads.address[i].ReadHeader })
	visit(BalancePath, func(i int32) *ReadHeader { return &t.reads.balance[i].ReadHeader })
	visit(NoncePath, func(i int32) *ReadHeader { return &t.reads.nonce[i].ReadHeader })
	visit(IncarnationPath, func(i int32) *ReadHeader { return &t.reads.incarnation[i].ReadHeader })
	visit(SelfDestructPath, func(i int32) *ReadHeader { return &t.reads.selfDestruct[i].ReadHeader })
	visit(CreateContractPath, func(i int32) *ReadHeader { return &t.reads.createContract[i].ReadHeader })
	visit(CodePath, func(i int32) *ReadHeader { return &t.reads.code[i].ReadHeader })
	visit(CodeHashPath, func(i int32) *ReadHeader { return &t.reads.codeHash[i].ReadHeader })
	visit(CodeSizePath, func(i int32) *ReadHeader { return &t.reads.codeSize[i].ReadHeader })
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
				readSetPut(&s.address, a, t.reads.address[r.ri[AddressPath]])
			}
			if r.has&pathBit(BalancePath) != 0 {
				readSetPut(&s.balance, a, t.reads.balance[r.ri[BalancePath]])
			}
			if r.has&pathBit(NoncePath) != 0 {
				readSetPut(&s.nonce, a, t.reads.nonce[r.ri[NoncePath]])
			}
			if r.has&pathBit(IncarnationPath) != 0 {
				readSetPut(&s.incarnation, a, t.reads.incarnation[r.ri[IncarnationPath]])
			}
			if r.has&pathBit(SelfDestructPath) != 0 {
				readSetPut(&s.selfDestruct, a, t.reads.selfDestruct[r.ri[SelfDestructPath]])
			}
			if r.has&pathBit(CreateContractPath) != 0 {
				readSetPut(&s.createContract, a, t.reads.createContract[r.ri[CreateContractPath]])
			}
			if r.has&pathBit(CodePath) != 0 {
				readSetPut(&s.code, a, t.reads.code[r.ri[CodePath]])
			}
			if r.has&pathBit(CodeHashPath) != 0 {
				readSetPut(&s.codeHash, a, t.reads.codeHash[r.ri[CodeHashPath]])
			}
			if r.has&pathBit(CodeSizePath) != 0 {
				readSetPut(&s.codeSize, a, t.reads.codeSize[r.ri[CodeSizePath]])
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

// writeView is the IntraBlockState's write set: the write fields of its txIO
// records. It hands out a WriteSet only through Snapshot, which copies every cell.
type writeView struct {
	t     *ioTable
	cells *vwArenas // nil until recycle
}

func pickWAddress(r *addrIO) **VersionedWrite[*accounts.Account]  { return &r.wAddress }
func pickWBalance(r *addrIO) **VersionedWrite[uint256.Int]        { return &r.wBalance }
func pickWNonce(r *addrIO) **VersionedWrite[uint64]               { return &r.wNonce }
func pickWIncarnation(r *addrIO) **VersionedWrite[uint64]         { return &r.wIncarnation }
func pickWSelfDestruct(r *addrIO) **VersionedWrite[bool]          { return &r.wSelfDestruct }
func pickWCreateContract(r *addrIO) **VersionedWrite[bool]        { return &r.wCreateContract }
func pickWCode(r *addrIO) **VersionedWrite[accounts.Code]         { return &r.wCode }
func pickWCodeHash(r *addrIO) **VersionedWrite[accounts.CodeHash] { return &r.wCodeHash }
func pickWCodeSize(r *addrIO) **VersionedWrite[int]               { return &r.wCodeSize }

func getW[T any](v *writeView, addr accounts.Address, pick func(*addrIO) **VersionedWrite[T]) (*VersionedWrite[T], bool) {
	if r := v.t.find(addr); r != nil {
		if w := *pick(r); w != nil {
			return w, true
		}
	}
	return nil, false
}

func setW[T any](v *writeView, addr accounts.Address, pick func(*addrIO) **VersionedWrite[T], vw *VersionedWrite[T]) {
	*pick(v.t.get(addr)) = vw
}

func delW[T any](v *writeView, addr accounts.Address, pick func(*addrIO) **VersionedWrite[T]) {
	if r := v.t.find(addr); r != nil {
		*pick(r) = nil
	}
}

func updW[T any](v *writeView, addr accounts.Address, pick func(*addrIO) **VersionedWrite[T], val T) {
	if w, ok := getW(v, addr, pick); ok {
		w.Val = val
	}
}

func (v *writeView) GetAddress(addr accounts.Address) (*VersionedWrite[*accounts.Account], bool) {
	return getW(v, addr, pickWAddress)
}

func (v *writeView) GetBalance(addr accounts.Address) (*VersionedWrite[uint256.Int], bool) {
	return getW(v, addr, pickWBalance)
}

func (v *writeView) GetNonce(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	return getW(v, addr, pickWNonce)
}

func (v *writeView) GetIncarnation(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	return getW(v, addr, pickWIncarnation)
}

func (v *writeView) GetSelfDestruct(addr accounts.Address) (*VersionedWrite[bool], bool) {
	return getW(v, addr, pickWSelfDestruct)
}

func (v *writeView) GetCreateContract(addr accounts.Address) (*VersionedWrite[bool], bool) {
	return getW(v, addr, pickWCreateContract)
}

func (v *writeView) GetCode(addr accounts.Address) (*VersionedWrite[accounts.Code], bool) {
	return getW(v, addr, pickWCode)
}

func (v *writeView) GetCodeHash(addr accounts.Address) (*VersionedWrite[accounts.CodeHash], bool) {
	return getW(v, addr, pickWCodeHash)
}

func (v *writeView) GetCodeSize(addr accounts.Address) (*VersionedWrite[int], bool) {
	return getW(v, addr, pickWCodeSize)
}

func (v *writeView) GetStorage(addr accounts.Address, key accounts.StorageKey) (*VersionedWrite[uint256.Int], bool) {
	if r := v.t.find(addr); r != nil {
		vw, ok := r.wStorage[key]
		return vw, ok
	}
	return nil, false
}

func (v *writeView) SetAddress(addr accounts.Address, vw *VersionedWrite[*accounts.Account]) {
	setW(v, addr, pickWAddress, vw)
}

func (v *writeView) SetBalance(addr accounts.Address, vw *VersionedWrite[uint256.Int]) {
	setW(v, addr, pickWBalance, vw)
}

func (v *writeView) SetNonce(addr accounts.Address, vw *VersionedWrite[uint64]) {
	setW(v, addr, pickWNonce, vw)
}

func (v *writeView) SetIncarnation(addr accounts.Address, vw *VersionedWrite[uint64]) {
	setW(v, addr, pickWIncarnation, vw)
}

func (v *writeView) SetSelfDestruct(addr accounts.Address, vw *VersionedWrite[bool]) {
	setW(v, addr, pickWSelfDestruct, vw)
}

func (v *writeView) SetCreateContract(addr accounts.Address, vw *VersionedWrite[bool]) {
	setW(v, addr, pickWCreateContract, vw)
}

func (v *writeView) SetCode(addr accounts.Address, vw *VersionedWrite[accounts.Code]) {
	setW(v, addr, pickWCode, vw)
}

func (v *writeView) SetCodeHash(addr accounts.Address, vw *VersionedWrite[accounts.CodeHash]) {
	setW(v, addr, pickWCodeHash, vw)
}

func (v *writeView) SetCodeSize(addr accounts.Address, vw *VersionedWrite[int]) {
	setW(v, addr, pickWCodeSize, vw)
}

func (v *writeView) SetStorage(addr accounts.Address, key accounts.StorageKey, vw *VersionedWrite[uint256.Int]) {
	r := v.t.get(addr)
	if r.wStorage == nil {
		r.wStorage = make(map[accounts.StorageKey]*VersionedWrite[uint256.Int])
	}
	r.wStorage[key] = vw
}

func (v *writeView) DelBalance(addr accounts.Address)      { delW(v, addr, pickWBalance) }
func (v *writeView) DelNonce(addr accounts.Address)        { delW(v, addr, pickWNonce) }
func (v *writeView) DelIncarnation(addr accounts.Address)  { delW(v, addr, pickWIncarnation) }
func (v *writeView) DelSelfDestruct(addr accounts.Address) { delW(v, addr, pickWSelfDestruct) }
func (v *writeView) DelCode(addr accounts.Address)         { delW(v, addr, pickWCode) }
func (v *writeView) DelCodeHash(addr accounts.Address)     { delW(v, addr, pickWCodeHash) }
func (v *writeView) DelCodeSize(addr accounts.Address)     { delW(v, addr, pickWCodeSize) }

func (v *writeView) DelStorage(addr accounts.Address, key accounts.StorageKey) {
	if r := v.t.find(addr); r != nil {
		delete(r.wStorage, key)
	}
}

func (v *writeView) updateBalance(addr accounts.Address, val uint256.Int, deleted bool) {
	if vw, ok := v.GetBalance(addr); ok {
		vw.Val = val
		vw.valStatus = ValueChanged
		if deleted {
			vw.valStatus = ValueDeleted
		}
	}
}

func (v *writeView) balanceDeleted(addr accounts.Address) bool {
	vw, ok := v.GetBalance(addr)
	return ok && vw.Deletes()
}

func (v *writeView) updateStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	if vw, ok := v.GetStorage(addr, key); ok {
		vw.Val = val
		vw.valStatus = storageValueStatus(vw.orig, val)
	}
}

func (v *writeView) updateNonce(addr accounts.Address, val uint64) { updW(v, addr, pickWNonce, val) }
func (v *writeView) updateIncarnation(addr accounts.Address, val uint64) {
	updW(v, addr, pickWIncarnation, val)
}
func (v *writeView) updateSelfDestruct(addr accounts.Address, val bool) {
	updW(v, addr, pickWSelfDestruct, val)
}
func (v *writeView) updateCode(addr accounts.Address, val accounts.Code) {
	updW(v, addr, pickWCode, val)
}
func (v *writeView) updateCodeHash(addr accounts.Address, val accounts.CodeHash) {
	updW(v, addr, pickWCodeHash, val)
}
func (v *writeView) updateCodeSize(addr accounts.Address, val int) { updW(v, addr, pickWCodeSize, val) }

func (v *writeView) Has(h WriteHeader) bool {
	r := v.t.find(h.Address)
	if r == nil {
		return false
	}
	switch h.Path {
	case AddressPath:
		return r.wAddress != nil
	case BalancePath:
		return r.wBalance != nil
	case NoncePath:
		return r.wNonce != nil
	case IncarnationPath:
		return r.wIncarnation != nil
	case SelfDestructPath:
		return r.wSelfDestruct != nil
	case CreateContractPath:
		return r.wCreateContract != nil
	case CodePath:
		return r.wCode != nil
	case CodeHashPath:
		return r.wCodeHash != nil
	case CodeSizePath:
		return r.wCodeSize != nil
	case StoragePath:
		_, ok := r.wStorage[h.Key]
		return ok
	}
	return false
}

func (v *writeView) deleteAddr(addr accounts.Address) {
	if r := v.t.find(addr); r != nil {
		r.clearWrites()
	}
}

func (v *writeView) copyAccount(a *accounts.Account) *accounts.Account {
	if v.cells != nil {
		return v.cells.accounts.copyOf(a)
	}
	return a.SelfCopy()
}

// newVW takes a cell from the view's arena, or from the shared pool while the
// state has not outlived a tx yet.
func newVW[T any](cells *vwArenas, arena func(*vwArenas) *vwArena[T], get func() *VersionedWrite[T]) *VersionedWrite[T] {
	if cells == nil {
		return get()
	}
	return arena(cells).alloc(get)
}

func (v *writeView) newVWAddress() *VersionedWrite[*accounts.Account] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[*accounts.Account] { return &a.address }, getVWAddress)
}
func (v *writeView) newVWBalance() *VersionedWrite[uint256.Int] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[uint256.Int] { return &a.balance }, getVWBalance)
}
func (v *writeView) newVWNonce() *VersionedWrite[uint64] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[uint64] { return &a.nonce }, getVWNonce)
}
func (v *writeView) newVWIncarnation() *VersionedWrite[uint64] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[uint64] { return &a.incarnation }, getVWIncarnation)
}
func (v *writeView) newVWSelfDestruct() *VersionedWrite[bool] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[bool] { return &a.selfDestruct }, getVWSelfDestruct)
}
func (v *writeView) newVWCreateContract() *VersionedWrite[bool] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[bool] { return &a.createContract }, getVWCreateContract)
}
func (v *writeView) newVWCode() *VersionedWrite[accounts.Code] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[accounts.Code] { return &a.code }, getVWCode)
}
func (v *writeView) newVWCodeHash() *VersionedWrite[accounts.CodeHash] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[accounts.CodeHash] { return &a.codeHash }, getVWCodeHash)
}
func (v *writeView) newVWCodeSize() *VersionedWrite[int] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[int] { return &a.codeSize }, getVWCodeSize)
}
func (v *writeView) newVWStorage() *VersionedWrite[uint256.Int] {
	return newVW(v.cells, func(a *vwArenas) *vwArena[uint256.Int] { return &a.storage }, getVWStorage)
}

func (v *writeView) ArenaBacked() bool { return v.cells != nil }

// recycle drops every write and takes cells from the view's own arenas from
// here on: slabs pay off only once the state outlives a tx.
func (v *writeView) recycle() {
	v.release()
	if v.cells == nil {
		v.cells = &vwArenas{}
	}
}

// release drops every write and rewinds the arenas.
func (v *writeView) release() {
	for i := range v.t.recs {
		v.t.recs[i].clearWrites()
	}
	if v.cells != nil {
		v.cells.reset()
	}
}

func (v *writeView) snapshotCreateFields(addr accounts.Address) *createWriteSnapshot {
	snap := &createWriteSnapshot{}
	r := v.t.find(addr)
	if r == nil {
		return snap
	}
	if r.wAddress != nil {
		snap.address = cloneVW(r.wAddress)
	}
	if r.wBalance != nil {
		snap.balance = cloneVW(r.wBalance)
	}
	if r.wNonce != nil {
		snap.nonce = cloneVW(r.wNonce)
	}
	if r.wIncarnation != nil {
		snap.incarnation = cloneVW(r.wIncarnation)
	}
	if r.wSelfDestruct != nil {
		snap.selfDestruct = cloneVW(r.wSelfDestruct)
	}
	if r.wCreateContract != nil {
		snap.createContract = cloneVW(r.wCreateContract)
	}
	if r.wCodeHash != nil {
		snap.codeHash = cloneVW(r.wCodeHash)
	}
	return snap
}

// restoreCreateFields sets the account-record writes back to snap, as WriteSet.restoreCreateFields.
func (v *writeView) restoreCreateFields(addr accounts.Address, snap *createWriteSnapshot) {
	r := v.t.get(addr)
	r.wAddress, r.wBalance, r.wNonce, r.wIncarnation, r.wSelfDestruct, r.wCreateContract, r.wCodeHash = nil, nil, nil, nil, nil, nil, nil
	if snap != nil {
		r.wAddress, r.wBalance, r.wNonce, r.wIncarnation = snap.address, snap.balance, snap.nonce, snap.incarnation
		r.wSelfDestruct, r.wCreateContract, r.wCodeHash = snap.selfDestruct, snap.createContract, snap.codeHash
	}
}

// Finalize applies the EIP-6780 net-zero storage wipe and returns the Snapshot, as WriteSet.Finalize.
func (v *writeView) Finalize() *WriteSet {
	for i := range v.t.recs {
		r := &v.t.recs[i]
		if r.wCreateContract == nil || !r.wCreateContract.Val || r.wSelfDestruct == nil || !r.wSelfDestruct.Val {
			continue
		}
		for _, vw := range r.wStorage {
			vw.Val = uint256.Int{}
		}
	}
	return v.Snapshot()
}

// Snapshot copies the writes into a WriteSet the caller owns, with the
// self-destruct filter of WriteSet.Snapshot.
func (v *writeView) Snapshot() *WriteSet {
	out := &WriteSet{}
	for i := range v.t.recs {
		r := &v.t.recs[i]
		addr := r.addr
		sd := false
		if r.wSelfDestruct != nil {
			out.SetSelfDestruct(addr, cloneVW(r.wSelfDestruct))
			sd = r.wSelfDestruct.Val
		}
		if r.wBalance != nil {
			out.SetBalance(addr, cloneVW(r.wBalance))
		}
		if r.wIncarnation != nil {
			out.SetIncarnation(addr, cloneVW(r.wIncarnation))
		}
		for k, vw := range r.wStorage {
			out.SetStorage(addr, k, cloneVW(vw))
		}
		if r.wCreateContract != nil {
			out.SetCreateContract(addr, cloneVW(r.wCreateContract))
		}
		if sd {
			continue
		}
		if r.wAddress != nil {
			c := cloneVW(r.wAddress)
			if v.cells != nil && c.Val != nil {
				c.Val = c.Val.SelfCopy()
			}
			out.SetAddress(addr, c)
		}
		if r.wNonce != nil {
			out.SetNonce(addr, cloneVW(r.wNonce))
		}
		if r.wCode != nil {
			out.SetCode(addr, cloneVW(r.wCode))
		}
		if r.wCodeHash != nil {
			out.SetCodeHash(addr, cloneVW(r.wCodeHash))
		}
		if r.wCodeSize != nil {
			out.SetCodeSize(addr, cloneVW(r.wCodeSize))
		}
	}
	return out
}
