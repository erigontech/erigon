package state

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"sync"

	"github.com/heimdalr/dag"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type ReadSource int

func (s ReadSource) String() string {
	switch s {
	case MapRead:
		return "version-map"
	case StorageRead:
		return "storage"
	case WriteSetRead:
		return "tx-writes"
	case ReadSetRead:
		return "tx-reads"
	case ProvisionalRead:
		return "provisional"
	default:
		return "unknown"
	}
}

func (s ReadSource) VersionedString(version Version) string {
	switch s {
	case MapRead:
		return fmt.Sprintf("version-map:%d.%d", version.TxIndex, version.Incarnation)
	case StorageRead:
		return "storage"
	case WriteSetRead:
		return "tx-writes"
	case ReadSetRead:
		return "tx-reads"
	case ProvisionalRead:
		return "provisional"
	default:
		return "unknown"
	}
}

const (
	UnknownSource ReadSource = iota
	MapRead
	StorageRead
	WriteSetRead
	ReadSetRead
	// ProvisionalRead: a nil probe mid-account-load that a later re-probe may adopt instead of aborting.
	ProvisionalRead
)

type ReadHeader struct {
	Source   ReadSource
	Version  Version
	internal bool // conflict-detection only; excluded from the block access list
}

type VersionedRead[T any] struct {
	ReadHeader
	Val T
}

type AccountView interface {
	Account() *accounts.Account
	IsNil() bool
}

type concreteAccountView struct{ acc *accounts.Account }

func (c concreteAccountView) Account() *accounts.Account { return c.acc }
func (c concreteAccountView) IsNil() bool                { return c.acc == nil }

func NewAccountView(acc *accounts.Account) AccountView { return concreteAccountView{acc} }

type ReadSet struct {
	address      map[accounts.Address]VersionedRead[AccountView]
	balance      map[accounts.Address]VersionedRead[uint256.Int]
	nonce        map[accounts.Address]VersionedRead[uint64]
	incarnation  map[accounts.Address]VersionedRead[uint64]
	selfDestruct map[accounts.Address]VersionedRead[bool]
	// selfDestructWitnesses holds other destruct versions a tx also depended on;
	// validation must re-check every one, not just the last-recorded destruct.
	selfDestructWitnesses map[accounts.Address][]VersionedRead[bool]
	createContract        map[accounts.Address]VersionedRead[bool]
	code                  map[accounts.Address]VersionedRead[[]byte]
	codeHash              map[accounts.Address]VersionedRead[accounts.CodeHash]
	codeSize              map[accounts.Address]VersionedRead[int]
	storage               map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int]

	// access carries EIP-7928 access marks, so the access set travels with the read-set.
	access AccessSet
}

func readSetPut[T any](m *map[accounts.Address]VersionedRead[T], addr accounts.Address, tr VersionedRead[T]) {
	if *m == nil {
		*m = make(map[accounts.Address]VersionedRead[T])
	}
	(*m)[addr] = tr
}

func (s *ReadSet) SetAddress(addr accounts.Address, tr VersionedRead[AccountView]) {
	readSetPut(&s.address, addr, tr)
}
func (s *ReadSet) SetBalance(addr accounts.Address, tr VersionedRead[uint256.Int]) {
	readSetPut(&s.balance, addr, tr)
}
func (s *ReadSet) SetNonce(addr accounts.Address, tr VersionedRead[uint64]) {
	readSetPut(&s.nonce, addr, tr)
}
func (s *ReadSet) SetIncarnation(addr accounts.Address, tr VersionedRead[uint64]) {
	readSetPut(&s.incarnation, addr, tr)
}
func (s *ReadSet) SetSelfDestruct(addr accounts.Address, tr VersionedRead[bool]) {
	if prev, ok := s.selfDestruct[addr]; ok && prev.Version != tr.Version {
		for _, w := range s.selfDestructWitnesses[addr] {
			if w.Version == prev.Version {
				readSetPut(&s.selfDestruct, addr, tr)
				return
			}
		}
		if s.selfDestructWitnesses == nil {
			s.selfDestructWitnesses = map[accounts.Address][]VersionedRead[bool]{}
		}
		s.selfDestructWitnesses[addr] = append(s.selfDestructWitnesses[addr], prev)
	}
	readSetPut(&s.selfDestruct, addr, tr)
}
func (s *ReadSet) SetCreateContract(addr accounts.Address, tr VersionedRead[bool]) {
	readSetPut(&s.createContract, addr, tr)
}
func (s *ReadSet) SetCode(addr accounts.Address, tr VersionedRead[[]byte]) {
	readSetPut(&s.code, addr, tr)
}
func (s *ReadSet) SetCodeHash(addr accounts.Address, tr VersionedRead[accounts.CodeHash]) {
	readSetPut(&s.codeHash, addr, tr)
}
func (s *ReadSet) SetCodeSize(addr accounts.Address, tr VersionedRead[int]) {
	readSetPut(&s.codeSize, addr, tr)
}
func (s *ReadSet) SetStorage(addr accounts.Address, key accounts.StorageKey, tr VersionedRead[uint256.Int]) {
	if s.storage == nil {
		s.storage = make(map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int])
	}
	inner := s.storage[addr]
	if inner == nil {
		inner = make(map[accounts.StorageKey]VersionedRead[uint256.Int])
		s.storage[addr] = inner
	}
	inner[key] = tr
}

func (s *ReadSet) GetAddress(addr accounts.Address) (VersionedRead[AccountView], bool) {
	tr, ok := s.address[addr]
	return tr, ok
}
func (s *ReadSet) GetBalance(addr accounts.Address) (VersionedRead[uint256.Int], bool) {
	tr, ok := s.balance[addr]
	return tr, ok
}
func (s *ReadSet) GetNonce(addr accounts.Address) (VersionedRead[uint64], bool) {
	tr, ok := s.nonce[addr]
	return tr, ok
}
func (s *ReadSet) GetIncarnation(addr accounts.Address) (VersionedRead[uint64], bool) {
	tr, ok := s.incarnation[addr]
	return tr, ok
}
func (s *ReadSet) GetSelfDestruct(addr accounts.Address) (VersionedRead[bool], bool) {
	tr, ok := s.selfDestruct[addr]
	return tr, ok
}
func (s *ReadSet) GetCreateContract(addr accounts.Address) (VersionedRead[bool], bool) {
	tr, ok := s.createContract[addr]
	return tr, ok
}
func (s *ReadSet) GetCode(addr accounts.Address) (VersionedRead[[]byte], bool) {
	tr, ok := s.code[addr]
	return tr, ok
}
func (s *ReadSet) GetCodeHash(addr accounts.Address) (VersionedRead[accounts.CodeHash], bool) {
	tr, ok := s.codeHash[addr]
	return tr, ok
}
func (s *ReadSet) GetCodeSize(addr accounts.Address) (VersionedRead[int], bool) {
	tr, ok := s.codeSize[addr]
	return tr, ok
}
func (s *ReadSet) GetStorage(addr accounts.Address, key accounts.StorageKey) (VersionedRead[uint256.Int], bool) {
	inner := s.storage[addr]
	if inner == nil {
		return VersionedRead[uint256.Int]{}, false
	}
	tr, ok := inner[key]
	return tr, ok
}

func (s *ReadSet) getHeader(addr accounts.Address, path AccountPath, key accounts.StorageKey) (ReadHeader, bool) {
	switch path {
	case AddressPath:
		tr, ok := s.address[addr]
		return tr.ReadHeader, ok
	case BalancePath:
		tr, ok := s.balance[addr]
		return tr.ReadHeader, ok
	case NoncePath:
		tr, ok := s.nonce[addr]
		return tr.ReadHeader, ok
	case IncarnationPath:
		tr, ok := s.incarnation[addr]
		return tr.ReadHeader, ok
	case SelfDestructPath:
		tr, ok := s.selfDestruct[addr]
		return tr.ReadHeader, ok
	case CreateContractPath:
		tr, ok := s.createContract[addr]
		return tr.ReadHeader, ok
	case CodePath:
		tr, ok := s.code[addr]
		return tr.ReadHeader, ok
	case CodeHashPath:
		tr, ok := s.codeHash[addr]
		return tr.ReadHeader, ok
	case CodeSizePath:
		tr, ok := s.codeSize[addr]
		return tr.ReadHeader, ok
	case StoragePath:
		inner := s.storage[addr]
		if inner == nil {
			return ReadHeader{}, false
		}
		tr, ok := inner[key]
		return tr.ReadHeader, ok
	}
	return ReadHeader{}, false
}

func (s *ReadSet) SetHeader(addr accounts.Address, path AccountPath, key accounts.StorageKey, hdr ReadHeader) {
	switch path {
	case AddressPath:
		s.SetAddress(addr, VersionedRead[AccountView]{hdr, nil})
	case BalancePath:
		s.SetBalance(addr, VersionedRead[uint256.Int]{ReadHeader: hdr})
	case NoncePath:
		s.SetNonce(addr, VersionedRead[uint64]{ReadHeader: hdr})
	case IncarnationPath:
		s.SetIncarnation(addr, VersionedRead[uint64]{ReadHeader: hdr})
	case SelfDestructPath:
		s.SetSelfDestruct(addr, VersionedRead[bool]{ReadHeader: hdr})
	case CreateContractPath:
		s.SetCreateContract(addr, VersionedRead[bool]{ReadHeader: hdr})
	case CodePath:
		s.SetCode(addr, VersionedRead[[]byte]{ReadHeader: hdr})
	case CodeHashPath:
		s.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{ReadHeader: hdr})
	case CodeSizePath:
		s.SetCodeSize(addr, VersionedRead[int]{ReadHeader: hdr})
	case StoragePath:
		s.SetStorage(addr, key, VersionedRead[uint256.Int]{ReadHeader: hdr})
	}
}

func scanAddrPath[T any](m map[accounts.Address]VersionedRead[T], addr accounts.Address, path AccountPath, fn func(AccountPath, accounts.StorageKey, *ReadHeader)) int {
	if tr, ok := m[addr]; ok {
		fn(path, accounts.NilKey, &tr.ReadHeader)
		m[addr] = tr
		return 1
	}
	return 0
}

func (s *ReadSet) ScanAddr(addr accounts.Address, fn func(path AccountPath, key accounts.StorageKey, hdr *ReadHeader)) int {
	n := scanAddrPath(s.address, addr, AddressPath, fn) +
		scanAddrPath(s.balance, addr, BalancePath, fn) +
		scanAddrPath(s.nonce, addr, NoncePath, fn) +
		scanAddrPath(s.incarnation, addr, IncarnationPath, fn) +
		scanAddrPath(s.selfDestruct, addr, SelfDestructPath, fn) +
		scanAddrPath(s.createContract, addr, CreateContractPath, fn) +
		scanAddrPath(s.code, addr, CodePath, fn) +
		scanAddrPath(s.codeHash, addr, CodeHashPath, fn) +
		scanAddrPath(s.codeSize, addr, CodeSizePath, fn)
	if inner, ok := s.storage[addr]; ok {
		for k, tr := range inner {
			fn(StoragePath, k, &tr.ReadHeader)
			inner[k] = tr
			n++
		}
	}
	return n
}

func (s *ReadSet) hasAddr(addr accounts.Address) bool {
	if _, ok := s.address[addr]; ok {
		return true
	}
	if _, ok := s.balance[addr]; ok {
		return true
	}
	if _, ok := s.nonce[addr]; ok {
		return true
	}
	if _, ok := s.incarnation[addr]; ok {
		return true
	}
	if _, ok := s.selfDestruct[addr]; ok {
		return true
	}
	if _, ok := s.createContract[addr]; ok {
		return true
	}
	if _, ok := s.code[addr]; ok {
		return true
	}
	if _, ok := s.codeHash[addr]; ok {
		return true
	}
	if _, ok := s.codeSize[addr]; ok {
		return true
	}
	_, ok := s.storage[addr]
	return ok
}

func (s *ReadSet) Delete(addr accounts.Address) {
	delete(s.address, addr)
	delete(s.balance, addr)
	delete(s.nonce, addr)
	delete(s.incarnation, addr)
	delete(s.selfDestruct, addr)
	delete(s.createContract, addr)
	delete(s.code, addr)
	delete(s.codeHash, addr)
	delete(s.codeSize, addr)
	delete(s.storage, addr)
}

func (s ReadSet) Len() int {
	n := len(s.address) + len(s.balance) + len(s.nonce) + len(s.incarnation) +
		len(s.selfDestruct) + len(s.createContract) +
		len(s.code) + len(s.codeHash) + len(s.codeSize)
	for _, inner := range s.storage {
		n += len(inner)
	}
	return n
}

func (s *ReadSet) mergeFrom(src ReadSet) {
	for a, tr := range src.address {
		readSetPut(&s.address, a, tr)
	}
	for a, tr := range src.balance {
		readSetPut(&s.balance, a, tr)
	}
	for a, tr := range src.nonce {
		readSetPut(&s.nonce, a, tr)
	}
	for a, tr := range src.incarnation {
		readSetPut(&s.incarnation, a, tr)
	}
	for a, tr := range src.selfDestruct {
		s.SetSelfDestruct(a, tr)
	}
	for a, trs := range src.selfDestructWitnesses {
		for _, tr := range trs {
			s.SetSelfDestruct(a, tr)
		}
	}
	for a, tr := range src.createContract {
		readSetPut(&s.createContract, a, tr)
	}
	for a, tr := range src.code {
		readSetPut(&s.code, a, tr)
	}
	for a, tr := range src.codeHash {
		readSetPut(&s.codeHash, a, tr)
	}
	for a, tr := range src.codeSize {
		readSetPut(&s.codeSize, a, tr)
	}
	for a, inner := range src.storage {
		for k, tr := range inner {
			s.SetStorage(a, k, tr)
		}
	}
	if len(src.access) > 0 {
		if s.access == nil {
			s.access = make(AccessSet, len(src.access))
		}
		maps.Copy(s.access, src.access)
	}
}

func (s ReadSet) Merge(o ReadSet) ReadSet {
	var out ReadSet
	out.mergeFrom(s)
	out.mergeFrom(o)
	return out
}

func (s *ReadSet) MergeFrom(o ReadSet) {
	s.mergeFrom(o)
}

func (s ReadSet) TraceReads(prefix string) {
	for addr, tr := range s.address {
		fmt.Println(prefix, "RD", traceReadStr(addr, AddressPath, accounts.NilKey, tr.ReadHeader, accountViewString(tr.Val)))
	}
	for addr, tr := range s.balance {
		fmt.Println(prefix, "RD", traceReadStr(addr, BalancePath, accounts.NilKey, tr.ReadHeader, valueString(BalancePath, tr.Val)))
	}
	for addr, tr := range s.nonce {
		fmt.Println(prefix, "RD", traceReadStr(addr, NoncePath, accounts.NilKey, tr.ReadHeader, valueString(NoncePath, tr.Val)))
	}
	for addr, tr := range s.incarnation {
		fmt.Println(prefix, "RD", traceReadStr(addr, IncarnationPath, accounts.NilKey, tr.ReadHeader, valueString(IncarnationPath, tr.Val)))
	}
	for addr, tr := range s.selfDestruct {
		fmt.Println(prefix, "RD", traceReadStr(addr, SelfDestructPath, accounts.NilKey, tr.ReadHeader, valueString(SelfDestructPath, tr.Val)))
	}
	for addr, tr := range s.createContract {
		fmt.Println(prefix, "RD", traceReadStr(addr, CreateContractPath, accounts.NilKey, tr.ReadHeader, valueString(CreateContractPath, tr.Val)))
	}
	for addr, tr := range s.code {
		fmt.Println(prefix, "RD", traceReadStr(addr, CodePath, accounts.NilKey, tr.ReadHeader, valueString(CodePath, tr.Val)))
	}
	for addr, tr := range s.codeHash {
		fmt.Println(prefix, "RD", traceReadStr(addr, CodeHashPath, accounts.NilKey, tr.ReadHeader, valueString(CodeHashPath, tr.Val)))
	}
	for addr, tr := range s.codeSize {
		fmt.Println(prefix, "RD", traceReadStr(addr, CodeSizePath, accounts.NilKey, tr.ReadHeader, valueString(CodeSizePath, tr.Val)))
	}
	for addr, inner := range s.storage {
		for key, tr := range inner {
			fmt.Println(prefix, "RD", traceReadStr(addr, StoragePath, key, tr.ReadHeader, valueString(StoragePath, tr.Val)))
		}
	}
}

func traceReadStr(addr accounts.Address, path AccountPath, key accounts.StorageKey, hdr ReadHeader, valStr string) string {
	return fmt.Sprintf("(%s) %x %s: %s", hdr.Source.VersionedString(hdr.Version), addr, AccountKey{Path: path, Key: key}, valStr)
}

func accountViewString(v AccountView) string {
	if v == nil || v.IsNil() {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", v.Account())
}

func eachHeaderOf[T any](m map[accounts.Address]VersionedRead[T], yield func(ReadHeader) bool) bool {
	for _, tr := range m {
		if !yield(tr.ReadHeader) {
			return false
		}
	}
	return true
}

func (s ReadSet) eachHeader(yield func(ReadHeader) bool) {
	if !eachHeaderOf(s.address, yield) ||
		!eachHeaderOf(s.balance, yield) ||
		!eachHeaderOf(s.nonce, yield) ||
		!eachHeaderOf(s.incarnation, yield) ||
		!eachHeaderOf(s.selfDestruct, yield) ||
		!eachHeaderOf(s.createContract, yield) ||
		!eachHeaderOf(s.code, yield) ||
		!eachHeaderOf(s.codeHash, yield) ||
		!eachHeaderOf(s.codeSize, yield) {
		return
	}
	for _, inner := range s.storage {
		for _, tr := range inner {
			if !yield(tr.ReadHeader) {
				return
			}
		}
	}
}

type WriteHeader struct {
	Address     accounts.Address
	Key         accounts.StorageKey
	Version     Version
	Path        AccountPath
	Reason      tracing.BalanceChangeReason
	NonceReason tracing.NonceChangeReason
}

func (h WriteHeader) String() string {
	return fmt.Sprintf("%x %s (%d.%d)", h.Address, AccountKey{Path: h.Path, Key: h.Key}, h.Version.TxIndex, h.Version.Incarnation)
}

type VersionedWrite[T any] struct {
	WriteHeader
	Val T
}

func cloneVW[T any](w *VersionedWrite[T]) *VersionedWrite[T] {
	c := *w
	return &c
}

func (w *VersionedWrite[T]) String() string {
	return fmt.Sprintf("%s: %v", w.WriteHeader, w.Val)
}

var (
	vwPoolAddress        = sync.Pool{New: func() any { return &VersionedWrite[*accounts.Account]{} }}
	vwPoolBalance        = sync.Pool{New: func() any { return &VersionedWrite[uint256.Int]{} }}
	vwPoolNonce          = sync.Pool{New: func() any { return &VersionedWrite[uint64]{} }}
	vwPoolIncarnation    = sync.Pool{New: func() any { return &VersionedWrite[uint64]{} }}
	vwPoolSelfDestruct   = sync.Pool{New: func() any { return &VersionedWrite[bool]{} }}
	vwPoolCreateContract = sync.Pool{New: func() any { return &VersionedWrite[bool]{} }}
	vwPoolCode           = sync.Pool{New: func() any { return &VersionedWrite[accounts.Code]{} }}
	vwPoolCodeHash       = sync.Pool{New: func() any { return &VersionedWrite[accounts.CodeHash]{} }}
	vwPoolCodeSize       = sync.Pool{New: func() any { return &VersionedWrite[int]{} }}
	vwPoolStorage        = sync.Pool{New: func() any { return &VersionedWrite[uint256.Int]{} }}
)

func getVWAddress() *VersionedWrite[*accounts.Account] {
	return vwPoolAddress.Get().(*VersionedWrite[*accounts.Account])
}
func getVWBalance() *VersionedWrite[uint256.Int] {
	return vwPoolBalance.Get().(*VersionedWrite[uint256.Int])
}
func getVWNonce() *VersionedWrite[uint64] { return vwPoolNonce.Get().(*VersionedWrite[uint64]) }
func getVWIncarnation() *VersionedWrite[uint64] {
	return vwPoolIncarnation.Get().(*VersionedWrite[uint64])
}
func getVWSelfDestruct() *VersionedWrite[bool] {
	return vwPoolSelfDestruct.Get().(*VersionedWrite[bool])
}
func getVWCreateContract() *VersionedWrite[bool] {
	return vwPoolCreateContract.Get().(*VersionedWrite[bool])
}
func getVWCode() *VersionedWrite[accounts.Code] {
	return vwPoolCode.Get().(*VersionedWrite[accounts.Code])
}
func getVWCodeHash() *VersionedWrite[accounts.CodeHash] {
	return vwPoolCodeHash.Get().(*VersionedWrite[accounts.CodeHash])
}
func getVWCodeSize() *VersionedWrite[int] { return vwPoolCodeSize.Get().(*VersionedWrite[int]) }
func getVWStorage() *VersionedWrite[uint256.Int] {
	return vwPoolStorage.Get().(*VersionedWrite[uint256.Int])
}

func releaseVWAddress(vw *VersionedWrite[*accounts.Account]) {
	vw.Val = nil // unpin
	vwPoolAddress.Put(vw)
}
func releaseVWBalance(vw *VersionedWrite[uint256.Int]) { vwPoolBalance.Put(vw) }
func releaseVWNonce(vw *VersionedWrite[uint64])        { vwPoolNonce.Put(vw) }
func releaseVWIncarnation(vw *VersionedWrite[uint64])  { vwPoolIncarnation.Put(vw) }
func releaseVWSelfDestruct(vw *VersionedWrite[bool])   { vwPoolSelfDestruct.Put(vw) }
func releaseVWCreateContract(vw *VersionedWrite[bool]) { vwPoolCreateContract.Put(vw) }
func releaseVWCode(vw *VersionedWrite[accounts.Code]) {
	vw.Val = accounts.Code{} // unpin bytecode
	vwPoolCode.Put(vw)
}
func releaseVWCodeHash(vw *VersionedWrite[accounts.CodeHash]) { vwPoolCodeHash.Put(vw) }
func releaseVWCodeSize(vw *VersionedWrite[int])               { vwPoolCodeSize.Put(vw) }
func releaseVWStorage(vw *VersionedWrite[uint256.Int])        { vwPoolStorage.Put(vw) }

type WriteSet struct {
	address        map[accounts.Address]*VersionedWrite[*accounts.Account]
	balance        map[accounts.Address]*VersionedWrite[uint256.Int]
	nonce          map[accounts.Address]*VersionedWrite[uint64]
	incarnation    map[accounts.Address]*VersionedWrite[uint64]
	selfDestruct   map[accounts.Address]*VersionedWrite[bool]
	createContract map[accounts.Address]*VersionedWrite[bool]
	code           map[accounts.Address]*VersionedWrite[accounts.Code]
	codeHash       map[accounts.Address]*VersionedWrite[accounts.CodeHash]
	codeSize       map[accounts.Address]*VersionedWrite[int]
	storage        map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int]
}

// vwMapPool pools the map container; put() assumes every VW was already released to its own pool.
type vwMapPool[T any] struct{ p sync.Pool }

func newVWMapPool[T any]() *vwMapPool[T] {
	return &vwMapPool[T]{p: sync.Pool{New: func() any {
		return make(map[accounts.Address]*VersionedWrite[T])
	}}}
}

func (mp *vwMapPool[T]) get() map[accounts.Address]*VersionedWrite[T] {
	return mp.p.Get().(map[accounts.Address]*VersionedWrite[T])
}

func (mp *vwMapPool[T]) put(m map[accounts.Address]*VersionedWrite[T]) {
	if m == nil {
		return
	}
	clear(m)
	mp.p.Put(m)
}

var (
	wsMapPoolAddress        = newVWMapPool[*accounts.Account]()
	wsMapPoolBalance        = newVWMapPool[uint256.Int]()
	wsMapPoolNonce          = newVWMapPool[uint64]()
	wsMapPoolIncarnation    = newVWMapPool[uint64]()
	wsMapPoolSelfDestruct   = newVWMapPool[bool]()
	wsMapPoolCreateContract = newVWMapPool[bool]()
	wsMapPoolCode           = newVWMapPool[accounts.Code]()
	wsMapPoolCodeHash       = newVWMapPool[accounts.CodeHash]()
	wsMapPoolCodeSize       = newVWMapPool[int]()
	wsMapPoolStorageInner   = sync.Pool{New: func() any {
		return make(map[accounts.StorageKey]*VersionedWrite[uint256.Int])
	}}
	wsMapPoolStorageOuter = sync.Pool{New: func() any {
		return make(map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int])
	}}
)

func wsGetStorageInner() map[accounts.StorageKey]*VersionedWrite[uint256.Int] {
	return wsMapPoolStorageInner.Get().(map[accounts.StorageKey]*VersionedWrite[uint256.Int])
}

func wsPutStorageInner(m map[accounts.StorageKey]*VersionedWrite[uint256.Int]) {
	if m == nil {
		return
	}
	clear(m)
	wsMapPoolStorageInner.Put(m)
}

func wsGetStorageOuter() map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int] {
	return wsMapPoolStorageOuter.Get().(map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int])
}

func wsPutStorageOuter(m map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int]) {
	if m == nil {
		return
	}
	clear(m)
	wsMapPoolStorageOuter.Put(m)
}

func writeSetPut[T any](m *map[accounts.Address]*VersionedWrite[T], addr accounts.Address, vw *VersionedWrite[T], pool *vwMapPool[T]) {
	if *m == nil {
		*m = pool.get()
	}
	(*m)[addr] = vw
}

func (s *WriteSet) SetAddress(addr accounts.Address, vw *VersionedWrite[*accounts.Account]) {
	writeSetPut(&s.address, addr, vw, wsMapPoolAddress)
}
func (s *WriteSet) SetBalance(addr accounts.Address, vw *VersionedWrite[uint256.Int]) {
	writeSetPut(&s.balance, addr, vw, wsMapPoolBalance)
}
func (s *WriteSet) SetNonce(addr accounts.Address, vw *VersionedWrite[uint64]) {
	writeSetPut(&s.nonce, addr, vw, wsMapPoolNonce)
}
func (s *WriteSet) SetIncarnation(addr accounts.Address, vw *VersionedWrite[uint64]) {
	writeSetPut(&s.incarnation, addr, vw, wsMapPoolIncarnation)
}
func (s *WriteSet) SetSelfDestruct(addr accounts.Address, vw *VersionedWrite[bool]) {
	writeSetPut(&s.selfDestruct, addr, vw, wsMapPoolSelfDestruct)
}
func (s *WriteSet) SetCreateContract(addr accounts.Address, vw *VersionedWrite[bool]) {
	writeSetPut(&s.createContract, addr, vw, wsMapPoolCreateContract)
}
func (s *WriteSet) SetCode(addr accounts.Address, vw *VersionedWrite[accounts.Code]) {
	writeSetPut(&s.code, addr, vw, wsMapPoolCode)
}
func (s *WriteSet) SetCodeHash(addr accounts.Address, vw *VersionedWrite[accounts.CodeHash]) {
	writeSetPut(&s.codeHash, addr, vw, wsMapPoolCodeHash)
}
func (s *WriteSet) SetCodeSize(addr accounts.Address, vw *VersionedWrite[int]) {
	writeSetPut(&s.codeSize, addr, vw, wsMapPoolCodeSize)
}
func (s *WriteSet) SetStorage(addr accounts.Address, key accounts.StorageKey, vw *VersionedWrite[uint256.Int]) {
	if s.storage == nil {
		s.storage = wsGetStorageOuter()
	}
	inner := s.storage[addr]
	if inner == nil {
		inner = wsGetStorageInner()
		s.storage[addr] = inner
	}
	inner[key] = vw
}

func (s *WriteSet) IsEmpty() bool {
	if s == nil {
		return true
	}
	return len(s.address) == 0 && len(s.balance) == 0 && len(s.nonce) == 0 &&
		len(s.incarnation) == 0 && len(s.selfDestruct) == 0 && len(s.createContract) == 0 &&
		len(s.code) == 0 && len(s.codeHash) == 0 && len(s.codeSize) == 0 && len(s.storage) == 0
}

func (s *WriteSet) Has(h WriteHeader) bool {
	return s.hasHeader(h)
}

// Filter returns a new WriteSet holding the writes matching keep; entries are shared, not cloned, with the receiver.
func (s *WriteSet) Filter(keep func(WriteHeader) bool) *WriteSet {
	if s == nil {
		return nil
	}
	out := &WriteSet{}
	for a, vw := range s.address {
		if keep(vw.WriteHeader) {
			out.SetAddress(a, vw)
		}
	}
	for a, vw := range s.balance {
		if keep(vw.WriteHeader) {
			out.SetBalance(a, vw)
		}
	}
	for a, vw := range s.nonce {
		if keep(vw.WriteHeader) {
			out.SetNonce(a, vw)
		}
	}
	for a, vw := range s.incarnation {
		if keep(vw.WriteHeader) {
			out.SetIncarnation(a, vw)
		}
	}
	for a, vw := range s.selfDestruct {
		if keep(vw.WriteHeader) {
			out.SetSelfDestruct(a, vw)
		}
	}
	for a, vw := range s.createContract {
		if keep(vw.WriteHeader) {
			out.SetCreateContract(a, vw)
		}
	}
	for a, vw := range s.code {
		if keep(vw.WriteHeader) {
			out.SetCode(a, vw)
		}
	}
	for a, vw := range s.codeHash {
		if keep(vw.WriteHeader) {
			out.SetCodeHash(a, vw)
		}
	}
	for a, vw := range s.codeSize {
		if keep(vw.WriteHeader) {
			out.SetCodeSize(a, vw)
		}
	}
	for a, inner := range s.storage {
		for k, vw := range inner {
			if keep(vw.WriteHeader) {
				out.SetStorage(a, k, vw)
			}
		}
	}
	return out
}

func (s *WriteSet) Finalize() *WriteSet {
	s.zeroSameTxCreateDestructStorage()
	return s.Snapshot()
}

// EIP-6780 + EIP-7928: same-tx create+destruct storage is zeroed here (not deleted) so the BAL folds it in as reads, not changes.
func (s *WriteSet) zeroSameTxCreateDestructStorage() {
	for addr, cc := range s.createContract {
		if !cc.Val {
			continue
		}
		if sd, ok := s.selfDestruct[addr]; !ok || !sd.Val {
			continue
		}
		if inner, ok := s.storage[addr]; ok {
			for _, vw := range inner {
				vw.Val = uint256.Int{}
			}
		}
	}
}

// Snapshot freezes the recorded writes (no reconciliation needed — the journal
// already tracks reverts). Self-destruct keeps only SelfDestruct/Balance/Incarnation/Storage/CreateContract; the rest drop.
func (s *WriteSet) Snapshot() *WriteSet {
	out := &WriteSet{}

	addrs := make(map[accounts.Address]struct{})
	for h := range s.AllHeaders() {
		addrs[h.Address] = struct{}{}
	}

	for addr := range addrs {
		sd := false
		if vw, ok := s.selfDestruct[addr]; ok {
			out.SetSelfDestruct(addr, cloneVW(vw))
			sd = vw.Val
		}
		if vw, ok := s.balance[addr]; ok {
			out.SetBalance(addr, cloneVW(vw))
		}
		if vw, ok := s.incarnation[addr]; ok {
			out.SetIncarnation(addr, cloneVW(vw))
		}
		if inner, ok := s.storage[addr]; ok {
			for k, vw := range inner {
				out.SetStorage(addr, k, cloneVW(vw))
			}
		}
		// CreateContract is kept under self-destruct for the BAL; apply skips it when the
		// same tx self-destructs, so it can't resurrect the account.
		if vw, ok := s.createContract[addr]; ok {
			out.SetCreateContract(addr, cloneVW(vw))
		}
		if !sd {
			if vw, ok := s.address[addr]; ok {
				out.SetAddress(addr, cloneVW(vw))
			}
			if vw, ok := s.nonce[addr]; ok {
				out.SetNonce(addr, cloneVW(vw))
			}
			if vw, ok := s.code[addr]; ok {
				out.SetCode(addr, cloneVW(vw))
			}
			if vw, ok := s.codeHash[addr]; ok {
				out.SetCodeHash(addr, cloneVW(vw))
			}
			if vw, ok := s.codeSize[addr]; ok {
				out.SetCodeSize(addr, cloneVW(vw))
			}
		}
	}
	return out
}

func (s *WriteSet) deleteAddr(addr accounts.Address) {
	delete(s.address, addr)
	delete(s.balance, addr)
	delete(s.nonce, addr)
	delete(s.incarnation, addr)
	delete(s.selfDestruct, addr)
	delete(s.createContract, addr)
	delete(s.code, addr)
	delete(s.codeHash, addr)
	delete(s.codeSize, addr)
	delete(s.storage, addr)
}

type createWriteSnapshot struct {
	address        *VersionedWrite[*accounts.Account]
	balance        *VersionedWrite[uint256.Int]
	incarnation    *VersionedWrite[uint64]
	selfDestruct   *VersionedWrite[bool]
	createContract *VersionedWrite[bool]
	codeHash       *VersionedWrite[accounts.CodeHash]
}

func (s *WriteSet) snapshotCreateFields(addr accounts.Address) *createWriteSnapshot {
	snap := &createWriteSnapshot{}
	if vw, ok := s.address[addr]; ok {
		snap.address = cloneVW(vw)
	}
	if vw, ok := s.balance[addr]; ok {
		snap.balance = cloneVW(vw)
	}
	if vw, ok := s.incarnation[addr]; ok {
		snap.incarnation = cloneVW(vw)
	}
	if vw, ok := s.selfDestruct[addr]; ok {
		snap.selfDestruct = cloneVW(vw)
	}
	if vw, ok := s.createContract[addr]; ok {
		snap.createContract = cloneVW(vw)
	}
	if vw, ok := s.codeHash[addr]; ok {
		snap.codeHash = cloneVW(vw)
	}
	return snap
}

// restoreCreateFields reverts to snap; a nil field in snap deletes that write.
func (s *WriteSet) restoreCreateFields(addr accounts.Address, snap *createWriteSnapshot) {
	delete(s.address, addr)
	delete(s.balance, addr)
	delete(s.incarnation, addr)
	delete(s.selfDestruct, addr)
	delete(s.createContract, addr)
	delete(s.codeHash, addr)
	if snap == nil {
		return
	}
	if snap.address != nil {
		s.SetAddress(addr, snap.address)
	}
	if snap.balance != nil {
		s.SetBalance(addr, snap.balance)
	}
	if snap.incarnation != nil {
		s.SetIncarnation(addr, snap.incarnation)
	}
	if snap.selfDestruct != nil {
		s.SetSelfDestruct(addr, snap.selfDestruct)
	}
	if snap.createContract != nil {
		s.SetCreateContract(addr, snap.createContract)
	}
	if snap.codeHash != nil {
		s.SetCodeHash(addr, snap.codeHash)
	}
}

// assertSelfDestructNormalized panics if a self-destructed address still has
// fields Normalize should drop — Apply would then leave a phantom account with a live incarnation, breaking a later CREATE2 there.
func (s *WriteSet) assertSelfDestructNormalized() {
	for addr, sdw := range s.selfDestruct {
		if !sdw.Val {
			continue
		}
		var field string
		switch {
		case s.nonce[addr] != nil:
			field = "nonce"
		case s.incarnation[addr] != nil:
			field = "incarnation"
		case s.codeHash[addr] != nil:
			field = "codeHash"
		default:
			continue
		}
		panic(fmt.Sprintf("write set not normalized: self-destructed %x keeps its %s write", addr.Value(), field))
	}
}

func (s *WriteSet) DeleteAccountFields(addr accounts.Address) {
	delete(s.balance, addr)
	delete(s.nonce, addr)
	delete(s.incarnation, addr)
	delete(s.codeHash, addr)
}

func (s *WriteSet) Count() int {
	if s == nil {
		return 0
	}
	n := len(s.address) + len(s.balance) + len(s.nonce) + len(s.incarnation) +
		len(s.selfDestruct) + len(s.createContract) + len(s.code) + len(s.codeHash) + len(s.codeSize)
	for _, inner := range s.storage {
		n += len(inner)
	}
	return n
}

func (s *WriteSet) GetAddress(addr accounts.Address) (*VersionedWrite[*accounts.Account], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.address[addr]
	return vw, ok
}
func (s *WriteSet) GetBalance(addr accounts.Address) (*VersionedWrite[uint256.Int], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.balance[addr]
	return vw, ok
}
func (s *WriteSet) GetNonce(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.nonce[addr]
	return vw, ok
}
func (s *WriteSet) GetIncarnation(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.incarnation[addr]
	return vw, ok
}
func (s *WriteSet) GetSelfDestruct(addr accounts.Address) (*VersionedWrite[bool], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.selfDestruct[addr]
	return vw, ok
}
func (s *WriteSet) GetCreateContract(addr accounts.Address) (*VersionedWrite[bool], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.createContract[addr]
	return vw, ok
}
func (s *WriteSet) GetCode(addr accounts.Address) (*VersionedWrite[accounts.Code], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.code[addr]
	return vw, ok
}
func (s *WriteSet) GetCodeHash(addr accounts.Address) (*VersionedWrite[accounts.CodeHash], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.codeHash[addr]
	return vw, ok
}
func (s *WriteSet) GetCodeSize(addr accounts.Address) (*VersionedWrite[int], bool) {
	if s == nil {
		return nil, false
	}
	vw, ok := s.codeSize[addr]
	return vw, ok
}
func (s *WriteSet) GetStorage(addr accounts.Address, key accounts.StorageKey) (*VersionedWrite[uint256.Int], bool) {
	if s == nil {
		return nil, false
	}
	inner := s.storage[addr]
	if inner == nil {
		return nil, false
	}
	vw, ok := inner[key]
	return vw, ok
}

// forEachAddr calls f once per path an address appears in; callers must tolerate repeats.
func (s *WriteSet) forEachAddr(f func(accounts.Address)) {
	if s == nil {
		return
	}
	for a := range s.address {
		f(a)
	}
	s.forEachFieldAddr(f)
}

func (s *WriteSet) forEachFieldAddr(f func(accounts.Address)) {
	if s == nil {
		return
	}
	for a := range s.balance {
		f(a)
	}
	for a := range s.nonce {
		f(a)
	}
	for a := range s.incarnation {
		f(a)
	}
	for a := range s.selfDestruct {
		f(a)
	}
	for a := range s.createContract {
		f(a)
	}
	for a := range s.code {
		f(a)
	}
	for a := range s.codeHash {
		f(a)
	}
	for a := range s.codeSize {
		f(a)
	}
	for a := range s.storage {
		f(a)
	}
}

func (s *WriteSet) addrs() map[accounts.Address]struct{} {
	out := map[accounts.Address]struct{}{}
	for a := range s.address {
		out[a] = struct{}{}
	}
	for a := range s.balance {
		out[a] = struct{}{}
	}
	for a := range s.nonce {
		out[a] = struct{}{}
	}
	for a := range s.incarnation {
		out[a] = struct{}{}
	}
	for a := range s.selfDestruct {
		out[a] = struct{}{}
	}
	for a := range s.createContract {
		out[a] = struct{}{}
	}
	for a := range s.code {
		out[a] = struct{}{}
	}
	for a := range s.codeHash {
		out[a] = struct{}{}
	}
	for a := range s.codeSize {
		out[a] = struct{}{}
	}
	for a := range s.storage {
		out[a] = struct{}{}
	}
	return out
}

// Per-path typed iterators over the write collections. Callers that care about
// self-destruct-vs-field priority must iterate SelfDestructs before the field writes explicitly.
func (s *WriteSet) Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]] {
	if s == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[uint256.Int](nil))
	}
	return maps.All(s.balance)
}
func (s *WriteSet) Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	if s == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[uint64](nil))
	}
	return maps.All(s.nonce)
}
func (s *WriteSet) Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	if s == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[uint64](nil))
	}
	return maps.All(s.incarnation)
}
func (s *WriteSet) SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	if s == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[bool](nil))
	}
	return maps.All(s.selfDestruct)
}
func (s *WriteSet) Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]] {
	if s == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[accounts.Code](nil))
	}
	return maps.All(s.code)
}
func (s *WriteSet) CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]] {
	if s == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[accounts.CodeHash](nil))
	}
	return maps.All(s.codeHash)
}
func (s *WriteSet) Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	if s == nil {
		return maps.All(map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int](nil))
	}
	return maps.All(s.storage)
}

func eachWriteHeaderOf[T any](m map[accounts.Address]*VersionedWrite[T], yield func(WriteHeader) bool) bool {
	for _, vw := range m {
		if !yield(vw.WriteHeader) {
			return false
		}
	}
	return true
}

func (s *WriteSet) AllHeaders() iter.Seq[WriteHeader] {
	return func(yield func(WriteHeader) bool) {
		if s == nil {
			return
		}
		if !eachWriteHeaderOf(s.address, yield) ||
			!eachWriteHeaderOf(s.balance, yield) ||
			!eachWriteHeaderOf(s.nonce, yield) ||
			!eachWriteHeaderOf(s.incarnation, yield) ||
			!eachWriteHeaderOf(s.selfDestruct, yield) ||
			!eachWriteHeaderOf(s.createContract, yield) ||
			!eachWriteHeaderOf(s.code, yield) ||
			!eachWriteHeaderOf(s.codeHash, yield) ||
			!eachWriteHeaderOf(s.codeSize, yield) {
			return
		}
		for _, inner := range s.storage {
			for _, vw := range inner {
				if !yield(vw.WriteHeader) {
					return
				}
			}
		}
	}
}

// ReleaseAndReset releases every write to its pool before returning the maps to
// theirs — order matters, or ReleaseMaps clears them first.
func (s *WriteSet) ReleaseAndReset() {
	for _, vw := range s.address {
		releaseVWAddress(vw)
	}
	for _, vw := range s.balance {
		releaseVWBalance(vw)
	}
	for _, vw := range s.nonce {
		releaseVWNonce(vw)
	}
	for _, vw := range s.incarnation {
		releaseVWIncarnation(vw)
	}
	for _, vw := range s.selfDestruct {
		releaseVWSelfDestruct(vw)
	}
	for _, vw := range s.createContract {
		releaseVWCreateContract(vw)
	}
	for _, vw := range s.code {
		releaseVWCode(vw)
	}
	for _, vw := range s.codeHash {
		releaseVWCodeHash(vw)
	}
	for _, vw := range s.codeSize {
		releaseVWCodeSize(vw)
	}
	for _, inner := range s.storage {
		for _, vw := range inner {
			releaseVWStorage(vw)
		}
	}
	s.ReleaseMaps()
}

// ReleaseMaps returns only the map containers; VersionedWrite values are left for
// GC since a merge may share them with other sets.
func (s *WriteSet) ReleaseMaps() {
	if s == nil {
		return
	}
	wsMapPoolAddress.put(s.address)
	wsMapPoolBalance.put(s.balance)
	wsMapPoolNonce.put(s.nonce)
	wsMapPoolIncarnation.put(s.incarnation)
	wsMapPoolSelfDestruct.put(s.selfDestruct)
	wsMapPoolCreateContract.put(s.createContract)
	wsMapPoolCode.put(s.code)
	wsMapPoolCodeHash.put(s.codeHash)
	wsMapPoolCodeSize.put(s.codeSize)
	for _, inner := range s.storage {
		wsPutStorageInner(inner)
	}
	wsPutStorageOuter(s.storage)
	*s = WriteSet{}
}

func (s *WriteSet) DelBalance(addr accounts.Address) {
	if vw, ok := s.balance[addr]; ok {
		releaseVWBalance(vw)
		delete(s.balance, addr)
	}
}
func (s *WriteSet) DelNonce(addr accounts.Address) {
	if vw, ok := s.nonce[addr]; ok {
		releaseVWNonce(vw)
		delete(s.nonce, addr)
	}
}
func (s *WriteSet) DelIncarnation(addr accounts.Address) {
	if vw, ok := s.incarnation[addr]; ok {
		releaseVWIncarnation(vw)
		delete(s.incarnation, addr)
	}
}
func (s *WriteSet) DelSelfDestruct(addr accounts.Address) {
	if vw, ok := s.selfDestruct[addr]; ok {
		releaseVWSelfDestruct(vw)
		delete(s.selfDestruct, addr)
	}
}
func (s *WriteSet) DelCode(addr accounts.Address) {
	if vw, ok := s.code[addr]; ok {
		releaseVWCode(vw)
		delete(s.code, addr)
	}
}
func (s *WriteSet) DelCodeHash(addr accounts.Address) {
	if vw, ok := s.codeHash[addr]; ok {
		releaseVWCodeHash(vw)
		delete(s.codeHash, addr)
	}
}
func (s *WriteSet) DelCodeSize(addr accounts.Address) {
	if vw, ok := s.codeSize[addr]; ok {
		releaseVWCodeSize(vw)
		delete(s.codeSize, addr)
	}
}
func (s *WriteSet) DelStorage(addr accounts.Address, key accounts.StorageKey) {
	if inner := s.storage[addr]; inner != nil {
		if vw, ok := inner[key]; ok {
			releaseVWStorage(vw)
			delete(inner, key)
		}
		if len(inner) == 0 {
			delete(s.storage, addr)
		}
	}
}

// updateBalance, updateNonce etc. mutate the existing per-path entry in place; a no-op if addr has none.
func (s *WriteSet) updateBalance(addr accounts.Address, val uint256.Int) {
	if vw, ok := s.balance[addr]; ok {
		vw.Val = val
	}
}

func (s *WriteSet) updateStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	if inner := s.storage[addr]; inner != nil {
		if vw, ok := inner[key]; ok {
			vw.Val = val
		}
	}
}

func (s *WriteSet) updateNonce(addr accounts.Address, val uint64) {
	if vw, ok := s.nonce[addr]; ok {
		vw.Val = val
	}
}

func (s *WriteSet) updateIncarnation(addr accounts.Address, val uint64) {
	if vw, ok := s.incarnation[addr]; ok {
		vw.Val = val
	}
}

func (s *WriteSet) updateSelfDestruct(addr accounts.Address, val bool) {
	if vw, ok := s.selfDestruct[addr]; ok {
		vw.Val = val
	}
}

func (s *WriteSet) updateCode(addr accounts.Address, val accounts.Code) {
	if vw, ok := s.code[addr]; ok {
		vw.Val = val
	}
}

func (s *WriteSet) updateCodeHash(addr accounts.Address, val accounts.CodeHash) {
	if vw, ok := s.codeHash[addr]; ok {
		vw.Val = val
	}
}

func (s *WriteSet) updateCodeSize(addr accounts.Address, val int) {
	if vw, ok := s.codeSize[addr]; ok {
		vw.Val = val
	}
}

func valueString(path AccountPath, value any) string {
	if value == nil {
		return "<nil>"
	}
	switch path {
	case AddressPath:
		return fmt.Sprintf("%+v", value)
	case BalancePath:
		num := value.(uint256.Int)
		return (&num).String()
	case StoragePath:
		num := value.(uint256.Int)
		return num.Hex()[2:]
	case NoncePath, IncarnationPath:
		return strconv.FormatUint(value.(uint64), 10)
	case CodePath:
		switch v := value.(type) {
		case accounts.Code:
			l := min(v.Len(), 40)
			return hex.EncodeToString(v.Bytes[0:l])
		case []byte:
			l := min(len(v), 40)
			return hex.EncodeToString(v[0:l])
		}
		return "<unknown-code>"
	}

	return fmt.Sprint(value)
}

var ErrDependency = errors.New("found dependency")

type versionedStateReader struct {
	txIndex     int
	reads       ReadSet
	versionMap  *VersionMap
	stateReader StateReader
}

func NewVersionedStateReader(txIndex int, reads ReadSet, versionMap *VersionMap, stateReader StateReader) *versionedStateReader {
	return &versionedStateReader{txIndex, reads, versionMap, stateReader}
}

func (vr *versionedStateReader) SetTrace(trace bool, tracePrefix string) {
	vr.stateReader.SetTrace(trace, tracePrefix)
}

func (vr *versionedStateReader) Trace() bool {
	return vr.stateReader.Trace()
}

func (vr *versionedStateReader) TracePrefix() string {
	return vr.stateReader.TracePrefix()
}

func (vr *versionedStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	r, recorded := vr.reads.GetAddress(address)
	if recorded && r.Val != nil && !r.Val.IsNil() {
		account := r.Val.Account()
		updated := vr.applyVersionedUpdates(address, *account)
		return &updated, nil
	}

	if vr.versionMap != nil {
		// Honor a prior self-destruct only if no later tx recreates the account.
		// EIP-161: an untipped-touch coinbase self-destructs; a later tip revives it and must surface here.
		if destroyed, _, revived := vr.versionMap.AccountLifecycle(address, vr.txIndex); destroyed && !revived {
			return nil, nil
		}
		if acc, ok := versionedUpdateAddress(vr.versionMap, address, vr.txIndex); ok && acc != nil {
			updated := vr.applyVersionedUpdates(address, *acc)
			return &updated, nil
		}
	}

	// A recorded AddressPath read with no account is the tx's final word that the address holds nothing.
	if vr.stateReader != nil && !recorded {
		account, err := vr.stateReader.ReadAccountData(address)

		if err != nil {
			return nil, err
		}

		if account != nil {
			updated := vr.applyVersionedUpdates(address, *account)
			return &updated, nil
		}
	}

	// BAL pre-population writes Balance/Nonce/Code/Storage but never AddressPath, so
	// a tip-only account (e.g. a fresh fee_recipient) reads nil here; synthesize an
	// empty account so applyVersionedUpdates can still apply the BAL-preloaded fields.
	if vr.versionMap != nil {
		var synth accounts.Account
		updated := vr.applyVersionedUpdates(address, synth)
		if updated != synth {
			return &updated, nil
		}
	}

	return nil, nil
}

// A Dependency (Estimate) cell holds the same latest in-block write a Done
// cell does, so these treat it as found too rather than falling back to the pre-block DB value.

func versionedUpdateAddress(vm *VersionMap, addr accounts.Address, txIndex int) (*accounts.Account, bool) {
	val, res, ok := vm.ReadAddress(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return nil, false
}

func versionedUpdateCode(vm *VersionMap, addr accounts.Address, txIndex int) ([]byte, bool) {
	val, res, ok := vm.ReadCode(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val.Bytes, true
	}
	return nil, false
}

func versionedUpdateStorage(vm *VersionMap, addr accounts.Address, key accounts.StorageKey, txIndex int) (uint256.Int, bool) {
	val, res, ok := vm.ReadStorage(addr, key, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return uint256.Int{}, false
}

// applyVersionedUpdates overlays per-field writes onto account: without it, a
// prior tx's partial write would be silently lost here and go uncaught by validation.
func (vr versionedStateReader) applyVersionedUpdates(address accounts.Address, account accounts.Account) accounts.Account {
	vr.versionMap.applySubFieldWrites(address, vr.txIndex, &account)
	return account
}

func (vr versionedStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	if r, ok := vr.reads.GetAddress(address); ok && r.Val != nil && !r.Val.IsNil() {
		account := r.Val.Account()
		updated := vr.applyVersionedUpdates(address, *account)
		return &updated, nil
	}

	if vr.stateReader != nil {
		account, err := vr.stateReader.ReadAccountDataForDebug(address)

		if err != nil {
			return nil, err
		}

		updated := vr.applyVersionedUpdates(address, *account)
		return &updated, nil
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if r, ok := vr.reads.GetStorage(address, key); ok {
		return r.Val, true, nil
	}

	if vr.versionMap != nil {
		if destructed, res, ok := vr.versionMap.ReadSelfDestruct(address, vr.txIndex); ok && res.Status() == MVReadResultDone {
			if destructed {
				return uint256.Int{}, false, nil
			}
		}
		if val, ok := versionedUpdateStorage(vr.versionMap, address, key, vr.txIndex); ok {
			return val, true, nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountStorage(address, key)
	}

	return uint256.Int{}, false, nil
}

func (vr versionedStateReader) HasStorage(address accounts.Address) (bool, error) {
	if _, ok := vr.reads.storage[address]; ok {
		return true, nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.HasStorage(address)
	}

	return false, nil
}

func (vr versionedStateReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	if r, ok := vr.reads.GetCode(address); ok && r.Val != nil {
		return r.Val, nil
	}

	// EIP-7702 delegation can set CodePath from an earlier tx in the same block.
	if vr.versionMap != nil {
		if destructed, res, ok := vr.versionMap.ReadSelfDestruct(address, vr.txIndex); ok && res.Status() == MVReadResultDone {
			if destructed {
				return nil, nil
			}
		}
		if code, ok := versionedUpdateCode(vr.versionMap, address, vr.txIndex); ok {
			return code, nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCode(address)
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	if r, ok := vr.reads.GetCode(address); ok && r.Val != nil {
		return len(r.Val), nil
	}

	if vr.versionMap != nil {
		if destructed, res, ok := vr.versionMap.ReadSelfDestruct(address, vr.txIndex); ok && res.Status() == MVReadResultDone {
			if destructed {
				return 0, nil
			}
		}
		if code, ok := versionedUpdateCode(vr.versionMap, address, vr.txIndex); ok {
			return len(code), nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCodeSize(address)
	}

	return 0, nil
}

func (vr versionedStateReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	if r, ok := vr.reads.GetAddress(address); ok && r.Val != nil && !r.Val.IsNil() {
		return r.Val.Account().Incarnation, nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountIncarnation(address)
	}

	return 0, nil
}

func SetAccountFieldFromMap(out *WriteSet, vm *VersionMap, addr accounts.Address, path AccountPath, ver Version, txIdx int) bool {
	switch path {
	case BalancePath:
		v, rr, found := vm.ReadBalance(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			out.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: v})
			return true
		}
	case NoncePath:
		v, rr, found := vm.ReadNonce(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			out.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: v})
			return true
		}
	case IncarnationPath:
		v, rr, found := vm.ReadIncarnation(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			out.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}, Val: v})
			return true
		}
	case CodeHashPath:
		v, rr, found := vm.ReadCodeHash(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			out.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: v})
			return true
		}
	}
	return false
}

func SetAccountFieldZero(out *WriteSet, addr accounts.Address, path AccountPath, ver Version) {
	switch path {
	case BalancePath:
		out.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}})
	case NoncePath:
		out.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}})
	case IncarnationPath:
		out.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}})
	case CodeHashPath:
		out.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: accounts.EmptyCodeHash})
	}
}

func SetAccountFieldFromAccount(out *WriteSet, addr accounts.Address, path AccountPath, ver Version, acc *accounts.Account) {
	switch path {
	case BalancePath:
		var v uint256.Int
		if acc != nil {
			v = acc.Balance
		}
		out.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: v})
	case NoncePath:
		var v uint64
		if acc != nil {
			v = acc.Nonce
		}
		out.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: v})
	case IncarnationPath:
		var v uint64
		if acc != nil {
			v = acc.Incarnation
		}
		out.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}, Val: v})
	case CodeHashPath:
		v := accounts.EmptyCodeHash
		if acc != nil {
			v = acc.CodeHash
		}
		out.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: v})
	}
}

func (s *WriteSet) TouchUpdates(updates *commitment.Updates) {
	if s == nil {
		return
	}
	for addr, w := range s.balance {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags:   commitment.BalanceUpdate,
			Balance: w.Val,
		})
	}
	for addr, w := range s.nonce {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags: commitment.NonceUpdate,
			Nonce: w.Val,
		})
	}
	for addr, w := range s.codeHash {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags:    commitment.CodeUpdate,
			CodeHash: w.Val.Value(),
		})
	}
	for addr, w := range s.code {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags:    commitment.CodeUpdate,
			CodeHash: w.Val.Hash.Value(),
		})
	}
	for addr, w := range s.selfDestruct {
		if w.Val {
			addrVal := addr.Value()
			updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
				Flags: commitment.DeleteUpdate,
			})
		}
	}
	for addr, inner := range s.storage {
		addrVal := addr.Value()
		for key, w := range inner {
			vBytes := w.Val.Bytes()
			keyVal := key.Value()
			composite := make([]byte, 20+32)
			copy(composite, addrVal[:])
			copy(composite[20:], keyVal[:])
			var u commitment.Update
			u.StorageLen = int8(len(vBytes))
			if len(vBytes) == 0 {
				u.Flags = commitment.DeleteUpdate
			} else {
				u.Flags = commitment.StorageUpdate
				copy(u.Storage[:], vBytes)
			}
			updates.TouchPlainKeyDirect(string(composite), &u)
		}
	}
}

func sortWriteHeaders(headers []WriteHeader) {
	slices.SortFunc(headers, func(a, b WriteHeader) int {
		if c := a.Address.Cmp(b.Address); c != 0 {
			return c
		}
		if a.Path != b.Path {
			if a.Path < b.Path {
				return -1
			}
			return 1
		}
		return a.Key.Cmp(b.Key)
	})
}

func (s *WriteSet) hasHeader(h WriteHeader) bool {
	if s == nil {
		return false
	}
	switch h.Path {
	case AddressPath:
		_, ok := s.address[h.Address]
		return ok
	case BalancePath:
		_, ok := s.balance[h.Address]
		return ok
	case NoncePath:
		_, ok := s.nonce[h.Address]
		return ok
	case IncarnationPath:
		_, ok := s.incarnation[h.Address]
		return ok
	case SelfDestructPath:
		_, ok := s.selfDestruct[h.Address]
		return ok
	case CreateContractPath:
		_, ok := s.createContract[h.Address]
		return ok
	case CodePath:
		_, ok := s.code[h.Address]
		return ok
	case CodeHashPath:
		_, ok := s.codeHash[h.Address]
		return ok
	case CodeSizePath:
		_, ok := s.codeSize[h.Address]
		return ok
	case StoragePath:
		if inner, ok := s.storage[h.Address]; ok {
			_, ok := inner[h.Key]
			return ok
		}
	}
	return false
}

// copyFrom clones each VersionedWrite so s shares none with src — otherwise
// in-place mutators on one side would leak edits to the other.
func (s *WriteSet) copyFrom(src *WriteSet) {
	if src == nil {
		return
	}
	for a, vw := range src.address {
		s.SetAddress(a, cloneVW(vw))
	}
	for a, vw := range src.balance {
		s.SetBalance(a, cloneVW(vw))
	}
	for a, vw := range src.nonce {
		s.SetNonce(a, cloneVW(vw))
	}
	for a, vw := range src.incarnation {
		s.SetIncarnation(a, cloneVW(vw))
	}
	for a, vw := range src.selfDestruct {
		s.SetSelfDestruct(a, cloneVW(vw))
	}
	for a, vw := range src.createContract {
		s.SetCreateContract(a, cloneVW(vw))
	}
	for a, vw := range src.code {
		s.SetCode(a, cloneVW(vw))
	}
	for a, vw := range src.codeHash {
		s.SetCodeHash(a, cloneVW(vw))
	}
	for a, vw := range src.codeSize {
		s.SetCodeSize(a, cloneVW(vw))
	}
	for a, inner := range src.storage {
		for key, vw := range inner {
			s.SetStorage(a, key, cloneVW(vw))
		}
	}
}

// copyMissingFrom, unlike copyFrom, shares src's *VersionedWrite pointers instead of cloning them.
func (s *WriteSet) copyMissingFrom(src *WriteSet) {
	if src == nil {
		return
	}
	for a, vw := range src.address {
		if _, ok := s.address[a]; !ok {
			s.SetAddress(a, vw)
		}
	}
	for a, vw := range src.balance {
		if _, ok := s.balance[a]; !ok {
			s.SetBalance(a, vw)
		}
	}
	for a, vw := range src.nonce {
		if _, ok := s.nonce[a]; !ok {
			s.SetNonce(a, vw)
		}
	}
	for a, vw := range src.incarnation {
		if _, ok := s.incarnation[a]; !ok {
			s.SetIncarnation(a, vw)
		}
	}
	for a, vw := range src.selfDestruct {
		if _, ok := s.selfDestruct[a]; !ok {
			s.SetSelfDestruct(a, vw)
		}
	}
	for a, vw := range src.createContract {
		if _, ok := s.createContract[a]; !ok {
			s.SetCreateContract(a, vw)
		}
	}
	for a, vw := range src.code {
		if _, ok := s.code[a]; !ok {
			s.SetCode(a, vw)
		}
	}
	for a, vw := range src.codeHash {
		if _, ok := s.codeHash[a]; !ok {
			s.SetCodeHash(a, vw)
		}
	}
	for a, vw := range src.codeSize {
		if _, ok := s.codeSize[a]; !ok {
			s.SetCodeSize(a, vw)
		}
	}
	for a, inner := range src.storage {
		own := s.storage[a]
		for key, vw := range inner {
			if _, ok := own[key]; !ok {
				s.SetStorage(a, key, vw)
			}
		}
	}
}

func (prev *WriteSet) Merge(next *WriteSet) *WriteSet {
	if prev.IsEmpty() {
		return next
	}
	if next.IsEmpty() {
		return prev
	}
	out := &WriteSet{}
	out.copyFrom(prev)
	out.copyFrom(next)
	return out
}

// MergeInto shares *VersionedWrite pointers instead of cloning (next must be
// exclusively owned; neither side mutates a shared value afterward). An empty
// side short-circuits to the other, so the result can be prev — compare identity before releasing or mutating it.
func (prev *WriteSet) MergeInto(next *WriteSet) *WriteSet {
	if prev.IsEmpty() {
		return next
	}
	if next.IsEmpty() {
		return prev
	}
	next.copyMissingFrom(prev)
	return next
}

func (writes *WriteSet) HasNewWrite(cmpSet *WriteSet) bool {
	if writes.IsEmpty() {
		return false
	}
	if cmpSet.IsEmpty() || writes.Count() > cmpSet.Count() {
		return true
	}
	for h := range writes.AllHeaders() {
		if !cmpSet.hasHeader(h) {
			return true
		}
	}
	return false
}

// StripBalanceWrite strips a stale speculative coinbase/burnt-contract balance
// write so finalize can re-apply the delta on top of the correct base balance.
func (writes *WriteSet) StripBalanceWrite(addr accounts.Address, readSet ReadSet) (stripped *WriteSet, delta uint256.Int, increase bool, found bool) {
	stripped = writes
	if writes == nil || addr.IsNil() {
		return
	}
	bw, hasWrite := writes.balance[addr]
	if !readSet.hasAddr(addr) {
		if hasWrite {
			delete(writes.balance, addr)
		}
		return
	}
	balRead, ok := readSet.GetBalance(addr)
	if !ok || !hasWrite {
		return
	}
	staleRead := balRead.Val
	staleWrite := bw.Val
	delete(writes.balance, addr)
	if staleWrite.Gt(&staleRead) {
		delta.Sub(&staleWrite, &staleRead)
		increase = true
		found = true
	} else if staleRead.Gt(&staleWrite) {
		delta.Sub(&staleRead, &staleWrite)
		found = true
	}
	return
}

// note that TxIndex starts at -1 (the begin system tx)
type VersionedIO struct {
	inputs  []versionedReadSet
	outputs []*WriteSet // write sets that should be checked during validation

	outputsReleased bool
}

func NewVersionedIO(numTx int) *VersionedIO {
	return &VersionedIO{
		inputs:  make([]versionedReadSet, numTx+1),
		outputs: make([]*WriteSet, numTx+1),
	}
}

func (io *VersionedIO) Len() int {
	if io == nil {
		return 0
	}
	return max(len(io.inputs), len(io.outputs))
}

func (io *VersionedIO) Inputs() []versionedReadSet {
	return io.inputs
}

func (io *VersionedIO) Outputs() []*WriteSet {
	return io.outputs
}

func (io *VersionedIO) ReadSet(txnIdx int) ReadSet {
	if len(io.inputs) <= txnIdx+1 {
		return ReadSet{}
	}
	return io.inputs[txnIdx+1].readSet
}

func (io *VersionedIO) ReadSetIncarnation(txnIdx int) int {
	if len(io.inputs) <= txnIdx+1 {
		return -1
	}
	if io.inputs[txnIdx+1].readSet.Len() > 0 {
		return io.inputs[txnIdx+1].incarnation
	}
	return 0
}

func (io *VersionedIO) WriteSet(txnIdx int) *WriteSet {
	io.assertOutputsLive("WriteSet")
	if len(io.outputs) <= txnIdx+1 {
		return nil
	}
	return io.outputs[txnIdx+1]
}

func (io *VersionedIO) WriteCount() (count int64) {
	io.assertOutputsLive("WriteCount")
	for _, output := range io.outputs {
		count += int64(output.Count())
	}

	return count
}

func (io *VersionedIO) assertOutputsLive(op string) {
	if dbg.AssertEnabled && io != nil && io.outputsReleased {
		panic("VersionedIO." + op + " after ReleaseOutputMaps: the write sets are emptied, so this read silently sees nothing")
	}
}

func (io *VersionedIO) ReleaseOutputMaps() {
	for _, output := range io.outputs {
		output.ReleaseMaps()
	}
	if dbg.AssertEnabled {
		io.outputsReleased = true
	}
}

func (io *VersionedIO) ReadCount() (count int64) {
	for _, input := range io.inputs {
		count += int64(input.readSet.Len())
	}

	return count
}

func (io *VersionedIO) HasReads(txnIdx int) bool {
	if len(io.inputs) <= txnIdx+1 {
		return false
	}
	return io.inputs[txnIdx+1].readSet.Len() > 0
}

func (io *VersionedIO) RecordReads(txVersion Version, input ReadSet) {
	if len(io.inputs) <= txVersion.TxIndex+1 {
		io.inputs = append(io.inputs, make([]versionedReadSet, txVersion.TxIndex+2-len(io.inputs))...)
	}
	io.inputs[txVersion.TxIndex+1] = versionedReadSet{txVersion.Incarnation, input}
}

func (io *VersionedIO) RecordWrites(txVersion Version, output *WriteSet) {
	txId := txVersion.TxIndex

	if len(io.outputs) <= txId+1 {
		io.outputs = append(io.outputs, make([]*WriteSet, txId+2-len(io.outputs))...)
	}
	io.outputs[txId+1] = output
}

func (io *VersionedIO) Merge(other *VersionedIO) *VersionedIO {
	mergedLen := max(io.Len(), other.Len())
	merged := NewVersionedIO(mergedLen - 1)

	for i := range mergedLen {
		if i < len(io.inputs) {
			if i < len(other.inputs) {
				merged.inputs[i] = io.inputs[i].Merge(other.inputs[i])
			} else {
				merged.inputs[i] = io.inputs[i].Merge(versionedReadSet{})
			}
		} else if i < len(other.inputs) {
			merged.inputs[i] = other.inputs[i].Merge(versionedReadSet{})
		}
		if i < len(io.outputs) {
			if i < len(other.outputs) {
				merged.outputs[i] = io.outputs[i].Merge(other.outputs[i])
			} else {
				merged.outputs[i] = io.outputs[i].Merge(nil)
			}
		} else if i < len(other.outputs) {
			merged.outputs[i] = other.outputs[i].Merge(nil)
		}
	}
	return merged
}

func (io *VersionedIO) mergeTx(version Version, reads ReadSet, writes *WriteSet) {
	idx := version.TxIndex + 1
	n := max(idx+1, len(io.inputs), len(io.outputs))
	if n > len(io.inputs) {
		io.inputs = append(io.inputs, make([]versionedReadSet, n-len(io.inputs))...)
	}
	if n > len(io.outputs) {
		io.outputs = append(io.outputs, make([]*WriteSet, n-len(io.outputs))...)
	}
	// Fold on typed reads OR access-only marks (the latter still needed for the
	// EIP-7928 BAL); Len() itself stays access-free so read-presence checks elsewhere stay correct.
	if reads.Len() > 0 || len(reads.access) > 0 {
		if io.inputs[idx].readSet.Len() == 0 && len(io.inputs[idx].readSet.access) == 0 {
			io.inputs[idx] = versionedReadSet{version.Incarnation, reads}
		} else {
			io.inputs[idx] = io.inputs[idx].Merge(versionedReadSet{version.Incarnation, reads})
		}
	}
	if !writes.IsEmpty() {
		io.outputs[idx] = io.outputs[idx].Merge(writes)
	}
}

// AsBlockAccessList assembles the EIP-7928 block access list. EIP-7928 and
// EIP-8246 activate together at Amsterdam, so balance writes always use no-burn SELFDESTRUCT semantics.
func (io *VersionedIO) AsBlockAccessList() types.BlockAccessList {
	if io == nil {
		return nil
	}

	ac := make(map[accounts.Address]*accountState)
	maxTxIndex := io.Len() - 1

	for txIndex := -1; txIndex <= maxTxIndex; txIndex++ {
		rs := io.ReadSet(txIndex)
		for addr, tr := range rs.balance {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr).updateReadBalance(tr.Val)
		}
		for addr, inner := range rs.storage {
			if addr.IsNil() {
				continue
			}
			for key, tr := range inner {
				if tr.internal {
					continue
				}
				ensureAccountState(ac, addr).updateReadStorage(key, tr.Val)
			}
		}
		for addr, tr := range rs.address {
			if addr.IsNil() || tr.internal {
				continue
			}
			if tr.Val == nil || tr.Val.IsNil() {
				continue
			}
			ensureAccountState(ac, addr)
		}
		for addr, tr := range rs.nonce {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr)
		}
		for addr, tr := range rs.incarnation {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr)
		}
		for addr, tr := range rs.selfDestruct {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr)
		}
		for addr, tr := range rs.createContract {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr)
		}
		for addr, tr := range rs.code {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr)
		}
		for addr, tr := range rs.codeHash {
			if addr.IsNil() || tr.internal {
				continue
			}
			account := ensureAccountState(ac, addr)
			// Only an empty code-hash read seen before any code change counts as the
			// pre-block baseline; a later empty read of an already-cleared value must not re-trigger it.
			if tr.Val.IsEmpty() && len(account.code.changes.entries) == 0 {
				account.initialCodeEmpty = true
			}
		}
		for addr, tr := range rs.codeSize {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr)
		}

		if writes := io.WriteSet(txIndex); writes != nil {
			for addr, w := range writes.SelfDestructs() {
				if addr.IsNil() || !w.Val {
					continue
				}
				ensureAccountState(ac, addr)
			}
			for addr, w := range writes.Balances() {
				if addr.IsNil() {
					continue
				}
				ensureAccountState(ac, addr).applyWriteBalance(w.Val, w.Version.blockAccessIndex())
			}
			for addr, w := range writes.Nonces() {
				if addr.IsNil() {
					continue
				}
				ensureAccountState(ac, addr).applyWriteNonce(w.Val, w.Version.blockAccessIndex())
			}
			for addr, w := range writes.Codes() {
				if addr.IsNil() {
					continue
				}
				ensureAccountState(ac, addr).applyWriteCode(w.Val, w.Version.blockAccessIndex())
			}
			for addr, byKey := range writes.Storages() {
				if addr.IsNil() {
					continue
				}
				account := ensureAccountState(ac, addr)
				for key, w := range byKey {
					account.applyWriteStorage(key, w.Val, w.Version.blockAccessIndex())
				}
			}
			for addr := range writes.addrs() {
				if addr.IsNil() {
					continue
				}
				account := ensureAccountState(ac, addr)
				if vw, ok := writes.GetCreateContract(addr); ok && vw.Val {
					account.initialCodeEmpty = true
				}
			}
		}

		isUserTx := txIndex >= 0
		for addr, opts := range io.ReadSet(txIndex).access {
			if addr.IsNil() {
				continue
			}

			account := ensureAccountState(ac, addr)
			if isUserTx && !opts.revertable {
				account.nonRevertableUserAccess = true
			}
		}
	}

	bal := make([]*types.AccountChanges, 0, len(ac))
	for _, account := range ac {
		account.finalize()
		account.changes.Normalize()
		// EIP-7928: SYSTEM_ADDRESS is excluded unless it has real account changes or a
		// non-revertable user access — the EIP-4788 system call touches it every block.
		if account.changes.Address == params.SystemAddress && !hasAccountChanges(account.changes) && !account.nonRevertableUserAccess {
			continue
		}
		bal = append(bal, account.changes)
	}

	slices.SortFunc(bal, func(a, b *types.AccountChanges) int {
		return a.Address.Cmp(b.Address)
	})

	return bal
}

func hasAccountChanges(ac *types.AccountChanges) bool {
	return len(ac.StorageChanges) > 0 || len(ac.StorageReads) > 0 ||
		len(ac.BalanceChanges) > 0 || len(ac.NonceChanges) > 0 ||
		len(ac.CodeChanges) > 0
}

type accountState struct {
	changes                 *types.AccountChanges
	balance                 *fieldTracker[uint256.Int]
	nonce                   *fieldTracker[uint64]
	code                    *fieldTracker[accounts.Code]
	balanceValue            *uint256.Int
	initialBalanceValue     *uint256.Int
	storageReadValues       map[accounts.StorageKey]uint256.Int
	nonRevertableUserAccess bool
	initialCodeEmpty        bool
}

func (a *accountState) finalize() {
	applyToBalance(a.balance, a.changes, a.initialBalanceValue)
	applyToNonce(a.nonce, a.changes)
	applyToCode(a.code, a.changes, a.initialCodeEmpty)
}

type fieldTracker[T any] struct {
	changes changeTracker[T]
}

func (ft *fieldTracker[T]) recordWrite(idx uint32, value T) {
	ft.changes.recordWrite(idx, value)
}

func newBalanceTracker() *fieldTracker[uint256.Int] {
	return &fieldTracker[uint256.Int]{}
}

func applyToBalance(bt *fieldTracker[uint256.Int], ac *types.AccountChanges, initialBalance *uint256.Int) {
	// If the first (lowest-index) entry equals the pre-block balance, it's a
	// net-zero change from a reverted tx (e.g. CALL-with-value then revert) — exclude it.
	firstFiltered := false
	bt.changes.apply(func(idx uint32, value uint256.Int) {
		if !firstFiltered {
			firstFiltered = true
			if initialBalance != nil && value.Eq(initialBalance) {
				return
			}
		}
		ac.BalanceChanges = append(ac.BalanceChanges, &types.BalanceChange{
			Index: idx,
			Value: value,
		})
	})
}

func newNonceTracker() *fieldTracker[uint64] {
	return &fieldTracker[uint64]{}
}

func applyToNonce(nt *fieldTracker[uint64], ac *types.AccountChanges) {
	nt.changes.apply(func(idx uint32, value uint64) {
		ac.NonceChanges = append(ac.NonceChanges, &types.NonceChange{
			Index: idx,
			Value: value,
		})
	})
}

func newCodeTracker() *fieldTracker[accounts.Code] {
	return &fieldTracker[accounts.Code]{}
}

func applyToCode(ct *fieldTracker[accounts.Code], ac *types.AccountChanges, initialCodeEmpty bool) {
	// A first code change back to empty, when pre-block code was already empty (e.g.
	// an EIP-7702 delegation set then cleared in the same tx), is net-zero — omit it.
	firstFiltered := false
	ct.changes.apply(func(idx uint32, value accounts.Code) {
		if !firstFiltered {
			firstFiltered = true
			if initialCodeEmpty && len(value.Bytes) == 0 {
				return
			}
		}
		ac.CodeChanges = append(ac.CodeChanges, &types.CodeChange{
			Index:    idx,
			Bytecode: bytes.Clone(value.Bytes),
		})
	})
}

// changeTracker's CodePath entries borrow bytecode owned by cache.StateCache;
// applyToCode clones it into the BAL CodeChange so the consensus-visible output owns its bytes.
type changeTracker[T any] struct {
	entries map[uint32]T
}

func (ct *changeTracker[T]) recordWrite(idx uint32, value T) {
	if ct.entries == nil {
		ct.entries = make(map[uint32]T)
	}
	ct.entries[idx] = value
}

func (ct *changeTracker[T]) apply(applyFn func(uint32, T)) {
	if len(ct.entries) == 0 {
		return
	}

	indices := make([]uint32, 0, len(ct.entries))
	for idx := range ct.entries {
		indices = append(indices, idx)
	}
	slices.Sort(indices)

	for _, idx := range indices {
		applyFn(idx, ct.entries[idx])
	}
}

func (a *accountState) setBalanceValue(v uint256.Int) {
	if a.balanceValue == nil {
		a.balanceValue = &uint256.Int{}
	}
	*a.balanceValue = v
}

func ensureAccountState(accounts map[accounts.Address]*accountState, addr accounts.Address) *accountState {
	if account, ok := accounts[addr]; ok {
		return account
	}
	account := &accountState{
		changes: &types.AccountChanges{Address: addr},
		balance: newBalanceTracker(),
		nonce:   newNonceTracker(),
		code:    newCodeTracker(),
	}
	accounts[addr] = account
	return account
}

func (account *accountState) applyWriteStorage(key accounts.StorageKey, val uint256.Int, accessIndex uint32) {
	// Skip a first storage write that restores the original read value — net-zero, so it stays a read, not a write.
	if !hasStorageWrite(account.changes, key) {
		if origVal, wasRead := account.storageReadValues[key]; wasRead && val.Eq(&origVal) {
			return
		}
	}
	addStorageUpdate(account.changes, key, val, accessIndex)
}

func (account *accountState) applyWriteNonce(val uint64, accessIndex uint32) {
	account.nonce.recordWrite(accessIndex, val)
}

func (account *accountState) applyWriteCode(val accounts.Code, accessIndex uint32) {
	account.code.recordWrite(accessIndex, val)
}

func (account *accountState) applyWriteBalance(val uint256.Int, accessIndex uint32) {
	{
		// A first zero-balance write is a no-op touch only if the pre-block balance
		// was also (implicitly) zero; if a prior read showed non-zero, a zero write
		// is a real depletion and must be recorded.
		if account.balanceValue == nil && val.IsZero() &&
			(account.initialBalanceValue == nil || account.initialBalanceValue.IsZero()) {
			if account.initialBalanceValue == nil {
				v := val
				account.initialBalanceValue = &v
			}
			account.setBalanceValue(val)
			return
		}
		if account.balanceValue != nil && val.Eq(account.balanceValue) {
			account.setBalanceValue(val)
			return
		}
		// A write matching the pre-block balance is a no-op ONLY if no intermediate
		// write was recorded — a later restore to the initial value must still be
		// recorded so parallel readers see it at that index.
		if account.initialBalanceValue != nil && val.Eq(account.initialBalanceValue) && len(account.balance.changes.entries) == 0 {
			account.setBalanceValue(val)
			return
		}
		account.setBalanceValue(val)
		account.balance.recordWrite(accessIndex, val)
	}
}

func (account *accountState) updateReadStorage(key accounts.StorageKey, val uint256.Int) {
	if account.storageReadValues == nil {
		account.storageReadValues = make(map[accounts.StorageKey]uint256.Int)
	}
	if _, exists := account.storageReadValues[key]; !exists {
		account.storageReadValues[key] = val
	}
	if hasStorageWrite(account.changes, key) {
		return
	}
	account.changes.StorageReads = append(account.changes.StorageReads, key)
}

func (account *accountState) updateReadBalance(val uint256.Int) {
	// A read after a write may reflect post-write state, so it must not become the pre-block baseline.
	if account.initialBalanceValue == nil && account.balanceValue == nil {
		v := val
		account.initialBalanceValue = &v
	}
	// Once a write exists, a later read must not override the tracked value.
	if len(account.balance.changes.entries) == 0 {
		account.setBalanceValue(val)
	}
}

func addStorageUpdate(ac *types.AccountChanges, slot accounts.StorageKey, val uint256.Int, txIndex uint32) {
	removeStorageRead(ac, slot)

	if ac.StorageChanges == nil {
		ac.StorageChanges = []*types.SlotChanges{{
			Slot:    slot,
			Changes: []*types.StorageChange{{Index: txIndex, Value: val}},
		}}
		return
	}

	for _, slotChange := range ac.StorageChanges {
		if slotChange.Slot == slot {
			if n := len(slotChange.Changes); n > 0 && val.Eq(&slotChange.Changes[n-1].Value) {
				return
			}
			slotChange.Changes = append(slotChange.Changes, &types.StorageChange{Index: txIndex, Value: val})
			return
		}
	}
	ac.StorageChanges = append(ac.StorageChanges, &types.SlotChanges{
		Slot:    slot,
		Changes: []*types.StorageChange{{Index: txIndex, Value: val}},
	})
}

func hasStorageWrite(ac *types.AccountChanges, slot accounts.StorageKey) bool {
	for _, sc := range ac.StorageChanges {
		if sc != nil && sc.Slot == slot {
			return true
		}
	}
	return false
}

func removeStorageRead(ac *types.AccountChanges, slot accounts.StorageKey) {
	if len(ac.StorageReads) == 0 {
		return
	}
	out := ac.StorageReads[:0]
	for _, s := range ac.StorageReads {
		if s != slot {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		ac.StorageReads = nil
	} else {
		ac.StorageReads = out
	}
}

type versionedReadSet struct {
	incarnation int
	readSet     ReadSet
}

func (s versionedReadSet) AllHeaders() iter.Seq[ReadHeader] {
	return func(yield func(ReadHeader) bool) {
		s.readSet.eachHeader(yield)
	}
}

func (s versionedReadSet) Merge(o versionedReadSet) versionedReadSet {
	if s.incarnation > o.incarnation {
		return versionedReadSet{
			incarnation: s.incarnation,
			readSet:     o.readSet.Merge(s.readSet),
		}
	}
	return versionedReadSet{
		incarnation: o.incarnation,
		readSet:     s.readSet.Merge(o.readSet),
	}
}

type DAG struct {
	*dag.DAG
}

type TxDep struct {
	Reads         ReadSet
	FullWriteList []*WriteSet
	Index         int
}

func HasReadDep(txFrom *WriteSet, txTo ReadSet) bool {
	if txFrom == nil {
		return false
	}
	for h := range txFrom.AllHeaders() {
		if _, ok := txTo.getHeader(h.Address, h.Path, h.Key); ok {
			return true
		}
	}
	return false
}

func BuildDAG(deps *VersionedIO, logger log.Logger) (d DAG) {
	d = DAG{dag.NewDAG()}
	ids := make(map[int]string)

	for i := len(deps.inputs) - 1; i > 0; i-- {
		txTo := deps.inputs[i]

		var txToId string

		if _, ok := ids[i]; ok {
			txToId = ids[i]
		} else {
			txToId, _ = d.AddVertex(i)
			ids[i] = txToId
		}

		for j := i - 1; j >= 0; j-- {
			txFrom := deps.outputs[j]

			if HasReadDep(txFrom, txTo.readSet) {
				var txFromId string
				if _, ok := ids[j]; ok {
					txFromId = ids[j]
				} else {
					txFromId, _ = d.AddVertex(j)
					ids[j] = txFromId
				}

				err := d.AddEdge(txFromId, txToId)
				if err != nil {
					logger.Warn("Failed to add edge", "from", txFromId, "to", txToId, "err", err)
				}
			}
		}
	}

	return
}

func depsHelper(dependencies map[int]map[int]bool, txFrom *WriteSet, txTo ReadSet, i int, j int) map[int]map[int]bool {
	if HasReadDep(txFrom, txTo) {
		dependencies[i][j] = true

		for k := range dependencies[i] {
			_, foundDep := dependencies[j][k]

			if foundDep {
				delete(dependencies[i], k)
			}
		}
	}

	return dependencies
}

func UpdateDeps(deps map[int]map[int]bool, t TxDep) map[int]map[int]bool {
	txTo := t.Reads

	deps[t.Index] = map[int]bool{}

	for j := 0; j <= t.Index-1; j++ {
		txFrom := t.FullWriteList[j]

		deps = depsHelper(deps, txFrom, txTo, t.Index, j)
	}

	return deps
}

func GetDep(deps *VersionedIO) map[int]map[int]bool {
	newDependencies := map[int]map[int]bool{}

	for i := 1; i < len(deps.inputs); i++ {
		txTo := deps.inputs[i]

		newDependencies[i] = map[int]bool{}

		for j := 0; j <= i-1; j++ {
			txFrom := deps.outputs[j]

			newDependencies = depsHelper(newDependencies, txFrom, txTo.readSet, i, j)
		}
	}

	return newDependencies
}

func (s *WriteSet) createdEmpty(addr accounts.Address) bool {
	created, ok := s.address[addr]
	if !ok || created.Val == nil {
		return false
	}
	account := *created.Val
	if written, ok := s.balance[addr]; ok {
		account.Balance = written.Val
	}
	if written, ok := s.nonce[addr]; ok {
		account.Nonce = written.Val
	}
	if written, ok := s.codeHash[addr]; ok {
		account.CodeHash = written.Val
	}
	if !account.Empty() {
		return false
	}
	_, hasCode := s.code[addr]
	_, hasIncarnation := s.incarnation[addr]
	_, destroyed := s.selfDestruct[addr]
	_, createdContract := s.createContract[addr]
	_, hasCodeSize := s.codeSize[addr]
	return !hasCode && !hasIncarnation && !destroyed && !createdContract && !hasCodeSize && len(s.storage[addr]) == 0
}
