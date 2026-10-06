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
	// ProvisionalRead marks a nil record probe made mid-account-load, before
	// the load has exposed any value to the EVM: a re-probe in the same load
	// that finds a freshly flushed cell adopts it instead of aborting.
	ProvisionalRead
)

// ReadHeader is the type-agnostic part of a versioned read: the tx-version
// that produced the value and the tier it came from.  Shared by every
// per-path read via embedding in VersionedRead.
type ReadHeader struct {
	Source   ReadSource
	Version  Version
	internal bool // conflict-detection only; excluded from the block access list
}

// VersionedRead is a single versioned read.  The per-path map it lives in fixes
// the address (map key) and path — and, for storage, the slot key — so the
// struct carries only the header and the path-typed value.
type VersionedRead[T any] struct {
	ReadHeader
	Val T
}

// AccountView abstracts the AddressPath read's account payload so the read
// can be backed by an already-materialised account today and, later, by a
// versionMap cell without changing consumers.  Scoped to the read path —
// deliberately not the package-wide accounts.Account.
type AccountView interface {
	Account() *accounts.Account
	IsNil() bool
}

// concreteAccountView is the AccountView backed by an already-materialised
// account.  It is pointer-shaped, so it costs no extra allocation when held
// in an AccountView interface value.
type concreteAccountView struct{ acc *accounts.Account }

func (c concreteAccountView) Account() *accounts.Account { return c.acc }
func (c concreteAccountView) IsNil() bool                { return c.acc == nil }

// NewAccountView wraps a materialised account as an AccountView.
func NewAccountView(acc *accounts.Account) AccountView { return concreteAccountView{acc} }

// ReadSet holds per-task versioned reads in per-path maps keyed by address
// (and by storage slot for the storage path).  Each read is held by value;
// the per-path split keeps the record path off the heap and collapses
// non-storage probes to a single map lookup.  All maps are lazily allocated
// on first write.
type ReadSet struct {
	address      map[accounts.Address]VersionedRead[AccountView]
	balance      map[accounts.Address]VersionedRead[uint256.Int]
	nonce        map[accounts.Address]VersionedRead[uint64]
	incarnation  map[accounts.Address]VersionedRead[uint64]
	selfDestruct map[accounts.Address]VersionedRead[bool]
	// selfDestructWitnesses holds the OTHER distinct destruct versions a tx
	// consumed when it recorded more than one (a wipe from each of several
	// destroy/recreate cycles of one address): validation must re-check every
	// destruct a conclusion depended on, not only the last-recorded one.
	selfDestructWitnesses map[accounts.Address][]VersionedRead[bool]
	createContract        map[accounts.Address]VersionedRead[bool]
	code                  map[accounts.Address]VersionedRead[accounts.Code]
	codeHash              map[accounts.Address]VersionedRead[accounts.CodeHash]
	codeSize              map[accounts.Address]VersionedRead[int]
	storage               map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int]

	// access carries EIP-7928 "address was accessed" marks (with the
	// non-revertable "real EVM access" bit) on the read side, so the access set
	// travels with the read-set rather than as a separate IntraBlockState cache.
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

func (s *ReadSet) SetCode(addr accounts.Address, tr VersionedRead[accounts.Code]) {
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

func (s *ReadSet) GetCode(addr accounts.Address) (VersionedRead[accounts.Code], bool) {
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

// getHeader probes the read set for path and returns the type-agnostic
// header only.  This is the single read-set probe versionedReadCore uses —
// uniform return type, so the runtime path switch works without the value
// type leaking into the probe.
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

// setHeader records a header-only read (zero value) at (addr, path, key).
// Used on the dependency-conflict paths where versionedReadCore records the
// read purely for ValidateVersion's version check — the value is never
// consulted there (it was zero in the legacy VersionedRead path too).
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
		s.SetCode(addr, VersionedRead[accounts.Code]{ReadHeader: hdr})
	case CodeHashPath:
		s.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{ReadHeader: hdr})
	case CodeSizePath:
		s.SetCodeSize(addr, VersionedRead[int]{ReadHeader: hdr})
	case StoragePath:
		s.SetStorage(addr, key, VersionedRead[uint256.Int]{ReadHeader: hdr})
	}
}

// scanAddrPath visits the addr entry of one non-storage path map, writing
// any header mutation back.  Helper for ScanAddr.
func scanAddrPath[T any](m map[accounts.Address]VersionedRead[T], addr accounts.Address, path AccountPath, fn func(AccountPath, accounts.StorageKey, *ReadHeader)) int {
	if tr, ok := m[addr]; ok {
		fn(path, accounts.NilKey, &tr.ReadHeader)
		m[addr] = tr
		return 1
	}
	return 0
}

// ScanAddr visits every entry under addr (all paths), passing the path, key
// and a pointer to the mutable header.  Any header mutation is written back
// into the map.  Returns the number of entries visited.
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

// hasAddr reports whether any path has an entry for addr.
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

// Delete removes every path entry for addr.
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

// Len returns the total entry count across all paths.  Value receiver so it
// can be called on a function-returned read set directly.
func (s ReadSet) Len() int {
	n := len(s.address) + len(s.balance) + len(s.nonce) + len(s.incarnation) +
		len(s.selfDestruct) + len(s.createContract) +
		len(s.code) + len(s.codeHash) + len(s.codeSize)
	for _, inner := range s.storage {
		n += len(inner)
	}
	return n
}

// mergeFrom copies every entry of src into s, overwriting on collision.
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

// Merge returns a new read set containing every entry of s then o.  On a
// collision o's entry wins.
func (s ReadSet) Merge(o ReadSet) ReadSet {
	var out ReadSet
	out.mergeFrom(s)
	out.mergeFrom(o)
	return out
}

// MergeFrom merges o into s in place, without allocating a new ReadSet.
func (s *ReadSet) MergeFrom(o ReadSet) {
	s.mergeFrom(o)
}

// TraceReads prints every read in path-major order, prefixed.  Debug only —
// keeps the per-path iteration inside this package.
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

// eachHeader visits the type-agnostic header of every read; the callback
// returns false to stop early.
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

// WriteHeader is the type-agnostic part of a versioned write: address,
// path, optional storage key, tx-version and balance-change reason.
// Shared across every per-path write via embedding in VersionedWrite[T].
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

// VersionedWrite is a single versioned write.  The per-path map it lives
// in fixes the address (map key), path — and, for storage, the slot key.
// The struct carries the header and the path-typed value.
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

// Per-path *VersionedWrite[T] pools.  Each pool sources fresh cleared
// instances; callers fill the WriteHeader + Val before inserting into the
// typed map on WriteSet.  Lifetime is per-tx — WriteSet.ReleaseAndReset
// walks the maps at tx-finalize and returns every VW to its pool.
//
// Slice-valued payloads (CodePath) have their Val cleared on release to
// avoid pinning external memory; same pattern as versionmap.go's WriteCell
// pool (releaseCellCode).
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

// WriteSet is the cell-pipeline target shape for versionedWrites.
// Symmetric with ReadSet — see that type for rationale.
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

	// released marks a ReleaseMaps that was not a reset for reuse: the pooled
	// maps leave the set reading as empty, so under assertions readers panic
	// instead.
	released bool
}

// Released reports whether ReleaseMaps pooled this set's maps and no later
// write revived it.
func (ws *WriteSet) Released() bool { return ws != nil && ws.released }

// The panic must stay inline with a constant message: an out-of-line helper
// costs a call, and IsEmpty then exceeds its inline budget.
func (ws *WriteSet) assertLive() {
	if dbg.AssertEnabled && ws != nil && ws.released {
		panic("WriteSet read after ReleaseMaps: the maps are pooled, so this read silently sees nothing")
	}
}

// vwMapPool[T] pools the per-path map[Address]*VersionedWrite[T] containers
// themselves, not just the *VersionedWrite[T] values (those are in vwPool*).
// put() walks no entries — by contract, callers release every VW to its
// vwPool before put() — but it does call clear() so the map header survives
// with cleared buckets ready for the next checkout.  Pooling the container
// preserves bucket capacity across txs; clear() is precisely the behaviour
// pool-resident containers want.
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

// writeSetPut lazily checks out a pooled map and inserts vw at addr.
// First write per tx pays vwMapPool.Get (cheap); subsequent writes are
// direct map insert.  ReleaseAndReset puts the map back on tx-finalize.
func writeSetPut[T any](s *WriteSet, m *map[accounts.Address]*VersionedWrite[T], addr accounts.Address, vw *VersionedWrite[T], pool *vwMapPool[T]) {
	if *m == nil {
		*m = pool.get()
		s.revive()
	}
	(*m)[addr] = vw
}

func (ws *WriteSet) revive() {
	ws.released = false
}

func (ws *WriteSet) SetAddress(addr accounts.Address, vw *VersionedWrite[*accounts.Account]) {
	writeSetPut(ws, &ws.address, addr, vw, wsMapPoolAddress)
}

func (ws *WriteSet) SetBalance(addr accounts.Address, vw *VersionedWrite[uint256.Int]) {
	writeSetPut(ws, &ws.balance, addr, vw, wsMapPoolBalance)
}

func (ws *WriteSet) SetNonce(addr accounts.Address, vw *VersionedWrite[uint64]) {
	writeSetPut(ws, &ws.nonce, addr, vw, wsMapPoolNonce)
}

func (ws *WriteSet) SetIncarnation(addr accounts.Address, vw *VersionedWrite[uint64]) {
	writeSetPut(ws, &ws.incarnation, addr, vw, wsMapPoolIncarnation)
}

func (ws *WriteSet) SetSelfDestruct(addr accounts.Address, vw *VersionedWrite[bool]) {
	writeSetPut(ws, &ws.selfDestruct, addr, vw, wsMapPoolSelfDestruct)
}

func (ws *WriteSet) SetCreateContract(addr accounts.Address, vw *VersionedWrite[bool]) {
	writeSetPut(ws, &ws.createContract, addr, vw, wsMapPoolCreateContract)
}

func (ws *WriteSet) SetCode(addr accounts.Address, vw *VersionedWrite[accounts.Code]) {
	writeSetPut(ws, &ws.code, addr, vw, wsMapPoolCode)
}

func (ws *WriteSet) SetCodeHash(addr accounts.Address, vw *VersionedWrite[accounts.CodeHash]) {
	writeSetPut(ws, &ws.codeHash, addr, vw, wsMapPoolCodeHash)
}

func (ws *WriteSet) SetCodeSize(addr accounts.Address, vw *VersionedWrite[int]) {
	writeSetPut(ws, &ws.codeSize, addr, vw, wsMapPoolCodeSize)
}

func (ws *WriteSet) SetStorage(addr accounts.Address, key accounts.StorageKey, vw *VersionedWrite[uint256.Int]) {
	if ws.storage == nil {
		ws.storage = wsGetStorageOuter()
		ws.revive()
	}
	inner := ws.storage[addr]
	if inner == nil {
		inner = wsGetStorageInner()
		ws.storage[addr] = inner
	}
	inner[key] = vw
}

func (ws *WriteSet) IsEmpty() bool {
	if ws == nil {
		return true
	}
	ws.assertLive()
	return len(ws.address) == 0 && len(ws.balance) == 0 && len(ws.nonce) == 0 &&
		len(ws.incarnation) == 0 && len(ws.selfDestruct) == 0 && len(ws.createContract) == 0 &&
		len(ws.code) == 0 && len(ws.codeHash) == 0 && len(ws.codeSize) == 0 && len(ws.storage) == 0
}

// Has reports whether a write exists at h's (Address, Path, Key).
func (ws *WriteSet) Has(h WriteHeader) bool {
	return ws.hasHeader(h)
}

// Filter returns a new WriteSet holding only the writes whose header satisfies
// keep. The kept writes are shared (not cloned) with the receiver.
func (ws *WriteSet) Filter(keep func(WriteHeader) bool) *WriteSet {
	if ws == nil {
		return nil
	}
	ws.assertLive()
	out := &WriteSet{}
	for a, vw := range ws.address {
		if keep(vw.WriteHeader) {
			out.SetAddress(a, vw)
		}
	}
	for a, vw := range ws.balance {
		if keep(vw.WriteHeader) {
			out.SetBalance(a, vw)
		}
	}
	for a, vw := range ws.nonce {
		if keep(vw.WriteHeader) {
			out.SetNonce(a, vw)
		}
	}
	for a, vw := range ws.incarnation {
		if keep(vw.WriteHeader) {
			out.SetIncarnation(a, vw)
		}
	}
	for a, vw := range ws.selfDestruct {
		if keep(vw.WriteHeader) {
			out.SetSelfDestruct(a, vw)
		}
	}
	for a, vw := range ws.createContract {
		if keep(vw.WriteHeader) {
			out.SetCreateContract(a, vw)
		}
	}
	for a, vw := range ws.code {
		if keep(vw.WriteHeader) {
			out.SetCode(a, vw)
		}
	}
	for a, vw := range ws.codeHash {
		if keep(vw.WriteHeader) {
			out.SetCodeHash(a, vw)
		}
	}
	for a, vw := range ws.codeSize {
		if keep(vw.WriteHeader) {
			out.SetCodeSize(a, vw)
		}
	}
	for a, inner := range ws.storage {
		for k, vw := range inner {
			if keep(vw.WriteHeader) {
				out.SetStorage(a, k, vw)
			}
		}
	}
	return out
}

// Finalize produces the committable write-set for a tx from the raw recorded
// writes: it applies the EIP-6780 net-zero storage wipe, then returns the
// filtered Snapshot. It operates on the write-set alone — no IntraBlockState —
// so the parallel finalize can be driven straight from the recorded IO.
func (ws *WriteSet) Finalize() *WriteSet {
	ws.zeroSameTxCreateDestructStorage()
	return ws.Snapshot()
}

// zeroSameTxCreateDestructStorage implements EIP-6780 + EIP-7928: when a
// contract is created and self-destructed in the same tx, its storage is wiped
// at end-of-tx, so the BAL must record the dirty slots as reads (net-zero), not
// changes. Zero the storage write values so AsBlockAccessList folds them away.
// The create+destruct pair is detectable from the write-set itself
// (createContract survives SELFDESTRUCT), so no stateObject is needed.
func (ws *WriteSet) zeroSameTxCreateDestructStorage() {
	for addr, cc := range ws.createContract {
		if !cc.Val {
			continue
		}
		if sd, ok := ws.selfDestruct[addr]; !ok || !sd.Val {
			continue
		}
		if inner, ok := ws.storage[addr]; ok {
			for _, vw := range inner {
				vw.Val = uint256.Int{}
			}
		}
	}
}

// Snapshot returns a frozen typed copy of the recorded writes. The journal keeps
// the write-set in step with reverts, so every address present is a surviving
// write — no dirty reconciliation is needed. Under self-destruct only
// SelfDestruct/Balance/Incarnation/Storage/CreateContract are kept (the BAL needs
// residual balance, resurrection needs the prior incarnation, the calculator
// needs per-slot deletes, and fee finalization needs the creation marker);
// Nonce/Code/CodeHash/CodeSize/Address drop.
func (ws *WriteSet) Snapshot() *WriteSet {
	out := &WriteSet{}

	addrs := make(map[accounts.Address]struct{})
	for h := range ws.AllHeaders() {
		addrs[h.Address] = struct{}{}
	}

	for addr := range addrs {
		sd := false
		if vw, ok := ws.selfDestruct[addr]; ok {
			out.SetSelfDestruct(addr, cloneVW(vw))
			sd = vw.Val
		}
		if vw, ok := ws.balance[addr]; ok {
			out.SetBalance(addr, cloneVW(vw))
		}
		if vw, ok := ws.incarnation[addr]; ok {
			out.SetIncarnation(addr, cloneVW(vw))
		}
		if inner, ok := ws.storage[addr]; ok {
			for k, vw := range inner {
				out.SetStorage(addr, k, cloneVW(vw))
			}
		}
		// CreateContract survives self-destruct: fee finalization and the BAL need
		// the creation marker, and zeroSameTxCreateDestructStorage detects the
		// create+destruct pair from it. ApplyVersionedWrites skips applying it under
		// self-destruct, so keeping it here does not resurrect the account.
		if vw, ok := ws.createContract[addr]; ok {
			out.SetCreateContract(addr, cloneVW(vw))
		}
		if !sd {
			if vw, ok := ws.address[addr]; ok {
				out.SetAddress(addr, cloneVW(vw))
			}
			if vw, ok := ws.nonce[addr]; ok {
				out.SetNonce(addr, cloneVW(vw))
			}
			if vw, ok := ws.code[addr]; ok {
				out.SetCode(addr, cloneVW(vw))
			}
			if vw, ok := ws.codeHash[addr]; ok {
				out.SetCodeHash(addr, cloneVW(vw))
			}
			if vw, ok := ws.codeSize[addr]; ok {
				out.SetCodeSize(addr, cloneVW(vw))
			}
		}
	}
	return out
}

func (ws *WriteSet) deleteAddr(addr accounts.Address) {
	delete(ws.address, addr)
	delete(ws.balance, addr)
	delete(ws.nonce, addr)
	delete(ws.incarnation, addr)
	delete(ws.selfDestruct, addr)
	delete(ws.createContract, addr)
	delete(ws.code, addr)
	delete(ws.codeHash, addr)
	delete(ws.codeSize, addr)
	delete(ws.storage, addr)
}

// createWriteSnapshot holds a deep copy of the account-record writes that
// createObject/createAccount overwrite when (re)creating an account. It lets a
// reverted recreation restore the prior writes rather than leave them
// overwritten — the record-level create journal entry maintaining its own
// field-level writes.
type createWriteSnapshot struct {
	address        *VersionedWrite[*accounts.Account]
	balance        *VersionedWrite[uint256.Int]
	incarnation    *VersionedWrite[uint64]
	selfDestruct   *VersionedWrite[bool]
	createContract *VersionedWrite[bool]
	codeHash       *VersionedWrite[accounts.CodeHash]
}

// snapshotCreateFields clones the account-record writes that a subsequent
// createObject/createAccount will overwrite, so a reverted recreation can
// restore them. Fields creation never writes (nonce/code/codeSize/storage) are
// not captured — they survive the recreation untouched.
func (ws *WriteSet) snapshotCreateFields(addr accounts.Address) *createWriteSnapshot {
	snap := &createWriteSnapshot{}
	if vw, ok := ws.address[addr]; ok {
		snap.address = cloneVW(vw)
	}
	if vw, ok := ws.balance[addr]; ok {
		snap.balance = cloneVW(vw)
	}
	if vw, ok := ws.incarnation[addr]; ok {
		snap.incarnation = cloneVW(vw)
	}
	if vw, ok := ws.selfDestruct[addr]; ok {
		snap.selfDestruct = cloneVW(vw)
	}
	if vw, ok := ws.createContract[addr]; ok {
		snap.createContract = cloneVW(vw)
	}
	if vw, ok := ws.codeHash[addr]; ok {
		snap.codeHash = cloneVW(vw)
	}
	return snap
}

// restoreCreateFields reverts the account-record writes overwritten by a
// recreation back to snap (nil entries in snap become deletions). Only the
// fields creation writes are touched.
func (ws *WriteSet) restoreCreateFields(addr accounts.Address, snap *createWriteSnapshot) {
	delete(ws.address, addr)
	delete(ws.balance, addr)
	delete(ws.incarnation, addr)
	delete(ws.selfDestruct, addr)
	delete(ws.createContract, addr)
	delete(ws.codeHash, addr)
	if snap == nil {
		return
	}
	if snap.address != nil {
		ws.SetAddress(addr, snap.address)
	}
	if snap.balance != nil {
		ws.SetBalance(addr, snap.balance)
	}
	if snap.incarnation != nil {
		ws.SetIncarnation(addr, snap.incarnation)
	}
	if snap.selfDestruct != nil {
		ws.SetSelfDestruct(addr, snap.selfDestruct)
	}
	if snap.createContract != nil {
		ws.SetCreateContract(addr, snap.createContract)
	}
	if snap.codeHash != nil {
		ws.SetCodeHash(addr, snap.codeHash)
	}
}

// assertSelfDestructNormalized panics if a self-destructed address still carries
// the account fields Normalize is required to drop. Any of them makes Apply
// compute pureDelete=false and take the cleanup-before-recreate branch, which
// writes the account back with a live incarnation instead of deleting it — a
// phantom account that breaks a later CREATE2 at the same address. Balance
// (retained under EIP-8246) and the storage-delete cascade are legal.
func (ws *WriteSet) assertSelfDestructNormalized() {
	for addr, sdw := range ws.selfDestruct {
		if !sdw.Val {
			continue
		}
		var field string
		switch {
		case ws.nonce[addr] != nil:
			field = "nonce"
		case ws.incarnation[addr] != nil:
			field = "incarnation"
		case ws.codeHash[addr] != nil:
			field = "codeHash"
		default:
			continue
		}
		panic(fmt.Sprintf("write set not normalized: self-destructed %x keeps its %s write", addr.Value(), field))
	}
}

// DeleteAccountFields removes the Balance/Nonce/Incarnation/CodeHash writes for
// addr, leaving storage/code/self-destruct intact.
func (ws *WriteSet) DeleteAccountFields(addr accounts.Address) {
	delete(ws.balance, addr)
	delete(ws.nonce, addr)
	delete(ws.incarnation, addr)
	delete(ws.codeHash, addr)
}

// Count is the total number of writes across all paths.
func (ws *WriteSet) Count() int {
	if ws == nil {
		return 0
	}
	ws.assertLive()
	n := len(ws.address) + len(ws.balance) + len(ws.nonce) + len(ws.incarnation) +
		len(ws.selfDestruct) + len(ws.createContract) + len(ws.code) + len(ws.codeHash) + len(ws.codeSize)
	for _, inner := range ws.storage {
		n += len(inner)
	}
	return n
}

func (ws *WriteSet) GetAddress(addr accounts.Address) (*VersionedWrite[*accounts.Account], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.address[addr]
	return vw, ok
}

func (ws *WriteSet) GetBalance(addr accounts.Address) (*VersionedWrite[uint256.Int], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.balance[addr]
	return vw, ok
}

func (ws *WriteSet) GetNonce(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.nonce[addr]
	return vw, ok
}

func (ws *WriteSet) GetIncarnation(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.incarnation[addr]
	return vw, ok
}

func (ws *WriteSet) GetSelfDestruct(addr accounts.Address) (*VersionedWrite[bool], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.selfDestruct[addr]
	return vw, ok
}

func (ws *WriteSet) GetCreateContract(addr accounts.Address) (*VersionedWrite[bool], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.createContract[addr]
	return vw, ok
}

func (ws *WriteSet) GetCode(addr accounts.Address) (*VersionedWrite[accounts.Code], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.code[addr]
	return vw, ok
}

func (ws *WriteSet) GetCodeHash(addr accounts.Address) (*VersionedWrite[accounts.CodeHash], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.codeHash[addr]
	return vw, ok
}

func (ws *WriteSet) GetCodeSize(addr accounts.Address) (*VersionedWrite[int], bool) {
	if ws == nil {
		return nil, false
	}
	vw, ok := ws.codeSize[addr]
	return vw, ok
}

func (ws *WriteSet) GetStorage(addr accounts.Address, key accounts.StorageKey) (*VersionedWrite[uint256.Int], bool) {
	if ws == nil {
		return nil, false
	}
	inner := ws.storage[addr]
	if inner == nil {
		return nil, false
	}
	vw, ok := inner[key]
	return vw, ok
}

// forEachAddr calls f for every address with at least one entry, allocating
// nothing. An address present in several paths is visited once per path, so
// callers must tolerate repeats (use addrs() when a deduped set is required).
func (ws *WriteSet) forEachAddr(f func(accounts.Address)) {
	if ws == nil {
		return
	}
	ws.assertLive()
	for a := range ws.address {
		f(a)
	}
	ws.forEachFieldAddr(f)
}

// forEachFieldAddr is forEachAddr restricted to field-level writes: it skips the
// record-level AddressPath map, whose entries carry no field of their own.
func (ws *WriteSet) forEachFieldAddr(f func(accounts.Address)) {
	if ws == nil {
		return
	}
	ws.assertLive()
	for a := range ws.balance {
		f(a)
	}
	for a := range ws.nonce {
		f(a)
	}
	for a := range ws.incarnation {
		f(a)
	}
	for a := range ws.selfDestruct {
		f(a)
	}
	for a := range ws.createContract {
		f(a)
	}
	for a := range ws.code {
		f(a)
	}
	for a := range ws.codeHash {
		f(a)
	}
	for a := range ws.codeSize {
		f(a)
	}
	for a := range ws.storage {
		f(a)
	}
}

// addrs returns the union of all addresses that have at least one entry.
// Iteration order across the union is non-deterministic — caller sorts
// before use if determinism matters.
func (ws *WriteSet) addrs() map[accounts.Address]struct{} {
	out := map[accounts.Address]struct{}{}
	for a := range ws.address {
		out[a] = struct{}{}
	}
	for a := range ws.balance {
		out[a] = struct{}{}
	}
	for a := range ws.nonce {
		out[a] = struct{}{}
	}
	for a := range ws.incarnation {
		out[a] = struct{}{}
	}
	for a := range ws.selfDestruct {
		out[a] = struct{}{}
	}
	for a := range ws.createContract {
		out[a] = struct{}{}
	}
	for a := range ws.code {
		out[a] = struct{}{}
	}
	for a := range ws.codeHash {
		out[a] = struct{}{}
	}
	for a := range ws.codeSize {
		out[a] = struct{}{}
	}
	for a := range ws.storage {
		out[a] = struct{}{}
	}
	return out
}

// Per-path typed iterators over the write collections. Consumers that depend on
// the self-destruct-vs-field priority iterate these in explicit order (e.g.
// SelfDestructs before the reviving field writes) rather than relying on a flat
// stream's element order.
func (ws *WriteSet) Addresses() iter.Seq2[accounts.Address, *VersionedWrite[*accounts.Account]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]*VersionedWrite[*accounts.Account] { return s.address })
}

func (ws *WriteSet) Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]*VersionedWrite[uint256.Int] { return s.balance })
}

func (ws *WriteSet) Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]*VersionedWrite[uint64] { return s.nonce })
}

func (ws *WriteSet) Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]*VersionedWrite[uint64] { return s.incarnation })
}

func (ws *WriteSet) SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]*VersionedWrite[bool] { return s.selfDestruct })
}

func (ws *WriteSet) Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]*VersionedWrite[accounts.Code] { return s.code })
}

func (ws *WriteSet) CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]*VersionedWrite[accounts.CodeHash] { return s.codeHash })
}

func (ws *WriteSet) Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return writeSetSeq(ws, func(s *WriteSet) map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int] {
		return s.storage
	})
}

// writeSetSeq defers both the liveness check and the map read to the moment the
// sequence runs. A sequence built before ReleaseMaps would otherwise walk the
// pooled maps and silently see nothing.
func writeSetSeq[T any](s *WriteSet, pick func(*WriteSet) map[accounts.Address]T) iter.Seq2[accounts.Address, T] {
	return func(yield func(accounts.Address, T) bool) {
		if s == nil {
			return
		}
		s.assertLive()
		for addr, v := range pick(s) {
			if !yield(addr, v) {
				return
			}
		}
	}
}

func eachWriteHeaderOf[T any](m map[accounts.Address]*VersionedWrite[T], yield func(WriteHeader) bool) bool {
	if len(m) == 0 {
		return true
	}
	for _, vw := range m {
		if !yield(vw.WriteHeader) {
			return false
		}
	}
	return true
}

// AllHeaders visits the type-agnostic header of every write; the value is read
// typed-by-path from the per-path maps, so nothing is erased to an interface.
func (ws *WriteSet) AllHeaders() iter.Seq[WriteHeader] {
	return func(yield func(WriteHeader) bool) {
		if ws == nil {
			return
		}
		ws.assertLive()
		if !eachWriteHeaderOf(ws.address, yield) ||
			!eachWriteHeaderOf(ws.balance, yield) ||
			!eachWriteHeaderOf(ws.nonce, yield) ||
			!eachWriteHeaderOf(ws.incarnation, yield) ||
			!eachWriteHeaderOf(ws.selfDestruct, yield) ||
			!eachWriteHeaderOf(ws.createContract, yield) ||
			!eachWriteHeaderOf(ws.code, yield) ||
			!eachWriteHeaderOf(ws.codeHash, yield) ||
			!eachWriteHeaderOf(ws.codeSize, yield) {
			return
		}
		for _, inner := range ws.storage {
			for _, vw := range inner {
				if !yield(vw.WriteHeader) {
					return
				}
			}
		}
	}
}

// ReleaseAndReset returns every *VersionedWrite[T] held by the set to its
// typed sync.Pool, then returns the per-path maps to their map-pools.
// Called at tx-finalize so both the VW values and the map buckets cycle
// through pools rather than getting GC'd. The values must go back before
// ReleaseMaps clears the maps that hold them.
func (ws *WriteSet) ReleaseAndReset() {
	for _, vw := range ws.address {
		releaseVWAddress(vw)
	}
	for _, vw := range ws.balance {
		releaseVWBalance(vw)
	}
	for _, vw := range ws.nonce {
		releaseVWNonce(vw)
	}
	for _, vw := range ws.incarnation {
		releaseVWIncarnation(vw)
	}
	for _, vw := range ws.selfDestruct {
		releaseVWSelfDestruct(vw)
	}
	for _, vw := range ws.createContract {
		releaseVWCreateContract(vw)
	}
	for _, vw := range ws.code {
		releaseVWCode(vw)
	}
	for _, vw := range ws.codeHash {
		releaseVWCodeHash(vw)
	}
	for _, vw := range ws.codeSize {
		releaseVWCodeSize(vw)
	}
	for _, inner := range ws.storage {
		for _, vw := range inner {
			releaseVWStorage(vw)
		}
	}
	ws.ReleaseMaps()
	ws.revive() // a reset hands the set back for reuse
}

// ReleaseMaps returns the map containers to their pools without releasing the
// VersionedWrite values, which stay owned by GC — they may be shared with other
// sets after MergeInto. Use ReleaseAndReset only for sets whose VWs came from
// the vwPools and are exclusively owned.
func (ws *WriteSet) ReleaseMaps() {
	if ws == nil {
		return
	}
	wsMapPoolAddress.put(ws.address)
	wsMapPoolBalance.put(ws.balance)
	wsMapPoolNonce.put(ws.nonce)
	wsMapPoolIncarnation.put(ws.incarnation)
	wsMapPoolSelfDestruct.put(ws.selfDestruct)
	wsMapPoolCreateContract.put(ws.createContract)
	wsMapPoolCode.put(ws.code)
	wsMapPoolCodeHash.put(ws.codeHash)
	wsMapPoolCodeSize.put(ws.codeSize)
	for _, inner := range ws.storage {
		wsPutStorageInner(inner)
	}
	wsPutStorageOuter(ws.storage)
	*ws = WriteSet{}
	ws.released = true
}

// Per-path typed delete methods.  Direct map access, no internal switch
// (mirrors the Set/Get/update* shape).  Each Del* releases the displaced
// *VersionedWrite[T] back to its pool — keeps the pool cycle closed so
// allocs land on Get and end at Del/ReleaseAndReset.

func (ws *WriteSet) DelBalance(addr accounts.Address) {
	if vw, ok := ws.balance[addr]; ok {
		releaseVWBalance(vw)
		delete(ws.balance, addr)
	}
}

func (ws *WriteSet) DelNonce(addr accounts.Address) {
	if vw, ok := ws.nonce[addr]; ok {
		releaseVWNonce(vw)
		delete(ws.nonce, addr)
	}
}

func (ws *WriteSet) DelIncarnation(addr accounts.Address) {
	if vw, ok := ws.incarnation[addr]; ok {
		releaseVWIncarnation(vw)
		delete(ws.incarnation, addr)
	}
}

func (ws *WriteSet) DelSelfDestruct(addr accounts.Address) {
	if vw, ok := ws.selfDestruct[addr]; ok {
		releaseVWSelfDestruct(vw)
		delete(ws.selfDestruct, addr)
	}
}

func (ws *WriteSet) DelCode(addr accounts.Address) {
	if vw, ok := ws.code[addr]; ok {
		releaseVWCode(vw)
		delete(ws.code, addr)
	}
}

func (ws *WriteSet) DelCodeHash(addr accounts.Address) {
	if vw, ok := ws.codeHash[addr]; ok {
		releaseVWCodeHash(vw)
		delete(ws.codeHash, addr)
	}
}

func (ws *WriteSet) DelCodeSize(addr accounts.Address) {
	if vw, ok := ws.codeSize[addr]; ok {
		releaseVWCodeSize(vw)
		delete(ws.codeSize, addr)
	}
}

func (ws *WriteSet) DelStorage(addr accounts.Address, key accounts.StorageKey) {
	if inner := ws.storage[addr]; inner != nil {
		if vw, ok := inner[key]; ok {
			releaseVWStorage(vw)
			delete(inner, key)
		}
		if len(inner) == 0 {
			delete(ws.storage, addr)
		}
	}
}

// updateBalance, updateNonce etc. are typed in-place mutations on the
// existing per-path entry.  No-op when no entry exists for addr.
func (ws *WriteSet) updateBalance(addr accounts.Address, val uint256.Int) {
	if vw, ok := ws.balance[addr]; ok {
		vw.Val = val
	}
}

func (ws *WriteSet) updateStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	if inner := ws.storage[addr]; inner != nil {
		if vw, ok := inner[key]; ok {
			vw.Val = val
		}
	}
}

func (ws *WriteSet) updateNonce(addr accounts.Address, val uint64) {
	if vw, ok := ws.nonce[addr]; ok {
		vw.Val = val
	}
}

func (ws *WriteSet) updateIncarnation(addr accounts.Address, val uint64) {
	if vw, ok := ws.incarnation[addr]; ok {
		vw.Val = val
	}
}

func (ws *WriteSet) updateSelfDestruct(addr accounts.Address, val bool) {
	if vw, ok := ws.selfDestruct[addr]; ok {
		vw.Val = val
	}
}

func (ws *WriteSet) updateCode(addr accounts.Address, val accounts.Code) {
	if vw, ok := ws.code[addr]; ok {
		vw.Val = val
	}
}

func (ws *WriteSet) updateCodeHash(addr accounts.Address, val accounts.CodeHash) {
	if vw, ok := ws.codeHash[addr]; ok {
		vw.Val = val
	}
}

func (ws *WriteSet) updateCodeSize(addr accounts.Address, val int) {
	if vw, ok := ws.codeSize[addr]; ok {
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
	if vr.stateReader != nil {
		vr.stateReader.SetTrace(trace, tracePrefix)
	}
}

func (vr *versionedStateReader) Trace() bool {
	return vr.stateReader != nil && vr.stateReader.Trace()
}

func (vr *versionedStateReader) TracePrefix() string {
	if vr.stateReader == nil {
		return ""
	}
	return vr.stateReader.TracePrefix()
}

func (vr *versionedStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	r, recorded := vr.reads.GetAddress(address)
	if recorded && r.Val != nil && !r.Val.IsNil() {
		account := r.Val.Account()
		updated := vr.applyVersionedUpdates(address, *account)
		return &updated, nil
	}

	// Check version map for AddressPath — handles accounts created by
	// prior transactions in the same block that aren't in the read set.
	if vr.versionMap != nil {
		// A prior tx may have self-destructed this account. Honor the
		// destruct ONLY if no subsequent write at a strictly higher
		// TxIndex re-creates the account. EIP-161 emits a SelfDestruct
		// for the coinbase when a TX touches it without tipping (Balance
		// stays 0 → empty-account prune); a later TX that tips the same
		// coinbase re-creates it, and we must surface the re-created
		// account so finalize accumulates the prior cumulative value.
		if destroyed, _, revived := vr.versionMap.AccountLifecycle(address, vr.txIndex); destroyed && !revived {
			return nil, nil
		}
		if acc, ok := versionedUpdateAddress(vr.versionMap, address, vr.txIndex); ok && acc != nil {
			updated := vr.applyVersionedUpdates(address, *acc)
			return &updated, nil
		}
	}

	// A recorded AddressPath read with no account is the tx's own conclusion that
	// the address holds nothing; the domain cannot say otherwise.
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

	// Account doesn't exist in state reader and no AddressPath entry in
	// versionMap. The BAL pre-population writes only BalancePath/NoncePath/
	// CodePath/StoragePath entries (NOT AddressPath, by design — see
	// validateRead invariant). For an account that is created mid-block
	// purely by tip credits (e.g. fee_recipient with no pre-state and no
	// transfers to it), the BAL has the post-tx balance at each TxIndex,
	// but ReadAccountData would return nil because it only checks
	// AddressPath / stateReader. Synthesize an empty account and let
	// applyVersionedUpdates apply the BAL-preloaded fields.
	if vr.versionMap != nil {
		// A cell whose value leaves the record EIP-161-empty is no evidence the
		// account exists: BAL pre-population writes zero balances and nonces
		// value-blind, and a wipe only clears a field to the default a record
		// with no cells already holds.
		var synth accounts.Account
		vr.versionMap.applySubFieldWrites(address, vr.txIndex, &synth)
		if synth.Nonce != 0 || !synth.Balance.IsZero() || !synth.CodeHash.IsEmpty() {
			return &synth, nil
		}
	}

	return nil, nil
}

// Per-path versionedUpdate helpers replace the legacy generic
// versionedUpdate[T] — they consume the typed VersionMap Read primitives
// directly so neither the call site nor the read path crosses an any
// boundary. Each returns the cell value if a Done OR Dependency (Estimate)
// write exists at txIndex (a Dependency cell holds the same latest in-block
// write a Done cell does; finalize reconstruction must consume it, not fall
// back to the pre-block DB value), otherwise zero value and ok=false.

func versionedUpdateAddress(vm *VersionMap, addr accounts.Address, txIndex int) (*accounts.Account, bool) {
	val, res, ok := vm.ReadAddress(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return nil, false
}

// applyVersionedUpdates overlays the version map's per-field writes onto account.
// The reader/writer API treats an account's sub-fields as one group, so a prior
// tx that wrote only a subset would otherwise be lost here — and those fields are
// not recorded as reads, so validation would not catch the miss either.
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
		if account == nil {
			return nil, nil
		}

		updated := vr.applyVersionedUpdates(address, *account)
		return &updated, nil
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	// A non-zero recorded read is the tx's own conclusion. A zero one may instead
	// be the zero recordWipedRead left behind, which is an absent slot rather than
	// one holding zero, so the map settles it.
	recorded, hasRecorded := vr.reads.GetStorage(address, key)
	if hasRecorded && !recorded.Val.IsZero() {
		return recorded.Val, true, nil
	}

	val, found, wiped := vr.versionMap.readStorageLive(address, key, vr.txIndex)
	if found {
		return val, true, nil
	}
	if wiped {
		return uint256.Int{}, false, nil
	}
	if hasRecorded {
		return recorded.Val, true, nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountStorage(address, key)
	}

	return uint256.Int{}, false, nil
}

// versionedCode resolves code from the read set and the version map, reporting
// whether either answered.
func (vr versionedStateReader) versionedCode(address accounts.Address) ([]byte, bool) {
	if r, ok := vr.reads.GetCode(address); ok && r.Val.Bytes != nil {
		return r.Val.Bytes, true
	}
	code, found, wiped := vr.versionMap.readCodeLive(address, vr.txIndex)
	if found {
		return code.Bytes, true
	}
	return nil, wiped
}

func (vr versionedStateReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	if code, ok := vr.versionedCode(address); ok {
		return code, nil
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCode(address)
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	if code, ok := vr.versionedCode(address); ok {
		return len(code), nil
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

// SetAccountFieldFromMap resolves an account field from the version map and
// sets it into out, returning whether a Done value was found.
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

// NewAccountFieldZeroWrite returns a typed *VersionedWrite[T] for path
// holding the zero value (used by the SD-earlier fallback that emits
// post-destruction defaults).  Path must be Balance/Nonce/Incarnation/CodeHash.
// SetAccountFieldZero sets the post-destruction zero value for path into out.
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

// NewAccountFieldWriteFromAccount returns a typed *VersionedWrite[T]
// populated from acc (may be nil — falls back to zero values except for
// CodeHash which becomes EmptyCodeHash).
// SetAccountFieldFromAccount sets path's value from acc into out.
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

// TouchUpdates feeds the write set directly to a commitment.Updates buffer
// via TouchPlainKeyDirect, one partial Update per write. The buffer merges
// per key (ModeUpdate and ModeParallel both accumulate flags additively).
// An address whose writes do not cover balance, nonce and code hash together
// costs one account read at fold time, where the trie fills in the rest.
func (ws *WriteSet) TouchUpdates(updates *commitment.Updates) {
	if ws == nil {
		return
	}
	ws.assertLive()
	for addr, w := range ws.balance {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags:   commitment.BalanceUpdate,
			Balance: w.Val,
		})
	}
	for addr, w := range ws.nonce {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags: commitment.NonceUpdate,
			Nonce: w.Val,
		})
	}
	for addr, w := range ws.codeHash {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags:    commitment.CodeUpdate,
			CodeHash: w.Val.Value(),
		})
	}
	for addr, w := range ws.code {
		addrVal := addr.Value()
		updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
			Flags:    commitment.CodeUpdate,
			CodeHash: w.Val.Hash.Value(),
		})
	}
	for addr, w := range ws.selfDestruct {
		if w.Val {
			addrVal := addr.Value()
			updates.TouchPlainKeyDirect(string(addrVal[:]), &commitment.Update{
				Flags: commitment.DeleteUpdate,
			})
		}
	}
	for addr, inner := range ws.storage {
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

// sortWriteHeaders sorts headers by (Address, Path, Key) for deterministic
// processing order; WriteSet map iteration is non-deterministic in Go.
// The sort relies on the AccountPath enum ordering defined in versionmap.go.
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

// hasHeader reports whether a write exists at h's (Address, Path, Key).
func (ws *WriteSet) hasHeader(h WriteHeader) bool {
	if ws == nil {
		return false
	}
	switch h.Path {
	case AddressPath:
		_, ok := ws.address[h.Address]
		return ok
	case BalancePath:
		_, ok := ws.balance[h.Address]
		return ok
	case NoncePath:
		_, ok := ws.nonce[h.Address]
		return ok
	case IncarnationPath:
		_, ok := ws.incarnation[h.Address]
		return ok
	case SelfDestructPath:
		_, ok := ws.selfDestruct[h.Address]
		return ok
	case CreateContractPath:
		_, ok := ws.createContract[h.Address]
		return ok
	case CodePath:
		_, ok := ws.code[h.Address]
		return ok
	case CodeHashPath:
		_, ok := ws.codeHash[h.Address]
		return ok
	case CodeSizePath:
		_, ok := ws.codeSize[h.Address]
		return ok
	case StoragePath:
		if inner, ok := ws.storage[h.Address]; ok {
			_, ok := inner[h.Key]
			return ok
		}
	}
	return false
}

// copyFrom merges src into s, value-copying each VersionedWrite so the result
// shares no *VersionedWrite with src — the in-place recordWrite mutators would
// otherwise make either side observe the other's later edits.
func (ws *WriteSet) copyFrom(src *WriteSet) {
	if src == nil {
		return
	}
	for a, vw := range src.address {
		ws.SetAddress(a, cloneVW(vw))
	}
	for a, vw := range src.balance {
		ws.SetBalance(a, cloneVW(vw))
	}
	for a, vw := range src.nonce {
		ws.SetNonce(a, cloneVW(vw))
	}
	for a, vw := range src.incarnation {
		ws.SetIncarnation(a, cloneVW(vw))
	}
	for a, vw := range src.selfDestruct {
		ws.SetSelfDestruct(a, cloneVW(vw))
	}
	for a, vw := range src.createContract {
		ws.SetCreateContract(a, cloneVW(vw))
	}
	for a, vw := range src.code {
		ws.SetCode(a, cloneVW(vw))
	}
	for a, vw := range src.codeHash {
		ws.SetCodeHash(a, cloneVW(vw))
	}
	for a, vw := range src.codeSize {
		ws.SetCodeSize(a, cloneVW(vw))
	}
	for a, inner := range src.storage {
		for key, vw := range inner {
			ws.SetStorage(a, key, cloneVW(vw))
		}
	}
}

// copyMissingFrom adds the entries s does not already hold, sharing src's
// *VersionedWrite pointers rather than cloning them — see MergeInto for what
// the sharing costs the caller.
func (ws *WriteSet) copyMissingFrom(src *WriteSet) {
	if src == nil {
		return
	}
	for a, vw := range src.address {
		if _, ok := ws.address[a]; !ok {
			ws.SetAddress(a, vw)
		}
	}
	for a, vw := range src.balance {
		if _, ok := ws.balance[a]; !ok {
			ws.SetBalance(a, vw)
		}
	}
	for a, vw := range src.nonce {
		if _, ok := ws.nonce[a]; !ok {
			ws.SetNonce(a, vw)
		}
	}
	for a, vw := range src.incarnation {
		if _, ok := ws.incarnation[a]; !ok {
			ws.SetIncarnation(a, vw)
		}
	}
	for a, vw := range src.selfDestruct {
		if _, ok := ws.selfDestruct[a]; !ok {
			ws.SetSelfDestruct(a, vw)
		}
	}
	for a, vw := range src.createContract {
		if _, ok := ws.createContract[a]; !ok {
			ws.SetCreateContract(a, vw)
		}
	}
	for a, vw := range src.code {
		if _, ok := ws.code[a]; !ok {
			ws.SetCode(a, vw)
		}
	}
	for a, vw := range src.codeHash {
		if _, ok := ws.codeHash[a]; !ok {
			ws.SetCodeHash(a, vw)
		}
	}
	for a, vw := range src.codeSize {
		if _, ok := ws.codeSize[a]; !ok {
			ws.SetCodeSize(a, vw)
		}
	}
	for a, inner := range src.storage {
		own := ws.storage[a]
		for key, vw := range inner {
			if _, ok := own[key]; !ok {
				ws.SetStorage(a, key, vw)
			}
		}
	}
}

// Merge returns the union of prev and next, with next winning on (addr,path,key).
func (ws *WriteSet) Merge(next *WriteSet) *WriteSet {
	if ws.IsEmpty() {
		return next
	}
	if next.IsEmpty() {
		return ws
	}
	out := &WriteSet{}
	out.copyFrom(ws)
	out.copyFrom(next)
	return out
}

// MergeInto folds s's entries into next in place and returns the surviving
// set, next winning on (addr, path, key). That is next, except that an empty
// side short-circuits to the other one — so the result can be s, and a
// caller that releases or mutates it must compare identity first. Unlike Merge
// it shares *VersionedWrite pointers instead of cloning, so next must be
// exclusively owned by the caller and neither side may mutate a shared
// VersionedWrite in place afterwards; s's maps are never touched, so
// map-level deletes on s stay safe.
func (ws *WriteSet) MergeInto(next *WriteSet) *WriteSet {
	if ws.IsEmpty() {
		return next
	}
	if next.IsEmpty() {
		return ws
	}
	next.copyMissingFrom(ws)
	return next
}

// hasNewWrite: returns true if the current set has a new write compared to the input
func (ws *WriteSet) HasNewWrite(cmpSet *WriteSet) bool {
	if ws.IsEmpty() {
		return false
	}
	if cmpSet.IsEmpty() || ws.Count() > cmpSet.Count() {
		return true
	}
	for h := range ws.AllHeaders() {
		if !cmpSet.hasHeader(h) {
			return true
		}
	}
	return false
}

// StripBalanceWrite removes the BalancePath write for addr from the write set
// and computes the TX's net balance delta by comparing the stale write with
// the stale read from readSet. This is used in finalize to prevent stale
// speculative coinbase/burnt-contract balance writes from being applied via
// ApplyVersionedWrites. The delta is returned so it can be applied separately
// on top of the correct base balance from the VersionedStateReader.
//
// Returns:
//   - stripped: the write set with the balance write removed
//   - delta: the absolute difference between stale write and stale read
//   - increase: true if the TX increased the balance, false if decreased
//   - found: true if both a stale read and write were found and a non-zero delta computed
func (ws *WriteSet) StripBalanceWrite(addr accounts.Address, readSet ReadSet) (stripped *WriteSet, delta uint256.Int, increase bool, found bool) {
	stripped = ws
	if ws == nil || addr.IsNil() {
		return
	}
	bw, hasWrite := ws.balance[addr]
	if !readSet.hasAddr(addr) {
		// TX didn't read this address — no delta to compute. Still strip the
		// write to prevent stale cache pollution.
		if hasWrite {
			delete(ws.balance, addr)
		}
		return
	}
	balRead, ok := readSet.GetBalance(addr)
	if !ok || !hasWrite {
		return
	}
	staleRead := balRead.Val
	staleWrite := bw.Val
	delete(ws.balance, addr)
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

	// outputsReleased marks ReleaseOutputMaps having run. A read after that
	// point sees emptied sets rather than failing, so under assertions the
	// accessors panic instead. Only written when dbg.AssertEnabled.
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

// ReleaseOutputMaps returns every recorded write set's map containers to their
// pools and empties the slots. Only for a VersionedIO whose last reader is
// done; the shared VersionedWrite values are left to GC.
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

// mergeTx folds a single transaction's reads, writes and accesses (recorded at
// version.TxIndex) into io at that index, accumulating into the slot rather
// than overwriting it the way RecordReads does. The three per-tx slices grow in
// lockstep so they stay equal length.
func (io *VersionedIO) mergeTx(version Version, reads ReadSet, writes *WriteSet) {
	idx := version.TxIndex + 1
	n := max(idx+1, len(io.inputs), len(io.outputs))
	if n > len(io.inputs) {
		io.inputs = append(io.inputs, make([]versionedReadSet, n-len(io.inputs))...)
	}
	if n > len(io.outputs) {
		io.outputs = append(io.outputs, make([]*WriteSet, n-len(io.outputs))...)
	}
	// Fold the read set when it carries typed reads OR access-only marks: an
	// access-only phase has Len()==0 but must still reach io.inputs for the
	// EIP-7928 BAL. Len() itself stays access-free so the parallel executor's
	// HasReads/ReadSetIncarnation/ReadCount keep their read-presence semantics.
	if reads.Len() > 0 || len(reads.access) > 0 {
		if io.inputs[idx].readSet.Len() == 0 && len(io.inputs[idx].readSet.access) == 0 {
			// Production call sites merge each tx once into an empty slot; hand the
			// read set over directly (like RecordReads) instead of deep-copying.
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
// EIP-8246 both activate with Amsterdam, so balance writes always use the
// no-burn SELFDESTRUCT semantics.
func (io *VersionedIO) AsBlockAccessList() types.BlockAccessList {
	if io == nil {
		return nil
	}

	ac := make(map[accounts.Address]*accountState)
	maxTxIndex := io.Len() - 1

	for txIndex := -1; txIndex <= maxTxIndex; txIndex++ {
		// EIP-7928 requires every accessed address to appear in the BAL, so each per-path read map must register its address.
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
			// Skip validation-only reads for non-existent accounts.
			// These are recorded when the version map has no entry
			// (MVReadResultNone) so that conflict detection works across
			// transactions, but they should not appear in the block access list.
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
			// A pre-block-empty code hash marks the baseline empty so applyToCode drops a
			// net-zero same-tx set-then-clear. Only an empty read seen before any code
			// change is the pre-block value — a later read of an already-cleared delegation
			// also reads empty and must not poison the baseline into dropping that clear.
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
			// Self-destruct is applied before the balance writes so the EIP-7928
			// burn (zeroing a non-zero balance written by the destroying tx) fires
			// — the priority is explicit in loop order.
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
			// EIP-7928 requires every touched address to appear. The value-carrying
			// paths above register via ensureAccountState; a single sweep over the
			// full address union covers the rest (incarnation/codeHash/create/
			// codeSize/address) and can't silently miss a future path — the repeat
			// ensureAccountState is a harmless get-or-create.
			for addr := range writes.addrs() {
				if addr.IsNil() {
					continue
				}
				account := ensureAccountState(ac, addr)
				// A contract created this block did not exist pre-block, so its pre-block
				// code is empty; applyToCode uses this to drop a net-zero empty code change.
				if vw, ok := writes.GetCreateContract(addr); ok && vw.Val {
					account.initialCodeEmpty = true
				}
			}
		}

		for addr := range io.ReadSet(txIndex).access {
			if addr.IsNil() {
				continue
			}
			ensureAccountState(ac, addr)
		}
	}

	bal := make(types.BlockAccessList, 0, len(ac))
	for _, account := range ac {
		account.finalize()
		account.changes.Normalize()
		bal = append(bal, *account.changes)
	}

	slices.SortFunc(bal, func(a, b types.AccountChanges) int {
		return a.Address.Cmp(b.Address)
	})

	return bal
}

type accountState struct {
	changes             *types.AccountChanges
	balance             *fieldTracker[uint256.Int]
	nonce               *fieldTracker[uint64]
	code                *fieldTracker[accounts.Code]
	balanceValue        *uint256.Int                        // tracks latest seen balance
	initialBalanceValue *uint256.Int                        // tracks pre-block balance for net-zero detection
	storageReadValues   map[accounts.StorageKey]uint256.Int // original read values for net-zero detection
	slotWrites          map[accounts.StorageKey]int         // slot -> index into changes.StorageChanges, built past slotIndexMin
	slotReads           map[accounts.StorageKey]int         // slot -> index into changes.StorageReads, built with slotWrites
	initialCodeEmpty    bool                                // pre-block code was empty (created contract or empty-codehash read)
}

// check pre- and post-values, add to BAL if different
func (account *accountState) finalize() {
	applyToBalance(account.balance, account.changes, account.initialBalanceValue)
	applyToNonce(account.nonce, account.changes)
	applyToCode(account.code, account.changes, account.initialCodeEmpty)
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
	// Get the sorted indices to identify the first (lowest-index) entry.
	// If the first entry equals the pre-block balance, it's a net-zero
	// change from a reverted tx (e.g. CALL with value then revert) and
	// must be excluded. Geth's scope-based BAL builder handles this via
	// ExitScope which converts reverted writes to reads.
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
	// A first code change back to empty when pre-block code was already empty
	// (e.g. an EIP-7702 delegation set then cleared in the same tx) is a net-zero
	// change and is omitted, matching EELS's post-vs-pre code-hash diff.
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

// changeTracker stores the latest written value per access index. For CodePath
// the entry borrows the canonical bytecode owned by cache.StateCache; applyToCode
// clones it into the BAL CodeChange so the consensus-visible output owns its bytes.
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

func (account *accountState) setBalanceValue(v uint256.Int) {
	if account.balanceValue == nil {
		account.balanceValue = &uint256.Int{}
	}
	*account.balanceValue = v
}

func newAccountState(addr accounts.Address) *accountState {
	return &accountState{
		changes: &types.AccountChanges{Address: addr.Value()},
		balance: newBalanceTracker(),
		nonce:   newNonceTracker(),
		code:    newCodeTracker(),
	}
}

func ensureAccountState(accounts map[accounts.Address]*accountState, addr accounts.Address) *accountState {
	if account, ok := accounts[addr]; ok {
		return account
	}
	account := newAccountState(addr)
	accounts[addr] = account
	return account
}

func (account *accountState) applyWriteStorage(key accounts.StorageKey, val uint256.Int, accessIndex uint32) {
	// Skip intra-tx net-zero storage writes: if this is the first write
	// to the slot (no prior tx wrote to it) and the written value equals
	// the original read value, it's a no-op that should remain as a read.
	at := account.storageWriteIndex(key)
	if at < 0 {
		if origVal, wasRead := account.storageReadValues[key]; wasRead && val.Eq(&origVal) {
			return
		}
	}
	account.addStorageUpdate(at, key, val, accessIndex)
}

func (account *accountState) applyWriteNonce(val uint64, accessIndex uint32) {
	account.nonce.recordWrite(accessIndex, val)
}

func (account *accountState) applyWriteCode(val accounts.Code, accessIndex uint32) {
	account.code.recordWrite(accessIndex, val)
}

func (account *accountState) applyWriteBalance(val uint256.Int, accessIndex uint32) {
	{
		// If we haven't seen a balance and the first write is zero, treat it
		// as a touch only when the pre-block balance is (or is implicitly) zero:
		//   - No prior read: the account wasn't accessed before, so its
		//     pre-block balance is implicitly zero (e.g. a newly CREATE'd
		//     contract or a receiver observed only via the post-write finalize
		//     path). Writing zero is a no-op.
		//   - Prior read showed zero: write matches initial state, no-op.
		// If a prior read showed a non-zero pre-block balance, the zero write
		// is a genuine depletion (e.g. a sender whose funds were consumed by
		// gas) and must be recorded as a real balance change.
		if account.balanceValue == nil && val.IsZero() &&
			(account.initialBalanceValue == nil || account.initialBalanceValue.IsZero()) {
			if account.initialBalanceValue == nil {
				v := val
				account.initialBalanceValue = &v
			}
			account.setBalanceValue(val)
			return
		}
		// Skip no-op writes.
		if account.balanceValue != nil && val.Eq(account.balanceValue) {
			account.setBalanceValue(val)
			return
		}
		// Skip balance writes that match the pre-block (initial) balance,
		// but ONLY when no intermediate balance changes have been recorded.
		// If intermediate changes exist (e.g. tx58 sets balance=0xa141,
		// tx78 restores initial 0x16ffd), the restoring write MUST be
		// recorded so parallel executors see the correct value at tx78.
		if account.initialBalanceValue != nil && val.Eq(account.initialBalanceValue) && len(account.balance.changes.entries) == 0 {
			account.setBalanceValue(val)
			return
		}
		account.setBalanceValue(val)
		account.balance.recordWrite(accessIndex, val)
	}
}

func (account *accountState) updateReadStorage(key accounts.StorageKey, val uint256.Int) {
	// Record the original read value for net-zero detection.
	// Only the first read for each slot is recorded (the original value).
	if account.storageReadValues == nil {
		account.storageReadValues = make(map[accounts.StorageKey]uint256.Int)
	}
	if _, exists := account.storageReadValues[key]; !exists {
		account.storageReadValues[key] = val
	}
	if account.hasStorageWrite(key) {
		return
	}
	if account.slotReads != nil {
		if _, seen := account.slotReads[key]; seen {
			return
		}
		account.slotReads[key] = len(account.changes.StorageReads)
	}
	account.changes.StorageReads = append(account.changes.StorageReads, key)
}

func (account *accountState) updateReadBalance(val uint256.Int) {
	// Record the initial (pre-block) balance for net-zero detection.
	// Only set from the first read AND only before any writes have
	// been recorded. A read that arrives after a write (e.g. the
	// block-end finalize in the parallel executor reading from a
	// fresh IBS, or a BAL-prepopulated read of a tx's predicted
	// write) reflects post-write state, not the pre-block balance,
	// and must not be used for net-zero filtering.
	if account.initialBalanceValue == nil && account.balanceValue == nil {
		v := val
		account.initialBalanceValue = &v
	}
	// Only update balanceValue from reads when no writes have been
	// recorded yet. After a write, balanceValue tracks the written
	// state; a stale read from the DB must not override it, or the
	// no-op check in updateWrite will incorrectly skip a real write.
	if len(account.balance.changes.entries) == 0 {
		account.setBalanceValue(val)
	}
}

// slotIndexMin is where scanning the slot lists stops being cheaper than a map
// probe. A storage-bloated block puts thousands of slots on one account and
// looks every one of them up.
const slotIndexMin = 16

// storageWriteIndex returns the position of slot in changes.StorageChanges, or -1.
func (account *accountState) storageWriteIndex(slot accounts.StorageKey) int {
	if account.slotWrites != nil {
		if i, ok := account.slotWrites[slot]; ok {
			return i
		}
		return -1
	}
	for i := range account.changes.StorageChanges {
		if account.changes.StorageChanges[i].Slot == slot {
			return i
		}
	}
	return -1
}

func (account *accountState) hasStorageWrite(slot accounts.StorageKey) bool {
	return account.storageWriteIndex(slot) >= 0
}

// addStorageUpdate records a write at slot, which storageWriteIndex already
// located as at (-1 when the slot is new). It is the only writer of
// changes.StorageChanges, so the index cannot drift from it.
func (account *accountState) addStorageUpdate(at int, slot accounts.StorageKey, val uint256.Int, txIndex uint32) {
	// If we already recorded a read for this slot, drop it because a write takes precedence.
	account.removeStorageRead(slot)

	ac := account.changes
	if at >= 0 {
		sc := &ac.StorageChanges[at]
		// EIP-7928 no-op filter: skip if value equals the slot's last recorded write.
		if n := len(sc.Changes); n > 0 && val.Eq(&sc.Changes[n-1].Value) {
			return
		}
		sc.Changes = append(sc.Changes, &types.StorageChange{Index: txIndex, Value: val})
		return
	}

	if account.slotWrites != nil {
		account.slotWrites[slot] = len(ac.StorageChanges)
	}
	ac.StorageChanges = append(ac.StorageChanges, types.SlotChanges{
		Slot:    slot,
		Changes: []*types.StorageChange{{Index: txIndex, Value: val}},
	})
	if account.slotWrites == nil && len(ac.StorageChanges) >= slotIndexMin {
		account.slotWrites = make(map[accounts.StorageKey]int, len(ac.StorageChanges))
		for i := range ac.StorageChanges {
			account.slotWrites[ac.StorageChanges[i].Slot] = i
		}
		// StorageReads can hold the same slot twice: below the threshold reads are
		// appended unchecked, and Normalize is what dedups them. The index needs
		// one entry per slot, so drop the repeats while building it.
		account.slotReads = make(map[accounts.StorageKey]int, len(ac.StorageReads))
		deduped := ac.StorageReads[:0]
		for _, slot := range ac.StorageReads {
			if _, seen := account.slotReads[slot]; seen {
				continue
			}
			account.slotReads[slot] = len(deduped)
			deduped = append(deduped, slot)
		}
		ac.StorageReads = deduped
	}
}

// removeStorageRead drops slot from StorageReads, because a write to it takes
// precedence. Past slotIndexMin the position is indexed and the last entry is
// swapped in; order does not survive Normalize's sort anyway.
func (account *accountState) removeStorageRead(slot accounts.StorageKey) {
	ac := account.changes
	if len(ac.StorageReads) == 0 {
		return
	}
	if account.slotReads == nil {
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
		return
	}
	i, ok := account.slotReads[slot]
	if !ok {
		return
	}
	delete(account.slotReads, slot)
	last := len(ac.StorageReads) - 1
	if i != last {
		ac.StorageReads[i] = ac.StorageReads[last]
		account.slotReads[ac.StorageReads[i]] = i
	}
	if last == 0 {
		ac.StorageReads = nil
		return
	}
	ac.StorageReads = ac.StorageReads[:last]
}

type versionedReadSet struct {
	incarnation int
	readSet     ReadSet
}

// AllHeaders iterates the type-agnostic header of every read in the set.
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

func (ws *WriteSet) createdEmpty(addr accounts.Address) bool {
	created, ok := ws.address[addr]
	if !ok || created.Val == nil {
		return false
	}
	account := *created.Val
	if written, ok := ws.balance[addr]; ok {
		account.Balance = written.Val
	}
	if written, ok := ws.nonce[addr]; ok {
		account.Nonce = written.Val
	}
	if written, ok := ws.codeHash[addr]; ok {
		account.CodeHash = written.Val
	}
	if !account.Empty() {
		return false
	}
	_, hasCode := ws.code[addr]
	_, hasIncarnation := ws.incarnation[addr]
	_, destroyed := ws.selfDestruct[addr]
	_, createdContract := ws.createContract[addr]
	_, hasCodeSize := ws.codeSize[addr]
	return !hasCode && !hasIncarnation && !destroyed && !createdContract && !hasCodeSize && len(ws.storage[addr]) == 0
}

// vwSlab hands out the writes of one IBS-owned set; reset rewinds it for the
// next tx, put returns a reverted write for reuse in the same tx.
type vwSlab[T any] struct {
	items []*VersionedWrite[T]
	used  int
	free  []*VersionedWrite[T]
}

func (s *vwSlab[T]) get() *VersionedWrite[T] {
	if n := len(s.free); n > 0 {
		vw := s.free[n-1]
		s.free = s.free[:n-1]
		return vw
	}
	if s.used == len(s.items) {
		s.items = append(s.items, new(VersionedWrite[T]))
	}
	vw := s.items[s.used]
	s.used++
	return vw
}

func (s *vwSlab[T]) put(vw *VersionedWrite[T]) { s.free = append(s.free, vw) }

func (s *vwSlab[T]) reset() {
	s.used = 0
	s.free = s.free[:0]
}

// acctIO is one address's in-flight reads and writes, its EIP-7928 access mark
// and the journal's dirty count for it, so one read-path step finds all of them
// with a single lookup.
type acctIO struct {
	addr     accounts.Address
	has      uint16 // 1<<path for each recorded non-storage path
	dirty    int32
	accessed bool
	access   accessOptions

	address        VersionedRead[AccountView]
	balance        VersionedRead[uint256.Int]
	nonce          VersionedRead[uint64]
	incarnation    VersionedRead[uint64]
	selfDestruct   VersionedRead[bool]
	sdWitnesses    []VersionedRead[bool]
	createContract VersionedRead[bool]
	code           VersionedRead[accounts.Code]
	codeHash       VersionedRead[accounts.CodeHash]
	codeSize       VersionedRead[int]
	storage        map[accounts.StorageKey]VersionedRead[uint256.Int]

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

// ioTable is an IntraBlockState's own read and write set: records in a slice
// reused across transactions, found through idx or the last-lookup memo. It is
// copied into a ReadSet / WriteSet only when handed over.
// ponytail: records past len keep stale pointers until reused; clear them if RSS matters.
type ioTable struct {
	idx  map[accounts.Address]int32
	recs []acctIO
	last int32
}

func pathBit(p AccountPath) uint16 { return 1 << p }

func (t *ioTable) find(addr accounts.Address) *acctIO {
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

// get returns addr's record, adding an empty one if needed. The pointer is valid
// only until the next get.
func (t *ioTable) get(addr accounts.Address) *acctIO {
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
		t.recs = append(t.recs, acctIO{})
	}
	r := &t.recs[i]
	*r = acctIO{addr: addr, storage: r.storage, sdWitnesses: r.sdWitnesses[:0], wStorage: r.wStorage}
	t.idx[addr] = int32(i)
	t.last = int32(i)
	return r
}

func (t *ioTable) entries() int {
	n := len(t.recs)
	for i := range t.recs {
		n += len(t.recs[i].storage) + len(t.recs[i].wStorage)
	}
	return n
}

func (t *ioTable) reset() {
	for i := range t.recs {
		clear(t.recs[i].storage)
		clear(t.recs[i].wStorage)
	}
	t.recs = t.recs[:0]
	clear(t.idx)
	t.last = 0
}

// clearReads drops the reads and access marks but keeps writes and dirty counts.
func (t *ioTable) clearReads() {
	for i := range t.recs {
		r := &t.recs[i]
		r.has = 0
		r.sdWitnesses = r.sdWitnesses[:0]
		clear(r.storage)
		r.accessed = false
		r.access = accessOptions{}
	}
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
		r.accessed = true
		r.access = accessOptions{revertable: revertable}
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
		t.recs[i].accessed = false
		t.recs[i].access = accessOptions{}
	}
}

func getRead[T any](t *ioTable, addr accounts.Address, path AccountPath, pick func(*acctIO) *VersionedRead[T]) (VersionedRead[T], bool) {
	if r := t.find(addr); r != nil && r.has&pathBit(path) != 0 {
		return *pick(r), true
	}
	return VersionedRead[T]{}, false
}

func setRead[T any](t *ioTable, addr accounts.Address, path AccountPath, pick func(*acctIO) *VersionedRead[T], tr VersionedRead[T]) {
	r := t.get(addr)
	*pick(r) = tr
	r.has |= pathBit(path)
}

func pickAddress(r *acctIO) *VersionedRead[AccountView]        { return &r.address }
func pickBalance(r *acctIO) *VersionedRead[uint256.Int]        { return &r.balance }
func pickNonce(r *acctIO) *VersionedRead[uint64]               { return &r.nonce }
func pickIncarnation(r *acctIO) *VersionedRead[uint64]         { return &r.incarnation }
func pickSelfDestruct(r *acctIO) *VersionedRead[bool]          { return &r.selfDestruct }
func pickCreateContract(r *acctIO) *VersionedRead[bool]        { return &r.createContract }
func pickCode(r *acctIO) *VersionedRead[accounts.Code]         { return &r.code }
func pickCodeHash(r *acctIO) *VersionedRead[accounts.CodeHash] { return &r.codeHash }
func pickCodeSize(r *acctIO) *VersionedRead[int]               { return &r.codeSize }

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

// SetSelfDestruct keeps every distinct destruct version the tx consumed, as
// ReadSet.SetSelfDestruct does.
func (t *ioTable) SetSelfDestruct(addr accounts.Address, tr VersionedRead[bool]) {
	r := t.get(addr)
	if r.has&pathBit(SelfDestructPath) != 0 && r.selfDestruct.Version != tr.Version {
		prev := r.selfDestruct
		if !slices.ContainsFunc(r.sdWitnesses, func(w VersionedRead[bool]) bool { return w.Version == prev.Version }) {
			r.sdWitnesses = append(r.sdWitnesses, prev)
		}
	}
	r.selfDestruct = tr
	r.has |= pathBit(SelfDestructPath)
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

// toReadSet copies the table into a ReadSet the caller owns.
func (t *ioTable) toReadSet() ReadSet {
	var s ReadSet
	for i := range t.recs {
		r := &t.recs[i]
		a := r.addr
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
		if len(r.sdWitnesses) > 0 {
			if s.selfDestructWitnesses == nil {
				s.selfDestructWitnesses = map[accounts.Address][]VersionedRead[bool]{}
			}
			s.selfDestructWitnesses[a] = slices.Clone(r.sdWitnesses)
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

// writeView is the IntraBlockState's write set: the write fields of its ioTable
// records, with the values drawn from per-path slabs.
// ponytail: slabs keep the largest tx's writes for the IBS lifetime; cap them if a worker's RSS matters.
type writeView struct {
	t     *ioTable
	slabs wsSlabs
	tmp   WriteSet
}

type wsSlabs struct {
	address        vwSlab[*accounts.Account]
	balance        vwSlab[uint256.Int]
	nonce          vwSlab[uint64]
	incarnation    vwSlab[uint64]
	selfDestruct   vwSlab[bool]
	createContract vwSlab[bool]
	code           vwSlab[accounts.Code]
	codeHash       vwSlab[accounts.CodeHash]
	codeSize       vwSlab[int]
	storage        vwSlab[uint256.Int]
}

func (v *writeView) recycler() *wsSlabs { return &v.slabs }

func pickWAddress(r *acctIO) **VersionedWrite[*accounts.Account]  { return &r.wAddress }
func pickWBalance(r *acctIO) **VersionedWrite[uint256.Int]        { return &r.wBalance }
func pickWNonce(r *acctIO) **VersionedWrite[uint64]               { return &r.wNonce }
func pickWIncarnation(r *acctIO) **VersionedWrite[uint64]         { return &r.wIncarnation }
func pickWSelfDestruct(r *acctIO) **VersionedWrite[bool]          { return &r.wSelfDestruct }
func pickWCreateContract(r *acctIO) **VersionedWrite[bool]        { return &r.wCreateContract }
func pickWCode(r *acctIO) **VersionedWrite[accounts.Code]         { return &r.wCode }
func pickWCodeHash(r *acctIO) **VersionedWrite[accounts.CodeHash] { return &r.wCodeHash }
func pickWCodeSize(r *acctIO) **VersionedWrite[int]               { return &r.wCodeSize }

func getW[T any](v *writeView, addr accounts.Address, pick func(*acctIO) **VersionedWrite[T]) (*VersionedWrite[T], bool) {
	if r := v.t.find(addr); r != nil {
		if w := *pick(r); w != nil {
			return w, true
		}
	}
	return nil, false
}

func setW[T any](v *writeView, addr accounts.Address, pick func(*acctIO) **VersionedWrite[T], vw *VersionedWrite[T]) {
	*pick(v.t.get(addr)) = vw
}

func delW[T any](v *writeView, addr accounts.Address, pick func(*acctIO) **VersionedWrite[T], s *vwSlab[T]) {
	if r := v.t.find(addr); r != nil {
		if w := *pick(r); w != nil {
			s.put(w)
			*pick(r) = nil
		}
	}
}

func updW[T any](v *writeView, addr accounts.Address, pick func(*acctIO) **VersionedWrite[T], val T) {
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
		w, ok := r.wStorage[key]
		return w, ok
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

func (v *writeView) DelBalance(addr accounts.Address) { delW(v, addr, pickWBalance, &v.slabs.balance) }
func (v *writeView) DelNonce(addr accounts.Address)   { delW(v, addr, pickWNonce, &v.slabs.nonce) }
func (v *writeView) DelIncarnation(addr accounts.Address) {
	delW(v, addr, pickWIncarnation, &v.slabs.incarnation)
}
func (v *writeView) DelSelfDestruct(addr accounts.Address) {
	delW(v, addr, pickWSelfDestruct, &v.slabs.selfDestruct)
}
func (v *writeView) DelCode(addr accounts.Address) { delW(v, addr, pickWCode, &v.slabs.code) }
func (v *writeView) DelCodeHash(addr accounts.Address) {
	delW(v, addr, pickWCodeHash, &v.slabs.codeHash)
}
func (v *writeView) DelCodeSize(addr accounts.Address) {
	delW(v, addr, pickWCodeSize, &v.slabs.codeSize)
}
func (v *writeView) DelStorage(addr accounts.Address, key accounts.StorageKey) {
	if r := v.t.find(addr); r != nil {
		if w, ok := r.wStorage[key]; ok {
			v.slabs.storage.put(w)
			delete(r.wStorage, key)
			if len(r.wStorage) == 0 {
				clear(r.wStorage) // drops the tombstones a revert leaves behind
			}
		}
	}
}

func (v *writeView) updateBalance(addr accounts.Address, val uint256.Int) {
	updW(v, addr, pickWBalance, val)
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
func (v *writeView) updateCodeSize(addr accounts.Address, val int) {
	updW(v, addr, pickWCodeSize, val)
}
func (v *writeView) updateStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	if w, ok := v.GetStorage(addr, key); ok {
		w.Val = val
	}
}

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

func (r *acctIO) hasWrite() bool {
	return r.wAddress != nil || r.wBalance != nil || r.wNonce != nil || r.wIncarnation != nil ||
		r.wSelfDestruct != nil || r.wCreateContract != nil || r.wCode != nil || r.wCodeHash != nil ||
		r.wCodeSize != nil || len(r.wStorage) > 0
}

func (v *writeView) forEachAddr(f func(accounts.Address)) {
	for i := range v.t.recs {
		if v.t.recs[i].hasWrite() {
			f(v.t.recs[i].addr)
		}
	}
}

func dropW[T any](p **VersionedWrite[T], s *vwSlab[T]) {
	if *p != nil {
		s.put(*p)
		*p = nil
	}
}

func (v *writeView) deleteAddr(addr accounts.Address) {
	r := v.t.find(addr)
	if r == nil {
		return
	}
	s := &v.slabs
	dropW(&r.wAddress, &s.address)
	dropW(&r.wBalance, &s.balance)
	dropW(&r.wNonce, &s.nonce)
	dropW(&r.wIncarnation, &s.incarnation)
	dropW(&r.wSelfDestruct, &s.selfDestruct)
	dropW(&r.wCreateContract, &s.createContract)
	dropW(&r.wCode, &s.code)
	dropW(&r.wCodeHash, &s.codeHash)
	dropW(&r.wCodeSize, &s.codeSize)
	for _, w := range r.wStorage {
		s.storage.put(w)
	}
	clear(r.wStorage)
}

func cloneOrNil[T any](w *VersionedWrite[T]) *VersionedWrite[T] {
	if w == nil {
		return nil
	}
	return cloneVW(w)
}

func (v *writeView) snapshotCreateFields(addr accounts.Address) *createWriteSnapshot {
	snap := &createWriteSnapshot{}
	if r := v.t.find(addr); r != nil {
		snap.address = cloneOrNil(r.wAddress)
		snap.balance = cloneOrNil(r.wBalance)
		snap.incarnation = cloneOrNil(r.wIncarnation)
		snap.selfDestruct = cloneOrNil(r.wSelfDestruct)
		snap.createContract = cloneOrNil(r.wCreateContract)
		snap.codeHash = cloneOrNil(r.wCodeHash)
	}
	return snap
}

func (v *writeView) restoreCreateFields(addr accounts.Address, snap *createWriteSnapshot) {
	r := v.t.get(addr)
	s := &v.slabs
	dropW(&r.wAddress, &s.address)
	dropW(&r.wBalance, &s.balance)
	dropW(&r.wIncarnation, &s.incarnation)
	dropW(&r.wSelfDestruct, &s.selfDestruct)
	dropW(&r.wCreateContract, &s.createContract)
	dropW(&r.wCodeHash, &s.codeHash)
	if snap == nil {
		return
	}
	r.wAddress = snap.address
	r.wBalance = snap.balance
	r.wIncarnation = snap.incarnation
	r.wSelfDestruct = snap.selfDestruct
	r.wCreateContract = snap.createContract
	r.wCodeHash = snap.codeHash
}

// live fills the reusable tmp set with the live writes, sharing their pointers.
func (v *writeView) live() *WriteSet {
	ws := &v.tmp
	for i := range v.t.recs {
		r := &v.t.recs[i]
		a := r.addr
		if r.wAddress != nil {
			ws.SetAddress(a, r.wAddress)
		}
		if r.wBalance != nil {
			ws.SetBalance(a, r.wBalance)
		}
		if r.wNonce != nil {
			ws.SetNonce(a, r.wNonce)
		}
		if r.wIncarnation != nil {
			ws.SetIncarnation(a, r.wIncarnation)
		}
		if r.wSelfDestruct != nil {
			ws.SetSelfDestruct(a, r.wSelfDestruct)
		}
		if r.wCreateContract != nil {
			ws.SetCreateContract(a, r.wCreateContract)
		}
		if r.wCode != nil {
			ws.SetCode(a, r.wCode)
		}
		if r.wCodeHash != nil {
			ws.SetCodeHash(a, r.wCodeHash)
		}
		if r.wCodeSize != nil {
			ws.SetCodeSize(a, r.wCodeSize)
		}
		for k, w := range r.wStorage {
			ws.SetStorage(a, k, w)
		}
	}
	return ws
}

// Snapshot returns a detached copy with WriteSet.Snapshot's self-destruct rules.
func (v *writeView) Snapshot() *WriteSet {
	out := &WriteSet{}
	for i := range v.t.recs {
		r := &v.t.recs[i]
		a := r.addr
		sd := false
		if r.wSelfDestruct != nil {
			out.SetSelfDestruct(a, cloneVW(r.wSelfDestruct))
			sd = r.wSelfDestruct.Val
		}
		if r.wBalance != nil {
			out.SetBalance(a, cloneVW(r.wBalance))
		}
		if r.wIncarnation != nil {
			out.SetIncarnation(a, cloneVW(r.wIncarnation))
		}
		for k, w := range r.wStorage {
			out.SetStorage(a, k, cloneVW(w))
		}
		if r.wCreateContract != nil {
			out.SetCreateContract(a, cloneVW(r.wCreateContract))
		}
		if sd {
			continue
		}
		if r.wAddress != nil {
			out.SetAddress(a, cloneVW(r.wAddress))
		}
		if r.wNonce != nil {
			out.SetNonce(a, cloneVW(r.wNonce))
		}
		if r.wCode != nil {
			out.SetCode(a, cloneVW(r.wCode))
		}
		if r.wCodeHash != nil {
			out.SetCodeHash(a, cloneVW(r.wCodeHash))
		}
		if r.wCodeSize != nil {
			out.SetCodeSize(a, cloneVW(r.wCodeSize))
		}
	}
	return out
}

// Finalize zeroes the storage of contracts created and destroyed in this tx,
// then snapshots, as WriteSet.Finalize.
func (v *writeView) Finalize() *WriteSet {
	for i := range v.t.recs {
		r := &v.t.recs[i]
		if r.wCreateContract != nil && r.wCreateContract.Val && r.wSelfDestruct != nil && r.wSelfDestruct.Val {
			for _, w := range r.wStorage {
				w.Val = uint256.Int{}
			}
		}
	}
	return v.Snapshot()
}

func (v *writeView) ReleaseAndReset() {
	for i := range v.t.recs {
		r := &v.t.recs[i]
		r.wAddress, r.wBalance, r.wNonce, r.wIncarnation = nil, nil, nil, nil
		r.wSelfDestruct, r.wCreateContract, r.wCode, r.wCodeHash, r.wCodeSize = nil, nil, nil, nil, nil
		clear(r.wStorage)
	}
	s := &v.slabs
	s.address.reset()
	s.balance.reset()
	s.nonce.reset()
	s.incarnation.reset()
	s.selfDestruct.reset()
	s.createContract.reset()
	s.code.reset()
	s.codeHash.reset()
	s.codeSize.reset()
	s.storage.reset()
}

func (v *writeView) Count() int {
	n := 0
	for i := range v.t.recs {
		r := &v.t.recs[i]
		for _, set := range []bool{r.wAddress != nil, r.wBalance != nil, r.wNonce != nil, r.wIncarnation != nil,
			r.wSelfDestruct != nil, r.wCreateContract != nil, r.wCode != nil, r.wCodeHash != nil, r.wCodeSize != nil} {
			if set {
				n++
			}
		}
		n += len(r.wStorage)
	}
	return n
}

// AllHeaders visits the header of every live write.
func (v *writeView) AllHeaders() iter.Seq[WriteHeader] {
	return v.live().AllHeaders()
}
