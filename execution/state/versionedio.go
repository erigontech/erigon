package state

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"sync"

	g "github.com/anacrolix/generics"
	"github.com/heimdalr/dag"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
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
	address        map[accounts.Address]VersionedRead[AccountView]
	balance        map[accounts.Address]VersionedRead[uint256.Int]
	nonce          map[accounts.Address]VersionedRead[uint64]
	incarnation    map[accounts.Address]VersionedRead[uint64]
	selfDestruct   map[accounts.Address]VersionedRead[bool]
	createContract map[accounts.Address]VersionedRead[bool]
	code           map[accounts.Address]VersionedRead[accounts.Code]
	codeHash       map[accounts.Address]VersionedRead[accounts.CodeHash]
	codeSize       map[accounts.Address]VersionedRead[int]
	storage        map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int]
	// slotMapPeak is the most slots each kept storage map has held: a cleared
	// map keeps the buckets it grew.
	slotMapPeak map[accounts.Address]int

	// access carries EIP-7928 "address was accessed" marks (with the
	// non-revertable "real EVM access" bit) on the read side, so the access set
	// travels with the read-set rather than as a separate IntraBlockState cache.
	access AccessSet
}

func (s *ReadSet) entries() int {
	n := len(s.address) + len(s.balance) + len(s.nonce) + len(s.incarnation) + len(s.selfDestruct) +
		len(s.createContract) + len(s.code) + len(s.codeHash) + len(s.codeSize)
	// Slots, not addresses: one contract's slot map is what a reused set keeps,
	// and a map never shrinks back.
	for _, inner := range s.storage {
		n += len(inner)
	}
	return n
}

func (s *ReadSet) clearForReuse() {
	clear(s.address)
	clear(s.balance)
	clear(s.nonce)
	clear(s.incarnation)
	clear(s.selfDestruct)
	clear(s.createContract)
	clear(s.code)
	clear(s.codeHash)
	clear(s.codeSize)
	// Keep the per-address slot maps: a call that reads one contract's slots
	// would otherwise regrow its map from scratch on every later call. Only
	// this call's contracts stay, or a run of calls on different contracts
	// would pin a map each while every entries() count looked small.
	if len(s.storage) <= maxReusedStorageAddrs {
		held := 0
		for addr, inner := range s.storage {
			if len(inner) == 0 {
				delete(s.storage, addr)
				delete(s.slotMapPeak, addr)
				continue
			}
			if s.slotMapPeak == nil {
				s.slotMapPeak = make(map[accounts.Address]int, len(s.storage))
			}
			peak := max(s.slotMapPeak[addr], len(inner))
			s.slotMapPeak[addr] = peak
			held += peak
			clear(inner)
		}
		if held > maxPooledEntries {
			s.storage, s.slotMapPeak = nil, nil
		}
	} else {
		// Nil, not cleared: a cleared map keeps the buckets those addresses grew.
		s.storage, s.slotMapPeak = nil, nil
	}
	s.access = nil
}

// maxReusedStorageAddrs bounds how many per-address slot maps a reused read set
// keeps; past it the outer map is dropped with them.
const maxReusedStorageAddrs = 64

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

// hasAnyReadAt reports whether the read set contains a read of any field of addr.
// A whole-account lifecycle write depends on such a reader regardless of which field
// it read (see HasReadDep / AffectsAccountLifecycle).
func (s *ReadSet) hasAnyReadAt(addr accounts.Address) bool {
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
	if _, ok := s.storage[addr]; ok {
		return true
	}
	return false
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
		readSetPut(&s.selfDestruct, a, tr)
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

// EachHeader visits the type-agnostic header of every read; the callback
// returns false to stop early.
func (s ReadSet) EachHeader(yield func(ReadHeader) bool) {
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

// RangeFullHeaders visits every read as (address, path, key, header) — like
// EachHeader but with the address/key the internal maps are keyed by, so a
// caller can build a reverse (key -> readers) index. Callback returns false to
// stop early.
func (s ReadSet) RangeFullHeaders(yield func(accounts.Address, AccountPath, accounts.StorageKey, ReadHeader) bool) {
	for a, tr := range s.address {
		if !yield(a, AddressPath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.balance {
		if !yield(a, BalancePath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.nonce {
		if !yield(a, NoncePath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.incarnation {
		if !yield(a, IncarnationPath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.selfDestruct {
		if !yield(a, SelfDestructPath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.createContract {
		if !yield(a, CreateContractPath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.code {
		if !yield(a, CodePath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.codeHash {
		if !yield(a, CodeHashPath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, tr := range s.codeSize {
		if !yield(a, CodeSizePath, accounts.StorageKey{}, tr.ReadHeader) {
			return
		}
	}
	for a, inner := range s.storage {
		for k, tr := range inner {
			if !yield(a, StoragePath, k, tr.ReadHeader) {
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
	valStatus   valueStatus
}

func (h WriteHeader) String() string {
	return fmt.Sprintf("%x %s (%d.%d)", h.Address, AccountKey{Path: h.Path, Key: h.Key}, h.Version.TxIndex, h.Version.Incarnation)
}

// Deletes reports whether this write instructs the reader/projection to delete the
// leaf (nil), rather than write its value. It is the branch-agnostic signal the
// writer stamps so a consumer need not re-derive the outcome from the value (or the
// fork). Used by the commitment projection in another package.
func (h WriteHeader) Deletes() bool { return h.valStatus == ValueDeleted }

// VersionedWrite is a single versioned write.  The per-path map it lives
// in fixes the address (map key), path — and, for storage, the slot key.
// The struct carries the header and the path-typed value. orig is the committed
// value the tx first read for this key, captured on the first write and held across
// re-writes so valStatus reflects the net transition (origin -> final), not the last
// step of an intra-tx oscillation.
type VersionedWrite[T any] struct {
	WriteHeader
	Val  T
	orig T
}

func cloneVW[T any](w *VersionedWrite[T]) *VersionedWrite[T] {
	c := *w
	return &c
}

func (w *VersionedWrite[T]) String() string {
	return fmt.Sprintf("%s: %v", w.WriteHeader, w.Val)
}

// Per-path *VersionedWrite[T] pools. Lifetime is per-tx — WriteSet.ReleaseAndReset
// returns every VW to its pool at tx-finalize. Slice-valued payloads (CodePath)
// have their Val cleared on release to avoid pinning external memory.
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

// Slabs double from a small first one, so a path that writes a handful of cells
// buys a handful and a path that writes thousands still amortizes. A reusing set
// keeps its slabs, so the cap is what one outlier may pin on it; past the cap
// the shared pools serve the cells, as they did before the arena, and the set
// keeps its list of those cells only up to vwMaxOverflow.
const (
	vwFirstSlab   = 4
	vwMaxCells    = 1024
	vwMaxOverflow = 4 * vwMaxCells
)

// vwArena hands out VersionedWrite cells from append-only slabs and recycles
// them on reset, which costs no atomics and no per-cell Put. A cell stays valid
// only until that reset, so nothing outside the owning set may hold one. Every
// cell it hands out is zero: a new slab starts zeroed, reset clears what it
// rewinds, and alloc clears a pooled cell.
type vwArena[T any] struct {
	slabs    [][]VersionedWrite[T]
	slab     int
	idx      int
	cap      int
	overflow []*VersionedWrite[T]
}

func (a *vwArena[T]) alloc(get func() *VersionedWrite[T]) *VersionedWrite[T] {
	if a.slab == len(a.slabs) {
		if a.cap == vwMaxCells {
			vw := get()
			*vw = VersionedWrite[T]{} // the pools clear only what pins memory
			a.overflow = append(a.overflow, vw)
			return vw
		}
		a.slabs = append(a.slabs, make([]VersionedWrite[T], a.nextSlab()))
		a.cap += len(a.slabs[a.slab])
	}
	vw := &a.slabs[a.slab][a.idx]
	a.idx++
	if a.idx == len(a.slabs[a.slab]) {
		a.slab++
		a.idx = 0
	}
	return vw
}

func (a *vwArena[T]) nextSlab() int {
	return min(max(a.cap, vwFirstSlab), vwMaxCells-a.cap)
}

func (a *vwArena[T]) reset(release func(*VersionedWrite[T])) {
	for s := 0; s <= a.slab && s < len(a.slabs); s++ {
		used := a.slabs[s]
		if s == a.slab {
			used = used[:a.idx]
		}
		clear(used)
	}
	a.slab, a.idx = 0, 0
	for _, vw := range a.overflow {
		release(vw)
	}
	clear(a.overflow)
	a.overflow = a.overflow[:0]
	if cap(a.overflow) > vwMaxOverflow {
		a.overflow = nil
	}
}

const acctChunk = 32

// acctSlab holds an arena-backed set's AddressPath values; a value stays valid
// until the arena resets, so Snapshot copies it out.
type acctSlab struct {
	chunks [][]accounts.Account
	n      int
}

func (s *acctSlab) copyOf(a *accounts.Account) *accounts.Account {
	i, j := s.n/acctChunk, s.n%acctChunk
	if i == len(s.chunks) {
		if s.n >= vwMaxCells {
			return a.SelfCopy()
		}
		s.chunks = append(s.chunks, make([]accounts.Account, acctChunk))
	}
	p := &s.chunks[i][j]
	*p = *a
	s.n++
	return p
}

func (s *acctSlab) reset() {
	for i := 0; i*acctChunk < s.n; i++ {
		clear(s.chunks[i][:min(acctChunk, s.n-i*acctChunk)])
	}
	s.n = 0
}

// copyAccount returns a copy of a for an AddressPath write.
func (ws *WriteSet) copyAccount(a *accounts.Account) *accounts.Account {
	if ws.cells != nil {
		return ws.cells.accounts.copyOf(a)
	}
	return a.SelfCopy()
}

func (ws *WriteSet) newVWAddress() *VersionedWrite[*accounts.Account] {
	if ws.cells != nil {
		return ws.cells.address.alloc(getVWAddress)
	}
	return getVWAddress()
}

func (ws *WriteSet) newVWBalance() *VersionedWrite[uint256.Int] {
	if ws.cells != nil {
		return ws.cells.balance.alloc(getVWBalance)
	}
	return getVWBalance()
}

func (ws *WriteSet) newVWNonce() *VersionedWrite[uint64] {
	if ws.cells != nil {
		return ws.cells.nonce.alloc(getVWNonce)
	}
	return getVWNonce()
}

func (ws *WriteSet) newVWIncarnation() *VersionedWrite[uint64] {
	if ws.cells != nil {
		return ws.cells.incarnation.alloc(getVWIncarnation)
	}
	return getVWIncarnation()
}

func (ws *WriteSet) newVWSelfDestruct() *VersionedWrite[bool] {
	if ws.cells != nil {
		return ws.cells.selfDestruct.alloc(getVWSelfDestruct)
	}
	return getVWSelfDestruct()
}

func (ws *WriteSet) newVWCreateContract() *VersionedWrite[bool] {
	if ws.cells != nil {
		return ws.cells.createContract.alloc(getVWCreateContract)
	}
	return getVWCreateContract()
}

func (ws *WriteSet) newVWCode() *VersionedWrite[accounts.Code] {
	if ws.cells != nil {
		return ws.cells.code.alloc(getVWCode)
	}
	return getVWCode()
}

func (ws *WriteSet) newVWCodeHash() *VersionedWrite[accounts.CodeHash] {
	if ws.cells != nil {
		return ws.cells.codeHash.alloc(getVWCodeHash)
	}
	return getVWCodeHash()
}

func (ws *WriteSet) newVWCodeSize() *VersionedWrite[int] {
	if ws.cells != nil {
		return ws.cells.codeSize.alloc(getVWCodeSize)
	}
	return getVWCodeSize()
}

func (ws *WriteSet) newVWStorage() *VersionedWrite[uint256.Int] {
	if ws.cells != nil {
		return ws.cells.storage.alloc(getVWStorage)
	}
	return getVWStorage()
}

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

	cells *vwArenas // nil until UseArena
}

type vwArenas struct {
	address        vwArena[*accounts.Account]
	balance        vwArena[uint256.Int]
	nonce          vwArena[uint64]
	incarnation    vwArena[uint64]
	selfDestruct   vwArena[bool]
	createContract vwArena[bool]
	code           vwArena[accounts.Code]
	codeHash       vwArena[accounts.CodeHash]
	codeSize       vwArena[int]
	storage        vwArena[uint256.Int]
	accounts       acctSlab
}

func (a *vwArenas) reset() {
	a.accounts.reset()
	a.address.reset(releaseVWAddress)
	a.balance.reset(releaseVWBalance)
	a.nonce.reset(releaseVWNonce)
	a.incarnation.reset(releaseVWIncarnation)
	a.selfDestruct.reset(releaseVWSelfDestruct)
	a.createContract.reset(releaseVWCreateContract)
	a.code.reset(releaseVWCode)
	a.codeHash.reset(releaseVWCodeHash)
	a.codeSize.reset(releaseVWCodeSize)
	a.storage.reset(releaseVWStorage)
}

func (ws *WriteSet) ArenaBacked() bool { return ws != nil && ws.cells != nil }

// assertNotArena trips when a set that recycles its cells would hand one out.
// Unreachable while every consumer takes a Snapshot clone; it is here so a
// future caller that passes the live set fails loudly instead of silently.
func (ws *WriteSet) assertNotArena(op string) {
	if dbg.AssertEnabled && ws.ArenaBacked() {
		panic("writeset: " + op + " of an arena-backed set")
	}
}

// UseArena routes this set's cells to its own slabs. Only a set the state keeps
// across txs may do it, and a set that shares its cells out must not (see
// assertNotArena and recycle).
func (ws *WriteSet) UseArena() {
	if ws.cells == nil {
		ws.cells = &vwArenas{}
	}
}

// recycle resets the set and takes its cells from its own slabs from here on.
// Slabs only pay off once the state outlives a tx: a one-shot state would buy
// a slab per path it touches and throw it away.
func (ws *WriteSet) recycle() {
	ws.ReleaseAndReset()
	ws.UseArena()
}

// vwMapPool[T] pools the per-path map[Address]*VersionedWrite[T] containers themselves (values live in vwPool*).
// put() calls clear() rather than freeing, so the map keeps its bucket capacity across txs; callers must release every VW to its vwPool first.
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
func writeSetPut[T any](m *map[accounts.Address]*VersionedWrite[T], addr accounts.Address, vw *VersionedWrite[T], pool *vwMapPool[T]) {
	if *m == nil {
		*m = pool.get()
	}
	(*m)[addr] = vw
}

func (ws *WriteSet) SetAddress(addr accounts.Address, vw *VersionedWrite[*accounts.Account]) {
	writeSetPut(&ws.address, addr, vw, wsMapPoolAddress)
}

func (ws *WriteSet) SetBalance(addr accounts.Address, vw *VersionedWrite[uint256.Int]) {
	writeSetPut(&ws.balance, addr, vw, wsMapPoolBalance)
}

func (ws *WriteSet) SetNonce(addr accounts.Address, vw *VersionedWrite[uint64]) {
	writeSetPut(&ws.nonce, addr, vw, wsMapPoolNonce)
}

func (ws *WriteSet) SetIncarnation(addr accounts.Address, vw *VersionedWrite[uint64]) {
	writeSetPut(&ws.incarnation, addr, vw, wsMapPoolIncarnation)
}

func (ws *WriteSet) SetSelfDestruct(addr accounts.Address, vw *VersionedWrite[bool]) {
	writeSetPut(&ws.selfDestruct, addr, vw, wsMapPoolSelfDestruct)
}

func (ws *WriteSet) SetCreateContract(addr accounts.Address, vw *VersionedWrite[bool]) {
	writeSetPut(&ws.createContract, addr, vw, wsMapPoolCreateContract)
}

func (ws *WriteSet) SetCode(addr accounts.Address, vw *VersionedWrite[accounts.Code]) {
	writeSetPut(&ws.code, addr, vw, wsMapPoolCode)
}

func (ws *WriteSet) SetCodeHash(addr accounts.Address, vw *VersionedWrite[accounts.CodeHash]) {
	writeSetPut(&ws.codeHash, addr, vw, wsMapPoolCodeHash)
}

func (ws *WriteSet) SetCodeSize(addr accounts.Address, vw *VersionedWrite[int]) {
	writeSetPut(&ws.codeSize, addr, vw, wsMapPoolCodeSize)
}

func (ws *WriteSet) SetStorage(addr accounts.Address, key accounts.StorageKey, vw *VersionedWrite[uint256.Int]) {
	if ws.storage == nil {
		ws.storage = wsGetStorageOuter()
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
	ws.assertNotArena("filter")
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
// at end-of-tx, so the BAL records dirty slots as reads (net-zero). Zero the
// storage write values so AsBlockAccessList folds them away.
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

// Snapshot returns a frozen typed copy of the recorded writes. Under self-destruct
// only SelfDestruct/Balance/Incarnation/Storage/CreateContract are kept (the BAL
// needs residual balance, later reads the incarnation cell as the storage-wipe
// signal, the calculator the per-slot deletes, and fee finalization the creation
// marker); the rest drop.
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
		// the creation marker. ApplyVersionedWrites skips applying it under
		// self-destruct, so keeping it here does not resurrect the account.
		if vw, ok := ws.createContract[addr]; ok {
			out.SetCreateContract(addr, cloneVW(vw))
		}
		if !sd {
			if vw, ok := ws.address[addr]; ok {
				c := cloneVW(vw)
				if ws.cells != nil && c.Val != nil {
					c.Val = c.Val.SelfCopy()
				}
				out.SetAddress(addr, c)
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
		} else if bw, ok := ws.balance[addr]; ok && !bw.Deletes() {
			// A balance-preserving destruct (EIP-8246) resolves at end-of-tx to a codeless
			// EOA — the domain apply writes NewAccount+balance (nonce 0, EmptyCodeHash). The
			// per-field writes were dropped above, so re-stamp the cleared nonce/code — but
			// only for a pre-existing account, whose base record the commitment would
			// otherwise read. An account created this block has no base, so the commitment
			// composes the codeless-EOA defaults on its own, and re-stamping nonce 0 would
			// leak a net-zero nonce change (0→1→0) into the block access list.
			if _, created := ws.createContract[addr]; !created {
				out.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: bw.Version}, Val: 0})
				out.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: bw.Version}, Val: accounts.EmptyCodeHash})
			}
		}
	}
	return out
}

// SetEmptyRemoval clears addr's whole footprint and stamps the account-removal signal: a
// balance write marked ValueDeleted, which commitment and the domain apply read to drop
// the leaf with no lifecycle re-derivation. A self-destruct marker is preserved across the
// clear: a self-destructed account's removal is unconditional (calcFees burns its delayed
// fee, the read path pins the absence), whereas an EIP-161 touched-empty EOA carries none
// and must not gain one. deleteAddr still clears the AddressPath so neither is mistaken for
// a same-tx SD+CREATE2 recreate.
func (ws *WriteSet) SetEmptyRemoval(addr accounts.Address, version Version) {
	ws.deleteAddr(addr)
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: version},
		Val:         true,
	})
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: version, valStatus: ValueDeleted},
	})
}

// SetContractRemoval is SetEmptyRemoval for a destroyed contract rather than a
// touched-empty EOA. It additionally stamps the CodeHash ValueDeleted — the "code now
// removed" signal that survives the footprint clear, letting the fee calc tell a
// destroyed contract (burn the delayed fee, stay removed) from an EIP-161 empty EOA
// (credit the fee, revive).
func (ws *WriteSet) SetContractRemoval(addr accounts.Address, version Version) {
	ws.SetEmptyRemoval(addr, version)
	ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{
		WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: version, valStatus: ValueDeleted},
		Val:         accounts.EmptyCodeHash,
	})
}

// MarkSelfDestructDeleted retracts a removal a worker already emitted: it stamps the
// SelfDestruct cell ValueDeleted, which the lifecycle reads as retracted — the account
// stays live and keeps its storage, while the cell still records that a wipe happened.
// calcFees uses it when a fee credit revives a touched-empty recipient the worker removed.
func (ws *WriteSet) MarkSelfDestructDeleted(addr accounts.Address, version Version) {
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: version, valStatus: ValueDeleted},
		Val:         true,
	})
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
// createObject/createAccount overwrite when (re)creating an account, so a
// reverted recreation can restore the prior writes.
type createWriteSnapshot struct {
	address        *VersionedWrite[*accounts.Account]
	balance        *VersionedWrite[uint256.Int]
	nonce          *VersionedWrite[uint64]
	incarnation    *VersionedWrite[uint64]
	selfDestruct   *VersionedWrite[bool]
	createContract *VersionedWrite[bool]
	codeHash       *VersionedWrite[accounts.CodeHash]
}

// snapshotCreateFields clones the account-record writes that a subsequent
// createObject/createAccount will overwrite, so a reverted recreation can
// restore them. code/codeSize/storage are excluded: they carry their own
// field-level journal entries that self-revert.
func (ws *WriteSet) snapshotCreateFields(addr accounts.Address) *createWriteSnapshot {
	snap := &createWriteSnapshot{}
	if vw, ok := ws.address[addr]; ok {
		snap.address = cloneVW(vw)
	}
	if vw, ok := ws.balance[addr]; ok {
		snap.balance = cloneVW(vw)
	}
	if vw, ok := ws.nonce[addr]; ok {
		snap.nonce = cloneVW(vw)
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
	delete(ws.nonce, addr)
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
	if snap.nonce != nil {
		ws.SetNonce(addr, snap.nonce)
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

// Count is the total number of writes across all paths.
func (ws *WriteSet) Count() int {
	if ws == nil {
		return 0
	}
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
	for a := range ws.address {
		f(a)
	}
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
func (ws *WriteSet) Balances() iter.Seq2[accounts.Address, *VersionedWrite[uint256.Int]] {
	if ws == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[uint256.Int](nil))
	}
	return maps.All(ws.balance)
}

func (ws *WriteSet) Nonces() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	if ws == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[uint64](nil))
	}
	return maps.All(ws.nonce)
}

func (ws *WriteSet) Incarnations() iter.Seq2[accounts.Address, *VersionedWrite[uint64]] {
	if ws == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[uint64](nil))
	}
	return maps.All(ws.incarnation)
}

func (ws *WriteSet) SelfDestructs() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	if ws == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[bool](nil))
	}
	return maps.All(ws.selfDestruct)
}

func (ws *WriteSet) CreateContracts() iter.Seq2[accounts.Address, *VersionedWrite[bool]] {
	if ws == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[bool](nil))
	}
	return maps.All(ws.createContract)
}

func (ws *WriteSet) Codes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.Code]] {
	if ws == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[accounts.Code](nil))
	}
	return maps.All(ws.code)
}

func (ws *WriteSet) CodeHashes() iter.Seq2[accounts.Address, *VersionedWrite[accounts.CodeHash]] {
	if ws == nil {
		return maps.All(map[accounts.Address]*VersionedWrite[accounts.CodeHash](nil))
	}
	return maps.All(ws.codeHash)
}

// StorageChanges has nothing to filter: a plain WriteSet carries no version map
// to compare a write against, so its writes are already the changed ones.
func (ws *WriteSet) StorageChanges() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	return ws.Storages()
}

func (ws *WriteSet) AccountChanges() iter.Seq[accounts.Address] { return accountChangeAddrs(ws) }

func (ws *WriteSet) Storages() iter.Seq2[accounts.Address, map[accounts.StorageKey]*VersionedWrite[uint256.Int]] {
	if ws == nil {
		return maps.All(map[accounts.Address]map[accounts.StorageKey]*VersionedWrite[uint256.Int](nil))
	}
	return maps.All(ws.storage)
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
		for h := range ws.accountHeaders() {
			if !yield(h) {
				return
			}
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

// accountHeaders is AllHeaders without the storage writes.
func (ws *WriteSet) accountHeaders() iter.Seq[WriteHeader] {
	return func(yield func(WriteHeader) bool) {
		if ws == nil {
			return
		}
		_ = eachWriteHeaderOf(ws.address, yield) &&
			eachWriteHeaderOf(ws.balance, yield) &&
			eachWriteHeaderOf(ws.nonce, yield) &&
			eachWriteHeaderOf(ws.incarnation, yield) &&
			eachWriteHeaderOf(ws.selfDestruct, yield) &&
			eachWriteHeaderOf(ws.createContract, yield) &&
			eachWriteHeaderOf(ws.code, yield) &&
			eachWriteHeaderOf(ws.codeHash, yield) &&
			eachWriteHeaderOf(ws.codeSize, yield)
	}
}

// ReleaseAndReset returns every *VersionedWrite[T] to its typed pool, or rewinds the set's arena, then the per-path maps to their map-pools (see vwMapPool). Called at tx-finalize.
func (ws *WriteSet) ReleaseAndReset() {
	if ws.cells != nil {
		ws.ReleaseMaps()
		ws.cells.reset()
		return
	}
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
}

// ReleaseMaps returns the map containers to their pools without releasing the
// VersionedWrite values. Use ReleaseAndReset only for sets whose VWs came from
// the vwPools and are exclusively owned.
func (ws *WriteSet) ReleaseMaps() {
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
	cells := ws.cells
	*ws = WriteSet{}
	ws.cells = cells
}

// delCell drops addr's cell, returning it to its pool unless the set recycles
// its own cells.
func delCell[K comparable, T any](ws *WriteSet, m map[K]*VersionedWrite[T], k K, release func(*VersionedWrite[T])) {
	vw, ok := m[k]
	if !ok {
		return
	}
	if ws.cells == nil {
		release(vw)
	}
	delete(m, k)
}

func (ws *WriteSet) DelBalance(addr accounts.Address) {
	delCell(ws, ws.balance, addr, releaseVWBalance)
}

func (ws *WriteSet) DelNonce(addr accounts.Address) {
	delCell(ws, ws.nonce, addr, releaseVWNonce)
}

func (ws *WriteSet) DelIncarnation(addr accounts.Address) {
	delCell(ws, ws.incarnation, addr, releaseVWIncarnation)
}

func (ws *WriteSet) DelSelfDestruct(addr accounts.Address) {
	delCell(ws, ws.selfDestruct, addr, releaseVWSelfDestruct)
}

func (ws *WriteSet) DelCode(addr accounts.Address) {
	delCell(ws, ws.code, addr, releaseVWCode)
}

func (ws *WriteSet) DelCodeHash(addr accounts.Address) {
	delCell(ws, ws.codeHash, addr, releaseVWCodeHash)
}

func (ws *WriteSet) DelCodeSize(addr accounts.Address) {
	delCell(ws, ws.codeSize, addr, releaseVWCodeSize)
}

func (ws *WriteSet) DelStorage(addr accounts.Address, key accounts.StorageKey) {
	inner := ws.storage[addr]
	if inner == nil {
		return
	}
	delCell(ws, inner, key, releaseVWStorage)
	if len(inner) == 0 {
		delete(ws.storage, addr)
	}
}

// updateBalance, updateNonce etc. are typed in-place mutations on the
// existing per-path entry.  No-op when no entry exists for addr.
func (ws *WriteSet) updateBalance(addr accounts.Address, val uint256.Int) {
	if vw, ok := ws.balance[addr]; ok {
		vw.Val = val
		// Reverting restores a live prior balance, so clear any ValueDeleted stamp a
		// self-destruct left on the cell — otherwise GetAccount still reads the
		// account as removed after its destruct was rolled back.
		vw.valStatus = ValueChanged
	}
}

func (ws *WriteSet) updateStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	if inner := ws.storage[addr]; inner != nil {
		if vw, ok := inner[key]; ok {
			vw.Val = val
			// A journal revert restores an earlier value; re-stamp against the
			// captured origin so valStatus tracks the restored value, not the
			// reverted write it replaced.
			vw.valStatus = storageValueStatus(vw.orig, val)
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
		if v, ok := value.(accounts.Code); ok {
			l := min(v.Len(), 40)
			return hex.EncodeToString(v.Bytes[0:l])
		}
		return "<unknown-code>"
	}

	return fmt.Sprint(value)
}

type versionedStateReader struct {
	txIndex     int
	reads       ReadSet
	versionMap  *VersionMap
	stateReader StateReader
}

func NewVersionedStateReader(txIndex int, reads ReadSet, versionMap *VersionMap, stateReader StateReader) *versionedStateReader {
	return &versionedStateReader{txIndex: txIndex, reads: reads, versionMap: versionMap, stateReader: stateReader}
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
	if r, ok := vr.reads.GetAddress(address); ok && r.Val != nil && !r.Val.IsNil() {
		account := r.Val.Account()
		updated := vr.applyVersionedUpdates(address, *account)
		return &updated, nil
	}

	// Check version map for AddressPath — handles accounts created by
	// prior transactions in the same block that aren't in the read set.
	if vr.versionMap != nil {
		// A prior tx may have self-destructed this account. Honor the destruct
		// only if no subsequent write at a strictly higher TxIndex re-creates it,
		// so a later tip to a pruned coinbase surfaces the re-created account.
		if vr.versionMap.IsNetAbsent(address, vr.txIndex) {
			// A self-destruct stamps its balance write ValueDeleted when it burns the
			// account; a balance-preserving destruct (EIP-8246) leaves a non-deleting
			// balance. Reconstruct the balance-only EOA only in the latter case — the
			// delete/preserve decision rides the write, so no fork flag is needed. A
			// net-absent account with a non-zero kept balance surfaces (an empty one is
			// EIP-161-removed).
			if _, res, ok := vr.versionMap.ReadBalance(address, vr.txIndex); ok && res.ValStatus() != ValueDeleted {
				var synth accounts.Account
				if updated := vr.applyVersionedUpdates(address, synth); !updated.Balance.IsZero() {
					return &updated, nil
				}
			}
			return nil, nil
		}
		if acc, ok := versionedUpdateAddress(vr.versionMap, address, vr.txIndex); ok && acc != nil {
			updated := vr.applyVersionedUpdates(address, *acc)
			return &updated, nil
		}
	}

	if vr.stateReader != nil {
		account, err := vr.stateReader.ReadAccountData(address)
		if err != nil {
			return nil, err
		}

		if account != nil {
			updated := vr.applyVersionedUpdates(address, *account)
			return &updated, nil
		}
	}

	// No stateReader account and no AddressPath entry. The BAL pre-population
	// writes only field paths (not AddressPath), so an account created mid-block
	// purely by tip credits has post-tx balances in the versionMap that
	// ReadAccountData would otherwise miss. Synthesize an empty account and let
	// applyVersionedUpdates apply the BAL-preloaded fields.
	if vr.versionMap != nil {
		var synth accounts.Account
		updated := vr.applyVersionedUpdates(address, synth)
		// Only return it if at least one field was applied; else this address has
		// no versionMap entries and must stay absent.
		if updated != synth {
			return &updated, nil
		}
	}

	return nil, nil
}

func (vr *versionedStateReader) ReadAccountByPlainAddr(address common.Address) (*accounts.Account, error) {
	return vr.ReadAccountData(accounts.InternAddress(address))
}

func (vr *versionedStateReader) HasAccount(address common.Address) (bool, error) {
	acc, err := vr.ReadAccountByPlainAddr(address)
	return acc != nil, err
}

// Per-path versionedUpdate helpers return the cell value if a Done or Dependency
// (Estimate) write exists at txIndex — a Dependency cell holds the same latest
// in-block write a Done cell does, and finalize reconstruction must consume it
// rather than fall back to the pre-block DB value — else zero value and ok=false.

func versionedUpdateAddress(vm *VersionMap, addr accounts.Address, txIndex int) (*accounts.Account, bool) {
	val, res, ok := vm.ReadAddress(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return nil, false
}

func versionedUpdateBalance(vm *VersionMap, addr accounts.Address, txIndex int) (uint256.Int, bool) {
	val, res, ok := vm.ReadBalance(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return uint256.Int{}, false
}

// destroyedAbove reports whether a Done destruct below txIndex sits at or above
// the cell res read. A destruct erases nonce and code hash without a cell of its
// own, so such a cell is stale.
func (vm *VersionMap) destroyedAbove(addr accounts.Address, res ReadResult, txIndex int) bool {
	_, ok := vm.FindDoneSelfDestructInRange(addr, res.DepIdx(), txIndex, true)
	return ok
}

func versionedUpdateNonce(vm *VersionMap, addr accounts.Address, txIndex int) (uint64, bool) {
	val, res, ok := vm.ReadNonce(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		if vm.destroyedAbove(addr, res, txIndex) {
			return 0, true
		}
		return val, true
	}
	return 0, false
}

func versionedUpdateIncarnation(vm *VersionMap, addr accounts.Address, txIndex int) (uint64, bool) {
	val, res, ok := vm.ReadIncarnation(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return 0, false
}

func versionedUpdateCode(vm *VersionMap, addr accounts.Address, txIndex int) (accounts.Code, bool) {
	val, res, ok := vm.ReadCode(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return accounts.Code{}, false
}

func versionedUpdateCodeHash(vm *VersionMap, addr accounts.Address, txIndex int) (accounts.CodeHash, bool) {
	val, res, ok := vm.ReadCodeHash(addr, txIndex)
	if ok && res.Status() != MVReadResultNone {
		if vm.destroyedAbove(addr, res, txIndex) {
			return accounts.EmptyCodeHash, true
		}
		return val, true
	}
	return accounts.CodeHash{}, false
}

func versionedUpdateStorage(vm *VersionMap, addr accounts.Address, key accounts.StorageKey, txIndex int) (uint256.Int, bool) {
	val, res, ok := liveStorage(vm, addr, key, txIndex)
	if ok && res.Status() != MVReadResultNone {
		return val, true
	}
	return uint256.Int{}, false
}

// applyVersionedUpdates overlays versionMap field cells onto account, so field-only updates from prior txs are not missed when only a subset of fields was read.
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

		if account != nil {
			updated := vr.applyVersionedUpdates(address, *account)
			return &updated, nil
		}
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if r, ok := vr.reads.GetStorage(address, key); ok {
		return r.Val, true, nil
	}

	// Check version map for storage written by prior transactions.
	if vr.versionMap != nil {
		if state, _, destroyedAt, _ := vr.versionMap.AccountLifecycleAt(address, vr.txIndex); state != LifecycleLive {
			// Self-destructed in-block: a slot cell at or below the destruct is
			// wiped to zero; only a write above it survives.
			if val, res, ok := vr.versionMap.ReadStorage(address, key, vr.txIndex); ok &&
				res.resolved() && res.DepIdx() > destroyedAt {
				return val, true, nil
			}
			return uint256.Int{}, false, nil
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

func (vr versionedStateReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	if r, ok := vr.reads.GetCode(address); ok && r.Val.Bytes != nil {
		return r.Val.Bytes, nil
	}

	// Check version map for CodePath entries written by prior transactions
	// (e.g. EIP-7702 delegation set by an earlier tx in the same block).
	if vr.versionMap != nil {
		if state, _, destroyedAt, _ := vr.versionMap.AccountLifecycleAt(address, vr.txIndex); state != LifecycleLive {
			if code, res, ok := vr.versionMap.ReadCode(address, vr.txIndex); ok &&
				res.resolved() && res.DepIdx() > destroyedAt {
				return code.Bytes, nil
			}
			return nil, nil
		}
		if code, ok := versionedUpdateCode(vr.versionMap, address, vr.txIndex); ok {
			return code.Bytes, nil
		}
	}

	if vr.stateReader != nil {
		return vr.stateReader.ReadAccountCode(address)
	}

	return nil, nil
}

func (vr versionedStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	if r, ok := vr.reads.GetCode(address); ok && r.Val.Bytes != nil {
		return len(r.Val.Bytes), nil
	}

	if vr.versionMap != nil {
		if state, _, destroyedAt, _ := vr.versionMap.AccountLifecycleAt(address, vr.txIndex); state != LifecycleLive {
			if code, res, ok := vr.versionMap.ReadCode(address, vr.txIndex); ok &&
				res.resolved() && res.DepIdx() > destroyedAt {
				return len(code.Bytes), nil
			}
			return 0, nil
		}
		if code, ok := versionedUpdateCode(vr.versionMap, address, vr.txIndex); ok {
			return len(code.Bytes), nil
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

// SetAccountFieldFromMap resolves an account field from the version map and
// sets it into out, returning whether a Done value was found.
func SetAccountFieldFromMap(out *WriteSet, vm *VersionMap, addr accounts.Address, path AccountPath, ver Version, txIdx int) bool {
	switch path {
	case BalancePath:
		v, rr, found := vm.ReadBalance(addr, txIdx)
		if found && rr.resolved() {
			out.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: v})
			return true
		}
	case NoncePath:
		v, rr, found := vm.ReadNonce(addr, txIdx)
		if found && rr.resolved() {
			out.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: v})
			return true
		}
	case IncarnationPath:
		v, rr, found := vm.ReadIncarnation(addr, txIdx)
		if found && rr.resolved() {
			out.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}, Val: v})
			return true
		}
	case CodeHashPath:
		v, rr, found := vm.ReadCodeHash(addr, txIdx)
		if found && rr.resolved() {
			out.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: v})
			return true
		}
	}
	return false
}

// SetAccountFieldZero sets the post-destruction zero value for path into out. Path must be Balance/Nonce/Incarnation/CodeHash.
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

// SetAccountFieldFromAccount sets path's value from acc into out (acc may be nil — falls back to zero, except CodeHash becomes EmptyCodeHash).
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
func (ws *WriteSet) TouchUpdates(updates *commitment.Updates) {
	if ws == nil {
		return
	}
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

// Merge returns the union of prev and next, with next winning on (addr,path,key).
func (ws *WriteSet) Merge(next *WriteSet) *WriteSet {
	ws.assertNotArena("merge")
	next.assertNotArena("merge")
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

// StripBalanceWrite removes the BalancePath write for addr and returns the tx's
// net balance delta (write minus read). Finalize applies the delta on top of the
// correct base balance from the VersionedStateReader instead of applying the
// stale speculative write directly. increase reports the delta's sign; found is
// true only when a stale read and write yielded a non-zero delta.
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
		return int(io.inputs[txnIdx+1].incarnation)
	}
	return 0
}

func (io *VersionedIO) WriteSet(txnIdx int) *WriteSet {
	if len(io.outputs) <= txnIdx+1 {
		return nil
	}
	return io.outputs[txnIdx+1]
}

func (io *VersionedIO) WriteCount() (count int64) {
	for _, output := range io.outputs {
		count += int64(output.Count())
	}

	return count
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
	// EIP-7928 BAL.
	if reads.Len() > 0 || len(reads.access) > 0 {
		if io.inputs[idx].readSet.Len() == 0 && len(io.inputs[idx].readSet.access) == 0 {
			// First merge into an empty slot: hand the read set over directly
			// instead of deep-copying.
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
	b := NewBlockAccessListBuilder()
	b.AddRemaining(io)
	return b.Build()
}

// BlockAccessListBuilder assembles the block access list one tx index at a time,
// so a block's list can be built while its txs are published.
type BlockAccessListBuilder struct {
	ac   map[accounts.Address]*accountState
	next int
}

func NewBlockAccessListBuilder() *BlockAccessListBuilder {
	return &BlockAccessListBuilder{ac: make(map[accounts.Address]*accountState), next: -1}
}

// Next is the tx index Add expects; indexes start at -1, the pre-block system calls.
func (b *BlockAccessListBuilder) Next() int { return b.next }

// AddRemaining adds every index of io from Next on.
func (b *BlockAccessListBuilder) AddRemaining(io *VersionedIO) {
	for txIndex := b.next; txIndex <= io.Len()-1; txIndex++ {
		b.Add(txIndex, io.ReadSet(txIndex), io.WriteSet(txIndex))
	}
}

// Add folds the reads and writes recorded for txIndex, which must be Next.
func (b *BlockAccessListBuilder) Add(txIndex int, rs ReadSet, writes *WriteSet) {
	if txIndex != b.next {
		panic(fmt.Sprintf("block access list: added tx %d, expected %d", txIndex, b.next))
	}
	b.next++
	ac := b.ac
	// EIP-7928 requires every accessed address to appear in the BAL, so each per-path read map must register its address.
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
		// A pre-block-empty code hash marks the baseline empty so applyToCode
		// drops a net-zero same-tx set-then-clear. Only an empty read seen
		// before any code change is the pre-block value.
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

	if writes != nil {
		// Self-destruct is applied before the balance writes so the EIP-7928
		// burn (zeroing a non-zero balance written by the destroying tx) fires.
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
		// EIP-7928 requires every touched address to appear; a sweep over the
		// full address union covers the paths the value-carrying loops above
		// didn't already register.
		for addr := range writes.addrs() {
			if addr.IsNil() {
				continue
			}
			account := ensureAccountState(ac, addr)
			// A contract created this block has empty pre-block code; applyToCode
			// uses this to drop a net-zero empty code change.
			if vw, ok := writes.GetCreateContract(addr); ok && vw.Val {
				account.initialCodeEmpty = true
			}
		}
	}

	for addr := range rs.access {
		if addr.IsNil() {
			continue
		}
		ensureAccountState(ac, addr)
	}
}

// Build finalizes the list; the builder must not be used after it.
func (b *BlockAccessListBuilder) Build() types.BlockAccessList {
	// Sorting AccountChanges by value moves 144-byte structs and calls
	// bytes.Compare per step; sort small integer keys instead.
	type sortKey struct {
		hi, mid uint64
		lo      uint32
		account *accountState
	}
	keys := make([]sortKey, 0, len(b.ac))
	for _, account := range b.ac {
		a := &account.changes.Address
		keys = append(keys, sortKey{binary.BigEndian.Uint64(a[:8]), binary.BigEndian.Uint64(a[8:16]), binary.BigEndian.Uint32(a[16:]), account})
	}
	slices.SortFunc(keys, func(a, b sortKey) int {
		if c := cmp.Compare(a.hi, b.hi); c != 0 {
			return c
		}
		if c := cmp.Compare(a.mid, b.mid); c != 0 {
			return c
		}
		return cmp.Compare(a.lo, b.lo)
	})

	bal := make(types.BlockAccessList, 0, len(keys))
	for _, k := range keys {
		k.account.finalize()
		k.account.changes.Normalize()
		bal = append(bal, k.account.changes)
	}
	return bal
}

type accountState struct {
	changes             types.AccountChanges
	balance             fieldTracker[uint256.Int]
	nonce               fieldTracker[uint64]
	code                fieldTracker[accounts.Code]
	balanceValue        g.Option[uint256.Int]               // tracks latest seen balance
	initialBalanceValue g.Option[uint256.Int]               // tracks pre-block balance for net-zero detection
	storageReadValues   map[accounts.StorageKey]uint256.Int // original read values for net-zero detection
	slotWrites          map[accounts.StorageKey]int         // slot -> index into changes.StorageChanges, built past slotIndexMin
	storageReads        []accounts.StorageKey               // copied into changes.StorageReads by finalize
	slotReads           map[accounts.StorageKey]int         // slot -> index into storageReads, built with slotWrites
	initialCodeEmpty    bool                                // pre-block code was empty (created contract or empty-codehash read)
}

// check pre- and post-values, add to BAL if different
func (account *accountState) finalize() {
	if len(account.storageReads) > 0 {
		account.changes.StorageReads = make([]common.Hash, len(account.storageReads))
		for i, slot := range account.storageReads {
			account.changes.StorageReads[i] = slot.Value()
		}
	}
	applyToBalance(&account.balance, &account.changes, account.initialBalanceValue)
	applyToNonce(&account.nonce, &account.changes)
	applyToCode(&account.code, &account.changes, account.initialCodeEmpty)
}

type fieldTracker[T any] struct {
	changes changeTracker[T]
}

func (ft *fieldTracker[T]) recordWrite(idx uint32, value T) {
	ft.changes.recordWrite(idx, value)
}

func applyToBalance(bt *fieldTracker[uint256.Int], ac *types.AccountChanges, initialBalance g.Option[uint256.Int]) {
	// Get the sorted indices to identify the first (lowest-index) entry.
	// If the first entry equals the pre-block balance, it's a net-zero
	// change from a reverted tx (e.g. CALL with value then revert) and
	// must be excluded. Geth's scope-based BAL builder handles this via
	// ExitScope which converts reverted writes to reads.
	firstFiltered := false
	bt.changes.apply(func(idx uint32, value uint256.Int) {
		if !firstFiltered {
			firstFiltered = true
			if initialBalance.Ok && value.Eq(&initialBalance.Value) {
				return
			}
		}
		ac.BalanceChanges = append(ac.BalanceChanges, &types.BalanceChange{
			Index: idx,
			Value: value,
		})
	})
}

func applyToNonce(nt *fieldTracker[uint64], ac *types.AccountChanges) {
	nt.changes.apply(func(idx uint32, value uint64) {
		ac.NonceChanges = append(ac.NonceChanges, &types.NonceChange{
			Index: idx,
			Value: value,
		})
	})
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

func newAccountState(addr accounts.Address) *accountState {
	return &accountState{changes: types.AccountChanges{Address: addr.Value()}}
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
		// A first zero write is a no-op touch only when the pre-block balance is
		// (or is implicitly) zero. A zero write over a non-zero pre-block balance
		// is a genuine depletion and must be recorded.
		if !account.balanceValue.Ok && val.IsZero() &&
			(!account.initialBalanceValue.Ok || account.initialBalanceValue.Value.IsZero()) {
			if !account.initialBalanceValue.Ok {
				account.initialBalanceValue.Set(val)
			}
			account.balanceValue.Set(val)
			return
		}
		// Skip no-op writes.
		if account.balanceValue.Ok && val.Eq(&account.balanceValue.Value) {
			account.balanceValue.Set(val)
			return
		}
		// Skip balance writes that match the pre-block (initial) balance,
		// but ONLY when no intermediate balance changes have been recorded.
		// If intermediate changes exist, a write restoring the initial value
		// MUST still be recorded so parallel executors see the correct value.
		if account.initialBalanceValue.Ok && val.Eq(&account.initialBalanceValue.Value) && len(account.balance.changes.entries) == 0 {
			account.balanceValue.Set(val)
			return
		}
		account.balanceValue.Set(val)
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
		account.slotReads[key] = len(account.storageReads)
	}
	account.storageReads = append(account.storageReads, key)
}

func (account *accountState) updateReadBalance(val uint256.Int) {
	// Record the pre-block balance for net-zero detection from the first read
	// only, and only before any write: a read arriving after a write reflects
	// post-write state, not the pre-block balance.
	if !account.initialBalanceValue.Ok && !account.balanceValue.Ok {
		account.initialBalanceValue.Set(val)
	}
	// After a write, balanceValue tracks the written state; a stale DB read must
	// not override it or the no-op check would skip a real write.
	if len(account.balance.changes.entries) == 0 {
		account.balanceValue.Set(val)
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

	ac := &account.changes
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
		account.slotReads = make(map[accounts.StorageKey]int, len(account.storageReads))
		deduped := account.storageReads[:0]
		for _, slot := range account.storageReads {
			if _, seen := account.slotReads[slot]; seen {
				continue
			}
			account.slotReads[slot] = len(deduped)
			deduped = append(deduped, slot)
		}
		account.storageReads = deduped
	}
}

// removeStorageRead drops slot from StorageReads, because a write to it takes
// precedence. Past slotIndexMin the position is indexed and the last entry is
// swapped in; order does not survive Normalize's sort anyway.
func (account *accountState) removeStorageRead(slot accounts.StorageKey) {
	reads := account.storageReads
	if len(reads) == 0 {
		return
	}
	if account.slotReads == nil {
		out := reads[:0]
		for _, s := range reads {
			if s != slot {
				out = append(out, s)
			}
		}
		account.storageReads = out
		return
	}
	i, ok := account.slotReads[slot]
	if !ok {
		return
	}
	delete(account.slotReads, slot)
	last := len(reads) - 1
	if i != last {
		reads[i] = reads[last]
		account.slotReads[reads[i]] = i
	}
	account.storageReads = reads[:last]
}

type versionedReadSet struct {
	incarnation Incarnation
	readSet     ReadSet
}

// AllHeaders iterates the type-agnostic header of every read in the set.
func (s versionedReadSet) AllHeaders() iter.Seq[ReadHeader] {
	return func(yield func(ReadHeader) bool) {
		s.readSet.EachHeader(yield)
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
	for addr, writes := range txFrom.storage {
		if anyCommonKey(writes, txTo.storage[addr]) {
			return true
		}
	}
	for h := range txFrom.accountHeaders() {
		if txTo.DependsOn(h) {
			return true
		}
	}
	return false
}

// DependsOnAt is HasReadDep for the writes ws makes at addr.
func (s ReadSet) DependsOnAt(ws *WriteSet, addr accounts.Address) bool {
	if ws == nil {
		return false
	}
	if anyCommonKey(ws.storage[addr], s.storage[addr]) {
		return true
	}
	for _, p := range [...]AccountPath{AddressPath, SelfDestructPath, BalancePath, NoncePath, IncarnationPath, CodePath, CodeHashPath, CodeSizePath, CreateContractPath} {
		h := WriteHeader{Address: addr, Path: p}
		if ws.hasHeader(h) && s.DependsOn(h) {
			return true
		}
	}
	return false
}

// anyCommonKey probes the smaller map's keys in the larger one.
func anyCommonKey[K comparable, A, B any](a map[K]A, b map[K]B) bool {
	if len(a) > len(b) {
		return anyCommonKey(b, a)
	}
	for k := range a {
		if _, ok := b[k]; ok {
			return true
		}
	}
	return false
}

// DependsOn reports whether a write of h can invalidate a read in s: a read of the same
// cell, or any read of the address when h changes the account lifecycle.
func (s ReadSet) DependsOn(h WriteHeader) bool {
	if _, ok := s.getHeader(h.Address, h.Path, h.Key); ok {
		return true
	}
	return h.Path.AffectsAccountLifecycle() && s.hasAnyReadAt(h.Address)
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
