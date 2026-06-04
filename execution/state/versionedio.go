package state

import (
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/heimdalr/dag"
	"github.com/holiman/uint256"

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
//
// touched is the address-level ephemeral-access layer: an address marked
// "touched" (a non-revertable EVM operation or an incidental revertable
// gas-calc access) that has no path read of its own — e.g. the coinbase or
// an EIP-7702 authority.  The map value is the revertable flag; a touch
// carries no version and is not conflict-detected.  It feeds the EIP-7928
// block access list only.
type ReadSet struct {
	address        map[accounts.Address]VersionedRead[AccountView]
	balance        map[accounts.Address]VersionedRead[uint256.Int]
	nonce          map[accounts.Address]VersionedRead[uint64]
	incarnation    map[accounts.Address]VersionedRead[uint64]
	selfDestruct   map[accounts.Address]VersionedRead[bool]
	createContract map[accounts.Address]VersionedRead[bool]
	code           map[accounts.Address]VersionedRead[[]byte]
	codeHash       map[accounts.Address]VersionedRead[accounts.CodeHash]
	codeSize       map[accounts.Address]VersionedRead[int]
	storage        map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int]
	touched        map[accounts.Address]bool // value = revertable
}

// Touch records an address-level ephemeral access.  revertable=false (a real
// EVM operation) is sticky — once an address has a non-revertable touch a
// later revertable touch does not downgrade it.
//
// A touch is non-versioned / not conflict-detected. That is safe only under
// the invariant that every touch is accompanied by a versioned read of the
// same address (which carries the conflict detection) or is deterministic
// (coinbase, EIP-7702 authority) — see MarkAddressAccess.
func (s *ReadSet) Touch(addr accounts.Address, revertable bool) {
	if s.touched == nil {
		s.touched = rsGetTouched()
	}
	if cur, ok := s.touched[addr]; ok {
		if cur && !revertable {
			s.touched[addr] = false
		}
		return
	}
	s.touched[addr] = revertable
}

// vrMapPool[T] pools the per-path map[Address]VersionedRead[T] containers.
// Same shape as vwMapPool but for value-typed VersionedRead (no inner
// pointer — no value pool needed, just the map header reuse).
type vrMapPool[T any] struct{ p sync.Pool }

func newVRMapPool[T any]() *vrMapPool[T] {
	return &vrMapPool[T]{p: sync.Pool{New: func() any {
		return make(map[accounts.Address]VersionedRead[T])
	}}}
}

func (mp *vrMapPool[T]) get() map[accounts.Address]VersionedRead[T] {
	return mp.p.Get().(map[accounts.Address]VersionedRead[T])
}

func (mp *vrMapPool[T]) put(m map[accounts.Address]VersionedRead[T]) {
	if m == nil {
		return
	}
	clear(m)
	mp.p.Put(m)
}

var (
	rsMapPoolAddress        = newVRMapPool[AccountView]()
	rsMapPoolBalance        = newVRMapPool[uint256.Int]()
	rsMapPoolNonce          = newVRMapPool[uint64]()
	rsMapPoolIncarnation    = newVRMapPool[uint64]()
	rsMapPoolSelfDestruct   = newVRMapPool[bool]()
	rsMapPoolCreateContract = newVRMapPool[bool]()
	rsMapPoolCode           = newVRMapPool[[]byte]()
	rsMapPoolCodeHash       = newVRMapPool[accounts.CodeHash]()
	rsMapPoolCodeSize       = newVRMapPool[int]()
	rsMapPoolStorageInner   = sync.Pool{New: func() any {
		return make(map[accounts.StorageKey]VersionedRead[uint256.Int])
	}}
	rsMapPoolStorageOuter = sync.Pool{New: func() any {
		return make(map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int])
	}}
	rsMapPoolTouched = sync.Pool{New: func() any { return make(map[accounts.Address]bool) }}
)

func rsGetStorageInner() map[accounts.StorageKey]VersionedRead[uint256.Int] {
	return rsMapPoolStorageInner.Get().(map[accounts.StorageKey]VersionedRead[uint256.Int])
}

func rsPutStorageInner(m map[accounts.StorageKey]VersionedRead[uint256.Int]) {
	if m == nil {
		return
	}
	clear(m)
	rsMapPoolStorageInner.Put(m)
}

func rsGetStorageOuter() map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int] {
	return rsMapPoolStorageOuter.Get().(map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int])
}

func rsPutStorageOuter(m map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int]) {
	if m == nil {
		return
	}
	clear(m)
	rsMapPoolStorageOuter.Put(m)
}

func rsGetTouched() map[accounts.Address]bool {
	return rsMapPoolTouched.Get().(map[accounts.Address]bool)
}

func rsPutTouched(m map[accounts.Address]bool) {
	if m == nil {
		return
	}
	clear(m)
	rsMapPoolTouched.Put(m)
}

// readSetPut lazily checks out a pooled map and inserts tr at addr.
func readSetPut[T any](m *map[accounts.Address]VersionedRead[T], addr accounts.Address, tr VersionedRead[T], pool *vrMapPool[T]) {
	if *m == nil {
		*m = pool.get()
	}
	(*m)[addr] = tr
}

func (s *ReadSet) SetAddress(addr accounts.Address, tr VersionedRead[AccountView]) {
	readSetPut(&s.address, addr, tr, rsMapPoolAddress)
}
func (s *ReadSet) SetBalance(addr accounts.Address, tr VersionedRead[uint256.Int]) {
	readSetPut(&s.balance, addr, tr, rsMapPoolBalance)
}
func (s *ReadSet) SetNonce(addr accounts.Address, tr VersionedRead[uint64]) {
	readSetPut(&s.nonce, addr, tr, rsMapPoolNonce)
}
func (s *ReadSet) SetIncarnation(addr accounts.Address, tr VersionedRead[uint64]) {
	readSetPut(&s.incarnation, addr, tr, rsMapPoolIncarnation)
}
func (s *ReadSet) SetSelfDestruct(addr accounts.Address, tr VersionedRead[bool]) {
	readSetPut(&s.selfDestruct, addr, tr, rsMapPoolSelfDestruct)
}
func (s *ReadSet) SetCreateContract(addr accounts.Address, tr VersionedRead[bool]) {
	readSetPut(&s.createContract, addr, tr, rsMapPoolCreateContract)
}
func (s *ReadSet) SetCode(addr accounts.Address, tr VersionedRead[[]byte]) {
	readSetPut(&s.code, addr, tr, rsMapPoolCode)
}
func (s *ReadSet) SetCodeHash(addr accounts.Address, tr VersionedRead[accounts.CodeHash]) {
	readSetPut(&s.codeHash, addr, tr, rsMapPoolCodeHash)
}
func (s *ReadSet) SetCodeSize(addr accounts.Address, tr VersionedRead[int]) {
	readSetPut(&s.codeSize, addr, tr, rsMapPoolCodeSize)
}
func (s *ReadSet) SetStorage(addr accounts.Address, key accounts.StorageKey, tr VersionedRead[uint256.Int]) {
	if s.storage == nil {
		s.storage = rsGetStorageOuter()
	}
	inner := s.storage[addr]
	if inner == nil {
		inner = rsGetStorageInner()
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
		s.SetCode(addr, VersionedRead[[]byte]{ReadHeader: hdr})
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

// Len returns the total entry count across all paths plus address-level
// touches.  Value receiver so it can be called on a function-returned read
// set directly.
func (s ReadSet) Len() int {
	n := len(s.address) + len(s.balance) + len(s.nonce) + len(s.incarnation) +
		len(s.selfDestruct) + len(s.createContract) +
		len(s.code) + len(s.codeHash) + len(s.codeSize) + len(s.touched)
	for _, inner := range s.storage {
		n += len(inner)
	}
	return n
}

// mergeFrom copies every entry of src into s, overwriting on collision.
func (s *ReadSet) mergeFrom(src ReadSet) {
	for a, tr := range src.address {
		readSetPut(&s.address, a, tr, rsMapPoolAddress)
	}
	for a, tr := range src.balance {
		readSetPut(&s.balance, a, tr, rsMapPoolBalance)
	}
	for a, tr := range src.nonce {
		readSetPut(&s.nonce, a, tr, rsMapPoolNonce)
	}
	for a, tr := range src.incarnation {
		readSetPut(&s.incarnation, a, tr, rsMapPoolIncarnation)
	}
	for a, tr := range src.selfDestruct {
		readSetPut(&s.selfDestruct, a, tr, rsMapPoolSelfDestruct)
	}
	for a, tr := range src.createContract {
		readSetPut(&s.createContract, a, tr, rsMapPoolCreateContract)
	}
	for a, tr := range src.code {
		readSetPut(&s.code, a, tr, rsMapPoolCode)
	}
	for a, tr := range src.codeHash {
		readSetPut(&s.codeHash, a, tr, rsMapPoolCodeHash)
	}
	for a, tr := range src.codeSize {
		readSetPut(&s.codeSize, a, tr, rsMapPoolCodeSize)
	}
	for a, inner := range src.storage {
		for k, tr := range inner {
			s.SetStorage(a, k, tr)
		}
	}
	for a, revertable := range src.touched {
		s.Touch(a, revertable)
	}
}

// Release returns every per-path map held by the set to its pool, then
// zeros the fields.  Mirrors WriteSet.ReleaseAndReset but no value-pool
// walk — VersionedRead is by-value, not a pointer.
//
// Callers: per-tx finalize on the IBS field (ResetVersionedIO /
// ResetVersionedReads); block-end on every io.inputs[].readSet held in
// the VersionedIO accumulator (see VersionedIO.Release).
func (s *ReadSet) Release() {
	rsMapPoolAddress.put(s.address)
	rsMapPoolBalance.put(s.balance)
	rsMapPoolNonce.put(s.nonce)
	rsMapPoolIncarnation.put(s.incarnation)
	rsMapPoolSelfDestruct.put(s.selfDestruct)
	rsMapPoolCreateContract.put(s.createContract)
	rsMapPoolCode.put(s.code)
	rsMapPoolCodeHash.put(s.codeHash)
	rsMapPoolCodeSize.put(s.codeSize)
	for _, inner := range s.storage {
		rsPutStorageInner(inner)
	}
	rsPutStorageOuter(s.storage)
	rsPutTouched(s.touched)
	*s = ReadSet{}
}

// Merge returns a new read set containing every entry of s then o.  On a
// collision o's entry wins.
func (s ReadSet) Merge(o ReadSet) ReadSet {
	var out ReadSet
	out.mergeFrom(s)
	out.mergeFrom(o)
	return out
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

// Balances iterates the balance reads.
func (s ReadSet) Balances() iter.Seq2[accounts.Address, VersionedRead[uint256.Int]] {
	return func(yield func(accounts.Address, VersionedRead[uint256.Int]) bool) {
		for a, tr := range s.balance {
			if !yield(a, tr) {
				return
			}
		}
	}
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
	Address accounts.Address
	Path    AccountPath
	Key     accounts.StorageKey
	Version Version
	Reason  tracing.BalanceChangeReason
}

// VersionedWrite is a single versioned write.  The per-path map it lives
// in fixes the address (map key), path — and, for storage, the slot key.
// The struct carries the header and the path-typed value.
type VersionedWrite[T any] struct {
	WriteHeader
	Val T
}

// AnyVersionedWrite is the type-erased view of a *VersionedWrite[T].
// Cold consumers (flush, BAL emit, debug) use this interface; the typed
// per-path write set carries the concrete *VersionedWrite[T] without
// crossing the any boundary on the hot path.
type AnyVersionedWrite interface {
	Header() WriteHeader
	ValAny() any
	Clone() AnyVersionedWrite
}

func (w *VersionedWrite[T]) Header() WriteHeader { return w.WriteHeader }
func (w *VersionedWrite[T]) ValAny() any         { return w.Val }
func (w *VersionedWrite[T]) Clone() AnyVersionedWrite {
	c := *w
	return &c
}

func (w *VersionedWrite[T]) String() string {
	return WriteString(w)
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

// WriteString formats an AnyVersionedWrite for trace/debug output.
// Boxes once per call via the AnyVersionedWrite interface — trace path only.
func WriteString(w AnyVersionedWrite) string {
	h := w.Header()
	return fmt.Sprintf("%x %s: %s (%d.%d)", h.Address, AccountKey{Path: h.Path, Key: h.Key}, valueStringFromAnyVW(w), h.Version.TxIndex, h.Version.Incarnation)
}

// valueStringFromAnyVW formats the typed Val for trace/debug output.
// Goes through the AnyVersionedWrite interface (boxing acceptable —
// trace path only).
func valueStringFromAnyVW(w AnyVersionedWrite) string {
	hdr := w.Header()
	switch hdr.Path {
	case AddressPath:
		acc, _ := w.ValAny().(*accounts.Account)
		return fmt.Sprintf("%+v", acc)
	case BalancePath:
		num, _ := w.ValAny().(uint256.Int)
		return (&num).String()
	case StoragePath:
		num, _ := w.ValAny().(uint256.Int)
		return fmt.Sprintf("%x", &num)
	case NoncePath, IncarnationPath:
		v, _ := w.ValAny().(uint64)
		return strconv.FormatUint(v, 10)
	case CodePath:
		code, _ := w.ValAny().(accounts.Code)
		l := min(len(code.Bytes), 40)
		return hex.EncodeToString(code.Bytes[0:l])
	case SelfDestructPath, CreateContractPath:
		v, _ := w.ValAny().(bool)
		if v {
			return "true"
		}
		return "false"
	case CodeHashPath:
		h, _ := w.ValAny().(accounts.CodeHash)
		return fmt.Sprintf("%x", h.Value())
	case CodeSizePath:
		v, _ := w.ValAny().(int)
		return strconv.Itoa(v)
	}
	return "<unknown-path>"
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

func (s *WriteSet) GetAddress(addr accounts.Address) (*VersionedWrite[*accounts.Account], bool) {
	vw, ok := s.address[addr]
	return vw, ok
}
func (s *WriteSet) GetBalance(addr accounts.Address) (*VersionedWrite[uint256.Int], bool) {
	vw, ok := s.balance[addr]
	return vw, ok
}
func (s *WriteSet) GetNonce(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	vw, ok := s.nonce[addr]
	return vw, ok
}
func (s *WriteSet) GetIncarnation(addr accounts.Address) (*VersionedWrite[uint64], bool) {
	vw, ok := s.incarnation[addr]
	return vw, ok
}
func (s *WriteSet) GetSelfDestruct(addr accounts.Address) (*VersionedWrite[bool], bool) {
	vw, ok := s.selfDestruct[addr]
	return vw, ok
}
func (s *WriteSet) GetCreateContract(addr accounts.Address) (*VersionedWrite[bool], bool) {
	vw, ok := s.createContract[addr]
	return vw, ok
}
func (s *WriteSet) GetCode(addr accounts.Address) (*VersionedWrite[accounts.Code], bool) {
	vw, ok := s.code[addr]
	return vw, ok
}
func (s *WriteSet) GetCodeHash(addr accounts.Address) (*VersionedWrite[accounts.CodeHash], bool) {
	vw, ok := s.codeHash[addr]
	return vw, ok
}
func (s *WriteSet) GetCodeSize(addr accounts.Address) (*VersionedWrite[int], bool) {
	vw, ok := s.codeSize[addr]
	return vw, ok
}
func (s *WriteSet) GetStorage(addr accounts.Address, key accounts.StorageKey) (*VersionedWrite[uint256.Int], bool) {
	inner := s.storage[addr]
	if inner == nil {
		return nil, false
	}
	vw, ok := inner[key]
	return vw, ok
}

// hasAddr reports whether any path has an entry for addr.
func (s *WriteSet) hasAddr(addr accounts.Address) bool {
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

// addrs returns the union of all addresses that have at least one entry.
// Iteration order across the union is non-deterministic — caller sorts
// before use if determinism matters.
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

// scanAddrMap visits the addr entry of one non-storage path map if present.
func scanAddrMap[T any](m map[accounts.Address]*VersionedWrite[T], addr accounts.Address, fn func(vw AnyVersionedWrite)) {
	if vw, ok := m[addr]; ok {
		fn(vw)
	}
}

// scanAddr visits every write under addr (all paths).
func (s *WriteSet) scanAddr(addr accounts.Address, fn func(vw AnyVersionedWrite)) {
	scanAddrMap(s.address, addr, fn)
	scanAddrMap(s.balance, addr, fn)
	scanAddrMap(s.nonce, addr, fn)
	scanAddrMap(s.incarnation, addr, fn)
	scanAddrMap(s.selfDestruct, addr, fn)
	scanAddrMap(s.createContract, addr, fn)
	scanAddrMap(s.code, addr, fn)
	scanAddrMap(s.codeHash, addr, fn)
	scanAddrMap(s.codeSize, addr, fn)
	if inner, ok := s.storage[addr]; ok {
		for _, vw := range inner {
			fn(vw)
		}
	}
}

// scanMap yields every entry of a per-path typed map through the
// AnyVersionedWrite interface; callback returns false to stop early.
func scanMap[T any](m map[accounts.Address]*VersionedWrite[T], fn func(vw AnyVersionedWrite) bool) bool {
	for _, vw := range m {
		if !fn(vw) {
			return false
		}
	}
	return true
}

// scan visits every write entry.  Iteration order is path-major.
func (s *WriteSet) scan(fn func(vw AnyVersionedWrite) bool) bool {
	if !scanMap(s.address, fn) ||
		!scanMap(s.balance, fn) ||
		!scanMap(s.nonce, fn) ||
		!scanMap(s.incarnation, fn) ||
		!scanMap(s.selfDestruct, fn) ||
		!scanMap(s.createContract, fn) ||
		!scanMap(s.code, fn) ||
		!scanMap(s.codeHash, fn) ||
		!scanMap(s.codeSize, fn) {
		return false
	}
	for _, inner := range s.storage {
		for _, vw := range inner {
			if !fn(vw) {
				return false
			}
		}
	}
	return true
}

func (s *WriteSet) delAddr(addr accounts.Address) {
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

// ReleaseAndReset returns every *VersionedWrite[T] held by the set to its
// typed sync.Pool, then returns the per-path maps to their map-pools.
// Called at tx-finalize so both the VW values and the map buckets cycle
// through pools rather than getting GC'd.
//
// Per-path sequence: walk the map releasing each value first; then the
// map-pool's put() does clear() + Put, preserving the bucket array for
// the next tx's checkout.  Pool-resident containers want clear() — the
// usual "clear doesn't free memory" critique becomes a feature here.
func (s *WriteSet) ReleaseAndReset() {
	for _, vw := range s.address {
		releaseVWAddress(vw)
	}
	wsMapPoolAddress.put(s.address)
	for _, vw := range s.balance {
		releaseVWBalance(vw)
	}
	wsMapPoolBalance.put(s.balance)
	for _, vw := range s.nonce {
		releaseVWNonce(vw)
	}
	wsMapPoolNonce.put(s.nonce)
	for _, vw := range s.incarnation {
		releaseVWIncarnation(vw)
	}
	wsMapPoolIncarnation.put(s.incarnation)
	for _, vw := range s.selfDestruct {
		releaseVWSelfDestruct(vw)
	}
	wsMapPoolSelfDestruct.put(s.selfDestruct)
	for _, vw := range s.createContract {
		releaseVWCreateContract(vw)
	}
	wsMapPoolCreateContract.put(s.createContract)
	for _, vw := range s.code {
		releaseVWCode(vw)
	}
	wsMapPoolCode.put(s.code)
	for _, vw := range s.codeHash {
		releaseVWCodeHash(vw)
	}
	wsMapPoolCodeHash.put(s.codeHash)
	for _, vw := range s.codeSize {
		releaseVWCodeSize(vw)
	}
	wsMapPoolCodeSize.put(s.codeSize)
	for _, inner := range s.storage {
		for _, vw := range inner {
			releaseVWStorage(vw)
		}
		wsPutStorageInner(inner)
	}
	wsPutStorageOuter(s.storage)
	*s = WriteSet{}
}

// Per-path typed delete methods.  Direct map access, no internal switch
// (mirrors the Set/Get/update* shape).  Each Del* releases the displaced
// *VersionedWrite[T] back to its pool — keeps the pool cycle closed so
// allocs land on Get and end at Del/ReleaseAndReset.

func (s *WriteSet) DelAddress(addr accounts.Address) {
	if vw, ok := s.address[addr]; ok {
		releaseVWAddress(vw)
		delete(s.address, addr)
	}
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
func (s *WriteSet) DelCreateContract(addr accounts.Address) {
	if vw, ok := s.createContract[addr]; ok {
		releaseVWCreateContract(vw)
		delete(s.createContract, addr)
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

// updateBalance, updateNonce etc. are typed in-place mutations on the
// existing per-path entry.  No-op when no entry exists for addr.
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

func (s *WriteSet) updateCreateContract(addr accounts.Address, val bool) {
	if vw, ok := s.createContract[addr]; ok {
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
	if r, ok := vr.reads.GetAddress(address); ok && r.Val != nil && !r.Val.IsNil() {
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
		if destructed, res, ok := vr.versionMap.ReadSelfDestruct(address, vr.txIndex); ok && res.Status() == MVReadResultDone {
			if destructed {
				destructTxIndex := res.DepIdx()
				revived := false
				revivalLimit := vr.txIndex - 1
				// AddressPath uses >= to catch same-tx metamorphic SD+CREATE2; subfields use >.
				if hi, ok := vr.versionMap.LatestTxIndex(address, AddressPath, accounts.NilKey, revivalLimit); ok && hi >= destructTxIndex {
					revived = true
				}
				if !revived {
					if hi, ok := vr.versionMap.LatestTxIndex(address, BalancePath, accounts.NilKey, revivalLimit); ok && hi > destructTxIndex {
						revived = true
					}
				}
				if !revived {
					if hi, ok := vr.versionMap.LatestTxIndex(address, NoncePath, accounts.NilKey, revivalLimit); ok && hi > destructTxIndex {
						revived = true
					}
				}
				if !revived {
					if hi, ok := vr.versionMap.LatestTxIndex(address, CodeHashPath, accounts.NilKey, revivalLimit); ok && hi > destructTxIndex {
						revived = true
					}
				}
				if !revived {
					return nil, nil
				}
			}
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
		var synth accounts.Account
		updated := vr.applyVersionedUpdates(address, synth)
		// Only return the synthesized account if applyVersionedUpdates
		// actually applied at least one field — otherwise we'd return a
		// zero account for addresses that have no versionMap entries.
		if updated != synth {
			return &updated, nil
		}
	}

	return nil, nil
}

// Per-path versionedUpdate helpers replace the legacy generic
// versionedUpdate[T] — they consume the typed VersionMap Read primitives
// directly so neither the call site nor the read path crosses an any
// boundary. Each returns the cell value if a Done write exists at txIndex,
// otherwise the zero value and ok=false.

func versionedUpdateAddress(vm *VersionMap, addr accounts.Address, txIndex int) (*accounts.Account, bool) {
	val, res, ok := vm.ReadAddress(addr, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return nil, false
}

func versionedUpdateSelfDestruct(vm *VersionMap, addr accounts.Address, txIndex int) (bool, bool) {
	val, res, ok := vm.ReadSelfDestruct(addr, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return false, false
}

func versionedUpdateBalance(vm *VersionMap, addr accounts.Address, txIndex int) (uint256.Int, bool) {
	val, res, ok := vm.ReadBalance(addr, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return uint256.Int{}, false
}

func versionedUpdateNonce(vm *VersionMap, addr accounts.Address, txIndex int) (uint64, bool) {
	val, res, ok := vm.ReadNonce(addr, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return 0, false
}

func versionedUpdateIncarnation(vm *VersionMap, addr accounts.Address, txIndex int) (uint64, bool) {
	val, res, ok := vm.ReadIncarnation(addr, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return 0, false
}

func versionedUpdateCode(vm *VersionMap, addr accounts.Address, txIndex int) ([]byte, bool) {
	val, res, ok := vm.ReadCode(addr, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return nil, false
}

func versionedUpdateCodeHash(vm *VersionMap, addr accounts.Address, txIndex int) (accounts.CodeHash, bool) {
	val, res, ok := vm.ReadCodeHash(addr, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return accounts.CodeHash{}, false
}

func versionedUpdateStorage(vm *VersionMap, addr accounts.Address, key accounts.StorageKey, txIndex int) (uint256.Int, bool) {
	val, res, ok := vm.ReadStorage(addr, key, txIndex)
	if ok && res.Status() == MVReadResultDone {
		return val, true
	}
	return uint256.Int{}, false
}

// applyVersionedUpdates applies updated from the version map to the account before returning it, this is necessary
// for the account obkect becuase the state reader/.writer api's treat the subfileds as a group and this
// may lead to updated from pervious transactions being missed where we only update a subset of the fiels as these won't
// be recored as reads and hence the varification process will miss them.  We don't want to creat a fail but
// we do  want to capture the updates
func (vr versionedStateReader) applyVersionedUpdates(address accounts.Address, account accounts.Account) accounts.Account {
	if update, ok := versionedUpdateBalance(vr.versionMap, address, vr.txIndex); ok {
		account.Balance = update
	}
	if update, ok := versionedUpdateNonce(vr.versionMap, address, vr.txIndex); ok {
		account.Nonce = update
	}
	if update, ok := versionedUpdateIncarnation(vr.versionMap, address, vr.txIndex); ok {
		account.Incarnation = update
	}
	if update, ok := versionedUpdateCodeHash(vr.versionMap, address, vr.txIndex); ok {
		account.CodeHash = update
	}
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

	// Check version map for storage written by prior transactions.
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

	// Check version map for CodePath entries written by prior transactions
	// (e.g. EIP-7702 delegation set by an earlier tx in the same block).
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

type VersionedWrites []AnyVersionedWrite

// NewAccountFieldWriteFromMap returns a typed *VersionedWrite[T] populated
// from a successful versionMap read at (addr, path, txIdx), or nil if no
// Done write is present. Used by parallel finalize paths that need to
// reconstruct the post-tx value for an account field missing from a tx's
// output write set.
func NewAccountFieldWriteFromMap(vm *VersionMap, addr accounts.Address, path AccountPath, ver Version, txIdx int) AnyVersionedWrite {
	switch path {
	case BalancePath:
		v, rr, found := vm.ReadBalance(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			return &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: v}
		}
	case NoncePath:
		v, rr, found := vm.ReadNonce(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: v}
		}
	case IncarnationPath:
		v, rr, found := vm.ReadIncarnation(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}, Val: v}
		}
	case CodeHashPath:
		v, rr, found := vm.ReadCodeHash(addr, txIdx)
		if found && rr.Status() == MVReadResultDone {
			return &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: v}
		}
	}
	return nil
}

// NewAccountFieldZeroWrite returns a typed *VersionedWrite[T] for path
// holding the zero value (used by the SD-earlier fallback that emits
// post-destruction defaults).  Path must be Balance/Nonce/Incarnation/CodeHash.
func NewAccountFieldZeroWrite(addr accounts.Address, path AccountPath, ver Version) AnyVersionedWrite {
	switch path {
	case BalancePath:
		return &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}}
	case NoncePath:
		return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}}
	case IncarnationPath:
		return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}}
	case CodeHashPath:
		return &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: accounts.EmptyCodeHash}
	}
	return nil
}

// NewAccountFieldWriteFromAccount returns a typed *VersionedWrite[T]
// populated from acc (may be nil — falls back to zero values except for
// CodeHash which becomes EmptyCodeHash).
func NewAccountFieldWriteFromAccount(addr accounts.Address, path AccountPath, ver Version, acc *accounts.Account) AnyVersionedWrite {
	switch path {
	case BalancePath:
		var v uint256.Int
		if acc != nil {
			v = acc.Balance
		}
		return &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: ver}, Val: v}
	case NoncePath:
		var v uint64
		if acc != nil {
			v = acc.Nonce
		}
		return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: ver}, Val: v}
	case IncarnationPath:
		var v uint64
		if acc != nil {
			v = acc.Incarnation
		}
		return &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath, Version: ver}, Val: v}
	case CodeHashPath:
		v := accounts.EmptyCodeHash
		if acc != nil {
			v = acc.CodeHash
		}
		return &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath, Version: ver}, Val: v}
	}
	return nil
}

// TouchUpdates feeds VersionedWrites directly to a commitment.Updates buffer
// via TouchPlainKeyDirect. Each VersionedWrite maps to a single Update with
// the appropriate key and flags. The Updates buffer handles per-key merging
// (same address gets accumulated flags from BalancePath, NoncePath, etc.).
//
// This is used by the commitment calculator to process writes received via
// the fan-out channel. No serialization/deserialization — the values pass
// through as-is.
func (writes VersionedWrites) TouchUpdates(updates *commitment.Updates) {
	for _, w := range writes {
		hdr := w.Header()
		addrVal := hdr.Address.Value()
		addrKey := string(addrVal[:])

		switch hdr.Path {
		case BalancePath:
			v, _ := w.ValAny().(uint256.Int)
			updates.TouchPlainKeyDirect(addrKey, &commitment.Update{
				Flags:   commitment.BalanceUpdate,
				Balance: v,
			})
		case NoncePath:
			v, _ := w.ValAny().(uint64)
			updates.TouchPlainKeyDirect(addrKey, &commitment.Update{
				Flags: commitment.NonceUpdate,
				Nonce: v,
			})
		case CodeHashPath:
			v, _ := w.ValAny().(accounts.CodeHash)
			updates.TouchPlainKeyDirect(addrKey, &commitment.Update{
				Flags:    commitment.CodeUpdate,
				CodeHash: v.Value(),
			})
		case CodePath:
			v, _ := w.ValAny().(accounts.Code)
			updates.TouchPlainKeyDirect(addrKey, &commitment.Update{
				Flags:    commitment.CodeUpdate,
				CodeHash: v.Hash.Value(),
			})
		case SelfDestructPath:
			v, _ := w.ValAny().(bool)
			if v {
				updates.TouchPlainKeyDirect(addrKey, &commitment.Update{
					Flags: commitment.DeleteUpdate,
				})
			}
		case StoragePath:
			val, _ := w.ValAny().(uint256.Int)
			vBytes := val.Bytes()
			keyVal := hdr.Key.Value()
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

// sortVersionedWrites sorts a VersionedWrites slice by (Address, Path, Key)
// to ensure deterministic processing order. VersionedWrites originate from
// WriteSet map iteration which has non-deterministic order in Go.
// The sort relies on the AccountPath enum ordering defined in versionmap.go.
func sortVersionedWrites(writes VersionedWrites) {
	sort.Slice(writes, func(i, j int) bool {
		hi, hj := writes[i].Header(), writes[j].Header()
		if c := hi.Address.Cmp(hj.Address); c != 0 {
			return c < 0
		}
		if hi.Path != hj.Path {
			return hi.Path < hj.Path
		}
		return hi.Key.Cmp(hj.Key) < 0
	})
}

func (prev VersionedWrites) Merge(next VersionedWrites) VersionedWrites {
	if len(prev) == 0 {
		return next
	}
	if len(next) == 0 {
		return prev
	}
	// Last-write-wins by (Address, Path, Key).
	type k struct {
		addr accounts.Address
		path AccountPath
		key  accounts.StorageKey
	}
	index := make(map[k]int, len(prev)+len(next))
	out := make(VersionedWrites, 0, len(prev)+len(next))
	for _, v := range prev {
		h := v.Header()
		key := k{h.Address, h.Path, h.Key}
		if i, ok := index[key]; ok {
			out[i] = v
		} else {
			index[key] = len(out)
			out = append(out, v)
		}
	}
	for _, v := range next {
		h := v.Header()
		key := k{h.Address, h.Path, h.Key}
		if i, ok := index[key]; ok {
			out[i] = v
		} else {
			index[key] = len(out)
			out = append(out, v)
		}
	}
	return out
}

// hasNewWrite: returns true if the current set has a new write compared to the input
func (writes VersionedWrites) HasNewWrite(cmpSet VersionedWrites) bool {
	if len(writes) == 0 {
		return false
	} else if len(cmpSet) == 0 || len(writes) > len(cmpSet) {
		return true
	}

	cmpMap := map[accounts.Address]map[AccountKey]struct{}{}

	for _, vw := range cmpSet {
		h := vw.Header()
		keys, ok := cmpMap[h.Address]
		if !ok {
			keys = map[AccountKey]struct{}{}
			cmpMap[h.Address] = keys
		}
		keys[AccountKey{h.Path, h.Key}] = struct{}{}
	}

	for _, v := range writes {
		h := v.Header()
		if _, ok := cmpMap[h.Address][AccountKey{h.Path, h.Key}]; !ok {
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
func (writes VersionedWrites) StripBalanceWrite(addr accounts.Address, readSet ReadSet) (stripped VersionedWrites, delta uint256.Int, increase bool, found bool) {
	stripped = writes
	if addr.IsNil() {
		return
	}

	if !readSet.hasAddr(addr) {
		// TX didn't read this address — no delta to compute.
		// Still strip the write to prevent stale cache pollution.
		for i, w := range stripped {
			h := w.Header()
			if h.Address == addr && h.Path == BalancePath {
				stripped = append(stripped[:i], stripped[i+1:]...)
				return
			}
		}
		return
	}

	balRead, ok := readSet.GetBalance(addr)
	if !ok {
		return
	}
	staleRead := balRead.Val

	for i, w := range stripped {
		h := w.Header()
		if h.Address == addr && h.Path == BalancePath {
			staleWrite, _ := w.ValAny().(uint256.Int)
			// Remove the stale absolute write
			stripped = append(stripped[:i], stripped[i+1:]...)
			// Compute the TX's net effect on this balance
			if staleWrite.Gt(&staleRead) {
				delta.Sub(&staleWrite, &staleRead)
				increase = true
				found = true
			} else if staleRead.Gt(&staleWrite) {
				delta.Sub(&staleRead, &staleWrite)
				increase = false
				found = true
			}
			return
		}
	}

	return
}

// SetAccountBalanceOrDelete replaces the BalancePath write for addr. If the
// address has no existing writes in the set, all four account fields (balance,
// nonce, incarnation, codeHash) are emitted so that applyVersionedWrites can
// reconstruct a complete account. Without the full set, it would create an
// account with nonce=0, incarnation=0, empty codeHash — wiping the real values.
//
// When emptyRemoval is true (EIP-161 SpuriousDragon), if the final account
// would be empty (balance=0, nonce=0, empty code), the existing writes for
// this address are stripped and a SelfDestructPath entry is emitted instead.
func (writes VersionedWrites) SetAccountBalanceOrDelete(addr accounts.Address, acc *accounts.Account, val uint256.Int, reason tracing.BalanceChangeReason, emptyRemoval bool) VersionedWrites {
	if acc == nil {
		a := accounts.NewAccount()
		acc = &a
	}

	// EIP-161: if the final account is empty, delete it.
	if emptyRemoval && val.IsZero() && acc.Nonce == 0 && acc.IsEmptyCodeHash() {
		// Strip any existing writes for this address and emit a delete.
		filtered := make(VersionedWrites, 0, len(writes)+1)
		for _, w := range writes {
			if w.Header().Address != addr {
				filtered = append(filtered, w)
			}
		}
		return append(filtered, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath}, Val: true})
	}

	for _, w := range writes {
		h := w.Header()
		if h.Address == addr && h.Path == BalancePath {
			if bw, ok := w.(*VersionedWrite[uint256.Int]); ok {
				bw.Val = val
				bw.Reason = reason
				return writes
			}
		}
	}
	// Account not in writes — emit complete account fields.
	return append(writes,
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Reason: reason}, Val: val},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: acc.Nonce},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: acc.Incarnation},
		&VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: acc.CodeHash},
	)
}

// note that TxIndex starts at -1 (the begin system tx)
type VersionedIO struct {
	inputs  []versionedReadSet
	outputs []VersionedWrites // write sets that should be checked during validation
}

func NewVersionedIO(numTx int) *VersionedIO {
	return &VersionedIO{
		inputs:  make([]versionedReadSet, numTx+1),
		outputs: make([]VersionedWrites, numTx+1),
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

func (io *VersionedIO) Outputs() []VersionedWrites {
	return io.outputs
}

func (io *VersionedIO) ReadSet(txnIdx int) ReadSet {
	if len(io.inputs) <= txnIdx+1 {
		return ReadSet{}
	}
	return io.inputs[txnIdx+1].readSet
}

func (io *VersionedIO) WriteSet(txnIdx int) VersionedWrites {
	if len(io.outputs) <= txnIdx+1 {
		return nil
	}
	return io.outputs[txnIdx+1]
}

func (io *VersionedIO) WriteCount() (count int64) {
	for _, output := range io.outputs {
		count += int64(len(output))
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
	// Re-execution at higher incarnation overwrites a previous tx's
	// ReadSet at the same TxIndex; release its pooled maps before drop.
	if prev := io.inputs[txVersion.TxIndex+1]; prev.readSet.Len() > 0 {
		prev.readSet.Release()
	}
	io.inputs[txVersion.TxIndex+1] = versionedReadSet{txVersion.Incarnation, input}
}

// Release returns every per-path map held by any io.inputs[i].readSet to its
// pool.  Called at block-end retirement of the VersionedIO so the maps recycle
// across blocks rather than getting GC'd.  The hand-over from IBS via
// VersionedReads()/RecordReads keeps the maps alive until exactly this point.
func (io *VersionedIO) Release() {
	if io == nil {
		return
	}
	for i := range io.inputs {
		io.inputs[i].readSet.Release()
		io.inputs[i] = versionedReadSet{}
	}
}

func (io *VersionedIO) RecordWrites(txVersion Version, output VersionedWrites) {
	txId := txVersion.TxIndex

	if len(io.outputs) <= txId+1 {
		io.outputs = append(io.outputs, make([]VersionedWrites, txId+2-len(io.outputs))...)
	}
	io.outputs[txId+1] = output
}

func (io *VersionedIO) Merge(other *VersionedIO) *VersionedIO {
	mergedLen := max(io.Len(), other.Len())
	merged := NewVersionedIO(mergedLen - 1)

	for i := 0; i < mergedLen; i++ {
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
			ensureAccountState(ac, addr)
		}
		for addr, tr := range rs.codeSize {
			if addr.IsNil() || tr.internal {
				continue
			}
			ensureAccountState(ac, addr)
		}

		writes := io.WriteSet(txIndex)
		sortVersionedWrites(writes)
		for _, vw := range writes {
			h := vw.Header()
			if h.Address.IsNil() {
				continue
			}
			account := ensureAccountState(ac, h.Address)
			accessIndex := h.Version.blockAccessIndex()
			account.updateWrite(vw, accessIndex)
		}

		isUserTx := txIndex >= 0
		for addr, revertable := range io.ReadSet(txIndex).touched {
			if addr.IsNil() {
				continue
			}

			account := ensureAccountState(ac, addr)
			// A non-revertable access means the address was the target of
			// an actual EVM operation (evm.Call, evm.Create, SELFDESTRUCT
			// with non-zero balance, BALANCE, EXTCODESIZE, etc.) — not just
			// a gas-calculation read. This is used to distinguish real state
			// access from incidental reads (e.g. Empty() in gas calc) for
			// the system address filter.
			if isUserTx && !revertable {
				account.nonRevertableUserAccess = true
			}
		}
	}

	bal := make([]*types.AccountChanges, 0, len(ac))
	for _, account := range ac {
		account.finalize()
		account.changes.Normalize()
		// The system address (0xff...fe) is touched during every block's system
		// call (EIP-4788 beacon root) because it is msg.sender. Per EIP-7928,
		// "SYSTEM_ADDRESS MUST NOT be included unless it experiences state access
		// itself." We use the non-revertable access flag from MarkAddressAccess
		// to distinguish real state access (evm.Call target, SELFDESTRUCT
		// beneficiary, BALANCE opcode, etc.) from incidental gas-calculation
		// reads (Empty() in statefulGasCall). Keep it when it has actual state
		// changes or when a user tx performed a non-revertable access to it.
		if account.changes.Address == params.SystemAddress && !hasAccountChanges(account.changes) && !account.nonRevertableUserAccess {
			continue
		}
		bal = append(bal, account.changes)
	}

	sort.Slice(bal, func(i, j int) bool {
		return bal[i].Address.Cmp(bal[j].Address) < 0
	})

	return bal
}

// hasAccountChanges returns true if the account has any state changes
// (storage, balance, nonce, or code) that belong in the BAL.
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
	balanceValue            *uint256.Int                        // tracks latest seen balance
	initialBalanceValue     *uint256.Int                        // tracks pre-block balance for net-zero detection
	selfDestructed          bool                                //
	selfDestructedAt        uint32                              // access index of the selfdestruct
	storageReadValues       map[accounts.StorageKey]uint256.Int // original read values for net-zero detection
	nonRevertableUserAccess bool                                // true if a user tx (txIndex >= 0) has non-revertable access
}

// check pre- and post-values, add to BAL if different
func (a *accountState) finalize() {
	applyToBalance(a.balance, a.changes, a.initialBalanceValue)
	applyToNonce(a.nonce, a.changes)
	applyToCode(a.code, a.changes)
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

func applyToCode(ct *fieldTracker[accounts.Code], ac *types.AccountChanges) {
	ct.changes.apply(func(idx uint32, value accounts.Code) {
		ac.CodeChanges = append(ac.CodeChanges, &types.CodeChange{
			Index:    idx,
			Bytecode: value.Bytes,
		})
	})
}

// changeTracker stores the latest written value per access index. For
// []byte (CodePath) the slice is a non-mutating borrow of the
// canonical bytecode owned by cache.StateCache CodeDomain (populated
// via the SD read path or block-readahead). Defensive clones on store
// or on apply would shadow that ownership for no gain — bytecode is
// the one variable-size payload in EVM state and never mutated in
// place by any consumer (verified by audit).
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

func (account *accountState) updateWrite(vw AnyVersionedWrite, accessIndex uint32) {
	hdr := vw.Header()
	switch hdr.Path {
	case StoragePath:
		val, _ := vw.ValAny().(uint256.Int)
		// Skip intra-tx net-zero storage writes: if this is the first write
		// to the slot (no prior tx wrote to it) and the written value equals
		// the original read value, it's a no-op that should remain as a read.
		if !hasStorageWrite(account.changes, hdr.Key) {
			if origVal, wasRead := account.storageReadValues[hdr.Key]; wasRead && val.Eq(&origVal) {
				return
			}
		}
		addStorageUpdate(account.changes, hdr.Key, val, accessIndex)
	case BalancePath:
		val, _ := vw.ValAny().(uint256.Int)
		// Skip non-zero balance writes for selfdestructed accounts within the
		// SAME transaction (e.g. priority fee applied during finalize of the
		// selfdestructing tx). Balance writes from LATER transactions (e.g. a
		// value transfer to the now-empty address) are real state changes that
		// must appear in the BAL.
		if account.selfDestructed && accessIndex == account.selfDestructedAt && !val.IsZero() {
			return
		}
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
	case NoncePath:
		val, _ := vw.ValAny().(uint64)
		account.nonce.recordWrite(accessIndex, val)
	case CodePath:
		val, _ := vw.ValAny().(accounts.Code)
		account.code.recordWrite(accessIndex, val)
	case SelfDestructPath:
		val, _ := vw.ValAny().(bool)
		if val {
			account.selfDestructed = true
			account.selfDestructedAt = accessIndex
		}
	default:
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
	if hasStorageWrite(account.changes, key) {
		return
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

func addStorageUpdate(ac *types.AccountChanges, slot accounts.StorageKey, val uint256.Int, txIndex uint32) {
	// If we already recorded a read for this slot, drop it because a write takes precedence.
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
			// EIP-7928 no-op filter: skip if value equals the slot's last recorded write.
			if n := len(slotChange.Changes); n > 0 && val.Eq(&slotChange.Changes[n-1].Value) {
				return
			}
			slotChange.Changes = append(slotChange.Changes, &types.StorageChange{Index: txIndex, Value: val})
			return
		}
	}
	if origVal, wasRead := account.storageReadValues[slot]; wasRead && val.Eq(&origVal) {
		return
	}
	removeStorageRead(ac, slot) // a real write supersedes any recorded read
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
	Index         int
	Reads         ReadSet
	FullWriteList []VersionedWrites
}

func HasReadDep(txFrom VersionedWrites, txTo ReadSet) bool {
	for _, rd := range txFrom {
		h := rd.Header()
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

func depsHelper(dependencies map[int]map[int]bool, txFrom VersionedWrites, txTo ReadSet, i int, j int) map[int]map[int]bool {
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
