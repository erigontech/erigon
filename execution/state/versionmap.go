package state

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// statusFlag is the cell's dependency status: how a reader treats it and whether
// it re-validates. Orthogonal to valueStatus.
type statusFlag uint8

const (
	FlagDone     statusFlag = 0
	FlagEstimate statusFlag = 1
	UnknownDep              = -3
)

// FlagValidated: a pre-seal write whose tx validated its read-set out-of-order. A
// reader continues on it (early break) like Done rather than pausing like an Estimate,
// but it stays revertible until the in-order seal promotes it to Done.
const FlagValidated statusFlag = 2

const flagDeleted statusFlag = 3

// flagSeeded asks noOpBind for the Done cell WriteChanges seeded, untouched since.
const flagSeeded statusFlag = 4

// valueStatus is the cell's value transition relative to what the writing tx read:
// the leaf effect a write has. Stamped at write time from (prev-presence, prev, value),
// never re-derived. Only the domain/commitment projection skips Unchanged; the BAL and
// read path see every cell regardless.
type valueStatus uint8

// ValueChanged is the zero value on purpose: an unstamped cell is treated as a real
// change and kept, so a path that forgets to stamp can never silently drop a write.
// Only ValueUnchanged is skipped by the domain/commitment projection.
const (
	ValueChanged   valueStatus = iota // updated existing value (and the fail-safe default)
	ValueUnchanged                    // value == prev: no leaf change (the noop case, the only skipped state)
	ValueCreated                      // no prior value: insert
	ValueDeleted                      // prior value removed
)

// AccountSource records where an AddressPath cell's record came from. AccountFromBase
// (the zero value) is the authoritative committed/exec record. AccountFromBAL is a
// partial existence record the BAL seed wrote: the account exists, but the BAL carries
// no whole-account record, so its field values are not here and must be read through to
// the committed base.
type AccountSource uint8

const (
	AccountFromBase AccountSource = iota
	AccountFromBAL
)

type AccountPath int8

func (p AccountPath) String() string {
	switch p {
	case AddressPath:
		return "Address"
	case BalancePath:
		return "Balance"
	case NoncePath:
		return "Nonce"
	case IncarnationPath:
		return "Incarnation"
	case CodePath:
		return "Code"
	case CodeHashPath:
		return "Code Hash"
	case CodeSizePath:
		return "Code Size"
	case SelfDestructPath:
		return "Destruct"
	case StoragePath:
		return "Storage"
	case CreateContractPath:
		return "CreateContract"
	default:
		return fmt.Sprintf(" Unknown %d", p)
	}
}

// Numeric order is load-bearing: SelfDestructPath must precede BalancePath so the selfDestructed flag is set before balance writes are evaluated.
const (
	AddressPath AccountPath = iota
	SelfDestructPath
	BalancePath
	NoncePath
	IncarnationPath
	CodePath
	CodeHashPath
	CodeSizePath
	StoragePath
	CreateContractPath
)

// AffectsAccountLifecycle reports whether a write to this path changes the account's
// whole-account lifecycle (create / self-destruct / revival), which re-derives every
// field. A reader of ANY field at the address therefore depends on such a write, not
// only a reader of the exact cell — the cross-path dependency the revalidation index
// (HasReadDep, markReadersDirty, revalCandidates) must honour.
func (p AccountPath) AffectsAccountLifecycle() bool {
	switch p {
	case AddressPath, SelfDestructPath, IncarnationPath, CreateContractPath:
		return true
	default:
		return false
	}
}

// BALSeeded reports whether a block access list pre-populates cells of path p.
func (p AccountPath) BALSeeded() bool {
	return p == BalancePath || p == NoncePath || p == CodePath || p == StoragePath
}

// AccountKey is a (Path, Key) pair used as a field selector within an AddressEntry
// and as a debug-printable identifier.
type AccountKey struct {
	Path AccountPath
	Key  accounts.StorageKey
}

func (k AccountKey) String() string {
	if k.Path == StoragePath {
		return fmt.Sprintf("%x", k.Key)
	}

	return k.Path.String()
}

// AddressEntry holds the multi-version cells for one address, organised per AccountPath.
// Each field is typed by the path's value-type contract, so a wrong-type write is a
// compile-time error. Every operation is (Path,Key)-scoped; the entry is never treated
// as a transactional whole.
type AddressEntry struct {
	Address        *btree.Map[int, *WriteCell[*accounts.Account]]
	SelfDestruct   *btree.Map[int, *WriteCell[bool]]
	Balance        *btree.Map[int, *WriteCell[uint256.Int]]
	Nonce          *btree.Map[int, *WriteCell[uint64]]
	Incarnation    *btree.Map[int, *WriteCell[uint64]]
	Code           *btree.Map[int, *WriteCell[accounts.Code]]
	CodeHash       *btree.Map[int, *WriteCell[accounts.CodeHash]]
	CodeSize       *btree.Map[int, *WriteCell[int]]
	CreateContract *btree.Map[int, *WriteCell[bool]]
	Storage        map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]
	// mu guards this account's cell maps. Held RLock for reads (readFloor and
	// the per-path scans) and Lock for writes (putCell / Delete).
	// Marks take Lock once a SelfDestruct cell was true or the map carries write
	// stamps: readers combine several cells' flags only through a destruct.
	mu         sync.RWMutex
	destructed atomic.Bool
}

// putCell sets/updates a typed cell at txIdx; caller holds e.mu.Lock(). Returns the (possibly new) map to assign back and the cell.
func putCell[T any](vm *VersionMap, cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, txIdx int, incarnation Incarnation, flag statusFlag, value T, valStatus valueStatus) (*btree.Map[int, *WriteCell[T]], *WriteCell[T]) {
	vm.assertUnsealed(txIdx, addr, path, accounts.NilKey)
	if cells == nil {
		cells = &btree.Map[int, *WriteCell[T]]{}
	}
	if ci, ok := cells.Get(txIdx); ok {
		have, haveFlag := ci.loadState()
		if have > incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %x %s, %v", addr, path, txIdx))
		}
		// On a BAL block an unsealed Done cell is a seed, which a speculative run may still change.
		if dbg.AssertEnabled && !vm.HasBAL && haveFlag == FlagDone && flag == FlagEstimate {
			panic(fmt.Errorf("versionMap: Done->Estimate downgrade addr=%x path=%s txIdx=%d inc %d->%d", addr, path, txIdx, have, incarnation))
		}
		ci.setState(incarnation, flag)
		ci.Value = value
		ci.valStatus = valStatus
		ci.accSource = AccountFromBase
		return cells, ci
	}
	cell := &WriteCell[T]{Value: value, valStatus: valStatus, accSource: AccountFromBase}
	cell.setState(incarnation, flag)
	cells.Set(txIdx, cell)
	return cells, cell
}

// markCellFlag sets the flag on an existing typed cell, panicking with msg if none is
// present at txIdx. When incarnation >= 0 the cell must be at that incarnation — a newer
// one means the flip targets a stale version, so panic rather than mark the wrong one.
func markCellFlag[T any](cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, incarnation Incarnation, flag statusFlag) {
	var ci *WriteCell[T]
	if cells != nil {
		ci, _ = cells.Get(txIdx)
	}
	if ci == nil {
		panic(missingCellMsg("markFlag", addr, path, key, txIdx))
	}
	// A cell at an EARLIER incarnation than the run is legitimate under the equal-value
	// write-side no-bump; only a NEWER one means the flip targets a stale version.
	switch have, outcome := ci.mark(incarnation, flag); outcome {
	case markStale:
		panic(staleMarkMsg(have, incarnation, addr, path, key, txIdx))
	case markDeleted:
		panic(missingCellMsg("markFlag", addr, path, key, txIdx))
	}
}

func staleMarkMsg(have, want Incarnation, addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) string {
	return fmt.Sprintf("markFlag: incarnation have=%d want=%d addr=%x path=%s key=%x txIdx=%d",
		have, want, addr.Value(), path, key.Value(), txIdx)
}

// missingCellMsg is called from panic paths only: the mark* helpers run once per
// written cell, and formatting an address plus a slot key on every call costs more
// than the check itself.
func missingCellMsg(what string, addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) string {
	return fmt.Sprintf("%s: missing cell addr=%x path=%s key=%x txIdx=%d", what, addr.Value(), path, key.Value(), txIdx)
}

// markCellComplete advances an existing cell to Done as a consistency check, not a
// write: the cell must already hold value at incarnation (published speculatively when
// the tx's result arrived). A missing cell, newer incarnation, or changed value is a
// one-value-per-version violation and panics. This is the commit-boundary enforcement point.
func markCellComplete[T any](cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, incarnation Incarnation, value T) {
	if cells == nil {
		panic(missingCellMsg("markComplete", addr, path, key, txIdx))
	}
	ci, ok := cells.Get(txIdx)
	if !ok {
		panic(missingCellMsg("markComplete", addr, path, key, txIdx))
	}
	if dbg.AssertEnabled {
		// A cell held at an EARLIER incarnation than the committing run is legitimate: an
		// equal-value re-execution keeps the existing incarnation (write-side no-bump), so
		// the value-equality check below is the real one-value-per-version guard. Only a
		// NEWER cell incarnation is a violation.
		if have := ci.loadInc(); have > incarnation {
			panic(fmt.Sprintf("markComplete: incarnation addr=%x path=%s txIdx=%d have=%d want=%d", addr.Value(), path, txIdx, have, incarnation))
		}
		if !sameCellValue(ci.Value, value) {
			panic(fmt.Sprintf("markComplete: value changed at published version addr=%x path=%s txIdx=%d inc=%d old=%v new=%v", addr.Value(), path, txIdx, incarnation, ci.Value, value))
		}
	}
	ci.mark(-1, FlagDone)
}

// sameCellValue compares accounts with Account.Equals: their Root is derived by commitment, not part of the value.
func sameCellValue(a, b any) bool {
	if x, ok := a.(*accounts.Account); ok {
		y, _ := b.(*accounts.Account)
		return x == y || eqAccount(x, y)
	}
	return reflect.DeepEqual(a, b)
}

type VersionMap struct {
	// Account lookup is lock-free. Each entry's RWMutex guards only its own cells.
	s atomic.Pointer[xsync.Map[accounts.Address, *AddressEntry]]
	// nonEmpty latches before the first entry is taken: nothing removes entries.
	nonEmpty atomic.Bool
	HasBAL   bool // When true, all significant writes are pre-populated from BAL
	bal      types.BlockAccessList

	// Origin layer: the committed base before the first unpublished tx (published txs are already
	// folded into it); every reader of it has a higher tx index. Immutable and
	// write-once, so it lives in lock-free maps separate from the per-account e.mu-guarded cells
	// (which carry the mutable in-block/OCC writes). A cold read resolves the origin here lock-free
	// at version originIndex; a hot account read by many workers no longer serializes on e.mu.
	originAccounts atomic.Pointer[xsync.Map[accounts.Address, *accounts.Account]] // nil ptr = committed-absent
	originStorage  atomic.Pointer[xsync.Map[originStorageKey, uint256.Int]]

	// sealed/sealedArmed enforce that a finalized tx's cells are immutable: no write/delete at TxIndex <= sealed. SealUpTo is single-writer; assertUnsealed is many-reader.
	sealed      atomic.Int64
	sealedArmed atomic.Bool

	// baseTxNum is the block's first global txNum (block-start system tx, TxIndex -1),
	// so a global txNum maps to the block-relative TxIndex: txNum = baseTxNum + TxIndex + 1.
	baseTxNum atomic.Uint64

	writeSeq atomic.Uint64
	stamps   *writeStamps

	// fieldMissHook runs after an account-field probe missed, before the
	// fallbacks probe the version map again. Tests set it to land a flush there.
	fieldMissHook func()
	// absentAccountHook runs after an account read found no account, before the
	// self-destruct and revival probes. Tests set it to land a flush there.
	absentAccountHook func()
}

type writeStamps struct {
	n        int
	inflight []atomic.Int32
	last     []atomic.Uint64
}

func (vm *VersionMap) InitStamps(lastTxIndex int) {
	n := lastTxIndex + 2
	vm.stamps = &writeStamps{n: n, inflight: make([]atomic.Int32, n+1), last: make([]atomic.Uint64, n+1)}
	vm.writeSeq.Store(1)
}

func (vm *VersionMap) WriteSeq() uint64 { return vm.writeSeq.Load() }

func (st *writeStamps) slot(txIdx int) int {
	if txIdx < -1 || txIdx > st.n-2 {
		panic(fmt.Sprintf("write stamp: txIdx %d outside [-1, %d]", txIdx, st.n-2))
	}
	return txIdx + 1
}

func (vm *VersionMap) mutBegin(txIdx int) {
	if st := vm.stamps; st != nil {
		for k := st.slot(txIdx) + 1; k <= st.n; k += k & -k {
			st.inflight[k].Add(1)
		}
	}
}

func (vm *VersionMap) mutEnd(txIdx int) {
	st := vm.stamps
	if st == nil {
		return
	}
	seq := vm.writeSeq.Add(1)
	slot := st.slot(txIdx)
	for k := slot + 1; k <= st.n; k += k & -k {
		for {
			old := st.last[k].Load()
			if old >= seq || st.last[k].CompareAndSwap(old, seq) {
				break
			}
		}
	}
	for k := slot + 1; k <= st.n; k += k & -k {
		st.inflight[k].Add(-1)
	}
}

// hasLifecycleCells is checked per entry, not per write set: a read of this account is
// validated against every lifecycle cell in the entry, so promoting any cell of it can
// turn a verdict Valid -> Invalid even when the promoting tx wrote only storage. On an
// entry without lifecycle cells a promotion can only move a verdict Invalid -> Valid.
func hasLifecycleCells(e *AddressEntry) bool {
	return e.SelfDestruct != nil && e.SelfDestruct.Len() > 0 ||
		e.Incarnation != nil && e.Incarnation.Len() > 0 ||
		e.CreateContract != nil && e.CreateContract.Len() > 0
}

func (vm *VersionMap) QuietBelow(txIdx int, stamp uint64) bool {
	st := vm.stamps
	if st == nil || stamp == 0 {
		return false
	}
	for k := st.slot(txIdx); k > 0; k -= k & -k {
		if st.inflight[k].Load() != 0 || st.last[k].Load() > stamp {
			return false
		}
	}
	return true
}

// lazyMap returns the map behind p, creating it on first use: a VersionMap is built per RPC call and most stay small.
func lazyMap[K comparable, V any](p *atomic.Pointer[xsync.Map[K, V]]) *xsync.Map[K, V] {
	if m := p.Load(); m != nil {
		return m
	}
	m := xsync.NewMap[K, V]()
	if !p.CompareAndSwap(nil, m) {
		return p.Load()
	}
	return m
}

type originStorageKey struct {
	addr accounts.Address
	key  accounts.StorageKey
}

// StoreOriginAccount records the committed base account write-once (idempotent across txs).
func (vm *VersionMap) StoreOriginAccount(addr accounts.Address, acc *accounts.Account) {
	lazyMap(&vm.originAccounts).LoadOrStore(addr, acc)
}

// LoadOriginAccount returns the committed base account and whether it was seeded.
func (vm *VersionMap) LoadOriginAccount(addr accounts.Address) (*accounts.Account, bool) {
	m := vm.originAccounts.Load()
	if m == nil {
		return nil, false
	}
	return m.Load(addr)
}

// StoreOriginStorage records the committed base slot value write-once.
func (vm *VersionMap) StoreOriginStorage(addr accounts.Address, key accounts.StorageKey, val uint256.Int) {
	lazyMap(&vm.originStorage).LoadOrStore(originStorageKey{addr, key}, val)
}

// LoadOriginStorage returns the committed base slot value and whether it was seeded.
func (vm *VersionMap) LoadOriginStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool) {
	m := vm.originStorage.Load()
	if m == nil {
		return uint256.Int{}, false
	}
	return m.Load(originStorageKey{addr, key})
}

// SealUpTo marks every tx at TxIndex <= txIndex as finalized/immutable. Monotonic:
// the frontier never regresses. Called from the single-threaded finalize sweep.
func (vm *VersionMap) SealUpTo(txIndex int) {
	if vm.sealedArmed.Load() && int64(txIndex) <= vm.sealed.Load() {
		return
	}
	vm.sealed.Store(int64(txIndex))
	vm.sealedArmed.Store(true)
}

// assertUnsealed panics if a mutation targets an already-sealed cell. Negative TxIndex
// is the pre-block system-call / block-init domain, written outside the OCC-validated
// regular-tx prefix the seal frontier tracks, so the invariant applies to regular txs only.
func (vm *VersionMap) assertUnsealed(txIdx int, addr accounts.Address, path AccountPath, key accounts.StorageKey) {
	if txIdx >= 0 && vm.sealedArmed.Load() && int64(txIdx) <= vm.sealed.Load() {
		panic(fmt.Sprintf("versionMap: write to sealed cell tx=%d sealedUpTo=%d addr=%x path=%s key=%x",
			txIdx, vm.sealed.Load(), addr.Value(), path, key.Value()))
	}
}

func NewVersionMap(changes types.BlockAccessList) *VersionMap {
	vm := &VersionMap{
		HasBAL: len(changes) > 0,
		bal:    changes,
	}
	vm.WriteChanges(changes)
	return vm
}

// seededStorage returns the value the access list seeds for addr's slot key at txIdx. The access
// list is validated, so its accounts, slots and changes are in increasing order.
func (vm *VersionMap) seededStorage(addr accounts.Address, key accounts.StorageKey, txIdx int) (uint256.Int, bool) {
	address := addr.Value()
	i := sort.Search(len(vm.bal), func(i int) bool { return bytes.Compare(vm.bal[i].Address[:], address[:]) >= 0 })
	if i == len(vm.bal) || vm.bal[i].Address != address {
		return uint256.Int{}, false
	}
	slots := vm.bal[i].StorageChanges
	j := sort.Search(len(slots), func(j int) bool { return slots[j].Slot.Cmp(key) >= 0 })
	if j == len(slots) || slots[j].Slot.Cmp(key) != 0 {
		return uint256.Int{}, false
	}
	changes := slots[j].Changes
	index := uint32(txIdx + 1)
	k := sort.Search(len(changes), func(k int) bool { return changes[k].Index >= index })
	if k == len(changes) || changes[k].Index != index {
		return uint256.Int{}, false
	}
	return changes[k].Value, true
}

// hasNoCells reports whether no tx wrote a cell of path, a lifecycle cell or an address cell of addr.
func (vm *VersionMap) hasNoCells(addr accounts.Address, path AccountPath) bool {
	e := vm.load(addr)
	if e == nil {
		return true
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if hasLifecycleCells(e) || e.Address != nil && e.Address.Len() > 0 {
		return false
	}
	switch path {
	case StoragePath:
		return len(e.Storage) == 0
	case BalancePath:
		return e.Balance == nil || e.Balance.Len() == 0
	case NoncePath:
		return e.Nonce == nil || e.Nonce.Len() == 0
	case CodePath:
		return e.Code == nil || e.Code.Len() == 0
	case CodeHashPath:
		return e.CodeHash == nil || e.CodeHash.Len() == 0
	case CodeSizePath:
		return e.CodeSize == nil || e.CodeSize.Len() == 0
	case AddressPath, SelfDestructPath, IncarnationPath, CreateContractPath:
		return true
	}
	return false
}

// load returns the AddressEntry for addr, or nil when absent. Lock-free.
func (vm *VersionMap) load(addr accounts.Address) *AddressEntry {
	if !vm.nonEmpty.Load() {
		return nil
	}
	m := vm.s.Load()
	if m == nil {
		return nil
	}
	e, _ := m.Load(addr)
	return e
}

// StorageKeys returns every storage slot key recorded for addr.
func (vm *VersionMap) StorageKeys(addr accounts.Address) []accounts.StorageKey {
	e := vm.load(addr)
	if e == nil {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.Storage) == 0 {
		return nil
	}
	keys := make([]accounts.StorageKey, 0, len(e.Storage))
	for k := range e.Storage {
		keys = append(keys, k)
	}
	return keys
}

// WriteChanges pre-populates the version map from a BAL (EIP-7928), routing each
// per-path change through the typed Write primitive so the value type is enforced at
// compile time.
func (vm *VersionMap) WriteChanges(changes types.BlockAccessList) {
	for i := range changes {
		accountChanges := &changes[i]
		if dbg.TraceBALFeed {
			fmt.Printf(
				"BAL-ACCT %x storage=%d balance=%d nonce=%d code=%d reads=%d\n",
				accountChanges.Address,
				len(accountChanges.StorageChanges),
				len(accountChanges.BalanceChanges),
				len(accountChanges.NonceChanges),
				len(accountChanges.CodeChanges),
				len(accountChanges.StorageReads),
			)
		}
		if len(accountChanges.StorageChanges) == 0 && len(accountChanges.BalanceChanges) == 0 &&
			len(accountChanges.NonceChanges) == 0 && len(accountChanges.CodeChanges) == 0 {
			continue
		}
		addr := accounts.InternAddress(accountChanges.Address)
		e := vm.entryOrCreate(addr)
		for _, storageChanges := range accountChanges.StorageChanges {
			for _, change := range storageChanges.Changes {
				if dbg.TraceBALFeed {
					fmt.Printf(
						"BAL-CELL %x storage[%x] balIdx=%d cell=%d val=%s\n",
						accountChanges.Address,
						storageChanges.Slot,
						change.Index,
						int(change.Index)-1,
						change.Value.Hex(),
					)
				}
				vm.WriteStorage(addr, storageChanges.Slot, Version{TxIndex: int(change.Index) - 1}, change.Value, true)
				seed(e.Storage[storageChanges.Slot], int(change.Index)-1)
			}
		}
		for _, balanceChange := range accountChanges.BalanceChanges {
			if dbg.TraceBALFeed {
				fmt.Printf(
					"BAL-CELL %x balance balIdx=%d cell=%d val=%v\n",
					accountChanges.Address,
					balanceChange.Index,
					int(balanceChange.Index)-1,
					&balanceChange.Value,
				)
			}
			vm.WriteBalance(addr, Version{TxIndex: int(balanceChange.Index) - 1}, balanceChange.Value, true)
			seed(e.Balance, int(balanceChange.Index)-1)
		}
		for _, nonceChange := range accountChanges.NonceChanges {
			if dbg.TraceBALFeed {
				fmt.Printf(
					"BAL-CELL %x nonce balIdx=%d cell=%d val=%d\n",
					accountChanges.Address,
					nonceChange.Index,
					int(nonceChange.Index)-1,
					nonceChange.Value,
				)
			}
			vm.WriteNonce(addr, Version{TxIndex: int(nonceChange.Index) - 1}, nonceChange.Value, true)
			seed(e.Nonce, int(nonceChange.Index)-1)
		}
		for _, codeChange := range accountChanges.CodeChanges {
			// Seed the whole code trio together: a CodePath cell without its
			// CodeHashPath/CodeSizePath siblings lets a reader see code but no code hash.
			code := accounts.NewCode(codeChange.Bytecode)
			v := Version{TxIndex: int(codeChange.Index) - 1}
			vm.WriteCode(addr, v, code, true)
			vm.WriteCodeHash(addr, v, code.Hash, true)
			vm.WriteCodeSize(addr, v, code.Len(), true)
			seed(e.Code, v.TxIndex)
			seed(e.CodeHash, v.TxIndex)
			seed(e.CodeSize, v.TxIndex)
		}
	}
}

func seed[T any](cells *btree.Map[int, *WriteCell[T]], txIdx int) {
	c, _ := cells.Get(txIdx)
	c.state.Or(seededCell)
}

// Typed Write primitives. Each takes the AccountPath-contracted value type directly so
// wrong-type writes are caught at compile time.

func (vm *VersionMap) WriteAddress(addr accounts.Address, v Version, value *accounts.Account, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Address, _ = putCell(vm, e.Address, addr, AddressPath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

// WriteAddressExists stamps an AddressPath cell that records only that the account
// exists at v — its value is not authoritative (AccountFromBAL), so field reads fall
// back to the committed base. Used to seed existence for a BAL-created account
// (whose field cells, not an AddressPath, are the real record) without a base lookup.
func (vm *VersionMap) WriteAddressExists(addr accounts.Address, v Version, value *accounts.Account) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	var cell *WriteCell[*accounts.Account]
	e.Address, cell = putCell(vm, e.Address, addr, AddressPath, v.TxIndex, v.Incarnation, flagFor(true), value, ValueChanged)
	cell.accSource = AccountFromBAL
}

func (vm *VersionMap) WriteSelfDestruct(addr accounts.Address, v Version, value bool, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.SelfDestruct, _ = putCell(vm, e.SelfDestruct, addr, SelfDestructPath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
	if value {
		e.destructed.Store(true)
	}
}

func (vm *VersionMap) WriteBalance(addr accounts.Address, v Version, value uint256.Int, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Balance, _ = putCell(vm, e.Balance, addr, BalancePath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

func (vm *VersionMap) WriteNonce(addr accounts.Address, v Version, value uint64, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Nonce, _ = putCell(vm, e.Nonce, addr, NoncePath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

func (vm *VersionMap) WriteIncarnation(addr accounts.Address, v Version, value uint64, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Incarnation, _ = putCell(vm, e.Incarnation, addr, IncarnationPath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

func (vm *VersionMap) WriteCode(addr accounts.Address, v Version, value accounts.Code, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Code, _ = putCell(vm, e.Code, addr, CodePath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

func (vm *VersionMap) WriteCodeHash(addr accounts.Address, v Version, value accounts.CodeHash, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CodeHash, _ = putCell(vm, e.CodeHash, addr, CodeHashPath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

func (vm *VersionMap) WriteCodeSize(addr accounts.Address, v Version, value int, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CodeSize, _ = putCell(vm, e.CodeSize, addr, CodeSizePath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

func (vm *VersionMap) WriteCreateContract(addr accounts.Address, v Version, value bool, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CreateContract, _ = putCell(vm, e.CreateContract, addr, CreateContractPath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

func (vm *VersionMap) WriteStorage(addr accounts.Address, key accounts.StorageKey, v Version, value uint256.Int, complete bool) {
	vm.mutBegin(v.TxIndex)
	defer vm.mutEnd(v.TxIndex)
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Storage == nil {
		e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
	}
	e.Storage[key], _ = putCell(vm, e.Storage[key], addr, StoragePath, v.TxIndex, v.Incarnation, flagFor(complete), value, ValueChanged)
}

// entryOrCreate returns the AddressEntry for addr, creating it if absent. The returned
// pointer is stable for the map's lifetime; the caller locks e.mu for the cell mutation.
func (vm *VersionMap) entryOrCreate(addr accounts.Address) *AddressEntry {
	// Latch before taking the entry, not after publishing it: a creator
	// descheduled in between would otherwise let this writer finish a cell
	// while load still answers from the empty fast path.
	if !vm.nonEmpty.Load() {
		vm.nonEmpty.Store(true)
	}
	m := lazyMap(&vm.s)
	if e, ok := m.Load(addr); ok {
		return e
	}
	e, _ := m.LoadOrStore(addr, &AddressEntry{})
	return e
}

func flagFor(complete bool) statusFlag {
	if complete {
		return FlagDone
	}
	return FlagEstimate
}

// Typed Read primitives. Each returns the typed value, a ReadResult holding the
// conflict-detection metadata (depIdx, incarnation), and ok=true when a cell exists.

// readFloor is the floor read shared by every typed ReadX: it descends sel(e)'s btree
// for the highest write strictly below txIdx and returns its value plus the depIdx and
// (for a Done/Validated floor) incarnation. sel returns nil when the path is unset.
func readFloor[T any](vm *VersionMap, addr accounts.Address, txIdx int, sel func(*AddressEntry) *btree.Map[int, *WriteCell[T]]) (val T, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	e := vm.load(addr)
	if e == nil {
		return val, res, false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	cells := sel(e)
	if cells == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[T]
	cells.Descend(txIdx-1, func(k int, v *WriteCell[T]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	res.valStatus = fv.valStatus
	if fv.accSource == AccountFromBAL {
		res.flags |= readFromBAL
	}
	inc, flag := fv.loadState()
	switch flag {
	case FlagDone:
		res.incarnation = inc
	case FlagValidated:
		res.incarnation = inc
		res.flags |= readValidated
	case FlagEstimate:
	default:
		panic("unknown flag value")
	}
	return fv.Value, res, true
}

// floorCell returns the highest write strictly below txIdx and its index, or
// (UnknownDep, nil) when none. Caller must hold the address entry's read lock.
func floorCell[T any](cells *btree.Map[int, *WriteCell[T]], txIdx int) (int, *WriteCell[T]) {
	if cells == nil {
		return UnknownDep, nil
	}
	fk := UnknownDep
	var fv *WriteCell[T]
	cells.Descend(txIdx-1, func(k int, v *WriteCell[T]) bool {
		fk, fv = k, v
		return false
	})
	return fk, fv
}

// accountFloors is addr's raw field and address floor cells at txIdx.
type accountFloors struct {
	balance                                                       uint256.Int
	nonce, incarnation                                            uint64
	codeHash                                                      accounts.CodeHash
	address                                                       *accounts.Account
	hasBalance, hasNonce, hasIncarnation, hasCodeHash, hasAddress bool
	balanceDeleted, addressFromBAL, wasDeleted                    bool
}

// accountFloorsAt reads addr's floor cells under one entry lock instead of one lock per field.
func (vm *VersionMap) accountFloorsAt(addr accounts.Address, txIdx int) (f accountFloors) {
	if vm == nil {
		return f
	}
	e := vm.load(addr)
	if e == nil {
		return f
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if _, c := floorCell(e.Balance, txIdx); c != nil {
		f.balance, f.hasBalance, f.balanceDeleted = c.Value, true, c.valStatus == ValueDeleted
	}
	if _, c := floorCell(e.Nonce, txIdx); c != nil {
		f.nonce, f.hasNonce = c.Value, true
	}
	if _, c := floorCell(e.Incarnation, txIdx); c != nil {
		f.incarnation, f.hasIncarnation = c.Value, true
	}
	if _, c := floorCell(e.CodeHash, txIdx); c != nil {
		f.codeHash, f.hasCodeHash = c.Value, true
	}
	if _, c := floorCell(e.Address, txIdx); c != nil {
		f.address, f.hasAddress, f.addressFromBAL = c.Value, true, c.accSource == AccountFromBAL
	}
	life, _, _, _ := accountLifecycleLocked(e, txIdx, false)
	f.wasDeleted = life != LifecycleLive
	return f
}

// applySubFieldWrites overlays the Balance/Nonce/Incarnation/CodeHash floor cells for
// addr onto account under a SINGLE e.mu.RLock. Destruct/lifecycle is resolved centrally
// by the read core, so these are plain same-txIdx floor reads; coalescing them here avoids
// the four separate e.mu acquisitions the per-field versionedUpdate* primitives each take.
func (vm *VersionMap) applySubFieldWrites(addr accounts.Address, txIdx int, account *accounts.Account) {
	if vm == nil {
		return
	}
	e := vm.load(addr)
	if e == nil {
		return
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if _, fv := floorCell(e.Balance, txIdx); fv != nil {
		account.Balance = fv.Value
	}
	// A destruct erases nonce and code hash without a cell of its own, so a cell
	// below a later destruct is stale.
	if k, fv := floorCell(e.Nonce, txIdx); fv != nil {
		account.Nonce = 0
		if _, wiped := findDoneSelfDestructLocked(e, k, txIdx, true); !wiped {
			account.Nonce = fv.Value
		}
	}
	if _, fv := floorCell(e.Incarnation, txIdx); fv != nil {
		account.Incarnation = fv.Value
	}
	if k, fv := floorCell(e.CodeHash, txIdx); fv != nil {
		account.CodeHash = accounts.EmptyCodeHash
		if _, wiped := findDoneSelfDestructLocked(e, k, txIdx, true); !wiped {
			account.CodeHash = fv.Value
		}
	}
}

// lifecycleVerdict is accountLifecycleAt's result folded into a floor read so the
// read path resolves both under a single RLock.
type lifecycleVerdict struct {
	state        AccountLifecycleState
	canonicalVer Version
	destroyedAt  int
	wipeInc      Incarnation
}

// readFloorLife is readFloor that also returns the account lifecycle verdict,
// computed under the SAME RLock. A storage/code read that needs the wipe verdict
// (post-SELFDESTRUCT staleness) takes one lock instead of two. A never-written
// account (no entry) reports LifecycleLive without locking.
func readFloorLife[T any](vm *VersionMap, addr accounts.Address, txIdx int, sel func(*AddressEntry) *btree.Map[int, *WriteCell[T]]) (val T, res ReadResult, ok bool, life lifecycleVerdict) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	life.state = LifecycleLive
	if vm == nil {
		return val, res, false, life
	}
	e := vm.load(addr)
	if e == nil {
		return val, res, false, life
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	life.state, life.canonicalVer, life.destroyedAt, life.wipeInc = accountLifecycleLocked(e, txIdx, false)
	cells := sel(e)
	if cells == nil {
		return val, res, false, life
	}
	fk := UnknownDep
	var fv *WriteCell[T]
	cells.Descend(txIdx-1, func(k int, v *WriteCell[T]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false, life
	}
	res.depIdx = fk
	res.valStatus = fv.valStatus
	if fv.accSource == AccountFromBAL {
		res.flags |= readFromBAL
	}
	inc, flag := fv.loadState()
	switch flag {
	case FlagDone:
		res.incarnation = inc
	case FlagValidated:
		res.incarnation = inc
		res.flags |= readValidated
	case FlagEstimate:
	default:
		panic("unknown flag value")
	}
	return fv.Value, res, true, life
}

// ReadStorageLife is ReadStorage folded with the lifecycle verdict (one RLock).
func (vm *VersionMap) ReadStorageLife(addr accounts.Address, key accounts.StorageKey, txIdx int) (uint256.Int, ReadResult, bool, lifecycleVerdict) {
	val, res, ok, life := readFloorLife(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint256.Int]] {
		if e.Storage == nil {
			return nil
		}
		return e.Storage[key]
	})
	if ok {
		return val, res, true, life
	}
	if ov, seeded := vm.LoadOriginStorage(addr, key); seeded {
		return ov, ReadResult{depIdx: originIndex}, true, life
	}
	return val, res, false, life
}

// ReadNonceLife is ReadNonce folded with the lifecycle verdict (one RLock).
func (vm *VersionMap) ReadNonceLife(addr accounts.Address, txIdx int) (uint64, ReadResult, bool, lifecycleVerdict) {
	return readFloorLife(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint64]] { return e.Nonce })
}

// ReadCodeLife is ReadCode folded with the lifecycle verdict (one RLock).
func (vm *VersionMap) ReadCodeLife(addr accounts.Address, txIdx int) (accounts.Code, ReadResult, bool, lifecycleVerdict) {
	return readFloorLife(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[accounts.Code]] { return e.Code })
}

// ReadCodeSizeLife is ReadCodeSize folded with the lifecycle verdict (one RLock).
func (vm *VersionMap) ReadCodeSizeLife(addr accounts.Address, txIdx int) (int, ReadResult, bool, lifecycleVerdict) {
	return readFloorLife(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[int]] { return e.CodeSize })
}

func (vm *VersionMap) ReadAddress(addr accounts.Address, txIdx int) (*accounts.Account, ReadResult, bool) {
	val, res, ok := readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[*accounts.Account]] { return e.Address })
	if ok {
		return val, res, true
	}
	// Origin fallback: the pre-block committed account lives in the lock-free origin layer at
	// originIndex (immutable), not as a cell. Resolving it here keeps reads and ValidateVersion
	// (which calls this via ReadStatus) consistent without taking the account's e.mu.
	if oacc, seeded := vm.LoadOriginAccount(addr); seeded {
		return oacc, ReadResult{depIdx: originIndex}, true
	}
	return val, res, false
}

func (vm *VersionMap) ReadSelfDestruct(addr accounts.Address, txIdx int) (bool, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[bool]] { return e.SelfDestruct })
}

func (vm *VersionMap) ReadBalance(addr accounts.Address, txIdx int) (uint256.Int, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint256.Int]] { return e.Balance })
}

func (vm *VersionMap) ReadNonce(addr accounts.Address, txIdx int) (uint64, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint64]] { return e.Nonce })
}

func (vm *VersionMap) ReadIncarnation(addr accounts.Address, txIdx int) (uint64, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint64]] { return e.Incarnation })
}

func (vm *VersionMap) ReadCode(addr accounts.Address, txIdx int) (accounts.Code, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[accounts.Code]] { return e.Code })
}

func (vm *VersionMap) ReadCodeHash(addr accounts.Address, txIdx int) (accounts.CodeHash, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[accounts.CodeHash]] { return e.CodeHash })
}

func (vm *VersionMap) ReadCodeSize(addr accounts.Address, txIdx int) (int, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[int]] { return e.CodeSize })
}

func (vm *VersionMap) ReadCreateContract(addr accounts.Address, txIdx int) (bool, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[bool]] { return e.CreateContract })
}

func (vm *VersionMap) ReadStorage(addr accounts.Address, key accounts.StorageKey, txIdx int) (uint256.Int, ReadResult, bool) {
	val, res, ok := readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint256.Int]] {
		if e.Storage == nil {
			return nil
		}
		return e.Storage[key]
	})
	if ok {
		return val, res, true
	}
	// Origin fallback: pre-block committed slot value at originIndex (immutable), lock-free.
	if ov, seeded := vm.LoadOriginStorage(addr, key); seeded {
		return ov, ReadResult{depIdx: originIndex}, true
	}
	return val, res, false
}

// ReadStatus returns a path's read outcome for callers that need only version/status
// (the validator, revival checks), dispatching to the typed ReadX and discarding the value.
func (vm *VersionMap) ReadStatus(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) ReadResult {
	var res ReadResult
	switch path {
	case AddressPath:
		_, res, _ = vm.ReadAddress(addr, txIdx)
	case BalancePath:
		_, res, _ = vm.ReadBalance(addr, txIdx)
	case NoncePath:
		_, res, _ = vm.ReadNonce(addr, txIdx)
	case IncarnationPath:
		_, res, _ = vm.ReadIncarnation(addr, txIdx)
	case CodePath:
		_, res, _ = vm.ReadCode(addr, txIdx)
	case CodeHashPath:
		_, res, _ = vm.ReadCodeHash(addr, txIdx)
	case CodeSizePath:
		_, res, _ = vm.ReadCodeSize(addr, txIdx)
	case SelfDestructPath:
		_, res, _ = vm.ReadSelfDestruct(addr, txIdx)
	case CreateContractPath:
		_, res, _ = vm.ReadCreateContract(addr, txIdx)
	case StoragePath:
		_, res, _ = vm.ReadStorage(addr, key, txIdx)
	default:
		panic(fmt.Errorf("ReadStatus: unknown path %v", path))
	}
	return res
}

// LatestTxIndex returns the largest TxIndex (≤ txIdxLimit) with a write for (addr, path,
// key), ok=false when none. Used to detect account revival after a SelfDestruct.
func (vm *VersionMap) LatestTxIndex(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdxLimit int) (int, bool) {
	if vm == nil {
		return 0, false
	}
	e := vm.load(addr)
	if e == nil {
		return 0, false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	fk := UnknownDep
	switch path {
	case AddressPath:
		if e.Address != nil {
			e.Address.Descend(txIdxLimit, func(k int, _ *WriteCell[*accounts.Account]) bool { fk = k; return false })
		}
	case SelfDestructPath:
		if e.SelfDestruct != nil {
			e.SelfDestruct.Descend(txIdxLimit, func(k int, _ *WriteCell[bool]) bool { fk = k; return false })
		}
	case BalancePath:
		if e.Balance != nil {
			e.Balance.Descend(txIdxLimit, func(k int, _ *WriteCell[uint256.Int]) bool { fk = k; return false })
		}
	case NoncePath:
		if e.Nonce != nil {
			e.Nonce.Descend(txIdxLimit, func(k int, _ *WriteCell[uint64]) bool { fk = k; return false })
		}
	case IncarnationPath:
		if e.Incarnation != nil {
			e.Incarnation.Descend(txIdxLimit, func(k int, _ *WriteCell[uint64]) bool { fk = k; return false })
		}
	case CodePath:
		if e.Code != nil {
			e.Code.Descend(txIdxLimit, func(k int, _ *WriteCell[accounts.Code]) bool { fk = k; return false })
		}
	case CodeHashPath:
		if e.CodeHash != nil {
			e.CodeHash.Descend(txIdxLimit, func(k int, _ *WriteCell[accounts.CodeHash]) bool { fk = k; return false })
		}
	case CodeSizePath:
		if e.CodeSize != nil {
			e.CodeSize.Descend(txIdxLimit, func(k int, _ *WriteCell[int]) bool { fk = k; return false })
		}
	case CreateContractPath:
		if e.CreateContract != nil {
			e.CreateContract.Descend(txIdxLimit, func(k int, _ *WriteCell[bool]) bool { fk = k; return false })
		}
	case StoragePath:
		if cells := e.Storage[key]; cells != nil {
			cells.Descend(txIdxLimit, func(k int, _ *WriteCell[uint256.Int]) bool { fk = k; return false })
		}
	default:
		return 0, false
	}
	if fk == UnknownDep {
		return 0, false
	}
	return fk, true
}

// AccountLifecycleState enumerates an account's existence at a txIdx, resolved once so
// readers, validation and the create decision branch on the same verdict.
type AccountLifecycleState uint8

const (
	// LifecycleLive: no Done SelfDestruct=true in effect at txIdx.
	LifecycleLive AccountLifecycleState = iota
	// LifecycleAbsent: destroyed with no revival above it; a pre-block base read is not stale.
	LifecycleAbsent
	// LifecycleRevived: destroyed but re-created above the destruct; storage on/before the
	// destruct is wiped and stale base reads are invalidated. EIP-8246 balance-preserve is
	// not resolved here — only a fork-aware caller can decide whether the account still exists.
	LifecycleRevived
)

// AccountLifecycleAt resolves an account's lifecycle in one pass: the state, the canonical version dependent reads must anchor on, and the destruct (wipe) TxIndex.
func (vm *VersionMap) AccountLifecycleAt(addr accounts.Address, txIdx int) (state AccountLifecycleState, canonicalVer Version, destroyedAt int, wipeInc Incarnation) {
	return vm.accountLifecycleAt(addr, txIdx, false)
}

// ResolvedAccountLifecycleAt counts only sealed (Done) revival writes. Anything
// above a destruct that is still revertible may be withdrawn, so validation must
// not take it as proof the account came back; the read path wants the opposite
// and uses AccountLifecycleAt.
func (vm *VersionMap) ResolvedAccountLifecycleAt(addr accounts.Address, txIdx int) (state AccountLifecycleState, canonicalVer Version, destroyedAt int, wipeInc Incarnation) {
	return vm.accountLifecycleAt(addr, txIdx, true)
}

func (vm *VersionMap) accountLifecycleAt(addr accounts.Address, txIdx int, resolvedRevivalsOnly bool) (state AccountLifecycleState, canonicalVer Version, destroyedAt int, wipeInc Incarnation) {
	if vm == nil {
		return LifecycleLive, Version{}, 0, 0
	}
	e := vm.load(addr)
	if e == nil {
		return LifecycleLive, Version{}, 0, 0
	}
	// One RLock for the whole verdict so it cannot observe the account mid-flush.
	e.mu.RLock()
	defer e.mu.RUnlock()
	return accountLifecycleLocked(e, txIdx, resolvedRevivalsOnly)
}

// accountLifecycleLocked computes the lifecycle verdict for an already-loaded
// entry; the caller must hold e.mu.RLock. Lets a floor read fold the lifecycle
// check into its own lock instead of taking e.mu a second time.
func accountLifecycleLocked(e *AddressEntry, txIdx int, resolvedRevivalsOnly bool) (state AccountLifecycleState, canonicalVer Version, destroyedAt int, wipeInc Incarnation) {
	if e.SelfDestruct == nil {
		return LifecycleLive, Version{}, 0, 0
	}

	var latest *WriteCell[bool]
	var latestIdx int
	haveLatest := false
	wiped := false
	e.SelfDestruct.Descend(txIdx-1, func(k int, v *WriteCell[bool]) bool {
		if !haveLatest {
			latest, latestIdx, haveLatest = v, k, true
		}
		// A validated (pre-seal) self-destruct is as authoritative as a Done one:
		// the read side (readSelfDestructMemo→resolved) already treats it as a
		// destruct, so this single lifecycle authority must too, or the two disagree
		// on a validated destruct and a wiped-slot read never settles.
		if v.settled() && v.Value {
			destroyedAt, wipeInc, wiped = k, v.loadInc(), true
			return false
		}
		return true
	})
	if !wiped {
		return LifecycleLive, Version{}, 0, 0
	}
	if latest.settled() {
		canonicalVer = Version{TxIndex: latestIdx, Incarnation: latest.loadInc()}
	} else {
		canonicalVer = Version{TxIndex: destroyedAt, Incarnation: wipeInc}
	}

	revivalLimit := txIdx - 1
	if hi, ok := highestBelow(e.Address, revivalLimit, resolvedRevivalsOnly); ok && hi >= destroyedAt {
		if c, _ := e.Address.Get(hi); hi > destroyedAt || c.accSource != AccountFromBAL {
			return LifecycleRevived, canonicalVer, destroyedAt, wipeInc
		}
	}
	if hi, ok := highestBelow(e.Balance, revivalLimit, resolvedRevivalsOnly); ok && hi > destroyedAt {
		return LifecycleRevived, canonicalVer, destroyedAt, wipeInc
	}
	if hi, ok := highestBelow(e.Nonce, revivalLimit, resolvedRevivalsOnly); ok && hi > destroyedAt {
		return LifecycleRevived, canonicalVer, destroyedAt, wipeInc
	}
	if hi, ok := highestBelow(e.CodeHash, revivalLimit, resolvedRevivalsOnly); ok && hi > destroyedAt {
		return LifecycleRevived, canonicalVer, destroyedAt, wipeInc
	}
	return LifecycleAbsent, canonicalVer, destroyedAt, wipeInc
}

// highestBelow returns the largest TxIndex ≤ limit present in cells, if any. The
// caller must hold the owning AddressEntry's lock.
func highestBelow[T any](cells *btree.Map[int, *WriteCell[T]], limit int, resolvedOnly bool) (int, bool) {
	if cells == nil {
		return 0, false
	}
	hi, ok := 0, false
	cells.Descend(limit, func(k int, v *WriteCell[T]) bool {
		// Only Done proves a lasting revival: a Validated cell is still revertible
		// until the in-order seal, and withdrawing it re-validates its own readers,
		// not the AddressPath reader this verdict serves.
		if resolvedOnly && v.loadFlag() != FlagDone {
			return true
		}
		hi, ok = k, true
		return false
	})
	return hi, ok
}

// FindDoneSelfDestructInRange returns the version of the highest Done
// SelfDestruct write with lo <= TxIdx < hi whose value == target, if any. It
// finds an in-block SELFDESTRUCT even when a later revival (SelfDestruct=false)
// hides it from latest-only ReadSelfDestruct.
func (vm *VersionMap) FindDoneSelfDestructInRange(addr accounts.Address, lo, hi int, target bool) (Version, bool) {
	if vm == nil || hi <= lo {
		return Version{}, false
	}
	e := vm.load(addr)
	if e == nil {
		return Version{}, false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return findDoneSelfDestructLocked(e, lo, hi, target)
}

// selfDestructInRange is FindDoneSelfDestructInRange but also counts a Validated
// (pre-seal) destruct as authoritative, matching accountLifecycleLocked. wipedCommitted
// uses it so a code / code-size read is invalidated by an in-flight destruct that wiped
// it before this tx — a Done-only scan misses the pre-seal destruct and the stale read
// commits. The validator's wiped-code check and the BAL cold-storage exemption both rely
// on this wider authority.
func (vm *VersionMap) selfDestructInRange(addr accounts.Address, lo, hi int, target bool) (Version, bool) {
	if vm == nil || hi <= lo {
		return Version{}, false
	}
	e := vm.load(addr)
	if e == nil {
		return Version{}, false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.SelfDestruct == nil {
		return Version{}, false
	}
	var ver Version
	found := false
	e.SelfDestruct.Descend(hi-1, func(k int, v *WriteCell[bool]) bool {
		if k < lo {
			return false
		}
		if v.settled() && v.Value == target {
			ver = Version{TxIndex: k, Incarnation: v.loadInc()}
			found = true
			return false
		}
		return true
	})
	return ver, found
}

// findDoneSelfDestructLocked is the destruct scan every consumer shares, for
// callers already holding e.mu. Estimate cells do not count: a verdict drawn from
// one is published before the round that retracts it, and the reconstruction
// readers consume it there without recording a read.
func findDoneSelfDestructLocked(e *AddressEntry, lo, hi int, target bool) (Version, bool) {
	if e.SelfDestruct == nil || hi <= lo {
		return Version{}, false
	}
	var ver Version
	found := false
	e.SelfDestruct.Descend(hi-1, func(k int, v *WriteCell[bool]) bool {
		if k < lo {
			return false
		}
		if v.loadFlag() == FlagDone && v.Value == target {
			ver = Version{TxIndex: k, Incarnation: v.loadInc()}
			found = true
			return false
		}
		return true
	})
	return ver, found
}

// IsNetAbsent reports whether the account reads as gone at txIdx (LifecycleAbsent; see AccountLifecycleAt).
func (vm *VersionMap) IsNetAbsent(addr accounts.Address, txIdx int) bool {
	state, _, _, _ := vm.AccountLifecycleAt(addr, txIdx)
	return state == LifecycleAbsent
}

// AccountLifecycleWithDestructDep is AccountLifecycleAt plus, unless the account is absent, the index
// of a destruct below txIdx that the verdict did not count as settled, or UnknownDep. Both come from
// one lock and one flag load per cell: a mark can move the destruct's flag between two separate probes.
func (vm *VersionMap) AccountLifecycleWithDestructDep(addr accounts.Address, txIdx int) (state AccountLifecycleState, dep int) {
	if vm == nil {
		return LifecycleLive, UnknownDep
	}
	e := vm.load(addr)
	if e == nil {
		return LifecycleLive, UnknownDep
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	state, _, _, _ = accountLifecycleLocked(e, txIdx, false)
	if state == LifecycleAbsent {
		return state, UnknownDep
	}
	fk, fv := floorCell(e.SelfDestruct, txIdx)
	// A settled destruct that a live verdict did not count was still in flight when the verdict ran.
	if fv != nil && (!fv.settled() || state == LifecycleLive && fv.Value) {
		return state, fk
	}
	return state, UnknownDep
}

// destructKeptBalance reports whether the destruct at destructTxIndex kept a non-zero balance
// (EIP-8246), so that the account survived it although the destruct dropped its record. A balance
// that is still an estimate is not a verdict yet, so it counts as kept.
func (vm *VersionMap) destructKeptBalance(addr accounts.Address, destructTxIndex int) bool {
	bal, res, ok := vm.ReadBalance(addr, destructTxIndex+1)
	return ok && (!res.resolved() || res.ValStatus() != ValueDeleted && !bal.IsZero())
}

// netAbsentDestruct reports whether a lower tx left addr net-absent via create+self-destruct with no revival above it, so a base read of it is not stale (see IsNetAbsent).
func (vm *VersionMap) netAbsentDestruct(addr accounts.Address, txIndex int) bool {
	destructed, sdRR, ok := vm.ReadSelfDestruct(addr, txIndex)
	if !ok || !destructed || (!sdRR.resolved() && sdRR.Status() != MVReadResultDependency) {
		return false
	}
	destructTxIndex := sdRR.DepIdx()
	revivalLimit := txIndex - 1
	if hi, ok := vm.LatestTxIndex(addr, AddressPath, accounts.NilKey, revivalLimit); ok && hi > destructTxIndex {
		return false
	}
	for _, p := range [...]AccountPath{BalancePath, NoncePath, CodeHashPath} {
		if hi, ok := vm.LatestTxIndex(addr, p, accounts.NilKey, revivalLimit); ok && hi > destructTxIndex {
			return false
		}
	}
	// EIP-8246: a self-destruct may preserve a non-zero balance/nonce, keeping the account
	// alive as balance-only. That preserve is written in the destruct tx itself, so the
	// strictly-above revival checks miss it — the account is not net-absent.
	if bal, _, ok := vm.ReadBalance(addr, txIndex); ok && !bal.IsZero() {
		return false
	}
	if nonce, _, ok := vm.ReadNonce(addr, txIndex); ok && nonce != 0 {
		return false
	}
	return true
}

// flushCell keeps the cell's incarnation when a re-execution flushes an equal value at
// the same txIndex, so readers bound to it stay valid. A reincarnated account always
// re-versions: across a removal an equal value does not mean an unchanged state.
func flushCell[T any](vm *VersionMap, e *AddressEntry, cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, vw *VersionedWrite[T], flag statusFlag, eq func(a, b T) bool, destructed bool) *btree.Map[int, *WriteCell[T]] {
	if !destructed && cells != nil {
		if ci, ok := cells.Get(vw.Version.TxIndex); ok && eq(ci.Value, vw.Val) {
			ci.setFlagUnlessDone(flag)
			ci.valStatus = vw.valStatus
			ci.accSource = AccountFromBase
			vw.bind(e, ci)
			return cells
		}
	}
	cells, cell := putCell(vm, cells, addr, path, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, vw.valStatus)
	vw.bind(e, cell)
	return cells
}

// noOpBind reports whether flushing vw would leave its cell unchanged — flushCell would take
// its no-bump branch and the in-place flag/valStatus update would itself be a no-op — and binds
// vw to that cell. Caller holds the entry's read lock. Mirrors flushCell so skipping the flush
// is behavior-identical to performing it.
func noOpBind[T any](e *AddressEntry, cells *btree.Map[int, *WriteCell[T]], vw *VersionedWrite[T], flag statusFlag, eq func(a, b T) bool) bool {
	if cells == nil {
		return false
	}
	ci, ok := cells.Get(vw.Version.TxIndex)
	if !ok || !eq(ci.Value, vw.Val) || ci.valStatus != vw.valStatus || ci.accSource != AccountFromBase {
		return false
	}
	if flag == flagSeeded {
		if !ci.seeded() {
			return false
		}
	} else if f := ci.loadFlag(); f != flag && !(f == FlagDone && flag == FlagEstimate) {
		return false
	}
	vw.bind(e, ci)
	return true
}

// flushAllNoOp reports whether every write this tx made to addr is already present, so the
// per-tx flush can be skipped without taking the write lock (and thus without parking the
// concurrent readers on a hot pre-seeded account). A net-zero storage write (ValueUnchanged)
// is omitted from the BAL and folded away by StorageChanges, so it is never materialized and
// counts as already-present, unless an earlier run of the tx left a cell the flush resets or removes.
// Only incarnation, createContract, and selfDestruct force the slow path.
// Caller holds e's read lock; only used on the BAL fast path.
func flushAllNoOp(e *AddressEntry, writes *WriteSet, addr accounts.Address, flag statusFlag) bool {
	if _, ok := writes.selfDestruct[addr]; ok {
		return false
	}
	if _, ok := writes.incarnation[addr]; ok {
		return false
	}
	if vw, ok := writes.code[addr]; ok && !noOpBind(e, e.Code, vw, flag, eqCode) {
		return false
	}
	if vw, ok := writes.codeSize[addr]; ok && !noOpBind(e, e.CodeSize, vw, flag, eqInt) {
		return false
	}
	if _, ok := writes.createContract[addr]; ok {
		return false
	}
	if vw, ok := writes.address[addr]; ok && !noOpBind(e, e.Address, vw, flag, eqAccount) {
		return false
	}
	if vw, ok := writes.balance[addr]; ok && !noOpBind(e, e.Balance, vw, flag, eqUint256) {
		return false
	}
	if vw, ok := writes.nonce[addr]; ok && !noOpBind(e, e.Nonce, vw, flag, eqUint64) {
		return false
	}
	if vw, ok := writes.codeHash[addr]; ok && !noOpBind(e, e.CodeHash, vw, flag, eqCodeHash) {
		return false
	}
	if inner, ok := writes.storage[addr]; ok {
		for key, vw := range inner {
			var cells *btree.Map[int, *WriteCell[uint256.Int]]
			if e.Storage != nil {
				cells = e.Storage[key]
			}
			if vw.valStatus == ValueUnchanged {
				if cells != nil {
					if c, ok := cells.Get(vw.Version.TxIndex); ok && c.loadFlag() != FlagDone {
						return false
					}
				}
				continue
			}
			if !noOpBind(e, cells, vw, flag, eqUint256) {
				return false
			}
		}
	}
	return true
}

// FlushVersionedWrites routes a tx's typed write collections into the version map. Each
// cell is positioned by the write's (txIndex, incarnation), so the loop order is irrelevant.
func (vm *VersionMap) FlushVersionedWrites(writes *WriteSet, complete bool) {
	if writes == nil {
		return
	}
	flag := flagFor(complete)
	// Flush per account under that account's lock so all of a tx's writes to one account
	// become visible atomically. Cross-account partial visibility is resolved by
	// commit-time ValidateVersion.
	seen := make(map[accounts.Address]struct{})
	wtx, stamped := 0, false
	defer func() {
		if stamped {
			vm.mutEnd(wtx)
		}
	}()
	writes.forEachAddr(func(addr accounts.Address) {
		if _, dup := seen[addr]; dup {
			return
		}
		seen[addr] = struct{}{}
		// Check-under-RLock: on a BAL block the cells are pre-seeded, so a tx's writes are
		// usually already present. Verify that under the read lock and skip the write lock
		// entirely when nothing changes, so a flush never parks the concurrent readers on a
		// hot account. Promotes to the write lock below only when a cell actually changes.
		if vm.HasBAL && !complete {
			if e := vm.load(addr); e != nil && flushAllNoOpUnderRLock(e, writes, addr, flag) {
				return
			}
		}
		if !stamped && vm.stamps != nil {
			wtx, stamped = writes.txIndex(), true
			vm.mutBegin(wtx)
		}
		e := vm.entryOrCreate(addr)
		e.mu.Lock()
		defer e.mu.Unlock()
		// CreateAccount also writes SelfDestruct=false for a fresh address, which is no removal.
		destructed := false
		if sd, ok := writes.selfDestruct[addr]; ok {
			destructed = sd.Val
			if !destructed {
				state, _, _, _ := accountLifecycleLocked(e, sd.Version.TxIndex, false)
				destructed = state != LifecycleLive
			}
		}
		if vw, ok := writes.address[addr]; ok {
			e.Address = flushCell(vm, e, e.Address, addr, AddressPath, vw, flag, eqAccount, destructed)
		}
		if vw, ok := writes.selfDestruct[addr]; ok {
			cells, cell := putCell(vm, e.SelfDestruct, addr, SelfDestructPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, vw.valStatus)
			e.SelfDestruct = cells
			vw.bind(e, cell)
			if vw.Val {
				e.destructed.Store(true)
			}
		}
		if vw, ok := writes.balance[addr]; ok {
			e.Balance = flushCell(vm, e, e.Balance, addr, BalancePath, vw, flag, eqUint256, destructed)
		}
		if vw, ok := writes.nonce[addr]; ok {
			e.Nonce = flushCell(vm, e, e.Nonce, addr, NoncePath, vw, flag, eqUint64, destructed)
		}
		if vw, ok := writes.incarnation[addr]; ok {
			cells, cell := putCell(vm, e.Incarnation, addr, IncarnationPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, vw.valStatus)
			e.Incarnation = cells
			vw.bind(e, cell)
		}
		if vw, ok := writes.code[addr]; ok {
			e.Code = flushCell(vm, e, e.Code, addr, CodePath, vw, flag, eqCode, destructed)
		}
		if vw, ok := writes.codeHash[addr]; ok {
			e.CodeHash = flushCell(vm, e, e.CodeHash, addr, CodeHashPath, vw, flag, eqCodeHash, destructed)
		}
		if vw, ok := writes.codeSize[addr]; ok {
			e.CodeSize = flushCell(vm, e, e.CodeSize, addr, CodeSizePath, vw, flag, eqInt, destructed)
		}
		if vw, ok := writes.createContract[addr]; ok {
			cells, cell := putCell(vm, e.CreateContract, addr, CreateContractPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, vw.valStatus)
			e.CreateContract = cells
			vw.bind(e, cell)
		}
		if inner, ok := writes.storage[addr]; ok {
			if e.Storage == nil {
				e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
			}
			for key, vw := range inner {
				// On a BAL block a net-zero write is omitted from the BAL and folded away by
				// StorageChanges, so it is never materialized here (nor sealed below). The slot
				// must read as the access list seeds it, so a cell an earlier run of the tx left
				// goes back to the seed or away. Before the seal a Done cell holds the seed.
				if vm.HasBAL && vw.valStatus == ValueUnchanged {
					if cells := e.Storage[key]; cells != nil {
						if c, ok := cells.Get(vw.Version.TxIndex); ok && c.loadFlag() != FlagDone {
							if seed, seeded := vm.seededStorage(addr, key, vw.Version.TxIndex); seeded {
								// As in flushCell, a changed value needs a new version, so that the
								// readers of the earlier value are invalidated.
								inc := c.loadInc()
								if !c.Value.Eq(&seed) {
									inc = vw.Version.Incarnation
								}
								c.Value, c.valStatus = seed, ValueChanged
								c.setState(inc, FlagDone)
							} else if c.setFlagUnlessDone(flagDeleted) {
								cells.Delete(vw.Version.TxIndex)
							}
						}
					}
					continue
				}
				e.Storage[key] = flushCell(vm, e, e.Storage[key], addr, StoragePath, vw, flag, eqUint256, destructed)
			}
		}
	})
}

// flushAllNoOpUnderRLock is flushAllNoOp under the account's read lock, released before the flush takes the write lock.
func flushAllNoOpUnderRLock(e *AddressEntry, writes *WriteSet, addr accounts.Address, flag statusFlag) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return flushAllNoOp(e, writes, addr, flag)
}

// WritesAlreadySeeded reports whether ws's writes to addr match BAL seed cells no value store
// has replaced, so publishing them changed nothing a reader saw.
func (vm *VersionMap) WritesAlreadySeeded(addr accounts.Address, ws *WriteSet) bool {
	if !vm.HasBAL {
		return false
	}
	// A net-zero write has no BAL cell, and a re-run may have dropped the cell a reader saw.
	for _, vw := range ws.storage[addr] {
		if vw.valStatus == ValueUnchanged {
			return false
		}
	}
	e := vm.load(addr)
	return e != nil && flushAllNoOpUnderRLock(e, ws, addr, flagSeeded)
}

// MarkWritesComplete advances every cell named by writes from Estimate to Done, writing
// no values (each cell must already hold the write's value at its incarnation). It is the
// commit-boundary check enforcing one-value-per-version — a mismatch panics.
func (vm *VersionMap) MarkWritesComplete(writes *WriteSet) {
	vm.markWrites(writes, FlagDone, nil)
}

// MarkWritesValidated flips every cell named by writes Estimate->Validated: the tx
// validated its read-set out-of-order but is not yet sealed, so a reader continues on it
// (early break) while it stays revertible until the in-order seal. A fee recipient's cells
// are left Estimate, so readers keep pausing until the seal. calcFees credits its fee, and
// may undo its EIP-161 removal, at the same version; while its SelfDestruct cell is Estimate,
// a reader cannot tell that the destruct wipes the tx's other writes.
func (vm *VersionMap) MarkWritesValidated(writes *WriteSet, isFeeRecipient func(accounts.Address) bool) {
	vm.markWrites(writes, FlagValidated, isFeeRecipient)
}

type cellMarks struct {
	vm     *VersionMap
	flag   statusFlag
	fee    func(accounts.Address) bool
	locked map[accounts.Address]struct{}
}

func (vm *VersionMap) markWrites(writes *WriteSet, flag statusFlag, isFeeRecipient func(accounts.Address) bool) {
	if writes == nil {
		return
	}
	m := cellMarks{vm: vm, flag: flag, fee: isFeeRecipient}
	for addr, vw := range writes.address {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.selfDestruct {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.balance {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.nonce {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.incarnation {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.code {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.codeHash {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.codeSize {
		markCell(&m, addr, vw)
	}
	for addr, vw := range writes.createContract {
		markCell(&m, addr, vw)
	}
	for addr, inner := range writes.storage {
		for _, vw := range inner {
			// Net-zero writes are never materialized on a BAL block (see FlushVersionedWrites),
			// so there is no cell to mark.
			if vm.HasBAL && vw.valStatus == ValueUnchanged {
				continue
			}
			markCell(&m, addr, vw)
		}
	}
	for addr := range m.locked {
		vm.markAccountLocked(addr, writes, flag)
	}
}

func markCell[T any](m *cellMarks, addr accounts.Address, vw *VersionedWrite[T]) {
	if m.fee != nil && m.fee(addr) {
		return
	}
	c, e := vw.cell, vw.entry
	if c == nil || e.destructed.Load() || m.vm.stamps != nil || (m.flag == FlagDone && dbg.AssertEnabled) {
		m.lock(addr)
		return
	}
	want := vw.Version.Incarnation
	if m.flag == FlagDone {
		want = -1
	}
	switch have, outcome := c.mark(want, m.flag); outcome {
	case markStale:
		panic(staleMarkMsg(have, want, addr, vw.Path, vw.Key, vw.Version.TxIndex))
	case markDeleted:
		m.lock(addr)
	}
}

func (m *cellMarks) lock(addr accounts.Address) {
	if m.locked == nil {
		m.locked = make(map[accounts.Address]struct{})
	}
	m.locked[addr] = struct{}{}
}

func (vm *VersionMap) markAccountLocked(addr accounts.Address, writes *WriteSet, flag statusFlag) {
	e := vm.load(addr)
	if e == nil {
		panic(fmt.Sprintf("mark: no entry addr=%x", addr.Value()))
	}
	e.mu.RLock()
	if e.destructed.Load() || vm.stamps != nil {
		e.mu.RUnlock()
		e.mu.Lock()
		defer e.mu.Unlock()
	} else {
		defer e.mu.RUnlock()
	}
	if vm.stamps != nil && hasLifecycleCells(e) {
		wtx := writes.txIndex()
		vm.mutBegin(wtx)
		defer vm.mutEnd(wtx)
	}
	if vw, ok := writes.address[addr]; ok {
		markLocked(e.Address, addr, AddressPath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.selfDestruct[addr]; ok {
		markLocked(e.SelfDestruct, addr, SelfDestructPath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.balance[addr]; ok {
		markLocked(e.Balance, addr, BalancePath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.nonce[addr]; ok {
		markLocked(e.Nonce, addr, NoncePath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.incarnation[addr]; ok {
		markLocked(e.Incarnation, addr, IncarnationPath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.code[addr]; ok {
		markLocked(e.Code, addr, CodePath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.codeHash[addr]; ok {
		markLocked(e.CodeHash, addr, CodeHashPath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.codeSize[addr]; ok {
		markLocked(e.CodeSize, addr, CodeSizePath, accounts.NilKey, vw, flag)
	}
	if vw, ok := writes.createContract[addr]; ok {
		markLocked(e.CreateContract, addr, CreateContractPath, accounts.NilKey, vw, flag)
	}
	if inner, ok := writes.storage[addr]; ok {
		for key, vw := range inner {
			if vm.HasBAL && vw.valStatus == ValueUnchanged {
				continue
			}
			markLocked(e.Storage[key], addr, StoragePath, key, vw, flag)
		}
	}
}

func markLocked[T any](cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, key accounts.StorageKey, vw *VersionedWrite[T], flag statusFlag) {
	if flag == FlagDone {
		markCellComplete(cells, addr, path, key, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		return
	}
	markCellFlag(cells, addr, path, key, vw.Version.TxIndex, vw.Version.Incarnation, flag)
}

func deleteCell[T any](vm *VersionMap, m *btree.Map[int, *WriteCell[T]], txIdx int, stamped *bool) {
	c, ok := m.Get(txIdx)
	if !ok {
		return
	}
	if !*stamped {
		vm.mutBegin(txIdx)
		*stamped = true
	}
	m.Delete(txIdx)
	c.mark(-1, flagDeleted)
}

func (vm *VersionMap) endDeleteStamp(txIdx int, stamped *bool) {
	if *stamped {
		vm.mutEnd(txIdx)
	}
}

func (vm *VersionMap) Delete(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, checkExists bool) {
	vm.assertUnsealed(txIdx, addr, path, key)
	e := vm.load(addr)
	if e == nil {
		if !checkExists {
			return
		}
		panic(errors.New("path must already exist"))
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var stamped bool
	defer vm.endDeleteStamp(txIdx, &stamped)
	var hasField bool
	switch path {
	case AddressPath:
		if e.Address != nil {
			hasField = true
			deleteCell(vm, e.Address, txIdx, &stamped)
		}
	case SelfDestructPath:
		if e.SelfDestruct != nil {
			hasField = true
			deleteCell(vm, e.SelfDestruct, txIdx, &stamped)
		}
	case BalancePath:
		if e.Balance != nil {
			hasField = true
			deleteCell(vm, e.Balance, txIdx, &stamped)
		}
	case NoncePath:
		if e.Nonce != nil {
			hasField = true
			deleteCell(vm, e.Nonce, txIdx, &stamped)
		}
	case IncarnationPath:
		if e.Incarnation != nil {
			hasField = true
			deleteCell(vm, e.Incarnation, txIdx, &stamped)
		}
	case CodePath:
		if e.Code != nil {
			hasField = true
			deleteCell(vm, e.Code, txIdx, &stamped)
		}
	case CodeHashPath:
		if e.CodeHash != nil {
			hasField = true
			deleteCell(vm, e.CodeHash, txIdx, &stamped)
		}
	case CodeSizePath:
		if e.CodeSize != nil {
			hasField = true
			deleteCell(vm, e.CodeSize, txIdx, &stamped)
		}
	case CreateContractPath:
		if e.CreateContract != nil {
			hasField = true
			deleteCell(vm, e.CreateContract, txIdx, &stamped)
		}
	case StoragePath:
		if cells := e.Storage[key]; cells != nil {
			hasField = true
			deleteCell(vm, cells, txIdx, &stamped)
		}
	default:
		panic(fmt.Errorf("Delete: unknown path %v", path))
	}
	if !hasField && checkExists {
		panic(errors.New("path must already exist"))
	}
}

func (vm *VersionMap) DeleteAll(addr accounts.Address, txIdx int) {
	vm.assertUnsealed(txIdx, addr, AddressPath, accounts.NilKey)
	e := vm.load(addr)
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var stamped bool
	defer vm.endDeleteStamp(txIdx, &stamped)
	if e.Address != nil {
		deleteCell(vm, e.Address, txIdx, &stamped)
	}
	if e.SelfDestruct != nil {
		deleteCell(vm, e.SelfDestruct, txIdx, &stamped)
	}
	if e.Balance != nil {
		deleteCell(vm, e.Balance, txIdx, &stamped)
	}
	if e.Nonce != nil {
		deleteCell(vm, e.Nonce, txIdx, &stamped)
	}
	if e.Incarnation != nil {
		deleteCell(vm, e.Incarnation, txIdx, &stamped)
	}
	if e.Code != nil {
		deleteCell(vm, e.Code, txIdx, &stamped)
	}
	if e.CodeHash != nil {
		deleteCell(vm, e.CodeHash, txIdx, &stamped)
	}
	if e.CodeSize != nil {
		deleteCell(vm, e.CodeSize, txIdx, &stamped)
	}
	if e.CreateContract != nil {
		deleteCell(vm, e.CreateContract, txIdx, &stamped)
	}
	for _, cells := range e.Storage {
		deleteCell(vm, cells, txIdx, &stamped)
	}
}

type VersionValidity int

func (v VersionValidity) String() string {
	switch v {
	case VersionValid:
		return "valid"
	case VersionInvalid:
		return "invalid"
	case VersionTooEarly:
		return "too early"
	default:
		return "unknown"
	}
}

const (
	VersionValid VersionValidity = iota
	VersionInvalid
	VersionTooEarly
)

func wipedByDestruct(path AccountPath) bool {
	return path == AddressPath || path == BalancePath || path == NoncePath || path == CodeSizePath || path == StoragePath
}

// liveStorage is the one path that needs a dedicated live reader: a slot's wipe after a
// destruct is a lifecycle verdict ReadStorage alone does not carry. Every other path
// validates on the plain ReadStatus result (version + status, no value), so ValidateReadSet
// uses ReadStatus directly for them.
func liveStorage(vm *VersionMap, a accounts.Address, k accounts.StorageKey, tx int) ReadResult {
	_, rr, _ := liveStorageValue(vm, a, k, tx)
	return rr
}

func liveStorageValue(vm *VersionMap, a accounts.Address, k accounts.StorageKey, tx int) (uint256.Int, ReadResult, bool) {
	// Wipe-aware: a slot of a destructed account reads zero unless a post-destruct write
	// revived it, anchored on the destruct ({destroyedAt, wipeInc}) so it validates by
	// version alone — a later store above the destruct resolves here and version-mismatches.
	if state, _, destroyedAt, wipeInc := vm.AccountLifecycleAt(a, tx); state != LifecycleLive {
		if val, res, ok := vm.ReadStorage(a, k, tx); ok && res.resolved() && res.DepIdx() > destroyedAt {
			return val, res, ok
		}
		return uint256.Int{}, ReadResult{depIdx: destroyedAt, incarnation: wipeInc}, true
	}
	return vm.ReadStorage(a, k, tx)
}

func eqUint256(a, b uint256.Int) bool { return a.Eq(&b) }
func eqUint64(a, b uint64) bool       { return a == b }
func eqCodeHash(a, b accounts.CodeHash) bool {
	return a == b
}
func eqCode(a, b accounts.Code) bool { return a.Hash == b.Hash && bytes.Equal(a.Bytes, b.Bytes) }
func eqInt(a, b int) bool            { return a == b }

func eqAccount(a, b *accounts.Account) bool {
	return a != nil && b != nil && a.Equals(b)
}

// validateReadImpl is the recursive validation core. Cross-validate probes pass
// recursive=true: a synthetic probe carries no recorded value of its own and must not
// invalidate on a bare Done entry.
func (vm *VersionMap) validateReadImpl(txIndex int, addr accounts.Address, path AccountPath, key accounts.StorageKey, source ReadSource, version Version,
	rr ReadResult,
	checkVersion func(readVersion, writeVersion Version) VersionValidity,
	traceInvalid bool, tracePrefix string, recursive bool,
) VersionValidity {
	if !recursive && source == MapRead && wipedByDestruct(path) {
		// A field read wiped by a destruct is recorded at the destruct's version. It
		// stays valid while that destruct holds and no later write revives the field.
		if destructed, sdRR, ok := vm.ReadSelfDestruct(addr, txIndex); ok && sdRR.resolved() && destructed &&
			version.TxIndex == sdRR.DepIdx() && version.Incarnation == sdRR.Incarnation() {
			if hi, ok := vm.LatestTxIndex(addr, path, key, txIndex-1); ok && hi > version.TxIndex {
				return VersionInvalid
			}
			return VersionValid
		}
	}
	if !recursive && source == MapRead && (path == CodePath || path == CodeSizePath) {
		// A wiped code read is recorded at an in-block destruct, which a later revival
		// may hide from the latest SelfDestruct cell; only a code write above it ends it.
		if sdVer, ok := vm.selfDestructInRange(addr, version.TxIndex, version.TxIndex+1, true); ok && sdVer == version {
			if hi, ok := vm.LatestTxIndex(addr, path, key, txIndex-1); ok && hi > version.TxIndex {
				return VersionInvalid
			}
			return VersionValid
		}
	}
	if path == StoragePath && source == StorageRead && version == UnknownVersion && rr.DepIdx() == originIndex {
		rr = ReadResult{depIdx: UnknownDep}
	}
	valid := VersionValid
	switch rr.Status() {
	case MVReadResultDone, MVReadResultValidated:
		switch {
		case source != MapRead && recursive:
			// Synthetic cross-validate probe: the outer entry's validation covers it.
		case source != MapRead && (path != StoragePath || version == UnknownVersion):
			// No concrete version to compare (only storage carries a seeded origin). A
			// BAL-seeded Done cell is a no-op, so stays valid — unless an in-range removal
			// wiped the slot (EIP-7928 has no removal field); otherwise invalidate
			// conservatively. Concrete-version storage reads fall to checkVersion below.
			balTrusted := vm.HasBAL && path.BALSeeded()
			if balTrusted && path == StoragePath {
				if _, removed := vm.selfDestructInRange(addr, version.TxIndex+1, txIndex, true); removed {
					balTrusted = false
				}
			}
			if !balTrusted {
				valid = VersionInvalid
			}
		default:
			valid = checkVersion(version, rr.Version())
			// An origin AddressPath read is the committed baseline; re-run the create/
			// destruct cross-checks so a concurrent lower-tx create or SELFDESTRUCT invalidates it.
			if valid == VersionValid && path == AddressPath && rr.Version().TxIndex == originIndex {
				valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, StorageRead,
					version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), checkVersion, traceInvalid, tracePrefix, true)
				if valid == VersionValid {
					if _, incRR, ok := vm.ReadIncarnation(addr, txIndex); ok && incRR.resolved() {
						valid = VersionInvalid
					}
				}
			}
		}
		// An AddressPath read predating a later self-destruct that nothing revived is
		// stale. checkVersion misses it — the destruct writes no AddressPath cell — so
		// resolve the lifecycle and invalidate the stale baseline read.
		if valid == VersionValid && path == AddressPath {
			if st, _, destroyedAt, _ := vm.ResolvedAccountLifecycleAt(addr, txIndex); st == LifecycleAbsent &&
				destroyedAt > rr.Version().TxIndex {
				valid = VersionInvalid
			}
		}
		// A later tx self-destructed the account (no revival), so a read predating the
		// destruct is stale; checkVersion misses it because the SD doesn't write the read's path.
		if valid == VersionValid && path != SelfDestructPath && path != AddressPath &&
			path != IncarnationPath && path != CreateContractPath {
			if destructed, sdRR, ok := vm.ReadSelfDestruct(addr, txIndex); ok && sdRR.resolved() && destructed {
				destructTxIndex := sdRR.DepIdx()
				if destructTxIndex > rr.Version().TxIndex {
					revivalLimit := txIndex - 1
					revived := false
					for _, p := range [...]AccountPath{BalancePath, NoncePath, CodeHashPath} {
						if hi, ok := vm.LatestTxIndex(addr, p, accounts.NilKey, revivalLimit); ok && hi > destructTxIndex {
							revived = true
							break
						}
					}
					// EIP-8246: a self-destruct that preserves a non-zero balance writes it
					// at the SD tx itself (index == destructTxIndex, which the strict > scan
					// above misses), leaving the account alive. A pre-EIP-8246 burn or an
					// Amsterdam move-out writes zero there, so a resolved non-zero balance at
					// or after the destruct is the signal the account survived.
					if !revived {
						if bal, balRR, ok := vm.ReadBalance(addr, txIndex); ok && balRR.resolved() &&
							balRR.DepIdx() >= destructTxIndex && !bal.IsZero() {
							revived = true
						}
					}
					if !revived {
						valid = VersionInvalid
					}
				}
			}
		}
		// A revival writes SelfDestruct=false above the wipe, so the latest cell hides it.
		// A nonce written at or before an in-block destruct reads as zero, like the read path.
		if valid == VersionValid && path == NoncePath {
			if _, ok := vm.FindDoneSelfDestructInRange(addr, rr.Version().TxIndex, txIndex, true); ok {
				valid = VersionInvalid
			}
		}
	case MVReadResultDependency:
		valid = VersionInvalid
	case MVReadResultNone:
		switch {
		case source == MapRead && !recursive &&
			(path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath):
			// A sub-field read with no dedicated cell is recorded folded onto AddressPath,
			// so validate it against AddressPath at that version.
			valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
				version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), checkVersion, traceInvalid, tracePrefix, true)
		case source != StorageRead:
			valid = VersionInvalid
		default:
			if valid = checkVersion(version, version); valid == VersionValid &&
				path != SelfDestructPath && !vm.netAbsentDestruct(addr, txIndex) {
				// Cross-validate any account-property read against AddressPath and
				// SelfDestructPath: a prior tx may have created or self-destructed the account.
				// Skipped for a net-absent create+self-destruct (the account was absent both
				// at base and after, so the base read is not stale — invalidating it livelocked).
				if path != AddressPath {
					if valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
						version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), checkVersion, traceInvalid, tracePrefix, true); valid == VersionValid {
						valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), checkVersion, traceInvalid, tracePrefix, true)
					} else {
						vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), checkVersion, traceInvalid, tracePrefix, true)
					}
				} else {
					valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
						version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), checkVersion, traceInvalid, tracePrefix, true)

					// A prior tx re-creating this account makes a nil AddressPath read stale;
					// IncarnationPath is the specific signal (written only by CreateAccount and
					// SelfDestruct), unlike BalancePath which overfires for every gas payer.
					if valid == VersionValid {
						if _, incRR, ok := vm.ReadIncarnation(addr, txIndex); ok && incRR.resolved() {
							valid = VersionInvalid
						}
					}
				}
			}
		}
	default:
		panic(fmt.Errorf("undefined vm read status: %v", rr.Status()))
	}

	if traceInvalid && valid == VersionInvalid {
		if len(tracePrefix) > 0 {
			tracePrefix += "  RD"
		} else {
			tracePrefix = "RD"
		}
		fmt.Printf("%s %x %s, %d %s, %s (%d.%d)!=(%d.%d) %s\n", tracePrefix, addr,
			AccountKey{path, key}.String(), txIndex, func() string {
				switch rr.Status() {
				case MVReadResultDone:
					return "done"
				case MVReadResultValidated:
					return "validated"
				case MVReadResultDependency:
					return "dependency"
				case MVReadResultNone:
					return "none"
				default:
					return "unknown"
				}
			}(),
			source, version.TxIndex, version.Incarnation, rr.depIdx, rr.incarnation, valid)
	}

	return valid
}

// ValidateVersion check if transaction's readSet is still valid based on the current multi-versioned memory
func (vm *VersionMap) ValidateVersion(txIdx int, lastIO *VersionedIO, checkVersion func(readVersion, writeVersion Version) VersionValidity, traceInvalid bool, tracePrefix string) (valid VersionValidity) {
	return vm.ValidateReadSet(txIdx, lastIO.ReadSet(txIdx), checkVersion, traceInvalid, tracePrefix)
}

// ValidateReadSet validates rs (a tx's read-set) against the current versionMap at
// txIdx. Split out of ValidateVersion so a worker can validate its own freshly-produced
// read-set in parallel, before the exec loop commits.
func (vm *VersionMap) ValidateReadSet(txIdx int, rs ReadSet, checkVersion func(readVersion, writeVersion Version) VersionValidity, traceInvalid bool, tracePrefix string) (valid VersionValidity) {
	valid = VersionValid
	// ok latches valid and stops the scan on the first invalid result.
	ok := func(v VersionValidity) bool { valid = v; return v == VersionValid }
	// validatePath validates any non-storage path on the plain ReadStatus result:
	// validation never compares the recorded value, so version + status is the
	// authoritative basis for every path except storage, which needs the wipe-aware
	// liveStorage reader.
	validatePath := func(addr accounts.Address, path AccountPath, hdr ReadHeader) VersionValidity {
		if (hdr.Version.TxIndex == originIndex || hdr.Source == StorageRead && hdr.Version == UnknownVersion) && vm.hasNoCells(addr, path) {
			return VersionValid
		}
		return vm.validateReadImpl(txIdx, addr, path, accounts.NilKey, hdr.Source, hdr.Version,
			vm.ReadStatus(addr, path, accounts.NilKey, txIdx), checkVersion, traceInvalid, tracePrefix, false)
	}

	for a, tr := range rs.address {
		if !ok(validatePath(a, AddressPath, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.balance {
		if !ok(validatePath(a, BalancePath, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.nonce {
		if !ok(validatePath(a, NoncePath, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.incarnation {
		if !ok(validatePath(a, IncarnationPath, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.codeHash {
		if !ok(validatePath(a, CodeHashPath, tr.ReadHeader)) {
			return
		}
	}
	for a, inner := range rs.storage {
		quiet := vm.hasNoCells(a, StoragePath)
		for k, tr := range inner {
			if quiet && tr.Version.TxIndex == originIndex {
				continue
			}
			if !ok(vm.validateReadImpl(txIdx, a, StoragePath, k, tr.Source, tr.Version, liveStorage(vm, a, k, txIdx), checkVersion, traceInvalid, tracePrefix, false)) {
				return
			}
		}
	}
	for a, tr := range rs.selfDestruct {
		if !ok(validatePath(a, SelfDestructPath, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.createContract {
		if !ok(validatePath(a, CreateContractPath, tr.ReadHeader)) {
			return
		}
	}
	// A balance-only revival writes no code cell for a committed code read to
	// version-mismatch against, but the destruct below it wiped the code.
	wipedCommitted := func(addr accounts.Address, hdr ReadHeader) VersionValidity {
		if hdr.Source == StorageRead || hdr.Source == MapRead {
			if _, ok := vm.selfDestructInRange(addr, hdr.Version.TxIndex+1, txIdx, true); ok {
				return VersionInvalid
			}
		}
		return VersionValid
	}
	for a, tr := range rs.code {
		if !ok(validatePath(a, CodePath, tr.ReadHeader)) {
			return
		}
		if len(tr.Val.Bytes) != 0 && !ok(wipedCommitted(a, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.codeSize {
		if !ok(validatePath(a, CodeSizePath, tr.ReadHeader)) {
			return
		}
		if tr.Val != 0 && !ok(wipedCommitted(a, tr.ReadHeader)) {
			return
		}
	}
	return
}

// WriteCell holds one version of a typed value on a (path, key) cell. T matches the
// AccountPath's value-type contract, so a wrong-T write is a compile-time error, and
// typed Read primitives consume Value directly without crossing the any boundary.
type WriteCell[T any] struct {
	// state is atomic so the exec loop can mark a cell without the entry lock: readers
	// of the same address do not wait for the commit.
	state     atomic.Uint32
	valStatus valueStatus
	accSource AccountSource
	Value     T
}

// seededCell marks a cell WriteChanges seeded from the BAL. A value store or a delete drops it and a
// status mark keeps it, so a set bit means every reader of the cell saw the seed.
const seededCell uint32 = 1 << 24

func packCellState(inc Incarnation, flag statusFlag) uint32 {
	return uint32(uint16(inc))<<8 | uint32(flag)
}

func unpackCellState(w uint32) (Incarnation, statusFlag) {
	return Incarnation(int16(uint16(w >> 8))), statusFlag(w & 0xff)
}

func (c *WriteCell[T]) loadState() (Incarnation, statusFlag) { return unpackCellState(c.state.Load()) }

func (c *WriteCell[T]) seeded() bool { return c.state.Load()&seededCell != 0 }

func (c *WriteCell[T]) loadFlag() statusFlag {
	_, f := c.loadState()
	return f
}

func (c *WriteCell[T]) loadInc() Incarnation {
	inc, _ := c.loadState()
	return inc
}

func (c *WriteCell[T]) setState(inc Incarnation, flag statusFlag) {
	c.state.Store(packCellState(inc, flag))
}

func markedState(old uint32, inc Incarnation, flag statusFlag) uint32 {
	return packCellState(inc, flag) | old&seededCell
}

func (c *WriteCell[T]) setFlagUnlessDone(flag statusFlag) bool {
	for {
		old := c.state.Load()
		inc, have := unpackCellState(old)
		if have == FlagDone && flag != FlagDone {
			return false
		}
		if c.state.CompareAndSwap(old, markedState(old, inc, flag)) {
			return true
		}
	}
}

type markOutcome uint8

const (
	marked markOutcome = iota
	markStale
	markDeleted
)

func (c *WriteCell[T]) mark(want Incarnation, flag statusFlag) (Incarnation, markOutcome) {
	for {
		old := c.state.Load()
		inc, f := unpackCellState(old)
		if f == flagDeleted {
			return inc, markDeleted
		}
		if want >= 0 && inc > want {
			return inc, markStale
		}
		// Done covers Validated, as it covers an Estimate flush (setFlagUnlessDone): a BAL-seeded
		// cell that the tx's flush matched stays Done.
		if f == FlagDone && flag == FlagValidated {
			return inc, marked
		}
		if c.state.CompareAndSwap(old, markedState(old, inc, flag)) {
			return inc, marked
		}
	}
}

// settled reports Done or Validated from one load: a mark can move the flag between two loads.
func (c *WriteCell[T]) settled() bool {
	f := c.loadFlag()
	return f == FlagDone || f == FlagValidated
}

// Incarnation is the Block-STM re-execution counter for a tx version. It is small
// (a handful even under heavy contention); int16 lets WriteCell pack it with the flag in one word.
// Distinct from accounts.Account.Incarnation (the uint64 storage incarnation).
type Incarnation int16

type Version struct {
	BlockNum    uint64
	TxNum       uint64
	TxIndex     int
	Incarnation Incarnation
}

var UnknownVersion = Version{TxIndex: UnknownDep, Incarnation: -1}

func (v Version) blockAccessIndex() uint32 {
	return uint32(v.TxIndex + 1)
}

const (
	MVReadResultDone       = 0
	MVReadResultDependency = 1
	MVReadResultNone       = 2
	MVReadResultValidated  = 3
)

type ReadResult struct {
	depIdx      int
	incarnation Incarnation
	valStatus   valueStatus
	flags       readFlags
}

// readFlags packs a read's boolean facts into one field: ReadResult stays at 4 fields, so the compiler keeps it in
// registers.
type readFlags uint8

const (
	// readValidated: the floor cell was Validated (pre-seal, revertible); the reader
	// continues on it like Done but the dep is not yet final.
	readValidated readFlags = 1 << iota
	readFromBAL             // the cell's record is an AccountFromBAL existence marker
)

func (res ReadResult) accSource() AccountSource {
	if res.flags&readFromBAL != 0 {
		return AccountFromBAL
	}
	return AccountFromBase
}

func (res *ReadResult) DepString() string {
	if res.depIdx == UnknownDep {
		return "unknown"
	}
	return fmt.Sprintf("%d.%d", res.depIdx, res.incarnation)
}

func (res *ReadResult) DepIdx() int {
	return res.depIdx
}

func (res *ReadResult) Incarnation() Incarnation {
	return res.incarnation
}

func (res *ReadResult) Version() Version {
	return Version{
		TxIndex:     res.depIdx,
		Incarnation: res.incarnation,
	}
}

// ValStatus reports the floor cell's write transition (ValueUnchanged for a no-op).
func (res *ReadResult) ValStatus() valueStatus {
	return res.valStatus
}

func (res ReadResult) Status() int {
	if res.depIdx != UnknownDep {
		if res.flags&readValidated != 0 {
			return MVReadResultValidated
		}
		if res.incarnation == -1 {
			return MVReadResultDependency
		} else {
			return MVReadResultDone
		}
	}

	return MVReadResultNone
}

// accountLiveAt reports whether any field cell below txIdx makes the account
// EIP-161-non-empty. Unresolved cells cannot prove death and count as live.
func (vm *VersionMap) accountLiveAt(addr accounts.Address, txIdx int) bool {
	if bal, rr, ok := vm.ReadBalance(addr, txIdx); ok && (!rr.resolved() || !bal.IsZero()) {
		return true
	}
	if nonce, rr, ok := vm.ReadNonce(addr, txIdx); ok && (!rr.resolved() || nonce != 0) {
		return true
	}
	if ch, rr, ok := vm.ReadCodeHash(addr, txIdx); ok && (!rr.resolved() || !(ch.IsEmpty() || ch.IsZero())) {
		return true
	}
	if c, rr, ok := vm.ReadCode(addr, txIdx); ok && (!rr.resolved() || len(c.Bytes) > 0) {
		return true
	}
	return false
}

// resolved reports whether the floor holds a value at a known version: Done (sealed) or
// Validated (pre-seal early-break). Consumers of the value treat both alike; only sealing
// logic requires Done specifically.
func (res ReadResult) resolved() bool {
	s := res.Status()
	return s == MVReadResultDone || s == MVReadResultValidated
}
