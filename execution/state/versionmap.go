package state

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type statusFlag uint

const FlagDone statusFlag = 0
const FlagEstimate statusFlag = 1
const UnknownDep = -3

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

// AccountKey is a (Path, Key) pair used as a selector for the field within
// an AddressEntry and as a debug-printable identifier. It is no longer used
// as an internal map key — VersionMap dispatches on Path via a switch on
// the AddressEntry struct so the inner map's composite-key hash is gone.
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

// AddressEntry holds the multi-version cells for one address, organised
// per AccountPath. Each field is typed by the AccountPath's value-type
// contract so adding the wrong type to a cell is a compile-time error
// rather than a runtime panic — and the storage layer carries the typed
// value end-to-end (no interface box on writes).
//
// Per-field granular: every operation is (Path,Key)-scoped; no consumer treats the entry as a transactional whole.
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
	// the per-path scans) and Lock for writes (putCell / Delete / markFlag).
	mu sync.RWMutex
}

// putCell sets/updates a typed cell at txIdx; caller holds e.mu.Lock(). Returns the (possibly new) map to assign back.
func putCell[T any](vm *VersionMap, cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, txIdx, incarnation int, flag statusFlag, value T, getCell func() *WriteCell[T]) *btree.Map[int, *WriteCell[T]] {
	vm.assertUnsealed(txIdx, addr, path, accounts.NilKey)
	if cells == nil {
		cells = &btree.Map[int, *WriteCell[T]]{}
	}
	if ci, ok := cells.Get(txIdx); ok {
		if ci.incarnation > incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %x %s, %v", addr, path, txIdx))
		}
		if dbg.AssertEnabled && ci.flag == FlagDone && flag == FlagEstimate {
			panic(fmt.Errorf("versionMap: Done->Estimate downgrade addr=%x path=%s txIdx=%d inc %d->%d", addr, path, txIdx, ci.incarnation, incarnation))
		}
		ci.flag = flag
		ci.incarnation = incarnation
		ci.Value = value
		return cells
	}
	cell := getCell()
	cell.flag = flag
	cell.incarnation = incarnation
	cell.Value = value
	cells.Set(txIdx, cell)
	return cells
}

// markCellFlag sets the flag on an existing typed cell. Panics with msg if no
// cell is present at txIdx — used by MarkEstimate/MarkComplete which require a
// prior write. When incarnation >= 0 the cell must be at that incarnation: a
// newer one means the flip targets a stale version, which the value-writing path
// would reject, so panic rather than mark the wrong incarnation.
func markCellFlag[T any](cells *btree.Map[int, *WriteCell[T]], txIdx, incarnation int, flag statusFlag, msg string) {
	if cells == nil {
		panic(msg)
	}
	ci, ok := cells.Get(txIdx)
	if !ok {
		panic(msg)
	}
	if incarnation >= 0 && ci.incarnation != incarnation {
		panic(fmt.Sprintf("%s: incarnation have=%d want=%d", msg, ci.incarnation, incarnation))
	}
	ci.flag = flag
}

// markCellComplete advances an existing cell to Done as a consistency check, not
// a write. The cell must already hold value at incarnation — the value published
// speculatively when the tx's result arrived is final, so committing only
// advances the status. A missing cell, a newer incarnation, or a changed value
// is a one-value-per-version violation and panics rather than being silently
// overwritten. This is the commit-boundary enforcement point.
func markCellComplete[T any](cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx, incarnation int, value T) {
	msg := fmt.Sprintf("markComplete: missing cell addr=%x path=%s key=%x txIdx=%d", addr.Value(), path, key.Value(), txIdx)
	if cells == nil {
		panic(msg)
	}
	ci, ok := cells.Get(txIdx)
	if !ok {
		panic(msg)
	}
	if dbg.AssertEnabled {
		if ci.incarnation != incarnation {
			panic(fmt.Sprintf("markComplete: incarnation addr=%x path=%s txIdx=%d have=%d want=%d", addr.Value(), path, txIdx, ci.incarnation, incarnation))
		}
		if !reflect.DeepEqual(ci.Value, value) {
			panic(fmt.Sprintf("markComplete: value changed at published version addr=%x path=%s txIdx=%d inc=%d old=%v new=%v", addr.Value(), path, txIdx, incarnation, ci.Value, value))
		}
	}
	ci.flag = FlagDone
}

type VersionMap struct {
	// address -> *AddressEntry; sync.Map so account lookup is lock-free. Each entry's RWMutex guards only its own cells.
	s      sync.Map // accounts.Address -> *AddressEntry
	trace  bool
	HasBAL bool // When true, all significant writes are pre-populated from BAL

	// sealed/sealedArmed enforce that a finalized tx's cells are immutable: no write/delete at TxIndex <= sealed. SealUpTo is single-writer; assertUnsealed is many-reader.
	sealed      atomic.Int64
	sealedArmed atomic.Bool

	// MapReadValueInvalidations counts how often the value-aware MapRead check
	// invalidates a version-consistent read. With write-side one-value-per-version
	// enforced (MarkWritesComplete), this should stay zero — a non-zero count is a
	// diagnostic that a write path mutated a value at a fixed version.
	MapReadValueInvalidations atomic.Int64
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

// assertUnsealed panics if a mutation targets a cell whose tx is already sealed.
// A cheap always-on invariant (armed-load short-circuits on the pre-seal path);
// never fires on correct code.
func (vm *VersionMap) assertUnsealed(txIdx int, addr accounts.Address, path AccountPath, key accounts.StorageKey) {
	// Negative TxIndex is the pre-block system-call / block-init domain (EIP-4788,
	// EIP-2935), written during setup and outside the OCC-validated regular-tx
	// prefix the seal frontier tracks. The invariant applies to regular txs only.
	if txIdx >= 0 && vm.sealedArmed.Load() && int64(txIdx) <= vm.sealed.Load() {
		panic(fmt.Sprintf("versionMap: write to sealed cell tx=%d sealedUpTo=%d addr=%x path=%s key=%x",
			txIdx, vm.sealed.Load(), addr.Value(), path, key.Value()))
	}
}

func NewVersionMap(changes types.BlockAccessList) *VersionMap {
	vm := &VersionMap{
		HasBAL: len(changes) > 0,
	}
	vm.WriteChanges(changes)
	return vm
}

// load returns the AddressEntry for addr, or nil when absent. Lock-free.
func (vm *VersionMap) load(addr accounts.Address) *AddressEntry {
	if e, ok := vm.s.Load(addr); ok {
		return e.(*AddressEntry)
	}
	return nil
}

func (vm *VersionMap) SetTrace(trace bool) {
	vm.trace = trace
}

// StorageKeys returns every storage slot key recorded for addr. Used by
// Normalize to emit synthetic delete entries for every slot of a
// selfdestructed contract, matching DomainDelPrefix behaviour from the
// sequential path.
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

// WriteChanges pre-populates the version map from a BAL (EIP-7928). Each
// per-path change is routed through the typed Write primitive so the value
// type is enforced at compile time — a future BAL field-type change that
// breaks the contract surfaces as a build error here rather than a runtime
// panic on the first read of the cell.
func (vm *VersionMap) WriteChanges(changes types.BlockAccessList) {
	for i := range changes {
		accountChanges := &changes[i]
		for _, storageChanges := range accountChanges.StorageChanges {
			for _, change := range storageChanges.Changes {
				vm.WriteStorage(accountChanges.Address, storageChanges.Slot, Version{TxIndex: int(change.Index) - 1}, change.Value, true)
			}
		}
		for _, balanceChange := range accountChanges.BalanceChanges {
			vm.WriteBalance(accountChanges.Address, Version{TxIndex: int(balanceChange.Index) - 1}, balanceChange.Value, true)
		}
		for _, nonceChange := range accountChanges.NonceChanges {
			vm.WriteNonce(accountChanges.Address, Version{TxIndex: int(nonceChange.Index) - 1}, nonceChange.Value, true)
		}
		for _, codeChange := range accountChanges.CodeChanges {
			// Seed the whole code trio so pre-population matches what tx execution
			// flushes together; a CodePath cell without its CodeHashPath/CodeSizePath
			// siblings lets a concurrent reader see code but no code hash.
			code := accounts.NewCode(codeChange.Bytecode)
			v := Version{TxIndex: int(codeChange.Index) - 1}
			vm.WriteCode(accountChanges.Address, v, code, true)
			vm.WriteCodeHash(accountChanges.Address, v, code.Hash, true)
			vm.WriteCodeSize(accountChanges.Address, v, code.Len(), true)
		}
	}
}

// Typed Write primitives. Each takes the AccountPath-contracted value type
// directly so wrong-type writes are caught at compile time — there is no
// runtime data.(T) assertion path through these.

func (vm *VersionMap) WriteAddress(addr accounts.Address, v Version, value *accounts.Account, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Address = putCell(vm, e.Address, addr, AddressPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellAccount)
}

// WriteOriginAddressOnce seeds addr's committed pre-block account at originIndex
// only if no origin cell exists yet. Origin is the immutable pre-block base for
// the life of the block; re-seeding it corrupts every fall-through read (calcFees
// obtains the coinbase via a floor-composed reader, so a re-seed would publish a
// mid-block, tip-inflated balance as the base). Mirrors the seed-once discipline
// seedStorageOrigin already applies to storage slots.
func (vm *VersionMap) WriteOriginAddressOnce(addr accounts.Address, value *accounts.Account) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Address != nil {
		if _, ok := e.Address.Get(originIndex); ok {
			return
		}
	}
	e.Address = putCell(vm, e.Address, addr, AddressPath, originIndex, 0, flagFor(true), value, getCellAccount)
}

func (vm *VersionMap) WriteSelfDestruct(addr accounts.Address, v Version, value bool, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.SelfDestruct = putCell(vm, e.SelfDestruct, addr, SelfDestructPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellSelfDestruct)
}

func (vm *VersionMap) WriteBalance(addr accounts.Address, v Version, value uint256.Int, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Balance = putCell(vm, e.Balance, addr, BalancePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellBalance)
}

func (vm *VersionMap) WriteNonce(addr accounts.Address, v Version, value uint64, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Nonce = putCell(vm, e.Nonce, addr, NoncePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellNonce)
}

func (vm *VersionMap) WriteIncarnation(addr accounts.Address, v Version, value uint64, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Incarnation = putCell(vm, e.Incarnation, addr, IncarnationPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellIncarnation)
}

func (vm *VersionMap) WriteCode(addr accounts.Address, v Version, value accounts.Code, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Code = putCell(vm, e.Code, addr, CodePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCode)
}

func (vm *VersionMap) WriteCodeHash(addr accounts.Address, v Version, value accounts.CodeHash, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CodeHash = putCell(vm, e.CodeHash, addr, CodeHashPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCodeHash)
}

func (vm *VersionMap) WriteCodeSize(addr accounts.Address, v Version, value int, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CodeSize = putCell(vm, e.CodeSize, addr, CodeSizePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCodeSize)
}

func (vm *VersionMap) WriteCreateContract(addr accounts.Address, v Version, value bool, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CreateContract = putCell(vm, e.CreateContract, addr, CreateContractPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCreateContract)
}

func (vm *VersionMap) WriteStorage(addr accounts.Address, key accounts.StorageKey, v Version, value uint256.Int, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Storage == nil {
		e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
	}
	e.Storage[key] = putCell(vm, e.Storage[key], addr, StoragePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellStorage)
}

// entryOrCreate returns the AddressEntry for addr, creating it if absent. The
// returned pointer is stable for the map's lifetime; the caller locks e.mu for
// the cell mutation. Self-synchronised via sync.Map — no caller lock required.
func (vm *VersionMap) entryOrCreate(addr accounts.Address) *AddressEntry {
	if e, ok := vm.s.Load(addr); ok {
		return e.(*AddressEntry)
	}
	e, _ := vm.s.LoadOrStore(addr, &AddressEntry{})
	return e.(*AddressEntry)
}

func flagFor(complete bool) statusFlag {
	if complete {
		return FlagDone
	}
	return FlagEstimate
}

// Typed Read primitives. Each returns the typed value, a ReadResult holding
// the conflict-detection metadata (depIdx, incarnation), and ok=true when a
// cell exists.

// readFloor performs the floor read shared by every typed ReadX primitive:
// it descends sel(e)'s btree for the highest write strictly below txIdx and
// returns its value plus the conflict-detection metadata (depIdx and, when the
// floor cell is Done, its incarnation). sel extracts the per-path cell map from
// the address entry, returning nil when the path is unset.
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
	switch fv.flag {
	case FlagDone:
		res.incarnation = fv.incarnation
	case FlagEstimate:
	default:
		panic("unknown flag value")
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadAddress(addr accounts.Address, txIdx int) (*accounts.Account, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[*accounts.Account]] { return e.Address })
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
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint256.Int]] {
		if e.Storage == nil {
			return nil
		}
		return e.Storage[key]
	})
}

// ReadStatus returns a path's read outcome (Status/Version/DepIdx/Incarnation)
// for callers that need only version/status (the validator's common path,
// revival checks). It dispatches to the typed ReadX and discards the value.
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

// LatestTxIndex returns the largest TxIndex (≤ txIdxLimit) at which a write
// exists for the given (addr, path, key). Returns ok=false when no entry
// exists at or below the limit. Used to detect account revival after a
// SelfDestruct: any newer non-SelfDestruct write at a strictly higher
// TxIndex re-creates the account.
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

// AccountLifecycleState enumerates an account's existence at a given txIdx, resolved once so readers, validation and the create decision branch on the same verdict.
type AccountLifecycleState uint8

const (
	// LifecycleLive: no Done SelfDestruct=true in effect at txIdx.
	LifecycleLive AccountLifecycleState = iota
	// LifecycleAbsent: destroyed with no revival above it; the account reads as gone and a pre-block base read of it is not stale.
	LifecycleAbsent
	// LifecycleRevived: destroyed but re-created above the destruct; storage on/before the destruct is wiped and stale base reads are invalidated.
	// EIP-8246 balance-preserve is not resolved here: only a fork-aware caller can decide whether the account still exists.
	LifecycleRevived
)

// AccountLifecycleAt resolves an account's lifecycle in one pass: the state, the canonical version dependent reads must anchor on, and the destruct (wipe) TxIndex.
func (vm *VersionMap) AccountLifecycleAt(addr accounts.Address, txIdx int) (state AccountLifecycleState, canonicalVer Version, destroyedAt int) {
	if vm == nil {
		return LifecycleLive, Version{}, 0
	}
	e := vm.load(addr)
	if e == nil {
		return LifecycleLive, Version{}, 0
	}
	// The whole verdict must be resolved under a single RLock so it cannot observe the account mid-flush and return an internally inconsistent lifecycle.
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.SelfDestruct == nil {
		return LifecycleLive, Version{}, 0
	}

	var latest *WriteCell[bool]
	var latestIdx, wipeInc int
	haveLatest := false
	wiped := false
	e.SelfDestruct.Descend(txIdx-1, func(k int, v *WriteCell[bool]) bool {
		if !haveLatest {
			latest, latestIdx, haveLatest = v, k, true
		}
		if v.flag == FlagDone && v.Value {
			destroyedAt, wipeInc, wiped = k, v.incarnation, true
			return false
		}
		return true
	})
	if !wiped {
		return LifecycleLive, Version{}, 0
	}
	if latest.flag == FlagDone {
		canonicalVer = Version{TxIndex: latestIdx, Incarnation: latest.incarnation}
	} else {
		canonicalVer = Version{TxIndex: destroyedAt, Incarnation: wipeInc}
	}

	revivalLimit := txIdx - 1
	if hi, ok := highestBelow(e.Address, revivalLimit); ok && hi >= destroyedAt {
		return LifecycleRevived, canonicalVer, destroyedAt
	}
	if hi, ok := highestBelow(e.Balance, revivalLimit); ok && hi > destroyedAt {
		return LifecycleRevived, canonicalVer, destroyedAt
	}
	if hi, ok := highestBelow(e.Nonce, revivalLimit); ok && hi > destroyedAt {
		return LifecycleRevived, canonicalVer, destroyedAt
	}
	if hi, ok := highestBelow(e.CodeHash, revivalLimit); ok && hi > destroyedAt {
		return LifecycleRevived, canonicalVer, destroyedAt
	}
	return LifecycleAbsent, canonicalVer, destroyedAt
}

// highestBelow returns the largest TxIndex ≤ limit present in cells, if any. The
// caller must hold the owning AddressEntry's lock.
func highestBelow[T any](cells *btree.Map[int, *WriteCell[T]], limit int) (int, bool) {
	if cells == nil {
		return 0, false
	}
	hi, ok := 0, false
	cells.Descend(limit, func(k int, _ *WriteCell[T]) bool {
		hi, ok = k, true
		return false
	})
	return hi, ok
}

// IsNetAbsent reports whether the account reads as gone at txIdx (LifecycleAbsent; see AccountLifecycleAt).
func (vm *VersionMap) IsNetAbsent(addr accounts.Address, txIdx int) bool {
	state, _, _ := vm.AccountLifecycleAt(addr, txIdx)
	return state == LifecycleAbsent
}

// netAbsentDestruct reports whether a lower tx left addr net-absent via create+self-destruct with no revival above it, so a base read of it is not stale (see IsNetAbsent).
func (vm *VersionMap) netAbsentDestruct(addr accounts.Address, txIndex int) bool {
	destructed, sdRR, ok := vm.ReadSelfDestruct(addr, txIndex)
	if !ok || !destructed || (sdRR.Status() != MVReadResultDone && sdRR.Status() != MVReadResultDependency) {
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
	// EIP-8246: a self-destruct may preserve a non-zero balance (or leave a
	// non-zero nonce), keeping the account alive as balance-only. That preserve is
	// written in the destruct tx itself, so the strictly-above revival checks above
	// miss it — the account is not net-absent, and a base read of it as absent is
	// stale and must be invalidated.
	if bal, _, ok := vm.ReadBalance(addr, txIndex); ok && !bal.IsZero() {
		return false
	}
	if nonce, _, ok := vm.ReadNonce(addr, txIndex); ok && nonce != 0 {
		return false
	}
	return true
}

// FlushVersionedWrites routes a tx's typed write collections into the version
// map. Each cell is positioned by the write's (txIndex, incarnation), so the
// per-path loop order does not affect the result.
func (vm *VersionMap) FlushVersionedWrites(writes *WriteSet, complete bool, tracePrefix string) {
	vm.flushVersionedWrites(writes, complete, tracePrefix, nil)
}

// FlushVersionedWritesFeeEstimate flushes writes, but for any address where
// feeEstimate returns true the Address and Balance cells are published as
// ESTIMATE regardless of `complete`. A fee recipient (coinbase / burnt) has no
// final balance until the postponed fee calc, so its balance must stay an
// in-flight dependency — even when the tx itself validates — so a downstream
// reader hits MVReadResultDependency (Dep) and pauses/re-executes rather than
// reading a committed-looking pre-tip value. calcFees is the sole writer that
// turns it Done (Estimate -> Done, the one transition allowed to change a value).
func (vm *VersionMap) FlushVersionedWritesFeeEstimate(writes *WriteSet, complete bool, tracePrefix string, feeEstimate func(accounts.Address) bool) {
	vm.flushVersionedWrites(writes, complete, tracePrefix, feeEstimate)
}

func (vm *VersionMap) flushVersionedWrites(writes *WriteSet, complete bool, tracePrefix string, feeEstimate func(accounts.Address) bool) {
	if writes == nil {
		return
	}
	flag := flagFor(complete)
	// Flush per account under that account's lock so all of a tx's writes to one
	// account (e.g. AddressPath + CodePath) become visible atomically — the
	// property the former global lock guaranteed, now scoped to the account. A
	// reader of a different account never contends. Cross-account partial
	// visibility is resolved by commit-time ValidateVersion.
	seen := make(map[accounts.Address]struct{})
	writes.forEachAddr(func(addr accounts.Address) {
		if _, dup := seen[addr]; dup {
			return
		}
		seen[addr] = struct{}{}
		// A fee recipient's Address/Balance is provisional until calcFees — keep it
		// an Estimate so readers depend on the finalized (tip-inclusive) value.
		feeBalFlag := flag
		if feeEstimate != nil && feeEstimate(addr) {
			feeBalFlag = FlagEstimate
		}
		e := vm.entryOrCreate(addr)
		e.mu.Lock()
		if vw, ok := writes.address[addr]; ok {
			e.Address = putCell(vm, e.Address, addr, AddressPath, vw.Version.TxIndex, vw.Version.Incarnation, feeBalFlag, vw.Val, getCellAccount)
		}
		if vw, ok := writes.selfDestruct[addr]; ok {
			e.SelfDestruct = putCell(vm, e.SelfDestruct, addr, SelfDestructPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellSelfDestruct)
		}
		if vw, ok := writes.balance[addr]; ok {
			e.Balance = putCell(vm, e.Balance, addr, BalancePath, vw.Version.TxIndex, vw.Version.Incarnation, feeBalFlag, vw.Val, getCellBalance)
		}
		if vw, ok := writes.nonce[addr]; ok {
			e.Nonce = putCell(vm, e.Nonce, addr, NoncePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellNonce)
		}
		if vw, ok := writes.incarnation[addr]; ok {
			e.Incarnation = putCell(vm, e.Incarnation, addr, IncarnationPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellIncarnation)
		}
		if vw, ok := writes.code[addr]; ok {
			e.Code = putCell(vm, e.Code, addr, CodePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCode)
		}
		if vw, ok := writes.codeHash[addr]; ok {
			e.CodeHash = putCell(vm, e.CodeHash, addr, CodeHashPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCodeHash)
		}
		if vw, ok := writes.codeSize[addr]; ok {
			e.CodeSize = putCell(vm, e.CodeSize, addr, CodeSizePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCodeSize)
		}
		if vw, ok := writes.createContract[addr]; ok {
			e.CreateContract = putCell(vm, e.CreateContract, addr, CreateContractPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCreateContract)
		}
		if inner, ok := writes.storage[addr]; ok {
			if e.Storage == nil {
				e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
			}
			for key, vw := range inner {
				e.Storage[key] = putCell(vm, e.Storage[key], addr, StoragePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellStorage)
			}
		}
		e.mu.Unlock()
	})
}

func (vm *VersionMap) MarkEstimate(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) {
	vm.assertUnsealed(txIdx, addr, path, key)
	e := vm.load(addr)
	if e == nil {
		panic(fmt.Errorf("markFlag: no entry for addr %x, path %s, txIdx %d", addr, path, txIdx))
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	markFlag(e, addr, path, key, txIdx, -1, FlagEstimate)
}

// MarkWritesComplete advances every cell named by writes from its speculative
// Estimate to Done. It writes no values: each cell must already hold the write's
// value at the write's incarnation (the flush when the tx's result arrived). It
// is the commit-boundary consistency check that enforces one-value-per-version —
// a mismatch panics rather than silently overwriting.
func (vm *VersionMap) MarkWritesComplete(writes *WriteSet) {
	if writes == nil {
		return
	}
	seen := make(map[accounts.Address]struct{})
	writes.forEachAddr(func(addr accounts.Address) {
		if _, dup := seen[addr]; dup {
			return
		}
		seen[addr] = struct{}{}
		e := vm.load(addr)
		if e == nil {
			panic(fmt.Sprintf("markComplete: no entry addr=%x", addr.Value()))
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		if vw, ok := writes.address[addr]; ok {
			markCellComplete(e.Address, addr, AddressPath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.selfDestruct[addr]; ok {
			markCellComplete(e.SelfDestruct, addr, SelfDestructPath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.balance[addr]; ok {
			markCellComplete(e.Balance, addr, BalancePath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.nonce[addr]; ok {
			markCellComplete(e.Nonce, addr, NoncePath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.incarnation[addr]; ok {
			markCellComplete(e.Incarnation, addr, IncarnationPath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.code[addr]; ok {
			markCellComplete(e.Code, addr, CodePath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.codeHash[addr]; ok {
			markCellComplete(e.CodeHash, addr, CodeHashPath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.codeSize[addr]; ok {
			markCellComplete(e.CodeSize, addr, CodeSizePath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if vw, ok := writes.createContract[addr]; ok {
			markCellComplete(e.CreateContract, addr, CreateContractPath, accounts.NilKey, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
		}
		if inner, ok := writes.storage[addr]; ok {
			for key, vw := range inner {
				markCellComplete(e.Storage[key], addr, StoragePath, key, vw.Version.TxIndex, vw.Version.Incarnation, vw.Val)
			}
		}
	})
}

// markFlag updates the flag on an existing (addr, path, key, txIdx) cell.
// Caller must hold e.mu.Lock(). Panics if no cell is present at txIdx. When
// incarnation >= 0 the cell must be at that incarnation.
func markFlag(e *AddressEntry, addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx, incarnation int, flag statusFlag) {
	msg := fmt.Sprintf("markFlag: missing cell. addr=%x path=%s key=%x txIdx=%d", addr, path, key, txIdx)
	switch path {
	case AddressPath:
		markCellFlag(e.Address, txIdx, incarnation, flag, msg)
	case SelfDestructPath:
		markCellFlag(e.SelfDestruct, txIdx, incarnation, flag, msg)
	case BalancePath:
		markCellFlag(e.Balance, txIdx, incarnation, flag, msg)
	case NoncePath:
		markCellFlag(e.Nonce, txIdx, incarnation, flag, msg)
	case IncarnationPath:
		markCellFlag(e.Incarnation, txIdx, incarnation, flag, msg)
	case CodePath:
		markCellFlag(e.Code, txIdx, incarnation, flag, msg)
	case CodeHashPath:
		markCellFlag(e.CodeHash, txIdx, incarnation, flag, msg)
	case CodeSizePath:
		markCellFlag(e.CodeSize, txIdx, incarnation, flag, msg)
	case CreateContractPath:
		markCellFlag(e.CreateContract, txIdx, incarnation, flag, msg)
	case StoragePath:
		markCellFlag(e.Storage[key], txIdx, incarnation, flag, msg)
	default:
		panic(fmt.Errorf("markFlag: unknown path %v", path))
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
	var hasField bool
	switch path {
	case AddressPath:
		if e.Address != nil {
			hasField = true
			if c, ok := e.Address.Delete(txIdx); ok {
				releaseCellAccount(c)
			}
		}
	case SelfDestructPath:
		if e.SelfDestruct != nil {
			hasField = true
			if c, ok := e.SelfDestruct.Delete(txIdx); ok {
				releaseCellSelfDestruct(c)
			}
		}
	case BalancePath:
		if e.Balance != nil {
			hasField = true
			if c, ok := e.Balance.Delete(txIdx); ok {
				releaseCellBalance(c)
			}
		}
	case NoncePath:
		if e.Nonce != nil {
			hasField = true
			if c, ok := e.Nonce.Delete(txIdx); ok {
				releaseCellNonce(c)
			}
		}
	case IncarnationPath:
		if e.Incarnation != nil {
			hasField = true
			if c, ok := e.Incarnation.Delete(txIdx); ok {
				releaseCellIncarnation(c)
			}
		}
	case CodePath:
		if e.Code != nil {
			hasField = true
			if c, ok := e.Code.Delete(txIdx); ok {
				releaseCellCode(c)
			}
		}
	case CodeHashPath:
		if e.CodeHash != nil {
			hasField = true
			if c, ok := e.CodeHash.Delete(txIdx); ok {
				releaseCellCodeHash(c)
			}
		}
	case CodeSizePath:
		if e.CodeSize != nil {
			hasField = true
			if c, ok := e.CodeSize.Delete(txIdx); ok {
				releaseCellCodeSize(c)
			}
		}
	case CreateContractPath:
		if e.CreateContract != nil {
			hasField = true
			if c, ok := e.CreateContract.Delete(txIdx); ok {
				releaseCellCreateContract(c)
			}
		}
	case StoragePath:
		if cells := e.Storage[key]; cells != nil {
			hasField = true
			if c, ok := cells.Delete(txIdx); ok {
				releaseCellStorage(c)
			}
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
	if e.Address != nil {
		if c, ok := e.Address.Delete(txIdx); ok {
			releaseCellAccount(c)
		}
	}
	if e.SelfDestruct != nil {
		if c, ok := e.SelfDestruct.Delete(txIdx); ok {
			releaseCellSelfDestruct(c)
		}
	}
	if e.Balance != nil {
		if c, ok := e.Balance.Delete(txIdx); ok {
			releaseCellBalance(c)
		}
	}
	if e.Nonce != nil {
		if c, ok := e.Nonce.Delete(txIdx); ok {
			releaseCellNonce(c)
		}
	}
	if e.Incarnation != nil {
		if c, ok := e.Incarnation.Delete(txIdx); ok {
			releaseCellIncarnation(c)
		}
	}
	if e.Code != nil {
		if c, ok := e.Code.Delete(txIdx); ok {
			releaseCellCode(c)
		}
	}
	if e.CodeHash != nil {
		if c, ok := e.CodeHash.Delete(txIdx); ok {
			releaseCellCodeHash(c)
		}
	}
	if e.CodeSize != nil {
		if c, ok := e.CodeSize.Delete(txIdx); ok {
			releaseCellCodeSize(c)
		}
	}
	if e.CreateContract != nil {
		if c, ok := e.CreateContract.Delete(txIdx); ok {
			releaseCellCreateContract(c)
		}
	}
	for _, cells := range e.Storage {
		if c, ok := cells.Delete(txIdx); ok {
			releaseCellStorage(c)
		}
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

// validateRead validates one typed read. The recorded value stays typed T and is
// never boxed into `any`: readLive fetches the live version-map value for the
// same path and eq compares them for the rare value tiebreaker. The recursive
// cross-path core (validateReadImpl) is value-less — it probes other paths of
// other types, so it cannot itself be generic over T.
func validateRead[T any](vm *VersionMap, txIndex int, addr accounts.Address, path AccountPath, key accounts.StorageKey, source ReadSource, version Version,
	readVal T,
	readLive func(*VersionMap, accounts.Address, accounts.StorageKey, int) (T, ReadResult, bool),
	eq func(a, b T) bool,
	checkVersion func(readVersion, writeVersion Version) VersionValidity,
	traceInvalid bool, tracePrefix string) VersionValidity {
	// One typed read supplies BOTH the status (for the version check) and the
	// live value (for the rare tiebreaker) — no second lookup, no boxing. The
	// tiebreaker branch in validateReadImpl only fires when rr is Done, so eq
	// compares against the value that came with rr.
	live, rr, ok := readLive(vm, addr, key, txIndex)
	matchesLive := func() bool { return ok && eq(readVal, live) }
	return vm.validateReadImpl(txIndex, addr, path, key, source, version, rr, matchesLive, checkVersion, traceInvalid, tracePrefix, false)
}

// Typed live-value readers (uniform signature so validateRead can thread them
// generically) and equality helpers for the value tiebreaker.
func liveBalance(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (uint256.Int, ReadResult, bool) {
	return vm.ReadBalance(a, tx)
}
func liveNonce(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (uint64, ReadResult, bool) {
	return vm.ReadNonce(a, tx)
}
func liveIncarnation(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (uint64, ReadResult, bool) {
	return vm.ReadIncarnation(a, tx)
}
func liveCodeHash(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (accounts.CodeHash, ReadResult, bool) {
	return vm.ReadCodeHash(a, tx)
}
func liveAddress(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (*accounts.Account, ReadResult, bool) {
	return vm.ReadAddress(a, tx)
}
func liveStorage(vm *VersionMap, a accounts.Address, k accounts.StorageKey, tx int) (uint256.Int, ReadResult, bool) {
	// Wipe-aware: a slot of a destructed (non-live) account reads zero unless a
	// post-destruct write revived it. Anchor the wiped read on the destruct
	// (canonicalVer) so a recorded wiped read validates against the destruct
	// dependency rather than the stale, wipe-blind pre-destruct floor value.
	if state, canonicalVer, destroyedAt := vm.AccountLifecycleAt(a, tx); state != LifecycleLive {
		if val, res, ok := vm.ReadStorage(a, k, tx); ok && res.Status() == MVReadResultDone && res.DepIdx() > destroyedAt {
			return val, res, ok
		}
		return uint256.Int{}, ReadResult{depIdx: canonicalVer.TxIndex, incarnation: canonicalVer.Incarnation}, true
	}
	return vm.ReadStorage(a, k, tx)
}

func eqUint256(a, b uint256.Int) bool { return a.Eq(&b) }
func eqUint64(a, b uint64) bool       { return a == b }
func eqCodeHash(a, b accounts.CodeHash) bool {
	return a == b
}
func eqAccount(a, b *accounts.Account) bool {
	return a != nil && b != nil && a.Equals(b)
}

// validateReadImpl is validateRead with a recursive flag: the cross-validate
// probes (AddressPath / SelfDestructPath / IncarnationPath) pass recursive=true
// so they can be distinguished from a top-level read — a synthetic probe carries
// no recorded value of its own and must not invalidate on a bare Done entry.
func (vm *VersionMap) validateReadImpl(txIndex int, addr accounts.Address, path AccountPath, key accounts.StorageKey, source ReadSource, version Version,
	rr ReadResult,
	matchesLive func() bool,
	checkVersion func(readVersion, writeVersion Version) VersionValidity,
	traceInvalid bool, tracePrefix string, recursive bool) VersionValidity {

	valid := VersionValid
	switch rr.Status() {
	case MVReadResultDone:
		if source != MapRead {
			// With BAL, Balance/Nonce/Code/Storage are pre-populated, so a Done entry where the read saw storage is a BAL no-op and stays valid; other paths mean a real concurrent change and must invalidate.
			isBALPrePopulatedPath := path == BalancePath || path == NoncePath ||
				path == CodePath || path == StoragePath
			if !vm.HasBAL || !isBALPrePopulatedPath {
				switch {
				case recursive && matchesLive == nil:
					// Synthetic cross-validate probe (no recorded value of its
					// own) — the outer entry's validation covers it. Without this
					// guard a recursive AddressPath/SelfDestructPath probe that
					// lands on a Done cell would over-invalidate.
				case matchesLive != nil && matchesLive():
					// Value tiebreaker: the read was served cold from committed
					// state and a concurrent worker's Done cell now shadows it, but
					// the cell holds the same value — so the recorded read is still
					// accurate and the tx need not re-execute. Evaluated typed by
					// the caller (no boxing). Only value paths supply matchesLive;
					// noValue paths (Code/CodeSize/CreateContract) fall through to
					// the conservative version-check invalidation below.
				default:
					valid = VersionInvalid
				}
			}
		} else {
			valid = checkVersion(version, rr.Version())
			// Value-aware MapRead: the version-only check can accept a read whose
			// cell value has since changed at the same (TxIndex,Incarnation) — the
			// one-final-value-per-version violation. Also compare the recorded value
			// against the live cell value and invalidate on mismatch so the reader
			// re-executes rather than committing stale.
			if valid == VersionValid && matchesLive != nil && !matchesLive() {
				valid = VersionInvalid
				vm.MapReadValueInvalidations.Add(1)
			}
			// An origin AddressPath read is the committed baseline; re-run the
			// create/destruct cross-checks so a concurrent lower-tx create or
			// SELFDESTRUCT still invalidates it.
			if valid == VersionValid && path == AddressPath && rr.Version().TxIndex == originIndex {
				valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, StorageRead,
					version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)
				if valid == VersionValid {
					if _, incRR, ok := vm.ReadIncarnation(addr, txIndex); ok && incRR.Status() == MVReadResultDone {
						valid = VersionInvalid
					}
				}
			}
		}
		// A later tx self-destructed the account (no revival), so a read predating
		// the destruct is stale; checkVersion alone misses it because the SD doesn't
		// write the read's own path.
		if valid == VersionValid && path != SelfDestructPath && path != AddressPath &&
			path != IncarnationPath && path != CreateContractPath && path != CodePath {
			if destructed, sdRR, ok := vm.ReadSelfDestruct(addr, txIndex); ok && sdRR.Status() == MVReadResultDone && destructed {
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
					if !revived {
						valid = VersionInvalid
					}
				}
			}
		}
	case MVReadResultDependency:
		valid = VersionInvalid
	case MVReadResultNone:
		switch {
		case source == MapRead && !recursive &&
			(path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath):
			// A sub-field read with no dedicated cell is recorded folded onto
			// AddressPath (its source/version), so validate it against AddressPath
			// at that version.
			valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
				version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)
		case source != StorageRead:
			valid = VersionInvalid
		default:
			if valid = checkVersion(version, version); valid == VersionValid &&
				path != SelfDestructPath && !vm.netAbsentDestruct(addr, txIndex) {
				// Cross-validate any account property read against AddressPath
				// and SelfDestructPath.  A prior tx may have created or
				// self-destructed the account, invalidating storage reads of
				// any property (code, storage slots, balance, nonce, etc.).
				// Skipped for a net-absent create+self-destruct (netAbsentDestruct):
				// the account was absent/empty both at base and after, so the base
				// read is not stale — invalidating it livelocked a same-block reader.
				if path != AddressPath {
					if valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
						version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true); valid == VersionValid {
						valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)
					} else {
						vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)
					}
				} else {
					valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
						version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)

					// A prior tx re-creating this account makes a nil AddressPath
					// storage read stale; IncarnationPath is the specific signal
					// (written only by CreateAccount and SelfDestruct), unlike
					// BalancePath which overfires for every gas payer.
					if valid == VersionValid {
						if _, incRR, ok := vm.ReadIncarnation(addr, txIndex); ok && incRR.Status() == MVReadResultDone {
							valid = VersionInvalid
						}
					}
				}
			}
		}
	default:
		panic(fmt.Errorf("undefined vm read status: %v", rr.Status()))
	}

	if vm.trace || (traceInvalid && valid == VersionInvalid) {
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

// ValidateReadSet validates rs (a tx's read-set) against the current versionMap
// at txIdx. Split out of ValidateVersion so a worker can validate its own
// freshly-produced read-set in parallel, before the exec loop commits.
func (vm *VersionMap) ValidateReadSet(txIdx int, rs ReadSet, checkVersion func(readVersion, writeVersion Version) VersionValidity, traceInvalid bool, tracePrefix string) (valid VersionValidity) {
	valid = VersionValid
	// ok checks one validity result, latching valid; ok==false stops the scan.
	ok := func(v VersionValidity) bool { valid = v; return v == VersionValid }
	// noValueRead validates a path whose recorded value carries no tiebreaker
	// (self-destruct / create-contract / code / code-size): the version/status
	// check is authoritative. One ReadStatus, no value comparison.
	noValueRead := func(addr accounts.Address, path AccountPath, key accounts.StorageKey, hdr ReadHeader) VersionValidity {
		return vm.validateReadImpl(txIdx, addr, path, key, hdr.Source, hdr.Version,
			vm.ReadStatus(addr, path, key, txIdx), nil, checkVersion, traceInvalid, tracePrefix, false)
	}

	// Value paths go through the generic validateRead so the recorded value stays
	// typed (never boxed) and the single typed read supplies both status and the
	// tiebreaker value.
	for a, tr := range rs.address {
		var rv *accounts.Account
		if tr.Val != nil {
			rv = tr.Val.Account()
		}
		if !ok(validateRead(vm, txIdx, a, AddressPath, accounts.NilKey, tr.Source, tr.Version, rv, liveAddress, eqAccount, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.balance {
		if !ok(validateRead(vm, txIdx, a, BalancePath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveBalance, eqUint256, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.nonce {
		if !ok(validateRead(vm, txIdx, a, NoncePath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveNonce, eqUint64, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.incarnation {
		if !ok(validateRead(vm, txIdx, a, IncarnationPath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveIncarnation, eqUint64, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.codeHash {
		if !ok(validateRead(vm, txIdx, a, CodeHashPath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveCodeHash, eqCodeHash, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, inner := range rs.storage {
		for k, tr := range inner {
			if !ok(validateRead(vm, txIdx, a, StoragePath, k, tr.Source, tr.Version, tr.Val, liveStorage, eqUint256, checkVersion, traceInvalid, tracePrefix)) {
				return
			}
		}
	}
	for a, tr := range rs.selfDestruct {
		if !ok(noValueRead(a, SelfDestructPath, accounts.NilKey, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.createContract {
		if !ok(noValueRead(a, CreateContractPath, accounts.NilKey, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.code {
		if !ok(noValueRead(a, CodePath, accounts.NilKey, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.codeSize {
		if !ok(noValueRead(a, CodeSizePath, accounts.NilKey, tr.ReadHeader)) {
			return
		}
	}
	return
}

// WriteCell holds one version of a typed value on a (path, key) cell. The
// type parameter T matches the AccountPath's value-type contract: writing
// the wrong T to a cell is a compile-time error, not a runtime panic.
//
// Typed Read primitives (ReadBalance / ReadStorage / etc.) consume Value
// directly without crossing the any boundary.
type WriteCell[T any] struct {
	flag        statusFlag
	incarnation int
	Value       T
}

// Per-T pools for *WriteCell[T]. Each VersionMap write goes through
// putCellFromPool which retrieves a zeroed cell from the path-corresponding
// pool; Delete/DeleteAll return cells to the same pool. The pools span
// VersionMap lifetimes — a freed cell from block N is recycled into
// block N+1's first write.
//
// Invariants:
//   - Get returns a zeroed cell (we overwrite all fields immediately, so the
//     prior contents are irrelevant; pool's New func returns a zero struct).
//   - Put on slice-valued types (ValBytes / []byte for CodePath) must clear
//     the slice header to avoid pinning bytecode in the pool entry —
//     handled in releaseCellCode below. Other types are value-shaped and
//     don't pin external memory.
var (
	cellPoolAccount        = sync.Pool{New: func() any { return &WriteCell[*accounts.Account]{} }}
	cellPoolSelfDestruct   = sync.Pool{New: func() any { return &WriteCell[bool]{} }}
	cellPoolBalance        = sync.Pool{New: func() any { return &WriteCell[uint256.Int]{} }}
	cellPoolNonce          = sync.Pool{New: func() any { return &WriteCell[uint64]{} }}
	cellPoolIncarnation    = sync.Pool{New: func() any { return &WriteCell[uint64]{} }}
	cellPoolCode           = sync.Pool{New: func() any { return &WriteCell[accounts.Code]{} }}
	cellPoolCodeHash       = sync.Pool{New: func() any { return &WriteCell[accounts.CodeHash]{} }}
	cellPoolCodeSize       = sync.Pool{New: func() any { return &WriteCell[int]{} }}
	cellPoolCreateContract = sync.Pool{New: func() any { return &WriteCell[bool]{} }}
	cellPoolStorage        = sync.Pool{New: func() any { return &WriteCell[uint256.Int]{} }}
)

// getCellAccount and the family of getCell* helpers each fetch a typed
// *WriteCell[T] from the per-path pool. Caller fills the fields before
// inserting into a btree.
func getCellAccount() *WriteCell[*accounts.Account] {
	return cellPoolAccount.Get().(*WriteCell[*accounts.Account])
}
func getCellSelfDestruct() *WriteCell[bool] { return cellPoolSelfDestruct.Get().(*WriteCell[bool]) }
func getCellBalance() *WriteCell[uint256.Int] {
	return cellPoolBalance.Get().(*WriteCell[uint256.Int])
}
func getCellNonce() *WriteCell[uint64] { return cellPoolNonce.Get().(*WriteCell[uint64]) }
func getCellIncarnation() *WriteCell[uint64] {
	return cellPoolIncarnation.Get().(*WriteCell[uint64])
}
func getCellCode() *WriteCell[accounts.Code] { return cellPoolCode.Get().(*WriteCell[accounts.Code]) }
func getCellCodeHash() *WriteCell[accounts.CodeHash] {
	return cellPoolCodeHash.Get().(*WriteCell[accounts.CodeHash])
}
func getCellCodeSize() *WriteCell[int] { return cellPoolCodeSize.Get().(*WriteCell[int]) }
func getCellCreateContract() *WriteCell[bool] {
	return cellPoolCreateContract.Get().(*WriteCell[bool])
}
func getCellStorage() *WriteCell[uint256.Int] {
	return cellPoolStorage.Get().(*WriteCell[uint256.Int])
}

// releaseCell* return a typed cell to its pool. For slice-valued types the
// payload slice header is cleared to avoid pinning external memory.
func releaseCellAccount(c *WriteCell[*accounts.Account]) {
	c.Value = nil
	cellPoolAccount.Put(c)
}
func releaseCellSelfDestruct(c *WriteCell[bool])   { cellPoolSelfDestruct.Put(c) }
func releaseCellBalance(c *WriteCell[uint256.Int]) { cellPoolBalance.Put(c) }
func releaseCellNonce(c *WriteCell[uint64])        { cellPoolNonce.Put(c) }
func releaseCellIncarnation(c *WriteCell[uint64])  { cellPoolIncarnation.Put(c) }
func releaseCellCode(c *WriteCell[accounts.Code]) {
	c.Value = accounts.Code{} // unpin bytecode
	cellPoolCode.Put(c)
}
func releaseCellCodeHash(c *WriteCell[accounts.CodeHash]) { cellPoolCodeHash.Put(c) }
func releaseCellCodeSize(c *WriteCell[int])               { cellPoolCodeSize.Put(c) }
func releaseCellCreateContract(c *WriteCell[bool])        { cellPoolCreateContract.Put(c) }
func releaseCellStorage(c *WriteCell[uint256.Int])        { cellPoolStorage.Put(c) }

type Version struct {
	BlockNum    uint64
	TxNum       uint64
	TxIndex     int
	Incarnation int
}

var UnknownVersion = Version{TxIndex: UnknownDep, Incarnation: -1}

func (v Version) blockAccessIndex() uint32 {
	return uint32(v.TxIndex + 1)
}

const (
	MVReadResultDone       = 0
	MVReadResultDependency = 1
	MVReadResultNone       = 2
)

type ReadResult struct {
	depIdx      int
	incarnation int
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

func (res *ReadResult) Incarnation() int {
	return res.incarnation
}

func (res *ReadResult) Version() Version {
	return Version{
		TxIndex:     res.depIdx,
		Incarnation: res.incarnation,
	}
}

func (mvr ReadResult) Status() int {
	if mvr.depIdx != UnknownDep {
		if mvr.incarnation == -1 {
			return MVReadResultDependency
		} else {
			return MVReadResultDone
		}
	}

	return MVReadResultNone
}
