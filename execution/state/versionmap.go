package state

import (
	"errors"
	"fmt"
	"sync"

	"github.com/holiman/uint256"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type statusFlag uint

const FlagDone statusFlag = 0
const FlagEstimate statusFlag = 1
const UnknownDep = -2

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

// AccountPath enum values. The numeric order matters: AsBlockAccessList
// sorts writes by Path to ensure deterministic processing. SelfDestructPath
// MUST precede BalancePath because updateWrite zeroes non-zero balance writes
// in the same tx as a selfdestruct — the selfDestructed flag must be set
// before balance writes are evaluated. Do not reorder without reviewing
// updateWrite in versionedio.go.
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
// Invariant — per-field independence: no consumer treats AddressEntry as
// a transactional whole. Reads, writes, mark-estimate/complete, delete
// and validation all operate at (Path, Key) granularity. Helpers that
// look like address-level operations (DeleteAll, StorageKeys) are pure
// iterations of per-field operations.
type AddressEntry struct {
	Address        *btree.Map[int, *WriteCell[*accounts.Account]]
	SelfDestruct   *btree.Map[int, *WriteCell[bool]]
	Balance        *btree.Map[int, *WriteCell[uint256.Int]]
	Nonce          *btree.Map[int, *WriteCell[uint64]]
	Incarnation    *btree.Map[int, *WriteCell[uint64]]
	Code           *btree.Map[int, *WriteCell[[]byte]]
	CodeHash       *btree.Map[int, *WriteCell[accounts.CodeHash]]
	CodeSize       *btree.Map[int, *WriteCell[int]]
	CreateContract *btree.Map[int, *WriteCell[bool]]
	Storage        map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]
}

// putCell sets or updates a typed cell at txIdx. Caller must hold vm.mu.Lock().
// Returns the (possibly newly-created) cell map for the caller to assign back
// to its AddressEntry field. `getCell` is the per-T pool fetcher (e.g.
// getCellBalance for the BalancePath); it is a static function-value, so
// passing it costs no allocation. The write path uses pool-supplied cells
// instead of `&WriteCell[T]{...}` literals — Delete/DeleteAll return them
// to the same pool for reuse across blocks.
func putCell[T any](cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, txIdx, incarnation int, flag statusFlag, value T, getCell func() *WriteCell[T]) *btree.Map[int, *WriteCell[T]] {
	if cells == nil {
		cells = &btree.Map[int, *WriteCell[T]]{}
	}
	if ci, ok := cells.Get(txIdx); ok {
		if ci.incarnation > incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %x %s, %v", addr, path, txIdx))
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

// markCellFlag sets the flag on an existing typed cell. Panics with msg if
// no cell is present at txIdx — used by MarkEstimate/MarkComplete which
// require a prior write.
func markCellFlag[T any](cells *btree.Map[int, *WriteCell[T]], txIdx int, flag statusFlag, msg string) {
	if cells == nil {
		panic(msg)
	}
	ci, ok := cells.Get(txIdx)
	if !ok {
		panic(msg)
	}
	ci.flag = flag
}

type VersionMap struct {
	mu     sync.RWMutex
	s      map[accounts.Address]*AddressEntry
	trace  bool
	HasBAL bool // When true, all significant writes are pre-populated from BAL
}

func NewVersionMap(changes []*types.AccountChanges) *VersionMap {
	vm := &VersionMap{
		s:      map[accounts.Address]*AddressEntry{},
		HasBAL: len(changes) > 0,
	}
	vm.WriteChanges(changes)
	return vm
}

func (vm *VersionMap) SetTrace(trace bool) {
	vm.trace = trace
}

// StorageKeys returns every storage slot key recorded for addr. Used by
// normalizeWriteSet to emit synthetic delete entries for every slot of a
// selfdestructed contract, matching DomainDelPrefix behaviour from the
// sequential path.
func (vm *VersionMap) StorageKeys(addr accounts.Address) []accounts.StorageKey {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, ok := vm.s[addr]
	if !ok || len(e.Storage) == 0 {
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
func (vm *VersionMap) WriteChanges(changes []*types.AccountChanges) {
	for _, accountChanges := range changes {
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
			vm.WriteCode(accountChanges.Address, Version{TxIndex: int(codeChange.Index) - 1}, codeChange.Bytecode, true)
		}
	}
}

// Typed Write primitives. Each takes the AccountPath-contracted value type
// directly so wrong-type writes are caught at compile time — there is no
// runtime data.(T) assertion path through these.

func (vm *VersionMap) WriteAddress(addr accounts.Address, v Version, value *accounts.Account, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Address = putCell(e.Address, addr, AddressPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellAccount)
}

func (vm *VersionMap) WriteSelfDestruct(addr accounts.Address, v Version, value bool, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.SelfDestruct = putCell(e.SelfDestruct, addr, SelfDestructPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellSelfDestruct)
}

func (vm *VersionMap) WriteBalance(addr accounts.Address, v Version, value uint256.Int, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Balance = putCell(e.Balance, addr, BalancePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellBalance)
}

func (vm *VersionMap) WriteNonce(addr accounts.Address, v Version, value uint64, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Nonce = putCell(e.Nonce, addr, NoncePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellNonce)
}

func (vm *VersionMap) WriteIncarnation(addr accounts.Address, v Version, value uint64, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Incarnation = putCell(e.Incarnation, addr, IncarnationPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellIncarnation)
}

func (vm *VersionMap) WriteCode(addr accounts.Address, v Version, value []byte, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Code = putCell(e.Code, addr, CodePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCode)
}

func (vm *VersionMap) WriteCodeHash(addr accounts.Address, v Version, value accounts.CodeHash, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.CodeHash = putCell(e.CodeHash, addr, CodeHashPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCodeHash)
}

func (vm *VersionMap) WriteCodeSize(addr accounts.Address, v Version, value int, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.CodeSize = putCell(e.CodeSize, addr, CodeSizePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCodeSize)
}

func (vm *VersionMap) WriteCreateContract(addr accounts.Address, v Version, value bool, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.CreateContract = putCell(e.CreateContract, addr, CreateContractPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCreateContract)
}

func (vm *VersionMap) WriteStorage(addr accounts.Address, key accounts.StorageKey, v Version, value uint256.Int, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	if e.Storage == nil {
		e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
	}
	e.Storage[key] = putCell(e.Storage[key], addr, StoragePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellStorage)
}

// entryOrCreate looks up the AddressEntry for addr, creating it if absent.
// Caller must hold vm.mu.Lock().
func (vm *VersionMap) entryOrCreate(addr accounts.Address) *AddressEntry {
	e, ok := vm.s[addr]
	if !ok {
		e = &AddressEntry{}
		vm.s[addr] = e
	}
	return e
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
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present {
		return val, res, false
	}
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

func (vm *VersionMap) ReadCode(addr accounts.Address, txIdx int) ([]byte, ReadResult, bool) {
	return readFloor(vm, addr, txIdx, func(e *AddressEntry) *btree.Map[int, *WriteCell[[]byte]] { return e.Code })
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
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	e, ok := vm.s[addr]
	if !ok {
		return 0, false
	}

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
			e.Code.Descend(txIdxLimit, func(k int, _ *WriteCell[[]byte]) bool { fk = k; return false })
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

// AnyDoneSelfDestructEquals reports whether any Done SelfDestruct write at
// TxIdx ≤ txIdxLimit has value == target. Detects a prior in-block
// SelfDestructPath=true write that a later revival flipped back to false
// — a case Read alone (latest-only) misses.
func (vm *VersionMap) AnyDoneSelfDestructEquals(addr accounts.Address, txIdxLimit int, target bool) bool {
	if vm == nil {
		return false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	e, present := vm.s[addr]
	if !present || e.SelfDestruct == nil {
		return false
	}
	found := false
	e.SelfDestruct.Descend(txIdxLimit, func(_ int, v *WriteCell[bool]) bool {
		if v.flag != FlagDone {
			return true
		}
		if v.Value == target {
			found = true
			return false
		}
		return true
	})
	return found
}

// FlushVersionedWrites atomically flushes all writes to the version map
// under a single lock acquisition. This prevents concurrent readers from
// observing a partially-flushed state (e.g. seeing an AddressPath write
// but not the corresponding CodePath write from the same transaction),
// which could cause non-deterministic BAL (EIP-7928) hashes during
// parallel execution.
// FlushVersionedWrites routes a tx's typed write collections into the version
// map. Each cell is positioned by the write's (txIndex, incarnation), so the
// per-path loop order does not affect the result.
func (vm *VersionMap) FlushVersionedWrites(writes *WriteSet, complete bool, tracePrefix string) {
	if writes == nil {
		return
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()
	flag := flagFor(complete)
	for addr, vw := range writes.address {
		e := vm.entryOrCreate(addr)
		e.Address = putCell(e.Address, addr, AddressPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellAccount)
	}
	for addr, vw := range writes.selfDestruct {
		e := vm.entryOrCreate(addr)
		e.SelfDestruct = putCell(e.SelfDestruct, addr, SelfDestructPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellSelfDestruct)
	}
	for addr, vw := range writes.balance {
		e := vm.entryOrCreate(addr)
		e.Balance = putCell(e.Balance, addr, BalancePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellBalance)
	}
	for addr, vw := range writes.nonce {
		e := vm.entryOrCreate(addr)
		e.Nonce = putCell(e.Nonce, addr, NoncePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellNonce)
	}
	for addr, vw := range writes.incarnation {
		e := vm.entryOrCreate(addr)
		e.Incarnation = putCell(e.Incarnation, addr, IncarnationPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellIncarnation)
	}
	for addr, vw := range writes.code {
		e := vm.entryOrCreate(addr)
		e.Code = putCell(e.Code, addr, CodePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val.Bytes, getCellCode)
	}
	for addr, vw := range writes.codeHash {
		e := vm.entryOrCreate(addr)
		e.CodeHash = putCell(e.CodeHash, addr, CodeHashPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCodeHash)
	}
	for addr, vw := range writes.codeSize {
		e := vm.entryOrCreate(addr)
		e.CodeSize = putCell(e.CodeSize, addr, CodeSizePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCodeSize)
	}
	for addr, vw := range writes.createContract {
		e := vm.entryOrCreate(addr)
		e.CreateContract = putCell(e.CreateContract, addr, CreateContractPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCreateContract)
	}
	for addr, inner := range writes.storage {
		e := vm.entryOrCreate(addr)
		if e.Storage == nil {
			e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
		}
		for key, vw := range inner {
			e.Storage[key] = putCell(e.Storage[key], addr, StoragePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellStorage)
		}
	}
}

func (vm *VersionMap) MarkEstimate(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.markFlag(addr, path, key, txIdx, FlagEstimate)
}

func (vm *VersionMap) MarkComplete(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.markFlag(addr, path, key, txIdx, FlagDone)
}

// markFlag updates the flag on an existing (addr, path, key, txIdx) cell.
// Caller must hold vm.mu.Lock(). Panics if no cell is present at txIdx —
// MarkEstimate/MarkComplete require a prior write.
func (vm *VersionMap) markFlag(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, flag statusFlag) {
	e, ok := vm.s[addr]
	if !ok {
		panic(fmt.Errorf("markFlag: no entry for addr %x, path %s, txIdx %d", addr, path, txIdx))
	}
	msg := fmt.Sprintf("markFlag: missing cell. addr=%x path=%s key=%x txIdx=%d", addr, path, key, txIdx)
	switch path {
	case AddressPath:
		markCellFlag(e.Address, txIdx, flag, msg)
	case SelfDestructPath:
		markCellFlag(e.SelfDestruct, txIdx, flag, msg)
	case BalancePath:
		markCellFlag(e.Balance, txIdx, flag, msg)
	case NoncePath:
		markCellFlag(e.Nonce, txIdx, flag, msg)
	case IncarnationPath:
		markCellFlag(e.Incarnation, txIdx, flag, msg)
	case CodePath:
		markCellFlag(e.Code, txIdx, flag, msg)
	case CodeHashPath:
		markCellFlag(e.CodeHash, txIdx, flag, msg)
	case CodeSizePath:
		markCellFlag(e.CodeSize, txIdx, flag, msg)
	case CreateContractPath:
		markCellFlag(e.CreateContract, txIdx, flag, msg)
	case StoragePath:
		markCellFlag(e.Storage[key], txIdx, flag, msg)
	default:
		panic(fmt.Errorf("markFlag: unknown path %v", path))
	}
}

func (vm *VersionMap) Delete(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, checkExists bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e, ok := vm.s[addr]
	if !ok {
		if !checkExists {
			return
		}
		panic(errors.New("path must already exist"))
	}
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
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e, ok := vm.s[addr]
	if !ok {
		return
	}
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
	return vm.ReadStorage(a, k, tx)
}

func eqUint256(a, b uint256.Int) bool { return a.Eq(&b) }
func eqUint64(a, b uint64) bool       { return a == b }
func eqCodeHash(a, b accounts.CodeHash) bool {
	return a == b
}
func eqAccount(a, b *accounts.Account) bool {
	return a != nil && b != nil && a.Balance.Eq(&b.Balance) && a.Nonce == b.Nonce &&
		a.Incarnation == b.Incarnation && a.CodeHash == b.CodeHash
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
			// When BAL is present, significant writes for BalancePath,
			// NoncePath, CodePath and StoragePath are pre-populated in the
			// VersionMap before execution.  If a read of one of those paths
			// was from storage (no VersionMap entry at execution time) but
			// the VersionMap now has an entry from a concurrent worker
			// flush, the entry is a BAL-filtered no-op write and the read
			// value is still correct.
			//
			// AddressPath and other paths are NOT pre-populated by the BAL,
			// so a new VersionMap entry means a real state change from a
			// concurrent worker (e.g. account creation) and must trigger
			// invalidation.
			isBALPrePopulatedPath := path == BalancePath || path == NoncePath ||
				path == CodePath || path == StoragePath
			if !vm.HasBAL || !isBALPrePopulatedPath {
				if recursive && matchesLive == nil {
					// Synthetic cross-validate probe (no recorded value of its
					// own) — the outer entry's validation covers it. Without this
					// guard a recursive AddressPath/SelfDestructPath probe that
					// lands on a Done cell would over-invalidate.
				} else if matchesLive != nil && matchesLive() {
					// Value tiebreaker: a Done entry now exists where the read
					// saw storage, but it holds the same value — read stays valid.
					// Evaluated typed by the caller; no boxing.
				} else {
					valid = VersionInvalid
				}
			}
		} else {
			valid = checkVersion(version, rr.Version())
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
		if source == MapRead && !recursive &&
			(path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath) {
			// A sub-field read with no dedicated cell is recorded folded onto
			// AddressPath (its source/version), so validate it against AddressPath
			// at that version.
			valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
				version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)
		} else if source != StorageRead {
			valid = VersionInvalid
		} else {
			if valid = checkVersion(version, version); valid == VersionValid {
				// Cross-validate any account property read against AddressPath
				// and SelfDestructPath.  A prior tx may have created or
				// self-destructed the account, invalidating storage reads of
				// any property (code, storage slots, balance, nonce, etc.).
				if path != AddressPath && path != SelfDestructPath {
					if valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
						version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true); valid == VersionValid {
						valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)
					} else {
						vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), nil, checkVersion, traceInvalid, tracePrefix, true)
					}
				} else if path == AddressPath {
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
			tracePrefix = tracePrefix + "  RD"
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
	rs := lastIO.ReadSet(txIdx)
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

// done requires the re-read to resolve to a committed (Done) cell, not an
// Estimate — an Estimate value can still change, so comparing against it is only
// safe if the caller guarantees no concurrent flush, which we don't assume here.
func done(res ReadResult, ok bool) bool { return ok && res.Status() == MVReadResultDone }

// valuesEqual compares a read value with a versionMap write value for the
// same path. Used as a tiebreaker: when the version/source check would
// invalidate but the actual values match, the read is still valid.
func valuesEqual(path AccountPath, readVal, writeVal any) bool {
	if readVal == nil || writeVal == nil {
		return readVal == nil && writeVal == nil
	}
	switch path {
	case BalancePath:
		rv, ok1 := readVal.(uint256.Int)
		wv, ok2 := writeVal.(uint256.Int)
		return ok1 && ok2 && rv.Eq(&wv)
	case NoncePath:
		rv, ok1 := readVal.(uint64)
		wv, ok2 := writeVal.(uint64)
		return ok1 && ok2 && rv == wv
	case IncarnationPath:
		rv, ok1 := readVal.(uint64)
		wv, ok2 := writeVal.(uint64)
		return ok1 && ok2 && rv == wv
	case CodeHashPath:
		rv, ok1 := readVal.(accounts.CodeHash)
		wv, ok2 := writeVal.(accounts.CodeHash)
		return ok1 && ok2 && rv == wv
	case AddressPath:
		// Record-level comparison — both should be *accounts.Account
		rv, ok1 := readVal.(*accounts.Account)
		wv, ok2 := writeVal.(*accounts.Account)
		if !ok1 || !ok2 || rv == nil || wv == nil {
			return false
		}
		return rv.Balance.Eq(&wv.Balance) && rv.Nonce == wv.Nonce &&
			rv.Incarnation == wv.Incarnation && rv.CodeHash == wv.CodeHash
	case StoragePath:
		rv, ok1 := readVal.(uint256.Int)
		wv, ok2 := writeVal.(uint256.Int)
		return ok1 && ok2 && rv.Eq(&wv)
	default:
		return false
	}
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
	cellPoolCode           = sync.Pool{New: func() any { return &WriteCell[[]byte]{} }}
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
func getCellCode() *WriteCell[[]byte] { return cellPoolCode.Get().(*WriteCell[[]byte]) }
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
func releaseCellCode(c *WriteCell[[]byte]) {
	c.Value = nil // unpin bytecode
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
