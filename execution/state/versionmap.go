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
// MUST precede BalancePath because updateWrite skips non-zero balance writes
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
// to its AddressEntry field. Write path is cold enough that the generic
// closure-escape cost is tolerable; the hot Read path inlines descend
// per-case instead.
func putCell[T any](cells *btree.Map[int, *WriteCell[T]], addr accounts.Address, path AccountPath, txIdx, incarnation int, flag statusFlag, value T) *btree.Map[int, *WriteCell[T]] {
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
		ci.boxed = value
		return cells
	}
	cells.Set(txIdx, &WriteCell[T]{flag: flag, incarnation: incarnation, Value: value, boxed: value})
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

func (vm *VersionMap) Write(addr accounts.Address, path AccountPath, key accounts.StorageKey, v Version, data any, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.writeLocked(addr, path, key, v, data, complete)
}

// writeLocked performs the write without acquiring the lock.
// Caller must hold vm.mu.Lock(). The `data any` argument is asserted to
// the AccountPath's contracted value type before storage; a wrong-type
// `data` panics here rather than masquerading as a wrong typed cell on a
// later read. Commit 2b introduces typed Write primitives so call sites
// can avoid this runtime assertion entirely.
func (vm *VersionMap) writeLocked(addr accounts.Address, path AccountPath, key accounts.StorageKey, v Version, data any, complete bool) {
	e, ok := vm.s[addr]
	if !ok {
		e = &AddressEntry{}
		vm.s[addr] = e
	}
	flag := FlagDone
	if !complete {
		flag = FlagEstimate
	}
	switch path {
	case AddressPath:
		e.Address = putCell(e.Address, addr, path, v.TxIndex, v.Incarnation, flag, data.(*accounts.Account))
	case SelfDestructPath:
		e.SelfDestruct = putCell(e.SelfDestruct, addr, path, v.TxIndex, v.Incarnation, flag, data.(bool))
	case BalancePath:
		e.Balance = putCell(e.Balance, addr, path, v.TxIndex, v.Incarnation, flag, data.(uint256.Int))
	case NoncePath:
		e.Nonce = putCell(e.Nonce, addr, path, v.TxIndex, v.Incarnation, flag, data.(uint64))
	case IncarnationPath:
		e.Incarnation = putCell(e.Incarnation, addr, path, v.TxIndex, v.Incarnation, flag, data.(uint64))
	case CodePath:
		e.Code = putCell(e.Code, addr, path, v.TxIndex, v.Incarnation, flag, data.([]byte))
	case CodeHashPath:
		e.CodeHash = putCell(e.CodeHash, addr, path, v.TxIndex, v.Incarnation, flag, data.(accounts.CodeHash))
	case CodeSizePath:
		e.CodeSize = putCell(e.CodeSize, addr, path, v.TxIndex, v.Incarnation, flag, data.(int))
	case CreateContractPath:
		e.CreateContract = putCell(e.CreateContract, addr, path, v.TxIndex, v.Incarnation, flag, data.(bool))
	case StoragePath:
		if e.Storage == nil {
			e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
		}
		e.Storage[key] = putCell(e.Storage[key], addr, path, v.TxIndex, v.Incarnation, flag, data.(uint256.Int))
	default:
		panic(fmt.Errorf("writeLocked: unknown path %v", path))
	}
}

// Typed Write primitives. Each takes the AccountPath-contracted value type
// directly so wrong-type writes are caught at compile time — there is no
// runtime data.(T) assertion path through these.

func (vm *VersionMap) WriteAddress(addr accounts.Address, v Version, value *accounts.Account, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Address = putCell(e.Address, addr, AddressPath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteSelfDestruct(addr accounts.Address, v Version, value bool, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.SelfDestruct = putCell(e.SelfDestruct, addr, SelfDestructPath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteBalance(addr accounts.Address, v Version, value uint256.Int, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Balance = putCell(e.Balance, addr, BalancePath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteNonce(addr accounts.Address, v Version, value uint64, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Nonce = putCell(e.Nonce, addr, NoncePath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteIncarnation(addr accounts.Address, v Version, value uint64, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Incarnation = putCell(e.Incarnation, addr, IncarnationPath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteCode(addr accounts.Address, v Version, value []byte, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.Code = putCell(e.Code, addr, CodePath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteCodeHash(addr accounts.Address, v Version, value accounts.CodeHash, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.CodeHash = putCell(e.CodeHash, addr, CodeHashPath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteCodeSize(addr accounts.Address, v Version, value int, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.CodeSize = putCell(e.CodeSize, addr, CodeSizePath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteCreateContract(addr accounts.Address, v Version, value bool, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	e.CreateContract = putCell(e.CreateContract, addr, CreateContractPath, v.TxIndex, v.Incarnation, flagFor(complete), value)
}

func (vm *VersionMap) WriteStorage(addr accounts.Address, key accounts.StorageKey, v Version, value uint256.Int, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	e := vm.entryOrCreate(addr)
	if e.Storage == nil {
		e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
	}
	e.Storage[key] = putCell(e.Storage[key], addr, StoragePath, v.TxIndex, v.Incarnation, flagFor(complete), value)
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
// cell exists. The any boundary in ReadResult.value is skipped — callers
// consume the typed value directly.

func (vm *VersionMap) ReadAddress(addr accounts.Address, txIdx int) (val *accounts.Account, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.Address == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[*accounts.Account]
	e.Address.Descend(txIdx-1, func(k int, v *WriteCell[*accounts.Account]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadSelfDestruct(addr accounts.Address, txIdx int) (val bool, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.SelfDestruct == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[bool]
	e.SelfDestruct.Descend(txIdx-1, func(k int, v *WriteCell[bool]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadBalance(addr accounts.Address, txIdx int) (val uint256.Int, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.Balance == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[uint256.Int]
	e.Balance.Descend(txIdx-1, func(k int, v *WriteCell[uint256.Int]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadNonce(addr accounts.Address, txIdx int) (val uint64, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.Nonce == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[uint64]
	e.Nonce.Descend(txIdx-1, func(k int, v *WriteCell[uint64]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadIncarnation(addr accounts.Address, txIdx int) (val uint64, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.Incarnation == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[uint64]
	e.Incarnation.Descend(txIdx-1, func(k int, v *WriteCell[uint64]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadCode(addr accounts.Address, txIdx int) (val []byte, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.Code == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[[]byte]
	e.Code.Descend(txIdx-1, func(k int, v *WriteCell[[]byte]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadCodeHash(addr accounts.Address, txIdx int) (val accounts.CodeHash, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.CodeHash == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[accounts.CodeHash]
	e.CodeHash.Descend(txIdx-1, func(k int, v *WriteCell[accounts.CodeHash]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadCodeSize(addr accounts.Address, txIdx int) (val int, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.CodeSize == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[int]
	e.CodeSize.Descend(txIdx-1, func(k int, v *WriteCell[int]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadCreateContract(addr accounts.Address, txIdx int) (val bool, res ReadResult, ok bool) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	if vm == nil {
		return val, res, false
	}
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	e, present := vm.s[addr]
	if !present || e.CreateContract == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[bool]
	e.CreateContract.Descend(txIdx-1, func(k int, v *WriteCell[bool]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) ReadStorage(addr accounts.Address, key accounts.StorageKey, txIdx int) (val uint256.Int, res ReadResult, ok bool) {
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
	cells := e.Storage[key]
	if cells == nil {
		return val, res, false
	}
	fk := UnknownDep
	var fv *WriteCell[uint256.Int]
	cells.Descend(txIdx-1, func(k int, v *WriteCell[uint256.Int]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false
	}
	res.depIdx = fk
	if fv.flag == FlagDone {
		res.incarnation = fv.incarnation
	}
	return fv.Value, res, true
}

func (vm *VersionMap) Read(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) (res ReadResult) {
	res.depIdx = UnknownDep
	res.incarnation = -1

	if vm == nil {
		return res
	}

	vm.mu.RLock()
	defer vm.mu.RUnlock()

	e, ok := vm.s[addr]
	if !ok {
		return
	}

	// Per-path Descend is inlined per case. A generic helper here costs an
	// extra heap alloc per Read (Go generic functions are dictionary-passed
	// rather than fully inlined, so the closure capture escapes once per
	// instantiation). Inlining the descend keeps reads at one alloc (the
	// any-box of fv.Value into res.value at the API boundary). Commit 2b
	// drops that any-box too via typed Read primitives.
	maxIdx := txIdx - 1
	switch path {
	case AddressPath:
		if e.Address == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[*accounts.Account]
		e.Address.Descend(maxIdx, func(k int, v *WriteCell[*accounts.Account]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case SelfDestructPath:
		if e.SelfDestruct == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[bool]
		e.SelfDestruct.Descend(maxIdx, func(k int, v *WriteCell[bool]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case BalancePath:
		if e.Balance == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[uint256.Int]
		e.Balance.Descend(maxIdx, func(k int, v *WriteCell[uint256.Int]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case NoncePath:
		if e.Nonce == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[uint64]
		e.Nonce.Descend(maxIdx, func(k int, v *WriteCell[uint64]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case IncarnationPath:
		if e.Incarnation == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[uint64]
		e.Incarnation.Descend(maxIdx, func(k int, v *WriteCell[uint64]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case CodePath:
		if e.Code == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[[]byte]
		e.Code.Descend(maxIdx, func(k int, v *WriteCell[[]byte]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case CodeHashPath:
		if e.CodeHash == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[accounts.CodeHash]
		e.CodeHash.Descend(maxIdx, func(k int, v *WriteCell[accounts.CodeHash]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case CodeSizePath:
		if e.CodeSize == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[int]
		e.CodeSize.Descend(maxIdx, func(k int, v *WriteCell[int]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case CreateContractPath:
		if e.CreateContract == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[bool]
		e.CreateContract.Descend(maxIdx, func(k int, v *WriteCell[bool]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	case StoragePath:
		cells := e.Storage[key]
		if cells == nil {
			return
		}
		fk := UnknownDep
		var fv *WriteCell[uint256.Int]
		cells.Descend(maxIdx, func(k int, v *WriteCell[uint256.Int]) bool {
			fk, fv = k, v
			return false
		})
		if fk != UnknownDep && fv != nil {
			res.depIdx = fk
			if fv.flag == FlagDone {
				res.incarnation = fv.incarnation
			}
			res.value = fv.boxed
		}
	}
	return
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

// FlushVersionedWrites atomically flushes all writes to the version map
// under a single lock acquisition. This prevents concurrent readers from
// observing a partially-flushed state (e.g. seeing an AddressPath write
// but not the corresponding CodePath write from the same transaction),
// which could cause non-deterministic BAL (EIP-7928) hashes during
// parallel execution.
func (vm *VersionMap) FlushVersionedWrites(writes VersionedWrites, complete bool, tracePrefix string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	for _, v := range writes {
		if vm.trace {
			fmt.Println(tracePrefix, "FLSH", v.String())
		}
		vm.writeLocked(v.Address, v.Path, v.Key, v.Version, v.Val, complete)
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
			e.Address.Delete(txIdx)
		}
	case SelfDestructPath:
		if e.SelfDestruct != nil {
			hasField = true
			e.SelfDestruct.Delete(txIdx)
		}
	case BalancePath:
		if e.Balance != nil {
			hasField = true
			e.Balance.Delete(txIdx)
		}
	case NoncePath:
		if e.Nonce != nil {
			hasField = true
			e.Nonce.Delete(txIdx)
		}
	case IncarnationPath:
		if e.Incarnation != nil {
			hasField = true
			e.Incarnation.Delete(txIdx)
		}
	case CodePath:
		if e.Code != nil {
			hasField = true
			e.Code.Delete(txIdx)
		}
	case CodeHashPath:
		if e.CodeHash != nil {
			hasField = true
			e.CodeHash.Delete(txIdx)
		}
	case CodeSizePath:
		if e.CodeSize != nil {
			hasField = true
			e.CodeSize.Delete(txIdx)
		}
	case CreateContractPath:
		if e.CreateContract != nil {
			hasField = true
			e.CreateContract.Delete(txIdx)
		}
	case StoragePath:
		if cells := e.Storage[key]; cells != nil {
			hasField = true
			cells.Delete(txIdx)
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
		e.Address.Delete(txIdx)
	}
	if e.SelfDestruct != nil {
		e.SelfDestruct.Delete(txIdx)
	}
	if e.Balance != nil {
		e.Balance.Delete(txIdx)
	}
	if e.Nonce != nil {
		e.Nonce.Delete(txIdx)
	}
	if e.Incarnation != nil {
		e.Incarnation.Delete(txIdx)
	}
	if e.Code != nil {
		e.Code.Delete(txIdx)
	}
	if e.CodeHash != nil {
		e.CodeHash.Delete(txIdx)
	}
	if e.CodeSize != nil {
		e.CodeSize.Delete(txIdx)
	}
	if e.CreateContract != nil {
		e.CreateContract.Delete(txIdx)
	}
	for _, cells := range e.Storage {
		cells.Delete(txIdx)
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

func (vm *VersionMap) validateRead(txIndex int, addr accounts.Address, path AccountPath, key accounts.StorageKey, source ReadSource, version Version,
	readVal any,
	checkVersion func(readVersion, writeVersion Version) VersionValidity,
	traceInvalid bool, tracePrefix string) VersionValidity {

	valid := VersionValid

	rr := vm.Read(addr, path, key, txIndex)
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
				// Value tiebreaker: if the StorageRead value matches the
				// versionMap Done value, the read is still valid despite
				// the source mismatch. This avoids unnecessary invalidation
				// when a prior TX wrote the same value that was in storage.
				if readVal != nil && rr.Value() != nil && valuesEqual(path, readVal, rr.Value()) {
					// Values match — read is valid
				} else {
					valid = VersionInvalid
				}
			}
		} else {
			valid = checkVersion(version, rr.Version())
		}
	case MVReadResultDependency:
		valid = VersionInvalid
	case MVReadResultNone:
		if source != StorageRead {
			valid = VersionInvalid
		} else {
			if valid = checkVersion(version, version); valid == VersionValid {
				// Cross-validate any account property read against AddressPath
				// and SelfDestructPath.  A prior tx may have created or
				// self-destructed the account, invalidating storage reads of
				// any property (code, storage slots, balance, nonce, etc.).
				if path != AddressPath && path != SelfDestructPath {
					if valid = vm.validateRead(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
						version, nil, checkVersion, traceInvalid, tracePrefix); valid == VersionValid {
						valid = vm.validateRead(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, nil, checkVersion, traceInvalid, tracePrefix)
					} else {
						vm.validateRead(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, nil, checkVersion, traceInvalid, tracePrefix)
					}
				} else if path == AddressPath {
					valid = vm.validateRead(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
						version, nil, checkVersion, traceInvalid, tracePrefix)

					// If a prior tx created this account, BalancePath will
					// have an entry at a lower txIndex (from BAL pre-population
					// or worker flush). A nil AddressPath read from storage
					// is then stale and must be invalidated.
					if valid == VersionValid {
						if _, balRR, ok := vm.ReadBalance(addr, txIndex); ok && balRR.Status() == MVReadResultDone {
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
	if readSet := lastIO.ReadSet(txIdx); readSet != nil {
		readSet.Scan(func(vr *VersionedRead) bool {
			valid = vm.validateRead(txIdx, vr.Address, vr.Path, vr.Key, vr.Source, vr.Version,
				vr.Val, checkVersion, traceInvalid, tracePrefix)
			return valid == VersionValid
		})
	}

	return
}

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
// directly without crossing the any boundary. The legacy any-shaped Read
// API still exists for the consumers that have not yet migrated; for those
// the boxed field caches the any form of Value populated once at write
// time, so the legacy Read doesn't re-box on every call. Once every
// consumer of Read is migrated to a typed primitive, boxed is dropped.
type WriteCell[T any] struct {
	flag        statusFlag
	incarnation int
	Value       T
	boxed       any
}

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
	value       any
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

func (res *ReadResult) Value() any {
	return res.value
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
