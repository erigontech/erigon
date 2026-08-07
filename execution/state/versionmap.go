package state

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/holiman/uint256"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common/dbg"
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
	Code           *btree.Map[int, *WriteCell[accounts.Code]]
	CodeHash       *btree.Map[int, *WriteCell[accounts.CodeHash]]
	CodeSize       *btree.Map[int, *WriteCell[int]]
	CreateContract *btree.Map[int, *WriteCell[bool]]
	Storage        map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]
	// mu guards this account's cell maps. Held RLock for reads (readFloor and
	// the per-path scans) and Lock for writes (putCell / Delete / markFlag).
	mu sync.RWMutex
}

// putCell sets or updates a typed cell at txIdx. Caller must hold e.mu.Lock().
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
	// s maps address → *AddressEntry as a sync.Map so account lookup is
	// lock-free on the read hot path (no shared reader-counter to contend on).
	// Each AddressEntry carries its own RWMutex guarding that account's cells,
	// so reads/writes of different accounts never contend — the global RWMutex
	// this replaced serialised every access. Per-read conflict detection is
	// unchanged; only the lock granularity moved from global to per-account.
	s     sync.Map // accounts.Address -> *AddressEntry
	trace bool
}

func NewVersionMap(changes []*types.AccountChanges) *VersionMap {
	vm := &VersionMap{}
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
func (vm *VersionMap) WriteChanges(changes []*types.AccountChanges) {
	for _, accountChanges := range changes {
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
				vm.WriteStorage(accountChanges.Address, storageChanges.Slot, Version{TxIndex: int(change.Index) - 1}, change.Value, true)
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
			vm.WriteBalance(accountChanges.Address, Version{TxIndex: int(balanceChange.Index) - 1}, balanceChange.Value, true)
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
			vm.WriteNonce(accountChanges.Address, Version{TxIndex: int(nonceChange.Index) - 1}, nonceChange.Value, true)
		}
		for _, codeChange := range accountChanges.CodeChanges {
			if dbg.TraceBALFeed {
				fmt.Printf(
					"BAL-CELL %x code balIdx=%d cell=%d len=%d\n",
					accountChanges.Address,
					codeChange.Index,
					int(codeChange.Index)-1,
					len(codeChange.Bytecode),
				)
			}
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
	e.Address = putCell(e.Address, addr, AddressPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellAccount)
}

func (vm *VersionMap) WriteSelfDestruct(addr accounts.Address, v Version, value bool, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.SelfDestruct = putCell(e.SelfDestruct, addr, SelfDestructPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellSelfDestruct)
}

func (vm *VersionMap) WriteBalance(addr accounts.Address, v Version, value uint256.Int, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Balance = putCell(e.Balance, addr, BalancePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellBalance)
}

func (vm *VersionMap) WriteNonce(addr accounts.Address, v Version, value uint64, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Nonce = putCell(e.Nonce, addr, NoncePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellNonce)
}

func (vm *VersionMap) WriteIncarnation(addr accounts.Address, v Version, value uint64, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Incarnation = putCell(e.Incarnation, addr, IncarnationPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellIncarnation)
}

func (vm *VersionMap) WriteCode(addr accounts.Address, v Version, value accounts.Code, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Code = putCell(e.Code, addr, CodePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCode)
}

func (vm *VersionMap) WriteCodeHash(addr accounts.Address, v Version, value accounts.CodeHash, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CodeHash = putCell(e.CodeHash, addr, CodeHashPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCodeHash)
}

func (vm *VersionMap) WriteCodeSize(addr accounts.Address, v Version, value int, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CodeSize = putCell(e.CodeSize, addr, CodeSizePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCodeSize)
}

func (vm *VersionMap) WriteCreateContract(addr accounts.Address, v Version, value bool, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CreateContract = putCell(e.CreateContract, addr, CreateContractPath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellCreateContract)
}

func (vm *VersionMap) WriteStorage(addr accounts.Address, key accounts.StorageKey, v Version, value uint256.Int, complete bool) {
	e := vm.entryOrCreate(addr)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Storage == nil {
		e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
	}
	e.Storage[key] = putCell(e.Storage[key], addr, StoragePath, v.TxIndex, v.Incarnation, flagFor(complete), value, getCellStorage)
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

// sdProbe carries the highest Done SelfDestruct=true index below txIdx,
// resolved under the same entry lock as another path's floor read.
// resolved==false means it was not fetched and the consumer must scan itself.
type sdProbe struct {
	hiDoneTrue int
	resolved   bool
}

var unresolvedSD = sdProbe{hiDoneTrue: UnknownDep}

// destructedSince answers FindDoneSelfDestructInRange(addr, lo, hi, true)'s
// found bit. hi must be the txIdx the probe was resolved at, and lo at or above
// its floor index — the probe's scan already covers that range.
func (sd sdProbe) destructedSince(vm *VersionMap, addr accounts.Address, lo, hi int) bool {
	if sd.resolved {
		return sd.hiDoneTrue != UnknownDep && sd.hiDoneTrue >= lo
	}
	_, ok := vm.FindDoneSelfDestructInRange(addr, lo, hi, true)
	return ok
}

// readFloorWithSelfDestruct resolves sel's floor cell and the address's
// self-destruct net under one entry lookup and one read lock, where readFloor
// plus FindDoneSelfDestructInRange would take one of each. Validation needs
// both for the same address on every value-path read.
func readFloorWithSelfDestruct[T any](vm *VersionMap, addr accounts.Address, txIdx int, sel func(*AddressEntry) *btree.Map[int, *WriteCell[T]]) (val T, res ReadResult, ok bool, sd sdProbe) {
	res.depIdx = UnknownDep
	res.incarnation = -1
	sd.hiDoneTrue = UnknownDep
	if vm == nil {
		return val, res, false, sd
	}
	e := vm.load(addr)
	if e == nil {
		return val, res, false, sd
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	cells := sel(e)
	if cells == nil {
		return val, res, false, sd
	}
	fk := UnknownDep
	var fv *WriteCell[T]
	cells.Descend(txIdx-1, func(k int, v *WriteCell[T]) bool {
		fk, fv = k, v
		return false
	})
	if fk == UnknownDep || fv == nil {
		return val, res, false, sd
	}
	res.depIdx = fk
	switch fv.flag {
	case FlagDone:
		res.incarnation = fv.incarnation
	case FlagEstimate:
	default:
		panic("unknown flag value")
	}
	if e.SelfDestruct != nil {
		e.SelfDestruct.Descend(txIdx-1, func(k int, v *WriteCell[bool]) bool {
			if k < fk {
				return false
			}
			if v.flag == FlagDone && v.Value {
				sd.hiDoneTrue = k
				return false
			}
			return true
		})
	}
	sd.resolved = true
	return fv.Value, res, true, sd
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

// AccountLifecycle resolves an account's self-destruct/revival verdict at txIdx
// from the synthetic lifecycle paths, using a single revival definition that all
// consumers share (readers, validation, and the create decision) so they cannot
// diverge. destroyed reports a Done SelfDestruct write at TxIdx ≤ txIdx with
// value true; destroyedAt is that write's TxIndex. revived reports a re-creation
// strictly after the destruct and before txIdx: AddressPath ≥ destroyedAt
// (catches same-tx metamorphic SD+CREATE2, where both land at the same TxIdx) or
// any of {Balance,Nonce,CodeHash} > destroyedAt. A destroyed-and-not-revived
// account reads as gone.
func (vm *VersionMap) AccountLifecycle(addr accounts.Address, txIdx int) (destroyed bool, destroyedAt int, revived bool) {
	if vm == nil {
		return false, 0, false
	}
	d, sdRes, ok := vm.ReadSelfDestruct(addr, txIdx)
	if !ok || sdRes.Status() != MVReadResultDone || !d {
		return false, 0, false
	}
	destroyedAt = sdRes.DepIdx()
	revivalLimit := txIdx - 1
	if hi, ok := vm.LatestTxIndex(addr, AddressPath, accounts.NilKey, revivalLimit); ok && hi >= destroyedAt {
		return true, destroyedAt, true
	}
	for _, p := range [...]AccountPath{BalancePath, NoncePath, CodeHashPath} {
		if hi, ok := vm.LatestTxIndex(addr, p, accounts.NilKey, revivalLimit); ok && hi > destroyedAt {
			return true, destroyedAt, true
		}
	}
	return true, destroyedAt, false
}

// AnyDoneSelfDestructEquals reports whether any Done SelfDestruct write at
// TxIdx ≤ txIdxLimit has value == target. Detects a prior in-block
// SelfDestructPath=true write that a later revival flipped back to false
// — a case Read alone (latest-only) misses.
func (vm *VersionMap) AnyDoneSelfDestructEquals(addr accounts.Address, txIdxLimit int, target bool) bool {
	if vm == nil {
		return false
	}
	e := vm.load(addr)
	if e == nil {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.SelfDestruct == nil {
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

// selfDestructRevived reports whether any cell written after the destruct
// index (and below txIndex) shows the account alive again. Same-tx
// re-creation (metamorphic SD+CREATE2) writes both SelfDestructPath and
// AddressPath at the SAME TxIdx, so AddressPath uses >= (not strict >).
// LatestTxIndex counts Estimate cells: an in-flight post-destruct write is
// a possible revival, and treating it as one keeps the dead-account
// relaxations off until it resolves (fail-safe: the reader re-executes),
// mirroring accountLiveSince's estimate handling.
func (vm *VersionMap) selfDestructRevived(addr accounts.Address, destructTxIndex int, txIndex int) bool {
	revivalLimit := txIndex - 1
	if hi, ok := vm.LatestTxIndex(addr, AddressPath, accounts.NilKey, revivalLimit); ok && hi >= destructTxIndex {
		return true
	}
	for _, p := range [...]AccountPath{BalancePath, NoncePath, CodeHashPath} {
		if hi, ok := vm.LatestTxIndex(addr, p, accounts.NilKey, revivalLimit); ok && hi > destructTxIndex {
			return true
		}
	}
	return false
}

// destroyedAndUnrevived reports whether the latest destruct (highest Done
// SelfDestruct=true below txIndex, immune to a shadowing revival cell above
// it) has no later cell showing life again. Destroyed does not imply dead:
// beyond the strictly-later revival cells, a self-destruct that preserves a
// non-zero balance (EIP-8246) writes it AT the destruct index, so deadness
// additionally requires no live sub-field floor from that index on.
func (vm *VersionMap) destroyedAndUnrevived(addr accounts.Address, txIndex int) bool {
	sdVer, ok := vm.FindDoneSelfDestructInRange(addr, 0, txIndex, true)
	if !ok {
		return false
	}
	if vm.selfDestructRevived(addr, sdVer.TxIndex, txIndex) {
		return false
	}
	return !vm.accountLiveSince(addr, sdVer.TxIndex, txIndex)
}

// accountLiveSince reports whether any sub-field cell written at or after
// fromIdx (and below txIdx) shows the account EIP-161-non-empty — e.g. a
// self-destruct that preserves a non-zero balance writes it at the destruct
// index itself. Cells older than fromIdx are pre-destruct state and say
// nothing about life afterwards. Estimate cells cannot prove death and count
// as live (fail-safe: the reader re-executes).
func (vm *VersionMap) accountLiveSince(addr accounts.Address, fromIdx int, txIdx int) bool {
	if bal, rr, ok := vm.ReadBalance(addr, txIdx); ok && (rr.Status() != MVReadResultDone || (rr.DepIdx() >= fromIdx && !bal.IsZero())) {
		return true
	}
	if nonce, rr, ok := vm.ReadNonce(addr, txIdx); ok && (rr.Status() != MVReadResultDone || (rr.DepIdx() >= fromIdx && nonce != 0)) {
		return true
	}
	if ch, rr, ok := vm.ReadCodeHash(addr, txIdx); ok && (rr.Status() != MVReadResultDone || (rr.DepIdx() >= fromIdx && !(ch.IsEmpty() || ch.IsZero()))) {
		return true
	}
	if c, rr, ok := vm.ReadCode(addr, txIdx); ok && (rr.Status() != MVReadResultDone || (rr.DepIdx() >= fromIdx && len(c.Bytes) > 0)) {
		return true
	}
	return false
}

// FindDoneSelfDestructInRange returns the version of the highest Done
// SelfDestruct write with lo <= TxIdx < hi whose value == target, if any.
// Read-side mirror of AnyDoneSelfDestructEquals: it finds an in-block
// SELFDESTRUCT even when a later revival (SelfDestruct=false) hides it from
// latest-only ReadSelfDestruct.
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
	if e.SelfDestruct == nil {
		return Version{}, false
	}
	var ver Version
	found := false
	e.SelfDestruct.Descend(hi-1, func(k int, v *WriteCell[bool]) bool {
		if k < lo {
			return false
		}
		if v.flag == FlagDone && v.Value == target {
			ver = Version{TxIndex: k, Incarnation: v.incarnation}
			found = true
			return false
		}
		return true
	})
	return ver, found
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
		e := vm.entryOrCreate(addr)
		e.mu.Lock()
		if vw, ok := writes.address[addr]; ok {
			e.Address = putCell(e.Address, addr, AddressPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellAccount)
		}
		if vw, ok := writes.selfDestruct[addr]; ok {
			e.SelfDestruct = putCell(e.SelfDestruct, addr, SelfDestructPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellSelfDestruct)
		}
		if vw, ok := writes.balance[addr]; ok {
			e.Balance = putCell(e.Balance, addr, BalancePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellBalance)
		}
		if vw, ok := writes.nonce[addr]; ok {
			e.Nonce = putCell(e.Nonce, addr, NoncePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellNonce)
		}
		if vw, ok := writes.incarnation[addr]; ok {
			e.Incarnation = putCell(e.Incarnation, addr, IncarnationPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellIncarnation)
		}
		if vw, ok := writes.code[addr]; ok {
			e.Code = putCell(e.Code, addr, CodePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCode)
		}
		if vw, ok := writes.codeHash[addr]; ok {
			e.CodeHash = putCell(e.CodeHash, addr, CodeHashPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCodeHash)
		}
		if vw, ok := writes.codeSize[addr]; ok {
			e.CodeSize = putCell(e.CodeSize, addr, CodeSizePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCodeSize)
		}
		if vw, ok := writes.createContract[addr]; ok {
			e.CreateContract = putCell(e.CreateContract, addr, CreateContractPath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellCreateContract)
		}
		if inner, ok := writes.storage[addr]; ok {
			if e.Storage == nil {
				e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell[uint256.Int]]{}
			}
			for key, vw := range inner {
				e.Storage[key] = putCell(e.Storage[key], addr, StoragePath, vw.Version.TxIndex, vw.Version.Incarnation, flag, vw.Val, getCellStorage)
			}
		}
		e.mu.Unlock()
	})
}

func (vm *VersionMap) MarkEstimate(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) {
	e := vm.load(addr)
	if e == nil {
		panic(fmt.Errorf("markFlag: no entry for addr %x, path %s, txIdx %d", addr, path, txIdx))
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	markFlag(e, addr, path, key, txIdx, FlagEstimate)
}

// markFlag updates the flag on an existing (addr, path, key, txIdx) cell.
// Caller must hold e.mu.Lock(). Panics if no cell is present at txIdx —
// MarkEstimate requires a prior write.
func markFlag(e *AddressEntry, addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, flag statusFlag) {
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
	readLive func(*VersionMap, accounts.Address, accounts.StorageKey, int) (T, ReadResult, bool, sdProbe),
	eq func(a, b T) bool,
	isAbsent func(T) bool,
	recordField func(*accounts.Account) T,
	checkVersion func(readVersion, writeVersion Version) VersionValidity,
	traceInvalid bool, tracePrefix string) VersionValidity {
	// One typed read supplies BOTH the status (for the version check) and the
	// live value (for the rare tiebreaker) — no second lookup, no boxing. The
	// tiebreaker branch in validateReadImpl only fires when rr is Done, so eq
	// compares against the value that came with rr.
	live, rr, ok, sd := readLive(vm, addr, key, txIndex)
	matchesLive := func() bool { return ok && eq(readVal, live) }
	// A recorded zero/absent value means the read concluded absence; the
	// destroyed-account relaxations are only sound for those. Typed check —
	// the eq helpers carry dead-equivalence semantics, not zero-ness.
	absent := isAbsent(readVal)
	// A read folded onto the account record tiebreaks against the live record's
	// field: record churn that keeps the field unchanged is not a conflict. A
	// non-Done or absent record cannot prove equality (fail-safe: re-execute).
	var matchesRecord func() bool
	if recordField != nil {
		matchesRecord = func() bool {
			acc, arr, aok := vm.ReadAddress(addr, txIndex)
			if !aok || arr.Status() != MVReadResultDone || acc == nil {
				return false
			}
			return eq(readVal, recordField(acc))
		}
	}
	valid := vm.validateReadImpl(txIndex, addr, path, key, source, version, rr, sd, matchesLive, matchesRecord, absent, checkVersion, traceInvalid, tracePrefix, false)
	if dbg.TraceReexec && valid == VersionInvalid {
		fmt.Printf(
			"VINV tx=%d %x %s src=%s rv=(%d.%d) cell=(%d.%d,st=%d) readVal=%v live=%v liveOK=%v\n",
			txIndex,
			addr,
			AccountKey{path, key},
			source,
			version.TxIndex,
			version.Incarnation,
			rr.depIdx,
			rr.incarnation,
			rr.Status(),
			readVal,
			live,
			ok,
		)
	}
	return valid
}

// Typed live-value readers (uniform signature so validateRead can thread them
// generically) and equality helpers for the value tiebreaker. Paths whose
// validation consults the self-destruct net batch that scan into the same
// entry lock; the others leave the probe unresolved.
func liveBalance(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (uint256.Int, ReadResult, bool, sdProbe) {
	return readFloorWithSelfDestruct(vm, a, tx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint256.Int]] { return e.Balance })
}
func liveNonce(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (uint64, ReadResult, bool, sdProbe) {
	return readFloorWithSelfDestruct(vm, a, tx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint64]] { return e.Nonce })
}
func liveIncarnation(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (uint64, ReadResult, bool, sdProbe) {
	v, res, ok := vm.ReadIncarnation(a, tx)
	return v, res, ok, sdProbe{hiDoneTrue: UnknownDep}
}
func liveCodeHash(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (accounts.CodeHash, ReadResult, bool, sdProbe) {
	return readFloorWithSelfDestruct(vm, a, tx, func(e *AddressEntry) *btree.Map[int, *WriteCell[accounts.CodeHash]] { return e.CodeHash })
}
func liveAddress(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (*accounts.Account, ReadResult, bool, sdProbe) {
	v, res, ok := vm.ReadAddress(a, tx)
	return v, res, ok, sdProbe{hiDoneTrue: UnknownDep}
}
func liveStorage(vm *VersionMap, a accounts.Address, k accounts.StorageKey, tx int) (uint256.Int, ReadResult, bool, sdProbe) {
	return readFloorWithSelfDestruct(vm, a, tx, func(e *AddressEntry) *btree.Map[int, *WriteCell[uint256.Int]] {
		if e.Storage == nil {
			return nil
		}
		return e.Storage[k]
	})
}
func liveCode(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) ([]byte, ReadResult, bool, sdProbe) {
	c, res, ok, sd := readFloorWithSelfDestruct(vm, a, tx, func(e *AddressEntry) *btree.Map[int, *WriteCell[accounts.Code]] { return e.Code })
	return c.Bytes, res, ok, sd
}
func liveCodeSize(vm *VersionMap, a accounts.Address, _ accounts.StorageKey, tx int) (int, ReadResult, bool, sdProbe) {
	return readFloorWithSelfDestruct(vm, a, tx, func(e *AddressEntry) *btree.Map[int, *WriteCell[int]] { return e.CodeSize })
}

func eqUint256(a, b uint256.Int) bool { return a.Eq(&b) }

// Typed absence predicates (threaded like eq, so validateRead never boxes the
// recorded value): a zero/absent value means the read concluded absence.
func absentAccount(a *accounts.Account) bool   { return a == nil }
func absentBytes(b []byte) bool                { return len(b) == 0 }
func absentUint256(v uint256.Int) bool         { return v.IsZero() }
func absentUint64(v uint64) bool               { return v == 0 }
func absentInt(v int) bool                     { return v == 0 }
func absentCodeHash(ch accounts.CodeHash) bool { return ch.IsEmpty() || ch.IsZero() }
func eqUint64(a, b uint64) bool                { return a == b }
func eqInt(a, b int) bool                      { return a == b }
func eqCode(a, b []byte) bool                  { return bytes.Equal(a, b) }
func eqCodeHash(a, b accounts.CodeHash) bool {
	return a == b
}

// Record-field extractors for the fold tiebreaker: a sub-field read with no
// dedicated cell validates against the account record, by field value.
func recordBalance(a *accounts.Account) uint256.Int        { return a.Balance }
func recordNonce(a *accounts.Account) uint64               { return a.Nonce }
func recordIncarnation(a *accounts.Account) uint64         { return a.Incarnation }
func recordCodeHash(a *accounts.Account) accounts.CodeHash { return a.CodeHash }

// The AddressPath record tiebreakers are existence-only: the record's version
// churns as workers re-stamp it, but each sub-field (balance/nonce/codeHash)
// is recorded and validated as its own read. Under EIP-161 a nil read is
// additionally equivalent to a dead account — EVM-indistinguishable from a
// non-existent one. Before EIP-161 that does not hold (existing-empty accounts
// persist and CALL charges new-account gas on non-existence only), so the
// strict form applies there.
//
// The record cell is a creation-time snapshot: an account created empty and
// funded afterwards keeps an empty-shaped record next to a non-zero sub-field
// cell, so deadness must be assembled from the sub-field floors too.
func (vm *VersionMap) eqAccountDead(txIdx int, addr accounts.Address, isAura bool, a *accounts.Account, b *accounts.Account) bool {
	if EIP161EmptyRemoval(true, isAura, addr) && a.Empty() && b.Empty() && !vm.accountLiveAt(addr, txIdx) {
		return true
	}
	return a != nil && b != nil
}

// accountLiveAt reports whether any sub-field cell below txIdx makes the
// account EIP-161-non-empty. Estimate cells cannot prove death and count as
// live (fail-safe: the reader re-executes).
func (vm *VersionMap) accountLiveAt(addr accounts.Address, txIdx int) bool {
	if bal, rr, ok := vm.ReadBalance(addr, txIdx); ok && (rr.Status() != MVReadResultDone || !bal.IsZero()) {
		return true
	}
	if nonce, rr, ok := vm.ReadNonce(addr, txIdx); ok && (rr.Status() != MVReadResultDone || nonce != 0) {
		return true
	}
	if ch, rr, ok := vm.ReadCodeHash(addr, txIdx); ok && (rr.Status() != MVReadResultDone || !(ch.IsEmpty() || ch.IsZero())) {
		return true
	}
	if c, rr, ok := vm.ReadCode(addr, txIdx); ok && (rr.Status() != MVReadResultDone || len(c.Bytes) > 0) {
		return true
	}
	return false
}

func eqAccountStrict(a, b *accounts.Account) bool {
	return a != nil && b != nil
}

// validateReadImpl is validateRead with a recursive flag: the cross-validate
// probes (AddressPath / SelfDestructPath / IncarnationPath) pass recursive=true
// so they can be distinguished from a top-level read — a synthetic probe carries
// no recorded value of its own and must not invalidate on a bare Done entry.
func (vm *VersionMap) validateReadImpl(txIndex int, addr accounts.Address, path AccountPath, key accounts.StorageKey, source ReadSource, version Version,
	rr ReadResult,
	sd sdProbe,
	matchesLive func() bool,
	matchesRecord func() bool,
	absent bool,
	checkVersion func(readVersion, writeVersion Version) VersionValidity,
	traceInvalid bool, tracePrefix string, recursive bool) VersionValidity {

	valid := VersionValid
	invReason := ""
	switch rr.Status() {
	case MVReadResultDone:
		if source != MapRead {
			switch {
			case recursive && matchesLive == nil:
				// Synthetic cross-validate probe (no recorded value of its
				// own) — the outer entry's validation covers it. Without this
				// guard a recursive AddressPath/SelfDestructPath probe that
				// lands on a Done cell would over-invalidate.
			case matchesLive != nil && matchesLive():
				// Value tiebreaker: a Done entry now exists where the read
				// saw storage, but it holds the same value (e.g. a no-op write
				// of a BAL-pre-populated path) — read stays valid. Evaluated
				// typed by the caller; no boxing.
			default:
				valid = VersionInvalid
				invReason = "done-notmap"
			}
		} else {
			valid = checkVersion(version, rr.Version())
			if valid == VersionInvalid && matchesLive != nil && matchesLive() {
				// Value tiebreaker: the writer version churned (a lower tx
				// re-executed) but the read's value is unchanged — not a real
				// conflict, so the read stays valid and does not re-execute.
				valid = VersionValid
			}
			if valid == VersionInvalid {
				invReason = "done-vercheck"
			}
		}
		// A later destruct makes a read predating it stale; checkVersion alone
		// misses it because the SD doesn't write the read's own path. AddressPath
		// is existence-only, so it stays valid unless the account is dead
		// (destroyed, unrevived, no live floor).
		if valid == VersionValid && path == AddressPath {
			if _, ok := vm.FindDoneSelfDestructInRange(addr, rr.Version().TxIndex+1, txIndex, true); ok &&
				vm.destroyedAndUnrevived(addr, txIndex) {
				valid = VersionInvalid
				invReason = "sd-stale"
			}
		}
		if valid == VersionValid && !absent && path != SelfDestructPath && path != AddressPath &&
			path != IncarnationPath && path != CreateContractPath {
			// Range-scan mirroring the read path's per-path destruct resolution
			// (a re-creation flushes SelfDestruct=false above the wiping true
			// cell, so latest-only probing misses it). Only non-absent reads
			// consult the net: a destruct makes absence the truth, and a later
			// re-establishment writes a cell that becomes the floor, so a stale
			// absent read version-mismatches on its own. No revival relaxation,
			// for the same reason. Deploy-derived paths scan inclusive of the
			// floor index (a same-tx write+destruct wipes the cell itself);
			// Balance/CodeHash stay strictly-above — the destroyer's own cells
			// there (EIP-8246 preserved balance, reset code hash) are
			// post-destruct truth.
			lo := rr.Version().TxIndex + 1
			if path == StoragePath || path == CodePath || path == CodeSizePath || path == NoncePath {
				lo = rr.Version().TxIndex
			}
			if sd.destructedSince(vm, addr, lo, txIndex) {
				valid = VersionInvalid
				invReason = "sd-stale"
			}
		}
	case MVReadResultDependency:
		valid = VersionInvalid
		invReason = "dependency"
	case MVReadResultNone:
		// A wiped-by-destruct record: an absent value stamped with the version
		// of a Done SelfDestruct=true cell, with still no cell for the path
		// itself. Valid as recorded — a later re-establishment writes a cell
		// (version mismatch against this record), and the destruct going away
		// fails the recorded SelfDestruct witness.
		if source == MapRead && absent && version.TxIndex >= 0 {
			if _, ok := vm.FindDoneSelfDestructInRange(addr, version.TxIndex, version.TxIndex+1, true); ok {
				break
			}
		}
		switch {
		case source == MapRead && !recursive &&
			(path == BalancePath || path == NoncePath || path == IncarnationPath || path == CodeHashPath):
			// A sub-field read with no dedicated cell is recorded folded onto
			// AddressPath (its source/version), so validate it against AddressPath
			// at that version — with the record-field value as the tiebreaker.
			valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
				version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), unresolvedSD, matchesRecord, nil, absent, checkVersion, traceInvalid, tracePrefix, true)
			if valid == VersionInvalid {
				invReason = "fold-addr"
			}
		case source != StorageRead && source != ProvisionalRead:
			valid = VersionInvalid
			invReason = "none-notstorage"
		default:
			if valid = checkVersion(version, version); valid == VersionValid {
				// Cross-validate any account property read against AddressPath
				// and SelfDestructPath.  A prior tx may have created or
				// self-destructed the account, invalidating storage reads of
				// any property (code, storage slots, balance, nonce, etc.).
				if path != AddressPath && path != SelfDestructPath {
					if valid = vm.validateReadImpl(txIndex, addr, AddressPath, accounts.StorageKey{}, source,
						version, vm.ReadStatus(addr, AddressPath, accounts.StorageKey{}, txIndex), unresolvedSD, nil, nil, absent, checkVersion, traceInvalid, tracePrefix, true); valid == VersionValid {
						valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), unresolvedSD, nil, nil, absent, checkVersion, traceInvalid, tracePrefix, true)
						if valid == VersionInvalid {
							invReason = "xval-sd"
						}
					} else {
						invReason = "xval-addr"
						vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
							version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), unresolvedSD, nil, nil, absent, checkVersion, traceInvalid, tracePrefix, true)
					}
				} else if path == AddressPath {
					valid = vm.validateReadImpl(txIndex, addr, SelfDestructPath, accounts.StorageKey{}, source,
						version, vm.ReadStatus(addr, SelfDestructPath, accounts.StorageKey{}, txIndex), unresolvedSD, nil, nil, absent, checkVersion, traceInvalid, tracePrefix, true)
					if valid == VersionInvalid {
						invReason = "addr-xval-sd"
					}

					// A prior tx creating, destroying or re-creating this account
					// can make an AddressPath storage read stale; IncarnationPath
					// is the specific signal (written only by CreateAccount and
					// SelfDestruct), unlike BalancePath which overfires for every
					// gas payer. Non-recursive means the record IS the AddressPath
					// read and absent is recorded non-existence: it must match
					// cell-evidenced deadness — a nil read is valid only for a
					// destroyed-and-unrevived account, an alive read only for a
					// live one (e.g. EIP-8246 preserved balance). Recursive means
					// a sub-field storage read cross-validating its account and
					// absent is field emptiness: an empty read stays correct (an
					// in-block non-empty write would have left a cell the floor
					// probe finds), while a non-empty committed value is stale
					// under any lifecycle churn, which clears fields.
					if valid == VersionValid {
						if _, incRR, ok := vm.ReadIncarnation(addr, txIndex); ok && incRR.Status() == MVReadResultDone {
							stale := !absent
							if !recursive {
								stale = absent != vm.destroyedAndUnrevived(addr, txIndex)
							}
							if stale {
								valid = VersionInvalid
								invReason = "addr-inc-created"
							}
						}
					}
				}
			} else if valid == VersionInvalid {
				invReason = "none-vercheck"
			}
		}
	default:
		panic(fmt.Errorf("undefined vm read status: %v", rr.Status()))
	}

	if dbg.TraceReexec && valid == VersionInvalid && invReason != "" {
		fmt.Printf(
			"VINV-R tx=%d %x %s src=%s reason=%s recursive=%v rv=(%d.%d) cell=(%d.%d,st=%d)\n",
			txIndex,
			addr,
			AccountKey{path, key},
			source,
			invReason,
			recursive,
			version.TxIndex,
			version.Incarnation,
			rr.depIdx,
			rr.incarnation,
			rr.Status(),
		)
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
func (vm *VersionMap) ValidateVersion(txIdx int, lastIO *VersionedIO, checkVersion func(readVersion, writeVersion Version) VersionValidity, eip161 bool, isAura bool, traceInvalid bool, tracePrefix string) (valid VersionValidity) {
	rs := lastIO.ReadSet(txIdx)
	valid = VersionValid
	// ok checks one validity result, latching valid; ok==false stops the scan.
	ok := func(v VersionValidity) bool { valid = v; return v == VersionValid }
	// noValueRead validates a path whose recorded value carries no tiebreaker
	// (self-destruct / create-contract / code / code-size): the version/status
	// check is authoritative. One ReadStatus, no value comparison.
	noValueRead := func(addr accounts.Address, path AccountPath, key accounts.StorageKey, hdr ReadHeader) VersionValidity {
		return vm.validateReadImpl(txIdx, addr, path, key, hdr.Source, hdr.Version,
			vm.ReadStatus(addr, path, key, txIdx), unresolvedSD, nil, nil, false, checkVersion, traceInvalid, tracePrefix, false)
	}

	// Value paths go through the generic validateRead so the recorded value stays
	// typed (never boxed) and the single typed read supplies both status and the
	// tiebreaker value.
	for a, tr := range rs.address {
		var rv *accounts.Account
		if tr.Val != nil {
			rv = tr.Val.Account()
		}
		eqAccount := eqAccountStrict
		if eip161 {
			eqAccount = func(x *accounts.Account, y *accounts.Account) bool { return vm.eqAccountDead(txIdx, a, isAura, x, y) }
		}
		if !ok(validateRead(vm, txIdx, a, AddressPath, accounts.NilKey, tr.Source, tr.Version, rv, liveAddress, eqAccount, absentAccount, nil, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.balance {
		if !ok(validateRead(vm, txIdx, a, BalancePath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveBalance, eqUint256, absentUint256, recordBalance, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.nonce {
		if !ok(validateRead(vm, txIdx, a, NoncePath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveNonce, eqUint64, absentUint64, recordNonce, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.incarnation {
		if !ok(validateRead(vm, txIdx, a, IncarnationPath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveIncarnation, eqUint64, absentUint64, recordIncarnation, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.codeHash {
		if !ok(validateRead(vm, txIdx, a, CodeHashPath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveCodeHash, eqCodeHash, absentCodeHash, recordCodeHash, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, inner := range rs.storage {
		for k, tr := range inner {
			if !ok(validateRead(vm, txIdx, a, StoragePath, k, tr.Source, tr.Version, tr.Val, liveStorage, eqUint256, absentUint256, nil, checkVersion, traceInvalid, tracePrefix)) {
				return
			}
		}
	}
	validateSelfDestruct := func(a accounts.Address, tr VersionedRead[bool]) bool {
		// A MapRead record names the concrete SelfDestruct cell it consumed —
		// e.g. the historical destruct behind a wiped read — and a later
		// revival cell must not shadow it: valid iff the cell at the recorded
		// version is still Done with the recorded value (an Estimate or a
		// changed value re-executes, fail-safe). Storage-versioned records
		// keep the floor check: any destruct appearing invalidates them.
		if tr.Source == MapRead && tr.Version.TxIndex >= 0 {
			if _, found := vm.FindDoneSelfDestructInRange(a, tr.Version.TxIndex, tr.Version.TxIndex+1, tr.Val); !found {
				if traceInvalid && dbg.TraceReexec {
					fmt.Printf("VINV-R tx=%d %x SelfDestruct src=%s reason=sd-witness rv=(%d.%d)\n",
						txIdx, a, tr.Source, tr.Version.TxIndex, tr.Version.Incarnation)
				}
				valid = VersionInvalid
				return false
			}
			return true
		}
		return ok(noValueRead(a, SelfDestructPath, accounts.NilKey, tr.ReadHeader))
	}
	// Every distinct destruct a tx consumed is re-checked: a conclusion can
	// rest on several destroy/recreate cycles of one address, and losing any
	// one of them to re-execution invalidates the tx.
	for a, tr := range rs.selfDestruct {
		if !validateSelfDestruct(a, tr) {
			return
		}
		for _, w := range rs.selfDestructWitnesses[a] {
			if !validateSelfDestruct(a, w) {
				return
			}
		}
	}
	for a, tr := range rs.createContract {
		if !ok(noValueRead(a, CreateContractPath, accounts.NilKey, tr.ReadHeader)) {
			return
		}
	}
	for a, tr := range rs.code {
		if !ok(validateRead(vm, txIdx, a, CodePath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveCode, eqCode, absentBytes, nil, checkVersion, traceInvalid, tracePrefix)) {
			return
		}
	}
	for a, tr := range rs.codeSize {
		if !ok(validateRead(vm, txIdx, a, CodeSizePath, accounts.NilKey, tr.Source, tr.Version, tr.Val, liveCodeSize, eqInt, absentInt, nil, checkVersion, traceInvalid, tracePrefix)) {
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
