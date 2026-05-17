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
// per AccountPath. Every field is an independently addressable unit — the
// struct exists only to eliminate the inner map[AccountKey] hash lookup
// (~27 % of pre-restructure warm-read CPU was aeshashbody on AccountKey).
//
// Invariant — per-field independence: no consumer treats AddressEntry as
// a transactional whole. Reads, writes, mark-estimate/complete, delete and
// validation all operate at (Path, Key) granularity. Helpers that look
// like address-level operations (DeleteAll, StorageKeys) are pure
// iterations of per-field operations.
type AddressEntry struct {
	Address        *btree.Map[int, *WriteCell]
	SelfDestruct   *btree.Map[int, *WriteCell]
	Balance        *btree.Map[int, *WriteCell]
	Nonce          *btree.Map[int, *WriteCell]
	Incarnation    *btree.Map[int, *WriteCell]
	Code           *btree.Map[int, *WriteCell]
	CodeHash       *btree.Map[int, *WriteCell]
	CodeSize       *btree.Map[int, *WriteCell]
	CreateContract *btree.Map[int, *WriteCell]
	Storage        map[accounts.StorageKey]*btree.Map[int, *WriteCell]
}

// cells returns the existing cell map for (path, key), or nil if no write
// has yet been recorded for that field on this address. A nil return is
// the hot-path early-exit for warm reads on addresses that have never
// been touched on the queried field (e.g. the SelfDestruct probe on the
// overwhelming majority of warm reads, since selfdestructs are rare).
func (e *AddressEntry) cells(path AccountPath, key accounts.StorageKey) *btree.Map[int, *WriteCell] {
	switch path {
	case AddressPath:
		return e.Address
	case SelfDestructPath:
		return e.SelfDestruct
	case BalancePath:
		return e.Balance
	case NoncePath:
		return e.Nonce
	case IncarnationPath:
		return e.Incarnation
	case CodePath:
		return e.Code
	case CodeHashPath:
		return e.CodeHash
	case CodeSizePath:
		return e.CodeSize
	case CreateContractPath:
		return e.CreateContract
	case StoragePath:
		return e.Storage[key]
	}
	return nil
}

// cellsOrCreate returns the cell map for (path, key), creating an empty
// btree (and the Storage sub-map for StoragePath) if no write has yet
// been recorded for that field on this address.
func (e *AddressEntry) cellsOrCreate(path AccountPath, key accounts.StorageKey) *btree.Map[int, *WriteCell] {
	switch path {
	case AddressPath:
		if e.Address == nil {
			e.Address = &btree.Map[int, *WriteCell]{}
		}
		return e.Address
	case SelfDestructPath:
		if e.SelfDestruct == nil {
			e.SelfDestruct = &btree.Map[int, *WriteCell]{}
		}
		return e.SelfDestruct
	case BalancePath:
		if e.Balance == nil {
			e.Balance = &btree.Map[int, *WriteCell]{}
		}
		return e.Balance
	case NoncePath:
		if e.Nonce == nil {
			e.Nonce = &btree.Map[int, *WriteCell]{}
		}
		return e.Nonce
	case IncarnationPath:
		if e.Incarnation == nil {
			e.Incarnation = &btree.Map[int, *WriteCell]{}
		}
		return e.Incarnation
	case CodePath:
		if e.Code == nil {
			e.Code = &btree.Map[int, *WriteCell]{}
		}
		return e.Code
	case CodeHashPath:
		if e.CodeHash == nil {
			e.CodeHash = &btree.Map[int, *WriteCell]{}
		}
		return e.CodeHash
	case CodeSizePath:
		if e.CodeSize == nil {
			e.CodeSize = &btree.Map[int, *WriteCell]{}
		}
		return e.CodeSize
	case CreateContractPath:
		if e.CreateContract == nil {
			e.CreateContract = &btree.Map[int, *WriteCell]{}
		}
		return e.CreateContract
	case StoragePath:
		if e.Storage == nil {
			e.Storage = map[accounts.StorageKey]*btree.Map[int, *WriteCell]{}
		}
		cells, ok := e.Storage[key]
		if !ok {
			cells = &btree.Map[int, *WriteCell]{}
			e.Storage[key] = cells
		}
		return cells
	}
	return nil
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

func (vm *VersionMap) getKeyCells(addr accounts.Address, path AccountPath, key accounts.StorageKey, fNoKey func(addr accounts.Address, path AccountPath, key accounts.StorageKey) *btree.Map[int, *WriteCell]) (cells *btree.Map[int, *WriteCell]) {
	if e, ok := vm.s[addr]; ok {
		cells = e.cells(path, key)
	}

	if cells == nil && fNoKey != nil {
		cells = fNoKey(addr, path, key)
	}

	return
}

func (vm *VersionMap) WriteChanges(changes []*types.AccountChanges) {
	for _, accountChanges := range changes {
		for _, storageChanges := range accountChanges.StorageChanges {
			for _, change := range storageChanges.Changes {
				value := change.Value
				vm.Write(accountChanges.Address, StoragePath, storageChanges.Slot, Version{TxIndex: int(change.Index) - 1}, value, true)
			}
		}
		for _, balanceChange := range accountChanges.BalanceChanges {
			vm.Write(accountChanges.Address, BalancePath, accounts.NilKey, Version{TxIndex: int(balanceChange.Index) - 1}, balanceChange.Value, true)
		}
		for _, nonceChange := range accountChanges.NonceChanges {
			vm.Write(accountChanges.Address, NoncePath, accounts.NilKey, Version{TxIndex: int(nonceChange.Index) - 1}, nonceChange.Value, true)
		}
		for _, codeChange := range accountChanges.CodeChanges {
			vm.Write(accountChanges.Address, CodePath, accounts.NilKey, Version{TxIndex: int(codeChange.Index) - 1}, codeChange.Bytecode, true)
		}
	}

}

func (vm *VersionMap) Write(addr accounts.Address, path AccountPath, key accounts.StorageKey, v Version, data any, complete bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.writeLocked(addr, path, key, v, data, complete)
}

// writeLocked performs the write without acquiring the lock.
// Caller must hold vm.mu.Lock().
func (vm *VersionMap) writeLocked(addr accounts.Address, path AccountPath, key accounts.StorageKey, v Version, data any, complete bool) {
	e, ok := vm.s[addr]
	if !ok {
		e = &AddressEntry{}
		vm.s[addr] = e
	}
	cells := e.cellsOrCreate(path, key)

	ci, ok := cells.Get(v.TxIndex)

	var flag statusFlag = FlagDone

	if !complete {
		flag = FlagEstimate
	}

	if ok {
		if ci.incarnation > v.Incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %x %s, %v", addr, AccountKey{path, key}, v.TxIndex))
		}

		ci.flag = flag
		ci.incarnation = v.Incarnation
		ci.data = data
	} else {
		if ci, ok = cells.Get(v.TxIndex); !ok {
			cells.Set(v.TxIndex, &WriteCell{
				flag:        flag,
				incarnation: v.Incarnation,
				data:        data,
			})
		} else {
			ci.flag = flag
			ci.incarnation = v.Incarnation
			ci.data = data
		}
	}
}

func (vm *VersionMap) Read(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) (res ReadResult) {
	res.depIdx = UnknownDep
	res.incarnation = -1

	if vm == nil {
		return res
	}

	vm.mu.RLock()
	defer vm.mu.RUnlock()

	cells := vm.getKeyCells(addr, path, key, nil)

	if cells == nil {
		return
	}

	var floor = func(i int) (key int, val *WriteCell) {
		key = UnknownDep
		cells.Descend(i, func(k int, v *WriteCell) bool {
			key = k
			val = v
			return false
		})
		return key, val
	}

	fk, fv := floor(txIdx - 1)

	if fk != UnknownDep && fv != nil {
		switch fv.flag {
		case FlagEstimate:
			res.depIdx = fk
			res.value = fv.data
		case FlagDone:
			{
				res.depIdx = fk
				res.incarnation = fv.incarnation
				res.value = fv.data
			}
		default:
			panic(errors.New("should not happen - unknown flag value"))
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

	cells := vm.getKeyCells(addr, path, key, nil)
	if cells == nil {
		return 0, false
	}
	highest := UnknownDep
	cells.Descend(txIdxLimit, func(k int, _ *WriteCell) bool {
		highest = k
		return false
	})
	if highest == UnknownDep {
		return 0, false
	}
	return highest, true
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

	cells := vm.getKeyCells(addr, path, key, func(_ accounts.Address, _ AccountPath, _ accounts.StorageKey) *btree.Map[int, *WriteCell] {
		panic(errors.New("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x %s, cells keys: %v", txIdx, addr, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagEstimate
	}
}

func (vm *VersionMap) MarkComplete(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	cells := vm.getKeyCells(addr, path, key, func(_ accounts.Address, _ AccountPath, _ accounts.StorageKey) *btree.Map[int, *WriteCell] {
		panic(errors.New("path must already exist"))
	})

	if ci, ok := cells.Get(txIdx); !ok {
		panic(fmt.Sprintf("should not happen - cell should be present for path. TxIndex: %v, path, %x s, cells keys: %v", txIdx, AccountKey{path, key}, cells.Keys()))
	} else {
		ci.flag = FlagDone
	}
}

func (vm *VersionMap) Delete(addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int, checkExists bool) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	cells := vm.getKeyCells(addr, path, key, nil)

	if cells == nil {
		if !checkExists {
			return
		}

		panic(errors.New("path must already exist"))
	}

	cells.Delete(txIdx)
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
						balRR := vm.Read(addr, BalancePath, accounts.NilKey, txIndex)
						if balRR.Status() == MVReadResultDone {
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

type WriteCell struct {
	flag        statusFlag
	incarnation int
	data        any
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
