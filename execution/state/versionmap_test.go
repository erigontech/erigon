package state

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var randomness = rand.Intn(10) + 10

// valueFor returns a typed test value matching the AccountPath's value-type
// contract enforced by the typed AddressEntry. Tests pass the path so the
// VersionMap stores type-correct values without runtime conversion errors.
// Most tests pass AddressPath since the original byte-based helper was used
// almost exclusively for AddressPath; per-path-specific tests pass their path.
func valueFor(path AccountPath, txIdx, inc int) any {
	seed := uint64(txIdx*100 + inc)
	switch path {
	case BalancePath, StoragePath:
		return *uint256.NewInt(seed)
	case NoncePath, IncarnationPath:
		return seed
	case CodeSizePath:
		return int(seed)
	case SelfDestructPath, CreateContractPath:
		return (txIdx+inc)%2 == 1
	case CodePath:
		return accounts.NewCode(fmt.Appendf(nil, "%ver:%ver:%ver", txIdx*5, txIdx+inc, inc*5))
	case CodeHashPath:
		var h common.Hash
		h[0] = byte(txIdx)
		h[1] = byte(inc)
		return accounts.InternCodeHash(h)
	case AddressPath:
		a := &accounts.Account{}
		a.Balance.SetUint64(seed)
		return a
	}
	return nil
}

func getAddress(i int) accounts.Address {
	addr := common.BigToAddress(big.NewInt(int64(i % randomness)))
	return accounts.InternAddress(addr)
}

// writeFor dispatches a typed Write by path so the path-parameterized tests
// keep driving the VersionMap through one call shape after the generic
// Write(data any) primitive was removed.
func writeFor(vm *VersionMap, addr accounts.Address, path AccountPath, key accounts.StorageKey, v Version, value any, complete bool) {
	switch path {
	case AddressPath:
		vm.WriteAddress(addr, v, value.(*accounts.Account), complete)
	case SelfDestructPath:
		vm.WriteSelfDestruct(addr, v, value.(bool), complete)
	case BalancePath:
		vm.WriteBalance(addr, v, value.(uint256.Int), complete)
	case NoncePath:
		vm.WriteNonce(addr, v, value.(uint64), complete)
	case IncarnationPath:
		vm.WriteIncarnation(addr, v, value.(uint64), complete)
	case CodePath:
		vm.WriteCode(addr, v, value.(accounts.Code), complete)
	case CodeHashPath:
		vm.WriteCodeHash(addr, v, value.(accounts.CodeHash), complete)
	case CodeSizePath:
		vm.WriteCodeSize(addr, v, value.(int), complete)
	case CreateContractPath:
		vm.WriteCreateContract(addr, v, value.(bool), complete)
	case StoragePath:
		vm.WriteStorage(addr, key, v, value.(uint256.Int), complete)
	default:
		panic(fmt.Sprintf("writeFor: unhandled path %s", path))
	}
}

// readFor dispatches a typed Read by path, returning the typed value as any
// alongside the ReadResult metadata.
func readFor(vm *VersionMap, addr accounts.Address, path AccountPath, key accounts.StorageKey, txIdx int) (any, ReadResult, bool) {
	switch path {
	case AddressPath:
		return vm.ReadAddress(addr, txIdx)
	case SelfDestructPath:
		return vm.ReadSelfDestruct(addr, txIdx)
	case BalancePath:
		return vm.ReadBalance(addr, txIdx)
	case NoncePath:
		return vm.ReadNonce(addr, txIdx)
	case IncarnationPath:
		return vm.ReadIncarnation(addr, txIdx)
	case CodePath:
		return vm.ReadCode(addr, txIdx)
	case CodeHashPath:
		return vm.ReadCodeHash(addr, txIdx)
	case CodeSizePath:
		return vm.ReadCodeSize(addr, txIdx)
	case CreateContractPath:
		return vm.ReadCreateContract(addr, txIdx)
	case StoragePath:
		return vm.ReadStorage(addr, key, txIdx)
	default:
		panic(fmt.Sprintf("readFor: unhandled path %s", path))
	}
}

func TestHelperFunctions(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)
	ap2 := getAddress(2)

	mvh := NewVersionMap(nil)

	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 1}, valueFor(AddressPath, 0, 1), true)
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 2}, valueFor(AddressPath, 0, 2), true)
	_, res, _ := readFor(mvh, ap1, AddressPath, accounts.NilKey, 0)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	writeFor(mvh, ap2, AddressPath, accounts.NilKey, Version{0, 0, 1, 1}, valueFor(AddressPath, 1, 1), true)
	writeFor(mvh, ap2, AddressPath, accounts.NilKey, Version{0, 0, 1, 2}, valueFor(AddressPath, 1, 2), true)
	_, res, _ = readFor(mvh, ap2, AddressPath, accounts.NilKey, 1)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 2, 1}, valueFor(AddressPath, 2, 1), true)
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 2, 2}, valueFor(AddressPath, 2, 2), true)
	resVal, res, _ := readFor(mvh, ap1, AddressPath, accounts.NilKey, 2)
	require.Equal(t, 0, res.DepIdx())
	require.Equal(t, 2, res.Incarnation())
	require.Equal(t, valueFor(AddressPath, 0, 2), resVal)
	require.Equal(t, 0, res.Status())
}

func TestFlushMVWrite(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)
	ap2 := getAddress(2)

	mvh := NewVersionMap(nil)

	var res ReadResult
	var resVal any

	// A WriteSet holds at most one AddressPath cell per address, so each
	// versioned write is flushed in its own WriteSet to reproduce the
	// accumulation TestMVWriteRead drives through sequential Write calls.
	flushAddress := func(addr accounts.Address, ver Version) {
		ws := &WriteSet{}
		ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{
			WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: ver},
			Val:         valueFor(AddressPath, ver.TxIndex, ver.Incarnation).(*accounts.Account),
		})
		mvh.FlushVersionedWrites(ws, true, "")
	}

	flushAddress(ap1, Version{0, 0, 0, 1})
	flushAddress(ap1, Version{0, 0, 0, 2})
	flushAddress(ap2, Version{0, 0, 1, 1})
	flushAddress(ap2, Version{0, 0, 1, 2})
	flushAddress(ap1, Version{0, 0, 2, 1})
	flushAddress(ap1, Version{0, 0, 2, 2})

	_, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 0)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	_, res, _ = readFor(mvh, ap2, AddressPath, accounts.NilKey, 1)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	resVal, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 2)
	require.Equal(t, 0, res.DepIdx())
	require.Equal(t, 2, res.Incarnation())
	require.Equal(t, valueFor(AddressPath, 0, 2), resVal)
	require.Equal(t, 0, res.Status())
}

// TODO - handle panic

func TestLowerIncarnation(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)

	mvh := NewVersionMap(nil)

	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 2}, valueFor(AddressPath, 0, 2), true)
	readFor(mvh, ap1, AddressPath, accounts.NilKey, 0)
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 2}, valueFor(AddressPath, 1, 2), true)
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 5}, valueFor(AddressPath, 0, 5), true)
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 5}, valueFor(AddressPath, 1, 5), true)
}

func TestMarkEstimate(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)

	mvh := NewVersionMap(nil)

	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 7, 2}, valueFor(AddressPath, 7, 2), true)
	mvh.MarkEstimate(ap1, AddressPath, accounts.NilKey, 7)
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 7, 4}, valueFor(AddressPath, 7, 4), true)
}

func TestMVHashMapBasics(t *testing.T) {
	t.Parallel()

	// memory locations
	ap1 := getAddress(1)
	ap2 := getAddress(2)
	ap3 := getAddress(3)

	mvh := NewVersionMap(nil)

	_, res, _ := readFor(mvh, ap1, AddressPath, accounts.NilKey, 5)
	require.Equal(t, UnknownDep, res.depIdx)

	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 10, 1}, valueFor(AddressPath, 10, 1), true)

	_, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 9)
	require.Equal(t, UnknownDep, res.depIdx, "reads that should go the the DB return dependency -2")
	_, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 10)
	require.Equal(t, UnknownDep, res.depIdx, "Read returns entries from smaller txns, not txn 10")

	// Reads for a higher txn return the entry written by txn 10.
	resVal, res, _ := readFor(mvh, ap1, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 10, res.depIdx, "reads for a higher txn return the entry written by txn 10.")
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 10, 1), resVal)

	// More writes.
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 12, 0}, valueFor(AddressPath, 12, 0), true)
	writeFor(mvh, ap1, AddressPath, accounts.NilKey, Version{0, 0, 8, 3}, valueFor(AddressPath, 8, 3), true)

	// Verify reads.
	resVal, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 12, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 12, 0), resVal)

	resVal, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 10, 1), resVal)

	resVal, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 10)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 8, 3), resVal)

	// Mark the entry written by 10 as an estimate.
	mvh.MarkEstimate(ap1, AddressPath, accounts.NilKey, 10)

	_, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, -1, res.incarnation, "dep at tx 10 is now an estimate")

	// Delete the entry written by 10, write to a different ap.
	mvh.Delete(ap1, AddressPath, accounts.NilKey, 10, true)
	writeFor(mvh, ap2, AddressPath, accounts.NilKey, Version{0, 0, 10, 2}, valueFor(AddressPath, 10, 2), true)

	// Read by txn 11 no longer observes entry from txn 10.
	resVal, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 8, 3), resVal)

	// Reads, writes for ap2 and ap3.
	writeFor(mvh, ap2, AddressPath, accounts.NilKey, Version{0, 0, 5, 0}, valueFor(AddressPath, 5, 0), true)
	writeFor(mvh, ap3, AddressPath, accounts.NilKey, Version{0, 0, 20, 4}, valueFor(AddressPath, 20, 4), true)

	resVal, res, _ = readFor(mvh, ap2, AddressPath, accounts.NilKey, 10)
	require.Equal(t, 5, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 5, 0), resVal)

	resVal, res, _ = readFor(mvh, ap3, AddressPath, accounts.NilKey, 21)
	require.Equal(t, 20, res.depIdx)
	require.Equal(t, 4, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 20, 4), resVal)

	// Clear ap1 and ap3.
	mvh.Delete(ap1, AddressPath, accounts.NilKey, 12, true)
	mvh.Delete(ap1, AddressPath, accounts.NilKey, 8, true)
	mvh.Delete(ap3, AddressPath, accounts.NilKey, 20, true)

	// Reads from ap1 and ap3 go to db.
	_, res, _ = readFor(mvh, ap1, AddressPath, accounts.NilKey, 30)
	require.Equal(t, UnknownDep, res.depIdx)

	_, res, _ = readFor(mvh, ap3, AddressPath, accounts.NilKey, 30)
	require.Equal(t, UnknownDep, res.depIdx)

	// No-op delete at ap2 - doesn't panic because ap2 does exist
	mvh.Delete(ap2, AddressPath, accounts.NilKey, 11, true)

	// Read entry by txn 10 at ap2.
	resVal, res, _ = readFor(mvh, ap2, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, 2, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 10, 2), resVal)
}

// TestValidateRead_HasBAL_NoBypassForAddressPath verifies that when HasBAL is
// true, AddressPath is NOT bypassed — a new MVReadResultDone entry on
// AddressPath means a real state change (e.g. account creation) from a
// concurrent worker, and the read must be invalidated.
// TestReadSelfDestruct_FastPath covers the hasSelfDestruct short-circuit: a nil
// receiver and a map with no self-destruct both return the miss result (matching
// readFloor), and a real self-destruct is still observed once written.
func TestReadSelfDestruct_FastPath(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x5d})

	// nil receiver must not panic (matches readFloor's vm==nil handling).
	var nilVM *VersionMap
	_, res, ok := nilVM.ReadSelfDestruct(addr, 5)
	require.False(t, ok)
	require.Equal(t, MVReadResultNone, res.Status())

	// Fresh map, no self-destruct written: flag is false, fast-path miss.
	vm := NewVersionMap(nil)
	require.False(t, vm.hasSelfDestruct.Load())
	_, res, ok = vm.ReadSelfDestruct(addr, 5)
	require.False(t, ok)
	require.Equal(t, MVReadResultNone, res.Status())

	// After a self-destruct write the flag flips and the value is observed.
	vm.WriteSelfDestruct(addr, Version{TxIndex: 2, Incarnation: 0}, true, true)
	require.True(t, vm.hasSelfDestruct.Load())
	destructed, res, ok := vm.ReadSelfDestruct(addr, 5)
	require.True(t, ok)
	require.True(t, destructed)
	require.Equal(t, MVReadResultDone, res.Status())
}

func TestValidateRead_HasBAL_NoBypassForAddressPath(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)
	vm.HasBAL = true

	// A concurrent worker wrote to AddressPath at txIndex 0.
	writeFor(vm, addr, AddressPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(AddressPath, 0, 1), true)

	// Tx 2 originally read from storage (no map entry at execution time).
	io := NewVersionedIO(2)
	rs := ReadSet{}
	rs.SetAddress(addr, VersionedRead[AccountView]{
		ReadHeader: ReadHeader{Source: StorageRead, Version: Version{TxIndex: 2, Incarnation: 1}},
	})
	io.RecordReads(Version{TxIndex: 2, Incarnation: 1}, rs)

	valid := vm.ValidateVersion(2, io, checkVersionEqual, false, "")
	require.Equal(t, VersionInvalid, valid,
		"HasBAL should NOT bypass invalidation for AddressPath — new entry means real state change")
}

// TestValidateRead_NoHasBAL_InvalidatesAllPaths verifies the baseline behavior
// without HasBAL: any StorageRead that now finds a MVReadResultDone entry
// should be invalidated, regardless of path.
func TestValidateRead_NoHasBAL_InvalidatesAllPaths(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	for _, path := range []AccountPath{BalancePath, NoncePath, AddressPath} {
		t.Run(path.String(), func(t *testing.T) {
			vm := NewVersionMap(nil) // HasBAL = false
			require.False(t, vm.HasBAL)

			// A concurrent worker wrote at txIndex 0.
			writeFor(vm, addr, path, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(path, 0, 1), true)

			// Tx 2 originally read from storage (no map entry).
			io := NewVersionedIO(2)
			rs := ReadSet{}
			rs.SetHeader(addr, path, accounts.NilKey, ReadHeader{Source: StorageRead, Version: Version{TxIndex: 2, Incarnation: 1}})
			io.RecordReads(Version{TxIndex: 2, Incarnation: 1}, rs)

			valid := vm.ValidateVersion(2, io, checkVersionEqual, false, "")
			require.Equal(t, VersionInvalid, valid,
				"without HasBAL, StorageRead finding MVReadResultDone should invalidate for %s", path)
		})
	}
}

func BenchmarkWriteTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)

	const n = 10000
	randInts := make([]int, n)
	for i := range randInts {
		randInts[i] = rand.Intn(1000000000000000)
	}

	for i := 0; b.Loop(); i++ {
		idx := randInts[i%n]
		writeFor(mvh2, ap2, AddressPath, accounts.NilKey, Version{0, 0, idx, 1}, valueFor(AddressPath, idx, 1), true)
	}
}

func BenchmarkReadTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)
	txIdxSlice := []int{}

	for b.Loop() {
		txIdx := rand.Intn(1000000000000000)
		txIdxSlice = append(txIdxSlice, txIdx)
		writeFor(mvh2, ap2, AddressPath, accounts.NilKey, Version{0, 0, txIdx, 1}, valueFor(AddressPath, txIdx, 1), true)
	}

	b.ResetTimer()

	for _, value := range txIdxSlice {
		readFor(mvh2, ap2, AddressPath, accounts.NilKey, value)
	}
}

func TestTimeComplexity(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	// for 1000000 read and write with no dependency at different memory location
	mvh1 := NewVersionMap(nil)

	for i := range 1000000 {
		ap1 := getAddress(i)
		writeFor(mvh1, ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
		readFor(mvh1, ap1, AddressPath, accounts.NilKey, i)
	}

	// for 1000000 read and write with dependency at same memory location
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)

	for i := range 1000000 {
		writeFor(mvh2, ap2, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
		readFor(mvh2, ap2, AddressPath, accounts.NilKey, i)
	}
}

func TestWriteTimeSameLocationDifferentTxnIdx(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	for i := range 1000000 {
		writeFor(mvh1, ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
	}
}

func TestWriteTimeSameLocationSameTxnIdx(t *testing.T) {
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	for i := range 1000000 {
		writeFor(mvh1, ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, i}, valueFor(AddressPath, i, 1), true)
	}
}

func TestWriteTimeDifferentLocation(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	mvh1 := NewVersionMap(nil)

	for i := range 1000000 {
		ap1 := getAddress(i)
		writeFor(mvh1, ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
	}
}

func TestReadTimeSameLocation(t *testing.T) {
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	writeFor(mvh1, ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 1}, valueFor(AddressPath, 1, 1), true)

	for range 1000000 {
		readFor(mvh1, ap1, AddressPath, accounts.NilKey, 2)
	}
}

// TestValidateRead_StoragePath_ValueTiebreaker verifies that when a StoragePath
// read was from storage (source=StorageRead) but the versionMap now has a Done
// entry with the SAME value, validation considers it valid (value tiebreaker).
func TestValidateRead_StoragePath_ValueTiebreaker(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	storageKey := accounts.InternKey(common.BigToHash(big.NewInt(7)))
	storageVal := *uint256.NewInt(100)

	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)

	// TX 5 wrote storage value 100 to the versionMap.
	writeFor(vm, addr, StoragePath, storageKey, Version{TxIndex: 5, Incarnation: 1}, storageVal, true)

	// TX 10 originally read from storage (no versionMap entry at execution
	// time) and got value 100 — the same value TX 5 later wrote.
	io := NewVersionedIO(10)
	rs := ReadSet{}
	rs.SetStorage(addr, storageKey, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: StorageRead, Version: Version{TxIndex: UnknownDep, Incarnation: -1}},
		Val:        storageVal,
	})
	io.RecordReads(Version{TxIndex: 10, Incarnation: 1}, rs)

	valid := vm.ValidateVersion(10, io, checkVersionEqual, false, "")
	require.Equal(t, VersionValid, valid,
		"StoragePath read with matching value should be valid via tiebreaker")

	// Now test with a DIFFERENT value — should be invalid.
	vm2 := NewVersionMap(nil)
	writeFor(vm2, addr, StoragePath, storageKey, Version{TxIndex: 5, Incarnation: 1}, *uint256.NewInt(999), true)

	valid2 := vm2.ValidateVersion(10, io, checkVersionEqual, false, "")
	require.Equal(t, VersionInvalid, valid2,
		"StoragePath read with different value should be invalid")
}

// TestFlushEstimate_ValidTxNotMarkedEstimate verifies that when
// FlushVersionedWrites is called with complete=true for a valid TX,
// the entries are FlagDone (not FlagEstimate). This is critical:
// marking valid TX writes as Estimate causes downstream TXs to
// abort with ErrDependency, leading to livelocks.
func TestFlushEstimate_ValidTxNotMarkedEstimate(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	vm := NewVersionMap(nil)

	// Simulate: TX 5 is valid, flushed as Done (complete=true).
	writes := newWriteSet(
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Key: accounts.NilKey, Version: Version{TxIndex: 5, Incarnation: 1}}, Val: *uint256.NewInt(100)},
	)
	vm.FlushVersionedWrites(writes, true, "")

	// TX 10 reads should see FlagDone → MVReadResultDone.
	_, res, _ := readFor(vm, addr, BalancePath, accounts.NilKey, 10)
	require.Equal(t, MVReadResultDone, res.Status(),
		"valid TX flush should produce Done entries, not Estimate")
	require.Equal(t, 5, res.DepIdx())
	require.Equal(t, 1, res.Incarnation())

	// Simulate: TX 7 is invalid, flushed as Estimate (complete=false).
	writes2 := newWriteSet(
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Key: accounts.NilKey, Version: Version{TxIndex: 7, Incarnation: 2}}, Val: uint64(5)},
	)
	vm.FlushVersionedWrites(writes2, false, "")

	// TX 10 reads NoncePath should see FlagEstimate → MVReadResultDependency.
	_, res2, _ := readFor(vm, addr, NoncePath, accounts.NilKey, 10)
	require.Equal(t, MVReadResultDependency, res2.Status(),
		"invalid TX flush should produce Estimate entries")
	require.Equal(t, 7, res2.DepIdx())
}

func validateEqualVersion(readVersion, writeVersion Version) VersionValidity {
	if readVersion == writeVersion {
		return VersionValid
	}
	return VersionInvalid
}

// TestValidateRead_PriorAccountCreation_DetectedViaIncarnationPath covers the
// validateReadImpl AddressPath→IncarnationPath cross-check (versionmap.go): a
// prior tx created the account (writing IncarnationPath, which the BAL does not
// pre-populate), so a speculative storage-fallback AddressPath read is stale and
// must invalidate. Restored after the typed-vio rework removed the original.
func TestValidateRead_PriorAccountCreation_DetectedViaIncarnationPath(t *testing.T) {
	t.Parallel()
	addr := getAddress(99)

	vm := NewVersionMap(nil)
	vm.HasBAL = true
	// Post-flush state after tx 0 creates the account: BAL pre-populated
	// Balance/Nonce/CodeHash; the worker additionally flushed Incarnation
	// (CreateAccount writes it, BAL does not). AddressPath was BAL-filtered out.
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000), true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(0), true)
	writeFor(vm, addr, IncarnationPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, uint64(1), true)

	// Tx 1 speculatively read AddressPath from storage (no map entry at exec).
	io := NewVersionedIO(2)
	rs := ReadSet{}
	rs.SetAddress(addr, VersionedRead[AccountView]{
		ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
	})
	io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, rs)

	require.Equal(t, VersionInvalid, vm.ValidateVersion(1, io, validateEqualVersion, false, ""),
		"a prior IncarnationPath write (account created) must invalidate the stale AddressPath storage read")
}

// TestValidateRead_SDStaleness_InvalidatesPreDestructRead covers the
// validateReadImpl SD-staleness branch: a later tx self-destructed the account
// with no revival, so a version-consistent pre-destruct BalancePath read is
// stale and must invalidate. Restored after the typed-vio rework.
func TestValidateRead_SDStaleness_InvalidatesPreDestructRead(t *testing.T) {
	t.Parallel()
	addr := getAddress(77)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2, Incarnation: 0}, true, true)

	// Tx 5 read BalancePath as a MapRead at (0,0) — consistent on Balance alone,
	// but stale because tx 2's destruct came after.
	io := NewVersionedIO(5)
	rs := ReadSet{}
	rs.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0, Incarnation: 0}},
		Val:        *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)

	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, false, ""),
		"a later self-destruct with no revival must invalidate the pre-destruct BalancePath read")
}

// TestValidateRead_SDStaleness_RevivalKeepsReadValid is the converse: a revival
// write (NoncePath) after the destruct re-creates the account, so the
// pre-destruct read stays valid. Guards the revivalLimit branch.
func TestValidateRead_SDStaleness_RevivalKeepsReadValid(t *testing.T) {
	t.Parallel()
	addr := getAddress(78)

	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000), true)
	writeFor(vm, addr, SelfDestructPath, accounts.NilKey, Version{TxIndex: 2, Incarnation: 0}, true, true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: 3, Incarnation: 0}, uint64(1), true)

	io := NewVersionedIO(5)
	rs := ReadSet{}
	rs.SetBalance(addr, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0, Incarnation: 0}},
		Val:        *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)

	require.Equal(t, VersionValid, vm.ValidateVersion(5, io, validateEqualVersion, false, ""),
		"a revival write after the destruct re-creates the account — the read stays valid")
}

// TestVersionedWritePoolReuse_NoStaleFields guards the *VersionedWrite[T] pool
// invariant: getVW* returns a recycled cell that may still hold a prior write's
// contents, so the record path MUST wholesale-overwrite it. It drives the
// production recorder (recordWriteBalance → getVWBalance) after seeding the pool
// with a fully-poisoned cell, asserting nothing from the prior write survives —
// including Key/Reason, which a balance write omits and which a field-by-field
// assignment would leak. Not parallel: it seeds the process-global vwPoolBalance,
// so it must run without a concurrent pool user.
func TestVersionedWritePoolReuse_NoStaleFields(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), vm)
	ibs.SetTxContext(0, 3)

	// Seed the pool so the recorder's getVWBalance hands back a poisoned cell.
	vwPoolBalance.Put(&VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{
			Address: getAddress(1),
			Path:    NoncePath,
			Key:     accounts.InternKey([32]byte{0xff}),
			Version: Version{TxIndex: 999, Incarnation: 7},
			Reason:  tracing.BalanceChangeReason(0xab),
		},
		Val: *uint256.NewInt(0xdead),
	})

	addr := getAddress(2)
	want := *uint256.NewInt(42)
	ibs.recordWriteBalance(addr, want)

	vw, ok := ibs.versionedWrites.GetBalance(addr)
	require.True(t, ok, "recordWriteBalance must record a balance write")
	require.Equal(t, addr, vw.Address, "Address must not retain the recycled value")
	require.Equal(t, BalancePath, vw.Path, "Path must not retain the recycled value")
	require.Equal(t, accounts.StorageKey{}, vw.Key, "Key must reset to zero (BalancePath has no key)")
	require.Equal(t, tracing.BalanceChangeReason(0), vw.Reason, "Reason must reset to zero")
	require.True(t, vw.Val.Eq(&want), "Val must not retain the recycled value")
}

// TestBALPrePop_SameSenderTxs_NoConflicts ports the same-sender BAL
// conflict-detection coverage to the typed VersionMap API: when the BAL
// pre-populates balance/nonce for a run of same-sender txs, each tx's recorded
// reads must validate without spurious conflicts.
func TestBALPrePop_SameSenderTxs_NoConflicts(t *testing.T) {
	t.Parallel()

	sender := getAddress(7)
	coinbase := getAddress(8)
	const numTxs = 9

	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)
	vm.HasBAL = true

	for i := range numTxs {
		writeFor(vm, sender, BalancePath, accounts.NilKey, Version{TxIndex: i, Incarnation: 0}, *uint256.NewInt(uint64(1000 - i)), true)
		writeFor(vm, sender, NoncePath, accounts.NilKey, Version{TxIndex: i, Incarnation: 0}, uint64(i+1), true)
		writeFor(vm, coinbase, BalancePath, accounts.NilKey, Version{TxIndex: i, Incarnation: 0}, *uint256.NewInt(uint64((i + 1) * 50)), true)
	}

	sourceFor := func(r ReadResult) ReadSource {
		if r.Status() == MVReadResultDone {
			return MapRead
		}
		return StorageRead
	}

	io := NewVersionedIO(numTxs)
	for txIdx := range numTxs {
		rs := ReadSet{}
		rs.SetAddress(sender, VersionedRead[AccountView]{ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion}})
		balVal, balRes, _ := readFor(vm, sender, BalancePath, accounts.NilKey, txIdx)
		bv, _ := balVal.(uint256.Int)
		rs.SetBalance(sender, VersionedRead[uint256.Int]{ReadHeader: ReadHeader{Source: sourceFor(balRes), Version: balRes.Version()}, Val: bv})
		nonceVal, nonceRes, _ := readFor(vm, sender, NoncePath, accounts.NilKey, txIdx)
		nv, _ := nonceVal.(uint64)
		rs.SetNonce(sender, VersionedRead[uint64]{ReadHeader: ReadHeader{Source: sourceFor(nonceRes), Version: nonceRes.Version()}, Val: nv})
		rs.SetAddress(coinbase, VersionedRead[AccountView]{ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion}})
		cbBalVal, cbBalRes, _ := readFor(vm, coinbase, BalancePath, accounts.NilKey, txIdx)
		cbv, _ := cbBalVal.(uint256.Int)
		rs.SetBalance(coinbase, VersionedRead[uint256.Int]{ReadHeader: ReadHeader{Source: sourceFor(cbBalRes), Version: cbBalRes.Version()}, Val: cbv})
		io.RecordReads(Version{TxIndex: txIdx, Incarnation: 0}, rs)
	}

	for txIdx := range numTxs {
		valid := vm.ValidateVersion(txIdx, io, checkVersionEqual, false, "")
		require.Equal(t, VersionValid, valid,
			"tx %d: BAL-pre-populated reads should validate without conflicts; got %s", txIdx, valid)
	}
}

// TestNoBAL_SameSenderTxs_DetectsConflicts is the safety counterpart: without a
// BAL, a run of same-sender txs whose recorded StorageReads predate tx 0's
// flushed balance/nonce must each invalidate (else parallel exec would commit
// stale balances/nonces).
func TestNoBAL_SameSenderTxs_DetectsConflicts(t *testing.T) {
	t.Parallel()

	sender := getAddress(11)
	const numTxs = 9

	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)
	require.False(t, vm.HasBAL, "test exercises the no-BAL path")

	origBalance := *uint256.NewInt(1_000_000)
	origNonce := uint64(42)
	io := NewVersionedIO(numTxs)
	for txIdx := range numTxs {
		rs := ReadSet{}
		rs.SetBalance(sender, VersionedRead[uint256.Int]{ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion}, Val: origBalance})
		rs.SetNonce(sender, VersionedRead[uint64]{ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion}, Val: origNonce})
		rs.SetAddress(sender, VersionedRead[AccountView]{ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion}})
		io.RecordReads(Version{TxIndex: txIdx, Incarnation: 0}, rs)
	}

	postBalance := *uint256.NewInt(900_000)
	postNonce := origNonce + 1
	ws := &WriteSet{}
	ws.SetBalance(sender, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: sender, Path: BalancePath, Version: Version{TxIndex: 0, Incarnation: 0}}, Val: postBalance})
	ws.SetNonce(sender, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: sender, Path: NoncePath, Version: Version{TxIndex: 0, Incarnation: 0}}, Val: postNonce})
	vm.FlushVersionedWrites(ws, true, "")

	require.Equal(t, VersionValid, vm.ValidateVersion(0, io, checkVersionEqual, false, ""),
		"tx 0 should validate (no prior writes to conflict with)")

	for txIdx := 1; txIdx < numTxs; txIdx++ {
		valid := vm.ValidateVersion(txIdx, io, checkVersionEqual, false, "")
		require.Equal(t, VersionInvalid, valid,
			"tx %d: recorded StorageRead of sender.BalancePath conflicts with tx 0's flushed Done; got %s", txIdx, valid)
	}
}
