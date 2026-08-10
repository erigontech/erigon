package state

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
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

// TestValidateRead_NewAddressEntryInvalidatesStorageRead: a new
// MVReadResultDone entry on AddressPath means a real state change (e.g.
// account creation) from a concurrent worker, and a nil storage-sourced read
// must be invalidated.
func TestValidateRead_NewAddressEntryInvalidatesStorageRead(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)

	// A concurrent worker wrote to AddressPath at txIndex 0.
	writeFor(vm, addr, AddressPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(AddressPath, 0, 1), true)

	// Tx 2 originally read from storage (no map entry at execution time).
	io := NewVersionedIO(2)
	rs := ReadSet{}
	rs.SetAddress(addr, VersionedRead[AccountView]{
		ReadHeader: ReadHeader{Source: StorageRead, Version: Version{TxIndex: 2, Incarnation: 1}},
	})
	io.RecordReads(Version{TxIndex: 2, Incarnation: 1}, rs)

	valid := vm.ValidateVersion(2, io, checkVersionEqual, true, false, false, "")
	require.Equal(t, VersionInvalid, valid)
}

// TestValidateRead_ChangedValueInvalidatesStorageRead: a StorageRead that now
// finds a MVReadResultDone entry with a different value must be invalidated,
// regardless of path.
func TestValidateRead_ChangedValueInvalidatesStorageRead(t *testing.T) {
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
			vm := NewVersionMap(nil)

			// A concurrent worker wrote at txIndex 0.
			writeFor(vm, addr, path, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(path, 0, 1), true)

			// Tx 2 originally read from storage (no map entry).
			io := NewVersionedIO(2)
			rs := ReadSet{}
			rs.SetHeader(addr, path, accounts.NilKey, ReadHeader{Source: StorageRead, Version: Version{TxIndex: 2, Incarnation: 1}})
			io.RecordReads(Version{TxIndex: 2, Incarnation: 1}, rs)

			valid := vm.ValidateVersion(2, io, checkVersionEqual, true, false, false, "")
			require.Equal(t, VersionInvalid, valid)
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

	valid := vm.ValidateVersion(10, io, checkVersionEqual, true, false, false, "")
	require.Equal(t, VersionValid, valid,
		"StoragePath read with matching value should be valid via tiebreaker")

	// Now test with a DIFFERENT value — should be invalid.
	vm2 := NewVersionMap(nil)
	writeFor(vm2, addr, StoragePath, storageKey, Version{TxIndex: 5, Incarnation: 1}, *uint256.NewInt(999), true)

	valid2 := vm2.ValidateVersion(10, io, checkVersionEqual, true, false, false, "")
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

	require.Equal(t, VersionInvalid, vm.ValidateVersion(1, io, validateEqualVersion, true, false, false, ""))
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

	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, true, false, false, ""))
}

// A revival after the destruct re-creates the account fresh; it does not
// resurrect pre-destruct field values. A revival that re-establishes a field
// writes a cell above the destruct — which then is the read's floor — so a
// read still floored below the destruct is stale regardless of revival.
func TestValidateRead_SDStaleness_RevivalDoesNotResurrectPreDestructRead(t *testing.T) {
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

	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, true, false, false, ""))
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
	defer ibs.Close()
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
		valid := vm.ValidateVersion(txIdx, io, checkVersionEqual, true, false, false, "")
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

	require.Equal(t, VersionValid, vm.ValidateVersion(0, io, checkVersionEqual, true, false, false, ""))

	for txIdx := 1; txIdx < numTxs; txIdx++ {
		valid := vm.ValidateVersion(txIdx, io, checkVersionEqual, true, false, false, "")
		require.Equal(t, VersionInvalid, valid,
			"tx %d: recorded StorageRead of sender.BalancePath conflicts with tx 0's flushed Done; got %s", txIdx, valid)
	}
}

// fixedAccountReader returns a fixed account from the DB/state reader.
type fixedAccountReader struct {
	minimalStateReader
	acc *accounts.Account
}

func (r *fixedAccountReader) ReadAccountData(accounts.Address) (*accounts.Account, error) {
	return r.acc, nil
}

// A MapRead whose writer-version churned but whose value is unchanged stays
// valid — a version-only churn is not a real dependency.
func TestValidateRead_MapReadValueTiebreaker(t *testing.T) {
	vm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xab, 0x01})
	vm.WriteBalance(addr, Version{TxIndex: 5}, *uint256.NewInt(100), true)
	checkVersionEq := func(rv, wv Version) VersionValidity {
		if rv == wv {
			return VersionValid
		}
		return VersionInvalid
	}
	valid := validateRead(
		vm,
		10,
		addr,
		BalancePath,
		accounts.NilKey,
		MapRead,
		Version{TxIndex: 3},
		*uint256.NewInt(100),
		liveBalance,
		eqUint256,
		absentUint256,
		recordBalance,
		checkVersionEq,
		false,
		"",
	)
	assert.Equal(t, VersionValid, valid)
	valid2 := validateRead(
		vm,
		10,
		addr,
		BalancePath,
		accounts.NilKey,
		MapRead,
		Version{TxIndex: 3},
		*uint256.NewInt(999),
		liveBalance,
		eqUint256,
		absentUint256,
		recordBalance,
		checkVersionEq,
		false,
		"",
	)
	assert.Equal(t, VersionInvalid, valid2)
}

// readValueUnchanged is true when the recorded read's value equals the value
// just read (a version-only churn), false on a real value change.
func TestReadValueUnchanged(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xcd, 0x02})
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	defer ibs.Release(false)
	ibs.versionedReads = ReadSet{}
	ibs.versionedReads.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(100)})
	r := &readPathResult{mapBalanceVal: *uint256.NewInt(100)}
	assert.True(t, ibs.readValueUnchanged(addr, BalancePath, accounts.NilKey, r))
	r.mapBalanceVal = *uint256.NewInt(200)
	assert.False(t, ibs.readValueUnchanged(addr, BalancePath, accounts.NilKey, r))
}

// An account absent from both the versionMap AddressPath and the DB, but with
// a BAL-prepopulated balance cell proving an earlier tx created it, is
// synthesized at read time instead of read as non-existent — a read that would
// otherwise race the creator's flush and re-execute.
func TestGetVersionedAccount_SynthesizesCreatedFromBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xba, 0x01})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(500), true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, acc)
	assert.Equal(t, *uint256.NewInt(500), acc.Balance)
	rd, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.NotNil(t, rd.Val)
	require.NotNil(t, rd.Val.Account())
}

// A reader racing the creator's first flush must be shielded by the BAL feed:
// the pre-populated cells resolve the account before the creator's own writes
// land, so the recorded read is non-nil and survives both validation and the
// mid-execution dependency re-check once the creator flushes.
func TestBALFedReaderDoesNotRaceCreatorFlush(t *testing.T) {
	balFedChanges := func(addr accounts.Address) []*types.AccountChanges {
		return []*types.AccountChanges{{
			Address: addr,
			BalanceChanges: []*types.BalanceChange{{
				Index: 1,
				Value: *uint256.NewInt(53771),
			}},
		}}
	}
	contractFedChanges := func(addr accounts.Address) []*types.AccountChanges {
		return []*types.AccountChanges{{
			Address: addr,
			NonceChanges: []*types.NonceChange{{
				Index: 1,
				Value: 1,
			}},
			CodeChanges: []*types.CodeChange{{
				Index:    1,
				Bytecode: []byte{0x60, 0x00},
			}},
		}}
	}
	creatorFlush := func(vm *VersionMap, addr accounts.Address, balance uint64, nonce uint64) {
		ws := &WriteSet{}
		ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{
			WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 0}},
			Val:         &accounts.Account{CodeHash: accounts.EmptyCodeHash},
		})
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}},
			Val:         *uint256.NewInt(balance),
		})
		ws.SetNonce(addr, &VersionedWrite[uint64]{
			WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: Version{TxIndex: 0}},
			Val:         nonce,
		})
		vm.FlushVersionedWrites(ws, true, "")
	}
	type readFn struct {
		name string
		read func(t *testing.T, ibs *IntraBlockState, addr accounts.Address)
	}
	reads := []readFn{
		{"exist-then-balance", func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
			exists, err := ibs.Exist(addr)
			require.NoError(t, err)
			require.True(t, exists)
			_, err = ibs.GetBalance(addr)
			require.NoError(t, err)
		}},
		{"empty-check", func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
			empty, err := ibs.Empty(addr)
			require.NoError(t, err)
			require.False(t, empty)
		}},
		{"touch", func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
			require.NoError(t, ibs.TouchAccount(addr))
		}},
		{"record-first-then-exist", func(t *testing.T, ibs *IntraBlockState, addr accounts.Address) {
			_, _, _, err := ibs.getVersionedAccount(addr, false)
			require.NoError(t, err)
			so, err := ibs.getStateObject(addr, true)
			require.NoError(t, err)
			require.NotNil(t, so)
		}},
	}
	feeds := []struct {
		name    string
		changes func(accounts.Address) []*types.AccountChanges
		balance uint64
		nonce   uint64
	}{
		{"eoa-balance-fed", balFedChanges, 53771, 0},
		{"contract-nonce-code-fed", contractFedChanges, 0, 1},
	}
	for _, feed := range feeds {
		for _, rd := range reads {
			t.Run(feed.name+"/"+rd.name, func(t *testing.T) {
				addr := accounts.InternAddress([20]byte{0xfe, byte(len(rd.name))})
				vm := NewVersionMap(feed.changes(addr))
				ibs := NewWithVersionMap(&minimalStateReader{}, vm)
				defer ibs.Release(false)
				ibs.txIndex = 1
				rd.read(t, ibs, addr)
				tr, ok := ibs.versionedReads.GetAddress(addr)
				require.True(t, ok)
				require.NotNil(t, tr.Val)
				require.NotNil(t, tr.Val.Account())
				creatorFlush(vm, addr, feed.balance, feed.nonce)
				io := NewVersionedIO(2)
				io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, ibs.versionedReads)
				valid := vm.ValidateVersion(1, io, validateEqualVersion, true, false, false, "")
				require.Equal(t, VersionValid, valid)
				require.NotPanics(t, func() {
					_, err := ibs.GetBalance(addr)
					require.NoError(t, err)
				})
			})
		}
	}
}

// The provisional nil probe must not survive the destroyed-unrevived and
// EIP-8246 preserved-account conclusions: once the EVM consumes either, a
// later same-tx load must conflict with a fresh flush instead of adopting it.
func TestVersionedAccountBase_DestroyedExitDemotesProvisional(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd3, 0x01})
	reader := &codeReader{addr: addr, account: &accounts.Account{Nonce: 1, Balance: *uint256.NewInt(9), CodeHash: accounts.EmptyCodeHash}}
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Release(false)
	ibs.SetTxContext(1, 5)
	ibs.SetVersion(0)
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
	if tr, ok := ibs.versionedReads.GetAddress(addr); ok {
		require.NotEqual(t, ProvisionalRead, tr.Source)
	}
}

// EIP-8246 reconstruction with nothing preserved (zero balance floor) must
// resolve to absent, not dereference a nil preserved account.
func TestVersionedAccountBase_NilPreservedAccountResolvesAbsent(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd3, 0x02})
	reader := &codeReader{addr: addr, account: &accounts.Account{Nonce: 1, Balance: *uint256.NewInt(9), CodeHash: accounts.EmptyCodeHash}}
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	vm.WriteBalance(addr, Version{TxIndex: 1}, uint256.Int{}, true)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Release(false)
	ibs.eip8246 = true
	ibs.SetTxContext(1, 5)
	ibs.SetVersion(0)
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
	if tr, ok := ibs.versionedReads.GetAddress(addr); ok {
		require.NotEqual(t, ProvisionalRead, tr.Source)
	}
}

// A stale ALIVE read of an in-block-destroyed, unrevived account must
// invalidate: the destroyed-and-unrevived relaxation is only sound for reads
// that concluded ABSENCE.
func TestValidateRead_StaleAliveReadOfDestroyedAccountMustInvalidate(t *testing.T) {
	addr := getAddress(150)
	alive := &accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash([32]byte{0xaa})}
	io := NewVersionedIO(4)
	rs := ReadSet{}
	rs.SetAddress(addr, VersionedRead[AccountView]{
		ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		Val:        NewAccountView(alive),
	})
	io.RecordReads(Version{TxIndex: 3, Incarnation: 0}, rs)
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(3, io, validateEqualVersion, true, false, false, ""))
}

// CodePath twin of the stale-alive case: a recorded non-empty code read of a
// destroyed, unrevived account must invalidate through the cross-validate arm.
func TestValidateRead_StaleCodeReadOfDestroyedAccountMustInvalidate(t *testing.T) {
	addr := getAddress(151)
	io := NewVersionedIO(4)
	rs := ReadSet{}
	rs.SetCode(addr, VersionedRead[[]byte]{
		ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		Val:        []byte{0x60, 0x00},
	})
	io.RecordReads(Version{TxIndex: 3, Incarnation: 0}, rs)
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(3, io, validateEqualVersion, true, false, false, ""))
}

// The MapRead value tiebreaker must not bypass a LATER self-destruct: a read
// whose value matches a churned cell is only valid if the account was not
// destroyed (unrevived) after that cell.
func TestValidateRead_TiebreakerMustNotBypassLaterSD(t *testing.T) {
	addr := getAddress(160)
	alive := &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}
	io := NewVersionedIO(6)
	rs := ReadSet{}
	rs.SetAddress(addr, VersionedRead[AccountView]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}},
		Val:        NewAccountView(alive),
	})
	io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)
	vm := NewVersionMap(nil)
	vm.WriteAddress(addr, Version{TxIndex: 3}, alive, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 4}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 4}, 1, true)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(5, io, validateEqualVersion, true, false, false, ""))
}

// CodePath twin of the tiebreaker bypass, plus the version-MATCH variant
// (read at the cell's own version, SD after): both must see the lifecycle.
func TestValidateRead_CodeReadMustSeeLaterSD(t *testing.T) {
	addr := getAddress(161)
	code := []byte{0x60, 0x00}
	newIO := func(readVer Version) *VersionedIO {
		io := NewVersionedIO(6)
		rs := ReadSet{}
		rs.SetCode(addr, VersionedRead[[]byte]{ReadHeader: ReadHeader{Source: MapRead, Version: readVer}, Val: code})
		io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)
		return io
	}
	vm := NewVersionMap(nil)
	vm.WriteCode(addr, Version{TxIndex: 3}, accounts.NewCode(code), true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 4}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 4}, 1, true)
	t.Run("version-churn value-equal read sees the later SD", func(t *testing.T) {
		require.Equal(t, VersionInvalid, vm.ValidateVersion(5, newIO(Version{TxIndex: 0}), validateEqualVersion, true, false, false, ""))
	})
	t.Run("version-match read sees the later SD", func(t *testing.T) {
		require.Equal(t, VersionInvalid, vm.ValidateVersion(5, newIO(Version{TxIndex: 3}), validateEqualVersion, true, false, false, ""))
	})
}

// Destroyed-and-unrevived is not non-existent under EIP-8246: a self-destruct
// that preserves a non-zero balance leaves the account alive, so a nil record
// read racing the destroyer's flush must invalidate and re-execute. Only a
// cell-evidenced dead account (no live sub-field floors) relaxes the
// created-account incarnation check.
func TestValidateRead_NilReadOfPreservedBalanceDestructInvalid(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x82, 0x46})
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	newIO := func() *VersionedIO {
		io := NewVersionedIO(2)
		rs := ReadSet{}
		rs.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		io.RecordReads(Version{TxIndex: 1}, rs)
		return io
	}
	t.Run("preserved non-zero balance keeps the account alive", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
		vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(3), true)
		vm.WriteIncarnation(addr, Version{TxIndex: 0}, 1, true)
		require.Equal(t, VersionInvalid, vm.ValidateVersion(1, newIO(), checkVersionEqual, true, false, false, ""))
	})
	t.Run("zero-balance destroyed account stays relaxed", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
		vm.WriteBalance(addr, Version{TxIndex: 0}, uint256.Int{}, true)
		vm.WriteIncarnation(addr, Version{TxIndex: 0}, 1, true)
		require.Equal(t, VersionValid, vm.ValidateVersion(1, newIO(), checkVersionEqual, true, false, false, ""))
	})
}

// A self-destruct shadowed by a later revival cell must still invalidate
// pre-destruct reads: re-creation flushes SelfDestruct=false above the true
// cell, so latest-only probing misses the wipe. The read path already
// range-scans its floors; validation must mirror it or a stale slot value
// validates while a fresh read returns zero.
func TestValidateRead_StorageReadMustSeeShadowedSD(t *testing.T) {
	addr := getAddress(170)
	key := accounts.InternKey(common.BigToHash(big.NewInt(9)))
	val := *uint256.NewInt(5)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	newVM := func() *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteStorage(addr, key, Version{TxIndex: 0}, val, true)
		vm.WriteStorage(addr, key, Version{TxIndex: 1}, val, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
		vm.WriteIncarnation(addr, Version{TxIndex: 2}, 1, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 3}, false, true)
		vm.WriteAddress(addr, Version{TxIndex: 3}, &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}, true)
		vm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1), true)
		vm.WriteNonce(addr, Version{TxIndex: 3}, 1, true)
		return vm
	}
	newIO := func(readVer Version) *VersionedIO {
		io := NewVersionedIO(6)
		rs := ReadSet{}
		rs.SetStorage(addr, key, VersionedRead[uint256.Int]{
			ReadHeader: ReadHeader{Source: MapRead, Version: readVer},
			Val:        val,
		})
		io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)
		return io
	}
	t.Run("version-match read sees the shadowed SD", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(5, newIO(Version{TxIndex: 1}), checkVersionEqual, true, false, false, ""))
	})
	t.Run("value tiebreaker sees the shadowed SD", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(5, newIO(Version{TxIndex: 0}), checkVersionEqual, true, false, false, ""))
	})
}

// The metamorphic destroy-and-recreate scenario end to end: the AddressPath
// record is existence-only, so a pre-destruction record read of an account
// re-created with equal facets stays valid — every facet is recorded and
// validated as its own read — while the stale pre-recreation slot read is
// what carries the wipe and must invalidate.
func TestValidateRead_MetamorphicRecreateInvalidatesSlotNotRecord(t *testing.T) {
	addr := getAddress(171)
	key := accounts.InternKey(common.BigToHash(big.NewInt(9)))
	val := *uint256.NewInt(5)
	recreated := &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	newVM := func() *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 0}, recreated, true)
		vm.WriteStorage(addr, key, Version{TxIndex: 1}, val, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
		vm.WriteIncarnation(addr, Version{TxIndex: 2}, 1, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 3}, false, true)
		vm.WriteAddress(addr, Version{TxIndex: 3}, recreated, true)
		vm.WriteNonce(addr, Version{TxIndex: 3}, 1, true)
		return vm
	}
	t.Run("record read of the recreated-equal account stays valid", func(t *testing.T) {
		io := NewVersionedIO(6)
		rs := ReadSet{}
		rs.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}},
			Val:        NewAccountView(recreated),
		})
		io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)
		require.Equal(t, VersionValid, newVM().ValidateVersion(5, io, checkVersionEqual, true, false, false, ""))
	})
	t.Run("stale pre-recreation slot read invalidates", func(t *testing.T) {
		io := NewVersionedIO(6)
		rs := ReadSet{}
		rs.SetStorage(addr, key, VersionedRead[uint256.Int]{
			ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 1}},
			Val:        val,
		})
		io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(5, io, checkVersionEqual, true, false, false, ""))
	})
}

// Sub-field storage reads cross-validate their account recursively, where
// absent means field emptiness, not account non-existence. A balance-only
// preserved account (EIP-8246) legitimately has empty nonce/code — empty
// committed reads must stay valid — while a non-empty committed field value
// is stale under the destroyer's lifecycle churn.
func TestValidateRead_FieldReadsOfPreservedAccountCrossValidate(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x82, 0x47})
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	newVM := func() *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)
		vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(1), true)
		vm.WriteIncarnation(addr, Version{TxIndex: 0}, 1, true)
		return vm
	}
	t.Run("empty nonce and code reads stay valid", func(t *testing.T) {
		io := NewVersionedIO(2)
		rs := ReadSet{}
		rs.SetNonce(addr, VersionedRead[uint64]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		rs.SetCode(addr, VersionedRead[[]byte]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		io.RecordReads(Version{TxIndex: 1}, rs)
		require.Equal(t, VersionValid, newVM().ValidateVersion(1, io, checkVersionEqual, true, false, false, ""))
	})
	t.Run("stale non-empty nonce read invalidates", func(t *testing.T) {
		io := NewVersionedIO(2)
		rs := ReadSet{}
		rs.SetNonce(addr, VersionedRead[uint64]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
			Val:        5,
		})
		io.RecordReads(Version{TxIndex: 1}, rs)
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(1, io, checkVersionEqual, true, false, false, ""))
	})
}

// A sub-field read with no dedicated cell folds onto the account record for
// validation. A later record write that keeps the field's value unchanged
// (fee-merge churn re-stamps the coinbase record every tx) must not invalidate
// the folded read — the fold compares values, not record versions.
func TestValidateRead_FoldedNonceSurvivesRecordChurn(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xee, 0x2d})
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	newIO := func() *VersionedIO {
		io := NewVersionedIO(100)
		rs := ReadSet{}
		rs.SetNonce(addr, VersionedRead[uint64]{
			ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 4}},
			Val:        69,
		})
		io.RecordReads(Version{TxIndex: 99}, rs)
		return io
	}
	t.Run("same nonce in churned record stays valid", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 4}, &accounts.Account{Nonce: 69, CodeHash: accounts.EmptyCodeHash}, true)
		vm.WriteAddress(addr, Version{TxIndex: 98}, &accounts.Account{Nonce: 69, Balance: *uint256.NewInt(123), CodeHash: accounts.EmptyCodeHash}, true)
		require.Equal(t, VersionValid, vm.ValidateVersion(99, newIO(), checkVersionEqual, true, false, false, ""))
	})
	t.Run("changed nonce in churned record invalidates", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 4}, &accounts.Account{Nonce: 69, CodeHash: accounts.EmptyCodeHash}, true)
		vm.WriteAddress(addr, Version{TxIndex: 98}, &accounts.Account{Nonce: 70, Balance: *uint256.NewInt(123), CodeHash: accounts.EmptyCodeHash}, true)
		require.Equal(t, VersionInvalid, vm.ValidateVersion(99, newIO(), checkVersionEqual, true, false, false, ""))
	})
	t.Run("estimate record cannot prove equality", func(t *testing.T) {
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 4}, &accounts.Account{Nonce: 69, CodeHash: accounts.EmptyCodeHash}, true)
		vm.WriteAddress(addr, Version{TxIndex: 98}, nil, false)
		require.Equal(t, VersionInvalid, vm.ValidateVersion(99, newIO(), checkVersionEqual, true, false, false, ""))
	})
}

// A synthesized account's incarnation is a guess (the BAL carries no
// incarnation and an empty-code contract creation carries no code change
// either), so the load must not record it as an incarnation read: the
// creator's later Incarnation flush would spuriously invalidate the guess.
// A DB-loaded account's incarnation is an observation and stays recorded.
func TestSynthesizedAccountRecordsNoIncarnationGuess(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd9, 0x6c})
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	vm := NewVersionMap([]*types.AccountChanges{{
		Address:      addr,
		NonceChanges: []*types.NonceChange{{Index: 227, Value: 1}},
	}})
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Release(false)
	ibs.SetTxContext(1, 227)
	ibs.SetVersion(0)
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, acc)
	reads := ibs.VersionedReads()
	_, tracked := reads.GetIncarnation(addr)
	require.False(t, tracked)
	io := NewVersionedIO(228)
	io.RecordReads(Version{TxIndex: 227}, reads)
	vm.WriteAddress(addr, Version{TxIndex: 226}, &accounts.Account{Nonce: 1, Incarnation: 1, CodeHash: accounts.EmptyCodeHash}, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 226}, 1, true)
	require.Equal(t, VersionValid, vm.ValidateVersion(227, io, checkVersionEqual, true, false, false, ""))
}

// The DB-loaded twin (fund-then-deploy): a balance-only account CREATE2'd onto
// by the preceding tx. The reader's EVM-visible view is already deterministic
// (nonce/code from BAL cells, balance from the DB), so the pre-block
// incarnation must not be recorded either — the creator's Incarnation flush
// would spuriously invalidate it while normalization resolves the final
// incarnation from the map.
func TestDBLoadedAccountRecordsNoIncarnationDefault(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd9, 0x6d})
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	deployed := accounts.NewCode([]byte{0x60, 0x80, 0x60, 0x40})
	reader := &codeReader{addr: addr, account: &accounts.Account{Balance: *uint256.NewInt(9), CodeHash: accounts.EmptyCodeHash}}
	vm := NewVersionMap([]*types.AccountChanges{{
		Address:      addr,
		NonceChanges: []*types.NonceChange{{Index: 227, Value: 1}},
		CodeChanges:  []*types.CodeChange{{Index: 227, Bytecode: deployed.Bytes}},
	}})
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Release(false)
	ibs.SetTxContext(1, 227)
	ibs.SetVersion(0)
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, acc)
	reads := ibs.VersionedReads()
	_, tracked := reads.GetIncarnation(addr)
	require.False(t, tracked)
	io := NewVersionedIO(228)
	io.RecordReads(Version{TxIndex: 227}, reads)
	vm.WriteAddress(addr, Version{TxIndex: 226}, &accounts.Account{Nonce: 1, Incarnation: 1, Balance: *uint256.NewInt(9), CodeHash: deployed.Hash}, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 226}, 1, true)
	require.Equal(t, VersionValid, vm.ValidateVersion(227, io, checkVersionEqual, true, false, false, ""))
}

// A BAL code change determines the code's size and hash, so the pre-population
// must write those derived cells too: an EXTCODESIZE/EXTCODEHASH reader of a
// just-created contract otherwise misses the map, falls through to the DB, and
// races the creator's flush.
func TestBALPrePopulatesDerivedCodeCells(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xcd, 0x01})
	bytecode := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	vm := NewVersionMap([]*types.AccountChanges{{
		Address:     addr,
		CodeChanges: []*types.CodeChange{{Index: 3, Bytecode: bytecode}},
	}})
	size, sres, ok := vm.ReadCodeSize(addr, 5)
	require.True(t, ok)
	require.Equal(t, MVReadResultDone, sres.Status())
	require.Equal(t, len(bytecode), size)
	hash, hres, ok := vm.ReadCodeHash(addr, 5)
	require.True(t, ok)
	require.Equal(t, MVReadResultDone, hres.Status())
	require.Equal(t, accounts.NewCode(bytecode).Hash, hash)
	_, _, ok = vm.ReadCodeSize(addr, 2)
	require.False(t, ok)
}

func TestBALPrePopulatesDerivedCodeCells_ClearedCode(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xcd, 0x02})
	vm := NewVersionMap([]*types.AccountChanges{{
		Address:     addr,
		CodeChanges: []*types.CodeChange{{Index: 3, Bytecode: nil}},
	}})
	size, sres, ok := vm.ReadCodeSize(addr, 5)
	require.True(t, ok)
	require.Equal(t, MVReadResultDone, sres.Status())
	require.Zero(t, size)
	hash, hres, ok := vm.ReadCodeHash(addr, 5)
	require.True(t, ok)
	require.Equal(t, MVReadResultDone, hres.Status())
	require.Equal(t, accounts.EmptyCodeHash, hash)
}

// The provisional marker must not outlive its load: once a load concludes
// "absent" (no cells, no DB record — the no-BAL shape) the EVM has consumed
// that answer, so a later load in the same tx that finds the creator's flush
// must abort as a dependency, not silently adopt the new cell.
func TestAbsentConclusionThenCreatorFlushAborts(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xfc, 0x02})
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	defer ibs.Release(false)
	ibs.txIndex = 9
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
	ws := &WriteSet{}
	ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{
		WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 0}},
		Val:         &accounts.Account{CodeHash: accounts.EmptyCodeHash},
	})
	ws.SetNonce(addr, &VersionedWrite[uint64]{
		WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: Version{TxIndex: 0}},
		Val:         1,
	})
	vm.FlushVersionedWrites(ws, true, "")
	// The read-once fast path serves the repeat probe from the recorded read —
	// consistent with the absence the EVM already consumed, never a silent
	// adoption of the fresh cell — and commit-time validation catches the
	// conflict and re-executes.
	exists, err = ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exists)
	io := NewVersionedIO(10)
	io.RecordReads(Version{TxIndex: 9}, ibs.versionedReads)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(9, io, validateEqualVersion, true, false, false, ""))
}

// A creator flush landing between a load's provisional nil record-probe and
// its re-probe must not abort the loader: the nil was never exposed to the
// EVM, so the load re-reads the fresh cells and reconciles the record.
func TestBALFedReaderSurvivesCreatorFlushMidLoad(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xfd, 0x01})
	vm := NewVersionMap([]*types.AccountChanges{{
		Address: addr,
		NonceChanges: []*types.NonceChange{{
			Index: 1,
			Value: 1,
		}},
		CodeChanges: []*types.CodeChange{{
			Index:    1,
			Bytecode: []byte{0x60, 0x00},
		}},
	}})
	ibs := NewWithVersionMap(&minimalStateReader{}, vm)
	defer ibs.Release(false)
	ibs.txIndex = 9
	_, _, _, err := ibs.getVersionedAccount(addr, false)
	require.NoError(t, err)
	ws := &WriteSet{}
	ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{
		WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 0}},
		Val:         &accounts.Account{CodeHash: accounts.EmptyCodeHash},
	})
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}},
		Val:         uint256.Int{},
	})
	ws.SetNonce(addr, &VersionedWrite[uint64]{
		WriteHeader: WriteHeader{Address: addr, Path: NoncePath, Version: Version{TxIndex: 0}},
		Val:         1,
	})
	ws.SetSelfDestruct(addr, &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath, Version: Version{TxIndex: 0}},
		Val:         false,
	})
	vm.FlushVersionedWrites(ws, true, "")
	require.NotPanics(t, func() {
		so, err := ibs.getStateObject(addr, true)
		require.NoError(t, err)
		require.NotNil(t, so)
		nonce, err := ibs.GetNonce(addr)
		require.NoError(t, err)
		require.EqualValues(t, 1, nonce)
	})
	tr, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.NotNil(t, tr.Val)
	io := NewVersionedIO(10)
	io.RecordReads(Version{TxIndex: 9, Incarnation: 0}, ibs.versionedReads)
	require.Equal(t, VersionValid, vm.ValidateVersion(9, io, validateEqualVersion, true, false, false, ""))
}

// A cold account-field read resolves the account through readAccountInternal;
// when that probe is served from the read set (the reconciled Address entry of
// an earlier load), the field read must be recorded with the entry's
// UNDERLYING source, not the synthetic ReadSetRead — validation rejects
// tx-reads-sourced entries at MVReadResultNone, and since every re-execution
// repeats the same flow, the tx livelocks into "too many validator-invalid
// retries".
func TestColdFieldReadAfterReconciledLoadValidates(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x15, 0x24})
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}
	code := accounts.NewCode([]byte{0x60, 0x00, 0xf3})
	reader := &codeReader{addr: addr, account: &accounts.Account{Nonce: 1, Balance: *uint256.NewInt(5), CodeHash: code.Hash}, code: code.Bytes}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Release(false)
	ibs.SetTxContext(1, 60)
	ibs.SetVersion(0)
	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists)
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, code.Hash, ch)
	if tr, ok := ibs.versionedReads.GetCodeHash(addr); ok {
		require.NotEqual(t, ReadSetRead, tr.Source)
	}
	io := NewVersionedIO(61)
	io.RecordReads(Version{TxIndex: 60}, ibs.versionedReads)
	require.Equal(t, VersionValid, vm.ValidateVersion(60, io, checkVersionEqual, true, false, false, ""))
}

// A DB-present account resolved after an AddressPath map-miss must reconcile
// the recorded nil map-read marker with the loaded record: leaving the nil in
// place spuriously invalidates the reader once a later record cell (e.g. the
// calcFees coinbase record) is flushed at validation time.
func TestGetVersionedAccount_ReconcilesDBLoadedRecordRead(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xdb, 0x01})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(700), true)
	dbAcc := &accounts.Account{Balance: *uint256.NewInt(100), CodeHash: accounts.EmptyCodeHash}
	ibs := NewWithVersionMap(&fixedAccountReader{acc: dbAcc}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	require.NotNil(t, acc)
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *uint256.NewInt(700), bal)
	rd, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.NotNil(t, rd.Val)
	require.NotNil(t, rd.Val.Account())
}

// A recordRead=false record read (delegation resolution, journal reverts)
// still leaves refreshAccount's nil map-read marker in the read set; once the
// DB resolves the account the marker must be reconciled all the same —
// recordRead only means "don't add a read", not "leave a wrong one".
func TestGetStateObject_NoRecordReadStillReconciles(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xdb, 0x02})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(900), true)
	dbAcc := &accounts.Account{Balance: *uint256.NewInt(100), CodeHash: accounts.EmptyCodeHash}
	ibs := NewWithVersionMap(&fixedAccountReader{acc: dbAcc}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	so, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, so)
	rd, ok := ibs.versionedReads.GetAddress(addr)
	require.True(t, ok)
	require.NotNil(t, rd.Val)
	require.NotNil(t, rd.Val.Account())
}

// Cells proving only an EIP-161-empty state must not synthesize: an
// existing-empty account is not gas-equivalent to a non-existent one.
func TestGetVersionedAccount_NoSynthesisForEmpty(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xba, 0x03})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, uint256.Int{}, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	assert.Nil(t, acc)
}

// An estimate (non-Done) cell is a racing speculative write — no synthesis.
func TestGetVersionedAccount_NoSynthesisFromEstimate(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xba, 0x04})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(500), false)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	assert.Nil(t, acc)
}

// A destroyed account (SelfDestruct floor true) must not be synthesized.
func TestGetVersionedAccount_NoSynthesisAfterSelfDestruct(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xba, 0x05})
	mvhm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(500), true)
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 3}, true, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	acc, _, _, err := ibs.getVersionedAccount(addr, true)
	require.NoError(t, err)
	assert.Nil(t, acc)
}

// A created contract synthesized from its BAL code cell carries the code's
// hash and a fresh incarnation, and getStateObject loads the code bytes.
func TestGetStateObject_SynthesizedContractFromBAL(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0xba, 0x06})
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	mvhm.WriteCode(addr, Version{TxIndex: 2}, accounts.NewCode(code), true)
	mvhm.WriteNonce(addr, Version{TxIndex: 2}, 1, true)
	ibs := NewWithVersionMap(&minimalStateReader{}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so)
	assert.Equal(t, accounts.NewCode(code).Hash, so.data.CodeHash)
	assert.Equal(t, uint64(1), so.data.Nonce)
	assert.Equal(t, uint64(1), so.data.Incarnation)
	loaded, err := so.Code()
	require.NoError(t, err)
	assert.Equal(t, code, loaded)
}

// A BAL-funded account with a stale SelfDestructPath flag must not be dropped;
// only a genuinely empty destroyed account is deleted.
func TestGetStateObject_SelfDestructedButBALFunded_StaysAlive(t *testing.T) {
	mvhm := NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x5d, 0x03})
	mvhm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
	mvhm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1000), true)
	dbAcc := &accounts.Account{CodeHash: accounts.EmptyCodeHash}
	ibs := NewWithVersionMap(&fixedAccountReader{acc: dbAcc}, mvhm)
	defer ibs.Release(false)
	ibs.txIndex = 5
	so, err := ibs.getStateObject(addr, true)
	require.NoError(t, err)
	require.NotNil(t, so)
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	assert.Equal(t, *uint256.NewInt(1000), bal)
}

// Under EIP-161 a nil record read is equivalent to a dead one — a dead
// account is EVM-indistinguishable from a non-existent account; a live record
// still fails against either. Pre-EIP-161 existing-empty is observable (CALL
// new-account gas), so the strict form treats nil vs empty as a conflict.
func TestEqAccount_DeadEquivalence(t *testing.T) {
	empty := &accounts.Account{CodeHash: accounts.EmptyCodeHash}
	funded := &accounts.Account{Balance: *uint256.NewInt(5), CodeHash: accounts.EmptyCodeHash}
	vm := NewVersionMap(nil)
	addr := getAddress(89)
	assert.True(t, vm.eqAccountDead(2, addr, false, nil, empty))
	assert.True(t, vm.eqAccountDead(2, addr, false, empty, nil))
	assert.True(t, vm.eqAccountDead(2, addr, false, nil, nil))
	assert.False(t, vm.eqAccountDead(2, addr, false, nil, funded))
	assert.False(t, vm.eqAccountDead(2, addr, false, funded, nil))
	assert.True(t, vm.eqAccountDead(2, addr, false, funded, funded)) //nolint:gocritic
	vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(7), true)
	assert.False(t, vm.eqAccountDead(2, addr, false, nil, empty))
	assert.False(t, eqAccountStrict(nil, empty))
	assert.False(t, eqAccountStrict(empty, nil))
	assert.True(t, eqAccountStrict(empty, empty)) //nolint:gocritic
}

// Pre-EIP-161 a nil storage read validated against a created-empty record cell
// must invalidate (existing-empty is gas-observable); under EIP-161 the same
// read is valid (dead ≡ non-existent).
func TestValidateRead_NilVsEmptyRecordForkAware(t *testing.T) {
	t.Parallel()
	addr := getAddress(88)
	newIO := func() *VersionedIO {
		io := NewVersionedIO(3)
		rs := ReadSet{}
		rs.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		io.RecordReads(Version{TxIndex: 2, Incarnation: 0}, rs)
		return io
	}
	vm := NewVersionMap(nil)
	vm.WriteAddress(addr, Version{TxIndex: 0}, &accounts.Account{CodeHash: accounts.EmptyCodeHash}, true)
	require.Equal(t, VersionValid, vm.ValidateVersion(2, newIO(), validateEqualVersion, true, false, false, ""))
	require.Equal(t, VersionInvalid, vm.ValidateVersion(2, newIO(), validateEqualVersion, false, false, false, ""))
}

// AuRa retains its empty SystemAddress even under EIP-161, and EIP-1052 makes
// exists-empty vs non-existent observable — dead-equivalence must not apply
// there. Non-system addresses keep it, as does the SystemAddress off AuRa.
func TestValidateRead_AuraSystemAddressNotDeadEquivalent(t *testing.T) {
	t.Parallel()
	newIO := func(addr accounts.Address) *VersionedIO {
		io := NewVersionedIO(3)
		rs := ReadSet{}
		rs.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		io.RecordReads(Version{TxIndex: 2, Incarnation: 0}, rs)
		return io
	}
	newVM := func(addr accounts.Address) *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 0}, &accounts.Account{CodeHash: accounts.EmptyCodeHash}, true)
		return vm
	}
	sys := params.SystemAddress
	other := getAddress(180)
	t.Run("aura system address must re-execute", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM(sys).ValidateVersion(2, newIO(sys), validateEqualVersion, true, true, false, ""))
	})
	t.Run("system address off aura stays dead-equivalent", func(t *testing.T) {
		require.Equal(t, VersionValid, newVM(sys).ValidateVersion(2, newIO(sys), validateEqualVersion, true, false, false, ""))
	})
	t.Run("aura non-system address stays dead-equivalent", func(t *testing.T) {
		require.Equal(t, VersionValid, newVM(other).ValidateVersion(2, newIO(other), validateEqualVersion, true, true, false, ""))
	})
}

// The AddressPath record cell holds a creation-time snapshot: an account
// created empty and funded later in the same tx has an EIP-161-empty-shaped
// record next to a non-zero BalancePath cell. Dead-equivalence must assemble
// the sub-field cells before declaring the record dead — a nil read of a
// funded account is stale and must re-execute.
func TestValidateRead_NilReadOfCreatedThenFundedAccountInvalid(t *testing.T) {
	t.Parallel()
	newIO := func(addr accounts.Address) *VersionedIO {
		io := NewVersionedIO(3)
		rs := ReadSet{}
		rs.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		io.RecordReads(Version{TxIndex: 2, Incarnation: 0}, rs)
		return io
	}
	emptyRec := func() *accounts.Account { return &accounts.Account{CodeHash: accounts.EmptyCodeHash} }
	t.Run("balance-funded", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(90)
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 0}, emptyRec(), true)
		vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(55595), true)
		require.Equal(t, VersionInvalid, vm.ValidateVersion(2, newIO(addr), validateEqualVersion, true, false, false, ""))
	})
	t.Run("nonce-funded", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(91)
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 0}, emptyRec(), true)
		vm.WriteNonce(addr, Version{TxIndex: 0}, 1, true)
		require.Equal(t, VersionInvalid, vm.ValidateVersion(2, newIO(addr), validateEqualVersion, true, false, false, ""))
	})
	t.Run("code-funded", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(92)
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 0}, emptyRec(), true)
		vm.WriteCodeHash(addr, Version{TxIndex: 0}, accounts.InternCodeHash(common.Hash{0x01}), true)
		require.Equal(t, VersionInvalid, vm.ValidateVersion(2, newIO(addr), validateEqualVersion, true, false, false, ""))
	})
	t.Run("estimate-balance-not-provably-dead", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(93)
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 0}, emptyRec(), true)
		vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(55595), false)
		require.Equal(t, VersionInvalid, vm.ValidateVersion(2, newIO(addr), validateEqualVersion, true, false, false, ""))
	})
	t.Run("zero-valued-subcells-stay-dead", func(t *testing.T) {
		t.Parallel()
		addr := getAddress(94)
		vm := NewVersionMap(nil)
		vm.WriteAddress(addr, Version{TxIndex: 0}, emptyRec(), true)
		vm.WriteBalance(addr, Version{TxIndex: 0}, uint256.Int{}, true)
		vm.WriteCodeHash(addr, Version{TxIndex: 0}, accounts.EmptyCodeHash, true)
		require.Equal(t, VersionValid, vm.ValidateVersion(2, newIO(addr), validateEqualVersion, true, false, false, ""))
	})
}

// A nil AddressPath storage read of a created-then-destroyed account stays
// valid: the Incarnation cell belongs to a dead account. A later revival makes
// the same nil read stale again.
func TestValidateRead_NilReadOfDestroyedAccountStaysValid(t *testing.T) {
	t.Parallel()
	addr := getAddress(77)
	newIO := func() *VersionedIO {
		io := NewVersionedIO(4)
		rs := ReadSet{}
		rs.SetAddress(addr, VersionedRead[AccountView]{
			ReadHeader: ReadHeader{Source: StorageRead, Version: UnknownVersion},
		})
		io.RecordReads(Version{TxIndex: 3, Incarnation: 0}, rs)
		return io
	}
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	valid := vm.ValidateVersion(3, newIO(), validateEqualVersion, true, false, false, "")
	require.Equal(t, VersionValid, valid)
	vm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(100), true)
	valid = vm.ValidateVersion(3, newIO(), validateEqualVersion, true, false, false, "")
	require.Equal(t, VersionInvalid, valid)
}

// A same-tx SSTORE + SELFDESTRUCT flushes the slot cell and SD=true at the
// SAME index; the read path treats such a slot as wiped, so validation must
// too. The wiped-read twin guards the livelock: a re-executed reader's zero
// is recorded at the cell's own version and must stay valid.
func TestValidateRead_SameIndexSDStorage(t *testing.T) {
	addr := getAddress(210)
	key := accounts.InternKey(common.BigToHash(big.NewInt(1)))
	val := *uint256.NewInt(5)
	newVM := func() *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteStorage(addr, key, Version{TxIndex: 2}, val, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
		vm.WriteIncarnation(addr, Version{TxIndex: 2}, 1, true)
		return vm
	}
	newIO := func(readVer Version, readVal uint256.Int) *VersionedIO {
		io := NewVersionedIO(6)
		rs := ReadSet{}
		rs.SetStorage(addr, key, VersionedRead[uint256.Int]{ReadHeader: ReadHeader{Source: MapRead, Version: readVer}, Val: readVal})
		io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)
		return io
	}
	t.Run("version-match stale read invalidates", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(5, newIO(Version{TxIndex: 2}, val), validateEqualVersion, true, false, false, ""))
	})
	t.Run("value-tiebreaker stale read invalidates", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(5, newIO(Version{TxIndex: 0}, val), validateEqualVersion, true, false, false, ""))
	})
	t.Run("wiped zero read at the cell version stays valid", func(t *testing.T) {
		require.Equal(t, VersionValid, newVM().ValidateVersion(5, newIO(Version{TxIndex: 2}, uint256.Int{}), validateEqualVersion, true, false, false, ""))
	})
}

// Account revival is not code revival: deploy, destruct, then transfer-revival
// leaves the dead bytes as the only CodePath cell — reads of them are stale.
// The wiped-read twin guards the livelock: the trump's empty read recorded at
// the code cell's version must stay valid.
func TestValidateRead_CodeReadRevivedWithoutCode(t *testing.T) {
	addr := getAddress(211)
	code := []byte{0x60, 0x00}
	newVM := func() *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteCode(addr, Version{TxIndex: 1}, accounts.NewCode(code), true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
		vm.WriteIncarnation(addr, Version{TxIndex: 2}, 1, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 3}, false, true)
		vm.WriteAddress(addr, Version{TxIndex: 3}, &accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}, true)
		vm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1), true)
		return vm
	}
	newIO := func(hdr ReadHeader, readVal []byte) *VersionedIO {
		io := NewVersionedIO(6)
		rs := ReadSet{}
		rs.SetCode(addr, VersionedRead[[]byte]{ReadHeader: hdr, Val: readVal})
		io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)
		return io
	}
	t.Run("cold read of dead bytes invalidates", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(5, newIO(ReadHeader{Source: StorageRead, Version: UnknownVersion}, code), validateEqualVersion, true, false, false, ""))
	})
	t.Run("version-match read of dead bytes invalidates", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM().ValidateVersion(5, newIO(ReadHeader{Source: MapRead, Version: Version{TxIndex: 1}}, code), validateEqualVersion, true, false, false, ""))
	})
	t.Run("wiped empty read at the cell version stays valid", func(t *testing.T) {
		require.Equal(t, VersionValid, newVM().ValidateVersion(5, newIO(ReadHeader{Source: MapRead, Version: Version{TxIndex: 1}}, nil), validateEqualVersion, true, false, false, ""))
	})
}

// The pre-EIP-6780 metamorphic shape end to end (cells @1, SD=true @2,
// transfer-revival @3, reader @5): every conclusion the EVM consumes needs a
// recorded witness that validation accepts as long as the conclusion holds —
// wiped reads serve zero/empty, validate cleanly (no livelock), and a
// post-read redeploy flush must invalidate the code conclusion.
func TestMetamorphicShadowedDestruct_ReaderValidatorRoundTrip(t *testing.T) {
	code := []byte{0x60, 0x00}
	newVM := func(addr accounts.Address, withBalanceRevival bool) *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteCode(addr, Version{TxIndex: 1}, accounts.NewCode(code), true)
		vm.WriteCodeHash(addr, Version{TxIndex: 1}, accounts.InternCodeHash([32]byte{0xaa}), true)
		vm.WriteCodeSize(addr, Version{TxIndex: 1}, len(code), true)
		vm.WriteNonce(addr, Version{TxIndex: 1}, 1, true)
		vm.WriteStorage(addr, accounts.InternKey(common.BigToHash(big.NewInt(1))), Version{TxIndex: 1}, *uint256.NewInt(5), true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
		vm.WriteIncarnation(addr, Version{TxIndex: 2}, 1, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 3}, false, true)
		vm.WriteAddress(addr, Version{TxIndex: 3}, &accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}, true)
		if withBalanceRevival {
			vm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1), true)
		}
		return vm
	}
	newIBS := func(addr accounts.Address, vm *VersionMap) *IntraBlockState {
		ibs := NewWithVersionMap(&emptyReader{}, vm)
		t.Cleanup(func() { ibs.Release(false) })
		ibs.SetTxContext(0, 5)
		ibs.SetVersion(0)
		return ibs
	}
	validate := func(vm *VersionMap, ibs *IntraBlockState) VersionValidity {
		io := NewVersionedIO(6)
		io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, ibs.versionedReads)
		return vm.ValidateVersion(5, io, validateEqualVersion, true, false, false, "")
	}
	t.Run("code read is witnessed and a redeploy flush invalidates it", func(t *testing.T) {
		addr := getAddress(220)
		vm := newVM(addr, true)
		ibs := newIBS(addr, vm)
		got, err := ibs.GetCode(addr)
		require.NoError(t, err)
		require.Empty(t, got)
		_, recorded := ibs.versionedReads.GetCode(addr)
		require.True(t, recorded)
		require.Equal(t, VersionValid, validate(vm, ibs))
		vm.WriteCode(addr, Version{TxIndex: 4}, accounts.NewCode(code), true)
		require.Equal(t, VersionInvalid, validate(vm, ibs))
	})
	t.Run("nonce read serves the wipe and validates", func(t *testing.T) {
		addr := getAddress(221)
		vm := newVM(addr, true)
		ibs := newIBS(addr, vm)
		nonce, err := ibs.GetNonce(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(0), nonce)
		require.Equal(t, VersionValid, validate(vm, ibs))
	})
	t.Run("code size read serves the wipe and validates", func(t *testing.T) {
		addr := getAddress(222)
		vm := newVM(addr, true)
		ibs := newIBS(addr, vm)
		size, err := ibs.GetCodeSize(addr)
		require.NoError(t, err)
		require.Equal(t, 0, size)
		require.Equal(t, VersionValid, validate(vm, ibs))
	})
	t.Run("code hash read serves the wipe and validates", func(t *testing.T) {
		addr := getAddress(223)
		vm := newVM(addr, true)
		ibs := newIBS(addr, vm)
		ch, err := ibs.GetCodeHash(addr)
		require.NoError(t, err)
		require.True(t, ch.IsEmpty() || ch.IsZero())
		require.Equal(t, VersionValid, validate(vm, ibs))
	})
	t.Run("storage wiped-zero read validates without livelock", func(t *testing.T) {
		addr := getAddress(224)
		key := accounts.InternKey(common.BigToHash(big.NewInt(1)))
		vm := newVM(addr, true)
		ibs := newIBS(addr, vm)
		v, err := ibs.GetState(addr, key)
		require.NoError(t, err)
		require.True(t, v.IsZero())
		require.Equal(t, VersionValid, validate(vm, ibs))
	})
}

// A balance-only revival (withdrawal/reward-style AddBalance, no account
// record write) leaves no code/codeHash cells, so the committed fall-through
// must still resolve the destruct: the account is alive with wiped code —
// hash keccak(”), size 0 — and the recorded reads must validate, or the
// reader retries the same conclusion forever.
func TestCommittedReadsAfterDestructWithBalanceOnlyRevival(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xc0, 0xde})
	reader := &codeReader{addr: addr, account: &accounts.Account{Nonce: 1, CodeHash: accounts.InternCodeHash([32]byte{0xaa})}, code: []byte{0x60, 0x00}}
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 1}, 1, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, false, true)
	vm.WriteBalance(addr, Version{TxIndex: 2}, *uint256.NewInt(1), true)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Release(false)
	ibs.SetTxContext(1, 3)
	ibs.SetVersion(0)
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, ch)
	size, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	require.Equal(t, 0, size)
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(1), bal)
	io := NewVersionedIO(4)
	io.RecordReads(Version{TxIndex: 3, Incarnation: 0}, ibs.versionedReads)
	require.Equal(t, VersionValid, vm.ValidateVersion(3, io, validateEqualVersion, true, false, false, ""))
}

// A Done CodeHash cell below a shadowed destruct must not be served to a
// later reader: the wiped hash of an account still alive (revived balance-
// only) is keccak(”), nil only while it stays dead.
func TestCodeHashCellBelowShadowedDestruct(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xc0, 0xdf})
	vm := NewVersionMap(nil)
	vm.WriteCodeHash(addr, Version{TxIndex: 1}, accounts.InternCodeHash([32]byte{0xaa}), true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 2}, true, true)
	vm.WriteIncarnation(addr, Version{TxIndex: 2}, 1, true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 3}, false, true)
	vm.WriteBalance(addr, Version{TxIndex: 3}, *uint256.NewInt(1), true)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Release(false)
	ibs.SetTxContext(1, 4)
	ibs.SetVersion(0)
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, ch)
	io := NewVersionedIO(5)
	io.RecordReads(Version{TxIndex: 4, Incarnation: 0}, ibs.versionedReads)
	require.Equal(t, VersionValid, vm.ValidateVersion(4, io, validateEqualVersion, true, false, false, ""))
}

// A transaction can consume wipes from TWO distinct destructs of the same
// address (different slots, destroy/recreate cycles). Each wiped read records
// its destruct witness; both must be retained and validated — if the later
// witness silently replaces the earlier one (or vice versa), re-executing the
// lost destruct away leaves a stale wiped read undetected.
func TestValidateRead_DistinctDestructWitnessesBothValidated(t *testing.T) {
	addr := getAddress(230)
	keyA := accounts.InternKey(common.BigToHash(big.NewInt(1)))
	val := *uint256.NewInt(7)
	newVM := func(withSecondDestruct bool) *VersionMap {
		vm := NewVersionMap(nil)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 10}, true, true)
		vm.WriteIncarnation(addr, Version{TxIndex: 10}, 1, true)
		vm.WriteSelfDestruct(addr, Version{TxIndex: 11}, false, true)
		vm.WriteAddress(addr, Version{TxIndex: 11}, &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}, true)
		vm.WriteStorage(addr, keyA, Version{TxIndex: 15}, val, true)
		if withSecondDestruct {
			vm.WriteSelfDestruct(addr, Version{TxIndex: 20}, true, true)
			vm.WriteIncarnation(addr, Version{TxIndex: 20}, 2, true)
			vm.WriteSelfDestruct(addr, Version{TxIndex: 21}, false, true)
			vm.WriteAddress(addr, Version{TxIndex: 21}, &accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}, true)
		}
		return vm
	}
	newIO := func() *VersionedIO {
		io := NewVersionedIO(31)
		rs := ReadSet{}
		// Slot A read as the post-tx20 wipe: zero, floored at the pre-wipe cell.
		rs.SetStorage(addr, keyA, VersionedRead[uint256.Int]{
			ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 15}},
		})
		// Witness of the tx20 destruct, recorded first...
		rs.SetSelfDestruct(addr, VersionedRead[bool]{
			ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 20}},
			Val:        true,
		})
		// ...then a wiped read of another slot records the tx10 destruct.
		rs.SetSelfDestruct(addr, VersionedRead[bool]{
			ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: 10}},
			Val:        true,
		})
		io.RecordReads(Version{TxIndex: 30, Incarnation: 0}, rs)
		return io
	}
	t.Run("both destructs present validates", func(t *testing.T) {
		require.Equal(t, VersionValid, newVM(true).ValidateVersion(30, newIO(), validateEqualVersion, true, false, false, ""))
	})
	t.Run("the tx20 destruct re-executed away must invalidate", func(t *testing.T) {
		require.Equal(t, VersionInvalid, newVM(false).ValidateVersion(30, newIO(), validateEqualVersion, true, false, false, ""))
	})
}
