package state

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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
		return fmt.Appendf(nil, "%ver:%ver:%ver", txIdx*5, txIdx+inc, inc*5)
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

func TestHelperFunctions(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)
	ap2 := getAddress(2)

	mvh := NewVersionMap(nil)

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 1}, valueFor(AddressPath, 0, 1), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 2}, valueFor(AddressPath, 0, 2), true)
	res := mvh.Read(ap1, AddressPath, accounts.NilKey, 0)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 1, 1}, valueFor(AddressPath, 1, 1), true)
	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 1, 2}, valueFor(AddressPath, 1, 2), true)
	res = mvh.Read(ap2, AddressPath, accounts.NilKey, 1)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 2, 1}, valueFor(AddressPath, 2, 1), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 2, 2}, valueFor(AddressPath, 2, 2), true)
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 2)
	require.Equal(t, 0, res.DepIdx())
	require.Equal(t, 2, res.Incarnation())
	require.Equal(t, valueFor(AddressPath, 0, 2), res.Value())
	require.Equal(t, 0, res.Status())
}

func TestFlushMVWrite(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)
	ap2 := getAddress(2)

	mvh := NewVersionMap(nil)

	var res ReadResult

	wd := VersionedWrites{}

	wd = append(wd, &VersionedWrite{
		Address: ap1,
		Path:    AddressPath,
		Version: Version{0, 0, 0, 1},
		ValAcc: valueFor(AddressPath, 0, 1).(*accounts.Account),
	}, &VersionedWrite{
		Address: ap1,
		Path:    AddressPath,
		Version: Version{0, 0, 0, 2},
		ValAcc: valueFor(AddressPath, 0, 2).(*accounts.Account),
	}, &VersionedWrite{
		Address: ap2,
		Path:    AddressPath,
		Version: Version{0, 0, 1, 1},
		ValAcc: valueFor(AddressPath, 1, 1).(*accounts.Account),
	}, &VersionedWrite{
		Address: ap2,
		Path:    AddressPath,
		Version: Version{0, 0, 1, 2},
		ValAcc: valueFor(AddressPath, 1, 2).(*accounts.Account),
	}, &VersionedWrite{
		Address: ap1,
		Path:    AddressPath,
		Version: Version{0, 0, 2, 1},
		ValAcc: valueFor(AddressPath, 2, 1).(*accounts.Account),
	}, &VersionedWrite{
		Address: ap1,
		Path:    AddressPath,
		Version: Version{0, 0, 2, 2},
		ValAcc: valueFor(AddressPath, 2, 2).(*accounts.Account),
	})

	mvh.FlushVersionedWrites(wd, true, "")

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 0)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	res = mvh.Read(ap2, AddressPath, accounts.NilKey, 1)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 2)
	require.Equal(t, 0, res.DepIdx())
	require.Equal(t, 2, res.Incarnation())
	require.Equal(t, valueFor(AddressPath, 0, 2), res.Value())
	require.Equal(t, 0, res.Status())
}

// TODO - handle panic

func TestLowerIncarnation(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)

	mvh := NewVersionMap(nil)

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 2}, valueFor(AddressPath, 0, 2), true)
	mvh.Read(ap1, AddressPath, accounts.NilKey, 0)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 2}, valueFor(AddressPath, 1, 2), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 5}, valueFor(AddressPath, 0, 5), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 5}, valueFor(AddressPath, 1, 5), true)
}

func TestMarkEstimate(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)

	mvh := NewVersionMap(nil)

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 7, 2}, valueFor(AddressPath, 7, 2), true)
	mvh.MarkEstimate(ap1, AddressPath, accounts.NilKey, 7)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 7, 4}, valueFor(AddressPath, 7, 4), true)
}

func TestMVHashMapBasics(t *testing.T) {
	t.Parallel()

	// memory locations
	ap1 := getAddress(1)
	ap2 := getAddress(2)
	ap3 := getAddress(3)

	mvh := NewVersionMap(nil)

	res := mvh.Read(ap1, AddressPath, accounts.NilKey, 5)
	require.Equal(t, UnknownDep, res.depIdx)

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 10, 1}, valueFor(AddressPath, 10, 1), true)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 9)
	require.Equal(t, UnknownDep, res.depIdx, "reads that should go the the DB return dependency -2")
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 10)
	require.Equal(t, UnknownDep, res.depIdx, "Read returns entries from smaller txns, not txn 10")

	// Reads for a higher txn return the entry written by txn 10.
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 10, res.depIdx, "reads for a higher txn return the entry written by txn 10.")
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 10, 1), res.value)

	// More writes.
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 12, 0}, valueFor(AddressPath, 12, 0), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 8, 3}, valueFor(AddressPath, 8, 3), true)

	// Verify reads.
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 12, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 12, 0), res.value)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 10, 1), res.value)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 10)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 8, 3), res.value)

	// Mark the entry written by 10 as an estimate.
	mvh.MarkEstimate(ap1, AddressPath, accounts.NilKey, 10)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, -1, res.incarnation, "dep at tx 10 is now an estimate")

	// Delete the entry written by 10, write to a different ap.
	mvh.Delete(ap1, AddressPath, accounts.NilKey, 10, true)
	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 10, 2}, valueFor(AddressPath, 10, 2), true)

	// Read by txn 11 no longer observes entry from txn 10.
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 8, 3), res.value)

	// Reads, writes for ap2 and ap3.
	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 5, 0}, valueFor(AddressPath, 5, 0), true)
	mvh.Write(ap3, AddressPath, accounts.NilKey, Version{0, 0, 20, 4}, valueFor(AddressPath, 20, 4), true)

	res = mvh.Read(ap2, AddressPath, accounts.NilKey, 10)
	require.Equal(t, 5, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 5, 0), res.value)

	res = mvh.Read(ap3, AddressPath, accounts.NilKey, 21)
	require.Equal(t, 20, res.depIdx)
	require.Equal(t, 4, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 20, 4), res.value)

	// Clear ap1 and ap3.
	mvh.Delete(ap1, AddressPath, accounts.NilKey, 12, true)
	mvh.Delete(ap1, AddressPath, accounts.NilKey, 8, true)
	mvh.Delete(ap3, AddressPath, accounts.NilKey, 20, true)

	// Reads from ap1 and ap3 go to db.
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 30)
	require.Equal(t, UnknownDep, res.depIdx)

	res = mvh.Read(ap3, AddressPath, accounts.NilKey, 30)
	require.Equal(t, UnknownDep, res.depIdx)

	// No-op delete at ap2 - doesn't panic because ap2 does exist
	mvh.Delete(ap2, AddressPath, accounts.NilKey, 11, true)

	// Read entry by txn 10 at ap2.
	res = mvh.Read(ap2, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, 2, res.incarnation)
	require.Equal(t, valueFor(AddressPath, 10, 2), res.value)
}

// TestValidateRead_HasBAL_NoBypassForAddressPath verifies that when HasBAL is
// true, AddressPath is NOT bypassed — a new MVReadResultDone entry on
// AddressPath means a real state change (e.g. account creation) from a
// concurrent worker, and the read must be invalidated.
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
	vm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(AddressPath, 0, 1), true)

	// Tx 2 originally read from storage (no map entry at execution time).
	io := NewVersionedIO(2)
	rs := ReadSet{}
	rs.Set(VersionedRead{
		Address: addr,
		Path:    AddressPath,
		Source:  StorageRead,
		Version: Version{TxIndex: 2, Incarnation: 1},
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
			vm.Write(addr, path, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(path, 0, 1), true)

			// Tx 2 originally read from storage (no map entry).
			io := NewVersionedIO(2)
			rs := ReadSet{}
			rs.Set(VersionedRead{
				Address: addr,
				Path:    path,
				Source:  StorageRead,
				Version: Version{TxIndex: 2, Incarnation: 1},
			})
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
		mvh2.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, idx, 1}, valueFor(AddressPath, idx, 1), true)
	}
}

func BenchmarkReadTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)
	txIdxSlice := []int{}

	for b.Loop() {
		txIdx := rand.Intn(1000000000000000)
		txIdxSlice = append(txIdxSlice, txIdx)
		mvh2.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, txIdx, 1}, valueFor(AddressPath, txIdx, 1), true)
	}

	b.ResetTimer()

	for _, value := range txIdxSlice {
		mvh2.Read(ap2, AddressPath, accounts.NilKey, value)
	}
}

func TestTimeComplexity(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	// for 1000000 read and write with no dependency at different memory location
	mvh1 := NewVersionMap(nil)

	for i := 0; i < 1000000; i++ {
		ap1 := getAddress(i)
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
		mvh1.Read(ap1, AddressPath, accounts.NilKey, i)
	}

	// for 1000000 read and write with dependency at same memory location
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)

	for i := 0; i < 1000000; i++ {
		mvh2.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
		mvh2.Read(ap2, AddressPath, accounts.NilKey, i)
	}
}

func TestWriteTimeSameLocationDifferentTxnIdx(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	for i := 0; i < 1000000; i++ {
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
	}
}

func TestWriteTimeSameLocationSameTxnIdx(t *testing.T) {
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	for i := 0; i < 1000000; i++ {
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, i}, valueFor(AddressPath, i, 1), true)
	}
}

func TestWriteTimeDifferentLocation(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	mvh1 := NewVersionMap(nil)

	for i := 0; i < 1000000; i++ {
		ap1 := getAddress(i)
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(AddressPath, i, 1), true)
	}
}

func TestReadTimeSameLocation(t *testing.T) {
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 1}, valueFor(AddressPath, 1, 1), true)

	for i := 0; i < 1000000; i++ {
		mvh1.Read(ap1, AddressPath, accounts.NilKey, 2)
	}
}

// TestValuesEqual_StoragePath verifies that valuesEqual handles StoragePath
// (uint256.Int values) correctly. Without this, storage reads that matched the
// versionMap value were incorrectly invalidated (falling through to the default
// case which always returns false), causing livelocks in dense blocks.
func TestValuesEqual_StoragePath(t *testing.T) {
	t.Parallel()

	a := *uint256.NewInt(42)
	b := *uint256.NewInt(42)
	c := *uint256.NewInt(99)

	require.True(t, valuesEqual(StoragePath, a, b), "same storage values should be equal")
	require.False(t, valuesEqual(StoragePath, a, c), "different storage values should not be equal")
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
	vm.Write(addr, StoragePath, storageKey, Version{TxIndex: 5, Incarnation: 1}, storageVal, true)

	// TX 10 originally read from storage (no versionMap entry at execution
	// time) and got value 100 — the same value TX 5 later wrote.
	io := NewVersionedIO(10)
	rs := ReadSet{}
	rs.Set(VersionedRead{
		Address: addr,
		Path:    StoragePath,
		Key:     storageKey,
		Source:  StorageRead,
		Version: Version{TxIndex: UnknownDep, Incarnation: -1},
		ValU256:     storageVal,
	})
	io.RecordReads(Version{TxIndex: 10, Incarnation: 1}, rs)

	valid := vm.ValidateVersion(10, io, checkVersionEqual, false, "")
	require.Equal(t, VersionValid, valid,
		"StoragePath read with matching value should be valid via tiebreaker")

	// Now test with a DIFFERENT value — should be invalid.
	vm2 := NewVersionMap(nil)
	vm2.Write(addr, StoragePath, storageKey, Version{TxIndex: 5, Incarnation: 1}, *uint256.NewInt(999), true)

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
	writes := VersionedWrites{
		{Address: addr, Path: BalancePath, Key: accounts.NilKey,
			Version: Version{TxIndex: 5, Incarnation: 1}, ValU256: *uint256.NewInt(100)},
	}
	vm.FlushVersionedWrites(writes, true, "")

	// TX 10 reads should see FlagDone → MVReadResultDone.
	res := vm.Read(addr, BalancePath, accounts.NilKey, 10)
	require.Equal(t, MVReadResultDone, res.Status(),
		"valid TX flush should produce Done entries, not Estimate")
	require.Equal(t, 5, res.DepIdx())
	require.Equal(t, 1, res.Incarnation())

	// Simulate: TX 7 is invalid, flushed as Estimate (complete=false).
	writes2 := VersionedWrites{
		{Address: addr, Path: NoncePath, Key: accounts.NilKey,
			Version: Version{TxIndex: 7, Incarnation: 2}, ValU64: uint64(5)},
	}
	vm.FlushVersionedWrites(writes2, false, "")

	// TX 10 reads NoncePath should see FlagEstimate → MVReadResultDependency.
	res2 := vm.Read(addr, NoncePath, accounts.NilKey, 10)
	require.Equal(t, MVReadResultDependency, res2.Status(),
		"invalid TX flush should produce Estimate entries")
	require.Equal(t, 7, res2.DepIdx())
}
