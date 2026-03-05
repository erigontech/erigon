package state

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var randomness = rand.Intn(10) + 10

// create test data for a given txIdx and incarnation
func valueFor(txIdx, inc int) []byte {
	return fmt.Appendf(nil, "%ver:%ver:%ver", txIdx*5, txIdx+inc, inc*5)
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

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 1}, valueFor(0, 1), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 2}, valueFor(0, 2), true)
	res := mvh.Read(ap1, AddressPath, accounts.NilKey, 0)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 1, 1}, valueFor(1, 1), true)
	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 1, 2}, valueFor(1, 2), true)
	res = mvh.Read(ap2, AddressPath, accounts.NilKey, 1)
	require.Equal(t, UnknownDep, res.DepIdx())
	require.Equal(t, -1, res.Incarnation())
	require.Equal(t, 2, res.Status())

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 2, 1}, valueFor(2, 1), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 2, 2}, valueFor(2, 2), true)
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 2)
	require.Equal(t, 0, res.DepIdx())
	require.Equal(t, 2, res.Incarnation())
	require.Equal(t, valueFor(0, 2), res.Value().([]byte))
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
		Val:     valueFor(0, 1),
	}, &VersionedWrite{
		Address: ap1,
		Path:    AddressPath,
		Version: Version{0, 0, 0, 2},
		Val:     valueFor(0, 2),
	}, &VersionedWrite{
		Address: ap2,
		Path:    AddressPath,
		Version: Version{0, 0, 1, 1},
		Val:     valueFor(1, 1),
	}, &VersionedWrite{
		Address: ap2,
		Path:    AddressPath,
		Version: Version{0, 0, 1, 2},
		Val:     valueFor(1, 2),
	}, &VersionedWrite{
		Address: ap1,
		Path:    AddressPath,
		Version: Version{0, 0, 2, 1},
		Val:     valueFor(2, 1),
	}, &VersionedWrite{
		Address: ap1,
		Path:    AddressPath,
		Version: Version{0, 0, 2, 2},
		Val:     valueFor(2, 2),
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
	require.Equal(t, valueFor(0, 2), res.Value().([]byte))
	require.Equal(t, 0, res.Status())
}

// TODO - handle panic

func TestLowerIncarnation(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)

	mvh := NewVersionMap(nil)

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 2}, valueFor(0, 2), true)
	mvh.Read(ap1, AddressPath, accounts.NilKey, 0)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 2}, valueFor(1, 2), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 0, 5}, valueFor(0, 5), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 5}, valueFor(1, 5), true)
}

func TestMarkEstimate(t *testing.T) {
	t.Parallel()

	ap1 := getAddress(1)

	mvh := NewVersionMap(nil)

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 7, 2}, valueFor(7, 2), true)
	mvh.MarkEstimate(ap1, AddressPath, accounts.NilKey, 7)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 7, 4}, valueFor(7, 4), true)
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

	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 10, 1}, valueFor(10, 1), true)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 9)
	require.Equal(t, UnknownDep, res.depIdx, "reads that should go the the DB return dependency -2")
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 10)
	require.Equal(t, UnknownDep, res.depIdx, "Read returns entries from smaller txns, not txn 10")

	// Reads for a higher txn return the entry written by txn 10.
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 10, res.depIdx, "reads for a higher txn return the entry written by txn 10.")
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(10, 1), res.value)

	// More writes.
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 12, 0}, valueFor(12, 0), true)
	mvh.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 8, 3}, valueFor(8, 3), true)

	// Verify reads.
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 15)
	require.Equal(t, 12, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(12, 0), res.value)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, 1, res.incarnation)
	require.Equal(t, valueFor(10, 1), res.value)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 10)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(8, 3), res.value)

	// Mark the entry written by 10 as an estimate.
	mvh.MarkEstimate(ap1, AddressPath, accounts.NilKey, 10)

	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 10, res.depIdx)
	require.Equal(t, -1, res.incarnation, "dep at tx 10 is now an estimate")

	// Delete the entry written by 10, write to a different ap.
	mvh.Delete(ap1, AddressPath, accounts.NilKey, 10, true)
	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 10, 2}, valueFor(10, 2), true)

	// Read by txn 11 no longer observes entry from txn 10.
	res = mvh.Read(ap1, AddressPath, accounts.NilKey, 11)
	require.Equal(t, 8, res.depIdx)
	require.Equal(t, 3, res.incarnation)
	require.Equal(t, valueFor(8, 3), res.value)

	// Reads, writes for ap2 and ap3.
	mvh.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, 5, 0}, valueFor(5, 0), true)
	mvh.Write(ap3, AddressPath, accounts.NilKey, Version{0, 0, 20, 4}, valueFor(20, 4), true)

	res = mvh.Read(ap2, AddressPath, accounts.NilKey, 10)
	require.Equal(t, 5, res.depIdx)
	require.Equal(t, 0, res.incarnation)
	require.Equal(t, valueFor(5, 0), res.value)

	res = mvh.Read(ap3, AddressPath, accounts.NilKey, 21)
	require.Equal(t, 20, res.depIdx)
	require.Equal(t, 4, res.incarnation)
	require.Equal(t, valueFor(20, 4), res.value)

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
	require.Equal(t, valueFor(10, 2), res.value)
}

// TestValidateRead_HasBAL_BypassForPrePopulatedPaths verifies that when
// HasBAL is true, a StorageRead that now finds a MVReadResultDone entry on a
// BAL-prepopulated path (BalancePath, NoncePath, CodePath, StoragePath) is
// considered valid — the entry is a BAL-filtered no-op write and the original
// storage read value is still correct.
func TestValidateRead_HasBAL_BypassForPrePopulatedPaths(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	for _, path := range []AccountPath{BalancePath, NoncePath, CodePath, StoragePath} {
		t.Run(path.String(), func(t *testing.T) {
			vm := NewVersionMap(nil)
			vm.HasBAL = true

			// Simulate a BAL-prepopulated write at txIndex 0, incarnation 1.
			key := accounts.NilKey
			if path == StoragePath {
				key = accounts.InternKey(common.BigToHash(big.NewInt(1)))
			}
			vm.Write(addr, path, key, Version{TxIndex: 0, Incarnation: 1}, valueFor(0, 1), true)

			// Build a VersionedIO where tx 2 read from storage (no map entry
			// at execution time) with its own version as the read version.
			io := NewVersionedIO(2)
			rs := ReadSet{}
			rs.Set(VersionedRead{
				Address: addr,
				Path:    path,
				Key:     key,
				Source:  StorageRead,
				Version: Version{TxIndex: 2, Incarnation: 1},
			})
			io.RecordReads(Version{TxIndex: 2, Incarnation: 1}, rs)

			valid := vm.ValidateVersion(2, io, checkVersionEqual, false, "")
			require.Equal(t, VersionValid, valid,
				"HasBAL should bypass invalidation for %s when entry is from BAL pre-population", path)
		})
	}
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
	vm.Write(addr, AddressPath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(0, 1), true)

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
			vm.Write(addr, path, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(0, 1), true)

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

// TestValidateRead_HasBAL_AddressPathCrossCheckWithBalancePath verifies that
// when HasBAL is true and an AddressPath read returns MVReadResultNone (no
// entry in the version map), but BalancePath has a MVReadResultDone entry
// (from BAL pre-population), the AddressPath read is invalidated.
// See also TestValidateRead_NoHasBAL_AddressPathCrossCheckWithBalancePath
// for the same check without HasBAL (worker flush case).
func TestValidateRead_HasBAL_AddressPathCrossCheckWithBalancePath(t *testing.T) {
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

	// BAL pre-populated a BalancePath entry at txIndex 0 (simulating account
	// creation by a prior tx that set a balance).
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(0, 1), true)

	// No AddressPath entry exists (it's not BAL-pre-populated).
	// Tx 2 read AddressPath from storage during execution.
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
		"AddressPath read should be invalidated when BAL has a BalancePath entry from a prior tx (account may have been created)")
}

// TestValidateRead_NoHasBAL_AddressPathCrossCheckWithBalancePath verifies that
// even without HasBAL (no stored BAL body, e.g. p2p blocks), the BalancePath
// cross-check still catches stale AddressPath reads. This is the key fix:
// worker flushes create BalancePath entries that must be checked regardless
// of whether BAL pre-population was used.
func TestValidateRead_NoHasBAL_AddressPathCrossCheckWithBalancePath(t *testing.T) {
	t.Parallel()

	addr := getAddress(42)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil) // HasBAL = false
	require.False(t, vm.HasBAL)

	// A concurrent worker flushed a BalancePath entry at txIndex 0
	// (simulating account creation by a prior tx).
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 1}, valueFor(0, 1), true)

	// No AddressPath entry exists.
	// Tx 2 read AddressPath from storage during execution (got "account doesn't exist").
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
		"without HasBAL, AddressPath read should still be invalidated when BalancePath has an entry from a worker flush")
}

func BenchmarkWriteTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)

	randInts := []int{}
	for i := 0; i < b.N; i++ {
		randInts = append(randInts, rand.Intn(1000000000000000))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mvh2.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, randInts[i], 1}, valueFor(randInts[i], 1), true)
	}
}

func BenchmarkReadTimeSameLocationDifferentTxIdx(b *testing.B) {
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)
	txIdxSlice := []int{}

	for b.Loop() {
		txIdx := rand.Intn(1000000000000000)
		txIdxSlice = append(txIdxSlice, txIdx)
		mvh2.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, txIdx, 1}, valueFor(txIdx, 1), true)
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
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(i, 1), true)
		mvh1.Read(ap1, AddressPath, accounts.NilKey, i)
	}

	// for 1000000 read and write with dependency at same memory location
	mvh2 := NewVersionMap(nil)
	ap2 := getAddress(2)

	for i := 0; i < 1000000; i++ {
		mvh2.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(i, 1), true)
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
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(i, 1), true)
	}
}

func TestWriteTimeSameLocationSameTxnIdx(t *testing.T) {
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	for i := 0; i < 1000000; i++ {
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, i}, valueFor(i, 1), true)
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
		mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, i, 1}, valueFor(i, 1), true)
	}
}

func TestReadTimeSameLocation(t *testing.T) {
	t.Parallel()

	mvh1 := NewVersionMap(nil)
	ap1 := getAddress(1)

	mvh1.Write(ap1, AddressPath, accounts.NilKey, Version{0, 0, 1, 1}, valueFor(1, 1), true)

	for i := 0; i < 1000000; i++ {
		mvh1.Read(ap1, AddressPath, accounts.NilKey, 2)
	}
}
