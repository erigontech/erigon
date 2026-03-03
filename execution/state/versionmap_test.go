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
