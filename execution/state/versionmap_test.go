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

// TestBALPrePop_SameSenderTxs_NoConflicts simulates an EIP-7928 BAL where N
// transactions from the same sender each write BalancePath and NoncePath, and
// fees accrue to a single coinbase. With the BAL pre-populated into the
// VersionMap at FlagDone, every parallel worker should read the correct
// floor-N-1 value for BalancePath/NoncePath via the btree, and validation
// should produce zero conflicts.
//
// Worker reads simulated per tx K (K = 0..N-1):
//   - sender.AddressPath:   StorageRead at UnknownVersion (BAL doesn't
//     pre-populate AddressPath, so versionedRead with readStorage=nil falls
//     through to storage).
//   - sender.BalancePath:   MapRead at (K-1, 0) for K>=1 (btree floor hit),
//     StorageRead at UnknownVersion for K=0.
//   - sender.NoncePath:     same shape as BalancePath.
//   - coinbase.AddressPath: StorageRead at UnknownVersion.
//   - coinbase.BalancePath: same shape as sender.BalancePath.
//
// This test reproduces the BAL retry storm seen on the point_evaluation
// benchmark: with the current validateRead BalancePath cross-check on the
// AddressPath MVReadResultNone branch, every tx K>=1 is invalidated solely
// because BAL has a BalancePath entry below K, even though the underlying
// AddressPath read from storage is unchanged.
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

	for i := 0; i < numTxs; i++ {
		vm.Write(sender, BalancePath, accounts.NilKey,
			Version{TxIndex: i, Incarnation: 0}, *uint256.NewInt(uint64(1000 - i)), true)
		vm.Write(sender, NoncePath, accounts.NilKey,
			Version{TxIndex: i, Incarnation: 0}, uint64(i+1), true)
		vm.Write(coinbase, BalancePath, accounts.NilKey,
			Version{TxIndex: i, Incarnation: 0}, *uint256.NewInt(uint64((i + 1) * 50)), true)
	}

	sourceFor := func(r ReadResult) ReadSource {
		if r.Status() == MVReadResultDone {
			return MapRead
		}
		return StorageRead
	}

	io := NewVersionedIO(numTxs)
	for txIdx := 0; txIdx < numTxs; txIdx++ {
		rs := ReadSet{}
		rs.Set(VersionedRead{
			Address: sender, Path: AddressPath, Source: StorageRead, Version: UnknownVersion,
		})
		balRes := vm.Read(sender, BalancePath, accounts.NilKey, txIdx)
		rs.Set(VersionedRead{
			Address: sender, Path: BalancePath,
			Source:  sourceFor(balRes),
			Version: balRes.Version(),
			Val:     balRes.Value(),
		})
		nonceRes := vm.Read(sender, NoncePath, accounts.NilKey, txIdx)
		rs.Set(VersionedRead{
			Address: sender, Path: NoncePath,
			Source:  sourceFor(nonceRes),
			Version: nonceRes.Version(),
			Val:     nonceRes.Value(),
		})
		rs.Set(VersionedRead{
			Address: coinbase, Path: AddressPath, Source: StorageRead, Version: UnknownVersion,
		})
		cbBalRes := vm.Read(coinbase, BalancePath, accounts.NilKey, txIdx)
		rs.Set(VersionedRead{
			Address: coinbase, Path: BalancePath,
			Source:  sourceFor(cbBalRes),
			Version: cbBalRes.Version(),
			Val:     cbBalRes.Value(),
		})
		io.RecordReads(Version{TxIndex: txIdx, Incarnation: 0}, rs)
	}

	for txIdx := 0; txIdx < numTxs; txIdx++ {
		valid := vm.ValidateVersion(txIdx, io, checkVersionEqual, false, "")
		require.Equal(t, VersionValid, valid,
			"tx %d: BAL-pre-populated reads should validate without conflicts; got %s", txIdx, valid)
	}
}

// TestNoBAL_SameSenderTxs_DetectsConflicts is the safety counterpart of
// TestBALPrePop_SameSenderTxs_NoConflicts: when no BAL is supplied, removing
// the BalancePath cross-check from validateRead's AddressPath MVReadResultNone
// branch must NOT silently break the existing same-sender conflict-detection
// path (which now relies entirely on the MVReadResultDone arm's
// !vm.HasBAL || !isBALPrePopulatedPath gate + value-tiebreaker).
//
// Setup: 9 concurrent workers each read sender.BalancePath / sender.NoncePath
// from storage at UnknownVersion (vm has no entries yet). Then tx 0 commits
// and flushes BalancePath=V0, NoncePath=N0 at TxIndex=0. The other 8 txs'
// recorded StorageReads now conflict with the newly-Done vm entries (their
// recorded values differ from V0/N0), so each must invalidate.
//
// Without this guarantee, parallel execution of same-sender txs without BAL
// would commit stale balances/nonces — silently corrupting the chain.
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

	// Phase 1: 9 concurrent workers each read the sender's pre-block state
	// from storage (vm has no entries). Each records a (BalancePath,
	// StorageRead, UnknownVersion) read with the original storage value, and
	// the same for NoncePath. AddressPath also goes via the storage-fallback
	// path in production but isn't relevant to the conflict here.
	origBalance := *uint256.NewInt(1_000_000)
	origNonce := uint64(42)
	io := NewVersionedIO(numTxs)
	for txIdx := 0; txIdx < numTxs; txIdx++ {
		rs := ReadSet{}
		rs.Set(VersionedRead{
			Address: sender, Path: BalancePath,
			Source: StorageRead, Version: UnknownVersion,
			Val: origBalance,
		})
		rs.Set(VersionedRead{
			Address: sender, Path: NoncePath,
			Source: StorageRead, Version: UnknownVersion,
			Val: origNonce,
		})
		rs.Set(VersionedRead{
			Address: sender, Path: AddressPath,
			Source: StorageRead, Version: UnknownVersion,
		})
		io.RecordReads(Version{TxIndex: txIdx, Incarnation: 0}, rs)
	}

	// Phase 2: tx 0 finishes execution and flushes its writes. It debits
	// gas (balance changes) and increments the nonce.
	postBalance := *uint256.NewInt(900_000)
	postNonce := origNonce + 1
	vm.FlushVersionedWrites(VersionedWrites{
		{Address: sender, Path: BalancePath, Version: Version{TxIndex: 0, Incarnation: 0}, Val: postBalance},
		{Address: sender, Path: NoncePath, Version: Version{TxIndex: 0, Incarnation: 0}, Val: postNonce},
	}, true, "")

	// Phase 3: tx 0 itself revalidates clean — its recorded reads are all
	// (StorageRead, UnknownVersion) and vm.Read at txIdx=0 returns
	// MVReadResultNone (no prior writes at TxIdx < 0), which goes through
	// the StorageRead-valid branch.
	require.Equal(t, VersionValid, vm.ValidateVersion(0, io, checkVersionEqual, false, ""),
		"tx 0 should validate (no prior writes to conflict with)")

	// Phase 4: every other same-sender tx must invalidate. vm.Read at
	// txIdx=K for K>=1 now returns Done at (0, 0) for BalancePath/NoncePath;
	// the recorded StorageRead values differ from the post-flush values, so
	// the value-tiebreaker fails and validation returns Invalid.
	for txIdx := 1; txIdx < numTxs; txIdx++ {
		valid := vm.ValidateVersion(txIdx, io, checkVersionEqual, false, "")
		require.Equal(t, VersionInvalid, valid,
			"tx %d: recorded StorageRead of sender.BalancePath conflicts with tx 0's flushed Done; got %s", txIdx, valid)
	}
}

// CreateContractPath is the creation-specific cross-check signal: written
// only by the CREATE/CREATE2 path in IBS.CreateAccount, never by
// UpdateAccountData or BAL pre-population. This test asserts that a Done
// CreateContractPath entry at a prior txIdx invalidates a same-block
// AddressPath/StorageRead — the speculative "account does not exist" read
// must lose to the prior tx's deployment.
func TestValidateRead_PriorContractCreation_DetectedViaCreateContractPath(t *testing.T) {
	t.Parallel()

	addr := getAddress(99)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)
	vm.HasBAL = true

	// Simulate the post-flush state after tx 0 creates the contract. The
	// BAL pre-populated BalancePath / NoncePath / CodeHashPath; the worker
	// additionally flushed CreateContractPath (which BAL doesn't pre-populate)
	// because IBS.CreateAccount(contractCreation=true) writes it. AddressPath
	// was filtered out of the flush under HasBAL — that's the gap PR #19628
	// was trying to plug.
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000), true)
	vm.Write(addr, NoncePath, accounts.NilKey,
		Version{TxIndex: 0, Incarnation: 0}, uint64(0), true)
	vm.Write(addr, CreateContractPath, accounts.NilKey,
		Version{TxIndex: 0, Incarnation: 0}, true, true)

	// Tx 1 speculatively read AddressPath at exec time — vm had no
	// AddressPath entry, so versionedRead's storage-fallback path
	// recorded (StorageRead, UnknownVersion) with the pre-block storage
	// value (nil, since the account didn't exist before this block).
	io := NewVersionedIO(2)
	rs := ReadSet{}
	rs.Set(VersionedRead{
		Address: addr,
		Path:    AddressPath,
		Source:  StorageRead,
		Version: UnknownVersion,
	})
	io.RecordReads(Version{TxIndex: 1, Incarnation: 0}, rs)

	valid := vm.ValidateVersion(1, io, checkVersionEqual, false, "")
	require.Equal(t, VersionInvalid, valid,
		"tx 1: a prior tx wrote CreateContractPath (account created); the stale AddressPath storage-read MUST invalidate so the tx re-executes against the post-creation state")
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
		mvh2.Write(ap2, AddressPath, accounts.NilKey, Version{0, 0, idx, 1}, valueFor(idx, 1), true)
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
		Val:     storageVal,
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
			Version: Version{TxIndex: 5, Incarnation: 1}, Val: *uint256.NewInt(100)},
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
			Version: Version{TxIndex: 7, Incarnation: 2}, Val: uint64(5)},
	}
	vm.FlushVersionedWrites(writes2, false, "")

	// TX 10 reads NoncePath should see FlagEstimate → MVReadResultDependency.
	res2 := vm.Read(addr, NoncePath, accounts.NilKey, 10)
	require.Equal(t, MVReadResultDependency, res2.Status(),
		"invalid TX flush should produce Estimate entries")
	require.Equal(t, 7, res2.DepIdx())
}

// TestValidateRead_SDStaleness_InvalidatesPreDestructRead verifies the
// SD-staleness cross-check: a MapRead that is version-consistent on its own
// path must still be invalidated when a later tx self-destructed the account
// (with no subsequent revival). The serial path returns zero via the SD-zero
// short-circuit, so the read predates the destruct and is stale.
func TestValidateRead_SDStaleness_InvalidatesPreDestructRead(t *testing.T) {
	t.Parallel()

	addr := getAddress(77)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)

	// tx 0 wrote the account's balance; tx 2 self-destructed it.
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000), true)
	vm.Write(addr, SelfDestructPath, accounts.NilKey,
		Version{TxIndex: 2, Incarnation: 0}, true, true)

	// tx 5 read BalancePath as a MapRead at (0,0) — version-consistent on
	// BalancePath alone, but stale because tx 2's destruct came after.
	io := NewVersionedIO(5)
	rs := ReadSet{}
	rs.Set(VersionedRead{
		Address: addr,
		Path:    BalancePath,
		Source:  MapRead,
		Version: Version{TxIndex: 0, Incarnation: 0},
		Val:     *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)

	valid := vm.ValidateVersion(5, io, checkVersionEqual, false, "")
	require.Equal(t, VersionInvalid, valid,
		"tx 5: a later tx self-destructed the account with no revival — the pre-destruct BalancePath read must invalidate")
}

// TestValidateRead_SDStaleness_RevivalKeepsReadValid verifies the revival
// exemption of the SD-staleness check: when a tx after the destruct writes
// BalancePath/NoncePath/CodeHashPath (re-creating the account), the read is
// not stale and stays valid.
func TestValidateRead_SDStaleness_RevivalKeepsReadValid(t *testing.T) {
	t.Parallel()

	addr := getAddress(78)
	checkVersionEqual := func(readVersion, writeVersion Version) VersionValidity {
		if readVersion == writeVersion {
			return VersionValid
		}
		return VersionInvalid
	}

	vm := NewVersionMap(nil)

	// tx 0 wrote balance; tx 2 self-destructed; tx 3 revived the account by
	// writing NoncePath (a non-SelfDestruct write at a higher TxIndex).
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(1_000), true)
	vm.Write(addr, SelfDestructPath, accounts.NilKey,
		Version{TxIndex: 2, Incarnation: 0}, true, true)
	vm.Write(addr, NoncePath, accounts.NilKey,
		Version{TxIndex: 3, Incarnation: 0}, uint64(1), true)

	// tx 5 read BalancePath as a MapRead at (0,0). The destruct is followed
	// by a revival, so the read is not stale.
	io := NewVersionedIO(5)
	rs := ReadSet{}
	rs.Set(VersionedRead{
		Address: addr,
		Path:    BalancePath,
		Source:  MapRead,
		Version: Version{TxIndex: 0, Incarnation: 0},
		Val:     *uint256.NewInt(1_000),
	})
	io.RecordReads(Version{TxIndex: 5, Incarnation: 0}, rs)

	valid := vm.ValidateVersion(5, io, checkVersionEqual, false, "")
	require.Equal(t, VersionValid, valid,
		"tx 5: a revival write after the destruct re-creates the account — the BalancePath read stays valid")
}
