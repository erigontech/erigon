// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// takes first 100k keys from file
func pivotKeysFromKV(dataPath string) ([][]byte, error) {
	decomp, err := seg.NewDecompressor(dataPath)
	if err != nil {
		return nil, err
	}

	getter := decomp.MakeGetter()
	getter.Reset(0)

	key := make([]byte, 0, 64)

	listing := make([][]byte, 0, 1000)

	for getter.HasNext() {
		if len(listing) > 100000 {
			break
		}
		key, _ := getter.Next(key[:0])
		listing = append(listing, common.Copy(key))
		getter.Skip()
	}
	decomp.Close()

	return listing, nil
}

func generateKV(tb testing.TB, tmp string, keySize, valueSize, keyCount int, logger log.Logger, compressFlags seg.FileCompression) string {
	tb.Helper()

	rnd := newRnd(0)
	values := make([]byte, valueSize)

	dataPath := filepath.Join(tmp, fmt.Sprintf("%dk.kv", keyCount/1000))
	comp, err := seg.NewCompressor(context.Background(), "cmp", dataPath, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(tb, err)

	bufSize := 8 * datasize.KB
	if keyCount > 1000 { // windows CI can't handle much small parallel disk flush
		bufSize = 1 * datasize.MB
	}
	collector := etl.NewCollector(btindex.BtreeLogPrefix+" genCompress", tb.TempDir(), etl.NewSortableBuffer(bufSize), logger)
	defer collector.Close()

	for i := 0; i < keyCount; i++ {
		key := make([]byte, keySize)
		n, err := rnd.Read(key)
		require.Equal(tb, keySize, n)
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(i))
		require.NoError(tb, err)

		n, err = rnd.Read(values[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		err = collector.Collect(key, values[:n])
		require.NoError(tb, err)
	}

	writer := seg.NewWriter(comp, compressFlags)
	defer writer.Close()

	loader := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		_, err = writer.Write(k)
		require.NoError(tb, err)
		_, err = writer.Write(v)
		require.NoError(tb, err)
		return nil
	}

	err = collector.Load(nil, "", loader, etl.TransformArgs{})
	require.NoError(tb, err)

	err = comp.Compress()
	require.NoError(tb, err)
	comp.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()
	compPath := decomp.FilePath()
	ps := background.NewProgressSet()

	IndexFile := filepath.Join(tmp, fmt.Sprintf("%dk.bt", keyCount/1000))
	r := seg.NewReader(decomp.MakeGetter(), compressFlags)
	err = btindex.BuildBtreeIndexWithDecompressor(IndexFile, r, ps, tb.TempDir(), 777, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence)
	require.NoError(tb, err)

	return compPath
}

func testDbAndAggregatorv3(tb testing.TB, stepSize uint64) (kv.RwDB, *Aggregator) {
	tb.Helper()
	logger := log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg := NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(tb.Context(), db)
	tb.Cleanup(agg.Close)
	err := agg.OpenFolder()
	require.NoError(tb, err)
	return db, agg
}

// generate test data for table tests, containing n; n < 20 keys of length 20 bytes and values of length <= 16 bytes
func generateInputData(tb testing.TB, keySize, valueSize, keyCount int) ([][]byte, [][]byte) {
	tb.Helper()

	rnd := newRnd(0)
	values := make([][]byte, keyCount)
	keys := make([][]byte, keyCount)

	bk, bv := make([]byte, keySize), make([]byte, valueSize)
	for i := 0; i < keyCount; i++ {
		n, err := rnd.Read(bk)
		require.Equal(tb, keySize, n)
		require.NoError(tb, err)
		keys[i] = common.Copy(bk[:n])

		n, err = rnd.Read(bv[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		values[i] = common.Copy(bv[:n])
	}
	return keys, values
}

// also useful to decode given input into v3 account
func Test_helper_decodeAccountv3Bytes(t *testing.T) {
	t.Parallel()
	input, err := hex.DecodeString("000114000101")
	require.NoError(t, err)

	acc := accounts.Account{}
	_ = accounts.DeserialiseV3(&acc, input)
	fmt.Printf("input %x nonce %d balance %d codeHash %d\n", input, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash.Value())
}

func TestAggregator_CheckDependencyHistoryII(t *testing.T) {
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)

	generateAccountsFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCodeFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateStorageFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})

	require.NoError(t, agg.OpenFolder())

	aggTx := agg.BeginFilesRo()
	defer aggTx.Close()

	checkFn := func(files visibleFiles, merged bool) {
		if merged {
			require.Equal(t, 1, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[0].endTxNum/stepSize)
		} else {
			require.Equal(t, 2, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(1), files[0].endTxNum/stepSize)
			require.Equal(t, uint64(1), files[1].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[1].endTxNum/stepSize)
		}
	}

	checkFn(aggTx.d[kv.AccountsDomain].ht.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.files, true)
	checkFn(aggTx.d[kv.StorageDomain].ht.files, true)
	checkFn(aggTx.d[kv.AccountsDomain].ht.iit.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.iit.files, true)
	checkFn(aggTx.d[kv.StorageDomain].ht.iit.files, true)

	aggTx.Close()

	// delete merged code history file
	codeMergedFile := filepath.Join(agg.Dirs().SnapHistory, "v1.0-code.0-2.v")
	exist, err := dir.FileExist(codeMergedFile)
	require.NoError(t, err)
	require.True(t, exist)
	agg.closeDirtyFiles() // because windows

	require.NoError(t, dir.RemoveFile(codeMergedFile))

	require.NoError(t, agg.OpenFolder())
	aggTx = agg.BeginFilesRo()
	defer aggTx.Close()

	checkFn(aggTx.d[kv.AccountsDomain].ht.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.files, false)
	checkFn(aggTx.d[kv.StorageDomain].ht.files, true)
	checkFn(aggTx.d[kv.AccountsDomain].ht.iit.files, true)
	checkFn(aggTx.d[kv.CodeDomain].ht.iit.files, false)
	checkFn(aggTx.d[kv.StorageDomain].ht.iit.files, true)
}

func TestAggregator_CheckDependencyBtwnDomains(t *testing.T) {
	stepSize := uint64(10)
	// testDbAggregatorWithNoFiles(t,  stepSize * 32, &testAggConfig{
	// 	stepSize:                         10,
	// 	disableCommitmentBranchTransform: false,
	// })
	_, agg := testDbAndAggregatorv3(t, stepSize)

	require.NotNil(t, agg.d[kv.AccountsDomain].checker)
	require.NotNil(t, agg.d[kv.StorageDomain].checker)
	require.NotNil(t, agg.checker)
	require.Nil(t, agg.d[kv.CommitmentDomain].checker)

	generateAccountsFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCodeFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateStorageFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})

	require.NoError(t, agg.OpenFolder())

	aggTx := agg.BeginFilesRo()
	defer aggTx.Close()

	checkFn := func(files visibleFiles, merged bool) {
		if merged {
			require.Equal(t, 1, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[0].endTxNum/stepSize)
		} else {
			require.Equal(t, 2, len(files))
			require.Equal(t, uint64(0), files[0].startTxNum/stepSize)
			require.Equal(t, uint64(1), files[0].endTxNum/stepSize)
			require.Equal(t, uint64(1), files[1].startTxNum/stepSize)
			require.Equal(t, uint64(2), files[1].endTxNum/stepSize)
		}
	}
	checkFn(aggTx.d[kv.AccountsDomain].files, false)
	checkFn(aggTx.d[kv.CodeDomain].files, true)
	checkFn(aggTx.d[kv.StorageDomain].files, false)
	checkFn(aggTx.d[kv.CommitmentDomain].files, false)
}

func TestReceiptFilesVersionAdjust(t *testing.T) {
	touchFn := func(t *testing.T, dirs datadir.Dirs, file string) {
		t.Helper()
		fullpath := filepath.Join(dirs.SnapDomain, file)
		ofile, err := os.Create(fullpath)
		require.NoError(t, err)
		ofile.Close()
	}

	t.Run("v1.0 files", func(t *testing.T) {
		// Schema is global and edited by subtests
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)

		touchFn(t, dirs, "v1.0-receipt.0-2048.kv")
		touchFn(t, dirs, "v1.0-receipt.2048-2049.kv")

		agg := NewTest(dirs).Logger(logger).MustOpen(t.Context(), db)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].FileVersion.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.FileVersion.DataV

		require.Equal(kv_versions.Current, version.V1_1)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.Equal(v_versions.Current, version.V1_1)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

	t.Run("v1.1 files", func(t *testing.T) {
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)

		touchFn(t, dirs, "v1.1-receipt.0-2048.kv")
		touchFn(t, dirs, "v1.1-receipt.2048-2049.kv")

		agg := NewTest(dirs).Logger(logger).MustOpen(t.Context(), db)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].FileVersion.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.FileVersion.DataV

		require.Equal(kv_versions.Current, version.V1_1)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.Equal(v_versions.Current, version.V1_1)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

	t.Run("v2.0 files", func(t *testing.T) {
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)

		touchFn(t, dirs, "v2.0-receipt.0-2048.kv")
		touchFn(t, dirs, "v2.0-receipt.2048-2049.kv")

		agg := NewTest(dirs).Logger(logger).MustOpen(t.Context(), db)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].FileVersion.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.FileVersion.DataV

		require.True(kv_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.True(v_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

	t.Run("empty files", func(t *testing.T) {
		backup := statecfg.Schema
		t.Cleanup(func() {
			statecfg.Schema = backup
		})
		require, logger := require.New(t), log.New()
		dirs := datadir.New(t.TempDir())

		db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
		t.Cleanup(db.Close)
		agg := NewTest(dirs).Logger(logger).MustOpen(t.Context(), db)
		t.Cleanup(agg.Close)

		kv_versions := agg.d[kv.ReceiptDomain].FileVersion.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.FileVersion.DataV

		require.True(kv_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.True(v_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

}

func generateDomainFiles(t *testing.T, name string, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	ver := version.V1_0_standart
	domainR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})
	defer domainR.Close()
	populateFiles2(t, dirs, domainR, ranges)

	domainHR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapHistory, name, DataExtensionV, seg.CompressNone, ver).
			Accessor(dirs.SnapAccessors, ver).
			Build()
		return name, schema
	})
	defer domainHR.Close()
	populateFiles2(t, dirs, domainHR, ranges)

	domainII := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapIdx, name, DataExtensionEf, seg.CompressNone, ver).
			Accessor(dirs.SnapAccessors, ver).
			Build()
		return name, schema
	})
	defer domainII.Close()
	populateFiles2(t, dirs, domainII, ranges)
}

func generateAccountsFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateDomainFiles(t, "accounts", dirs, ranges)
}

func generateCodeFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateDomainFiles(t, "code", dirs, ranges)
}

func generateStorageFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateDomainFiles(t, "storage", dirs, ranges)
}

func generateCommitmentFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	ver := version.V1_0_standart
	commitmentR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		name = "commitment"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			Accessor(dirs.SnapDomain, ver).
			Build()
		return name, schema
	})
	defer commitmentR.Close()
	populateFiles2(t, dirs, commitmentR, ranges)
}

// generateCommitmentHistoryAndIndexFiles creates only the history (.v) and inverted-index (.ef)
// files for the commitment domain, without touching the KV domain files. Use this when you want
// commitment values to appear already-merged while history still needs merging.
func generateCommitmentHistoryAndIndexFiles(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	ver := version.V1_0_standart
	histRepo := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (string, SnapNameSchema) {
		schema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
			Data(dirs.SnapHistory, "commitment", DataExtensionV, seg.CompressNone, ver).
			Accessor(dirs.SnapAccessors, ver).
			Build()
		return "commitment", schema
	})
	defer histRepo.Close()
	populateFiles2(t, dirs, histRepo, ranges)

	idxRepo := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (string, SnapNameSchema) {
		schema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
			Data(dirs.SnapIdx, "commitment", DataExtensionEf, seg.CompressNone, ver).
			Accessor(dirs.SnapAccessors, ver).
			Build()
		return "commitment", schema
	})
	defer idxRepo.Close()
	populateFiles2(t, dirs, idxRepo, ranges)
}

// TestAggregator_CommittedTxNumGuard verifies the stepFullyCommitted predicate
// used by buildFilesInBackground: a step S should only be collated when
// committedTxNum+1 >= firstTxNum(S+1), meaning all txNums in step S have been
// committed to the DB. ComputeCommitment writes the last txNum of the block
// (e.g. firstTxNum(S+1)-1 when the step boundary aligns with a block), so
// the +1 avoids an off-by-one that would delay collation unnecessarily.
func TestAggregator_CommittedTxNumGuard(t *testing.T) {
	t.Parallel()
	stepSize := uint64(100)

	// Step 5 covers txNums [500, 600). firstTxNum(6) = 600.
	assert.False(t, stepFullyCommitted(550, 5, stepSize),
		"guard should block: committed txNum is mid-step")
	assert.True(t, stepFullyCommitted(0, 5, stepSize),
		"guard should be bypassed when committedTxNum is 0 (no commitment)")
	assert.False(t, stepFullyCommitted(598, 5, stepSize),
		"guard should block: committed txNum is 1 before last txNum of step")

	// committedTxNum = 599 = lastTxNumOfStep(5) = firstTxNum(6)-1
	// This is the value ComputeCommitment writes at the step boundary.
	assert.True(t, stepFullyCommitted(599, 5, stepSize),
		"guard should allow: committed txNum is last txNum of the step")
	assert.True(t, stepFullyCommitted(600, 5, stepSize),
		"guard should allow: committed txNum is past the step")
	assert.True(t, stepFullyCommitted(1000, 5, stepSize),
		"guard should allow: committed txNum is well past the step")
}

// TestStepFullyCommitted_ZeroIsGenesisNotSentinel proves that the bypass
// `committedTxNum == 0 → return true` is wrong when genesis is committed at txNum=0.
// In that case committedTxNum=0 does NOT mean "no commitment yet" — it means only
// txNum=0 has been committed. Step 0 covers txNums [0, stepSize), so txNums 1…
// stepSize-1 are still uncommitted. The guard must block, but it doesn't.
//
// This test FAILS with the current implementation, proving the bypass is incorrect.
func TestStepFullyCommitted_ZeroIsGenesisNotSentinel(t *testing.T) {
	t.Parallel()
	stepSize := uint64(100)

	// Genesis committed at txNum=0 → committedTxNum=0.
	// Step 0 covers [0, 100). Only txNum=0 is committed.
	// The guard MUST block (txNums 1-99 are not yet committed).
	// BUG: stepFullyCommitted(0, 0, stepSize) returns true (bypass triggered).
	assert.False(t, stepFullyCommitted(0, 0, stepSize),
		"genesis committed at txNum=0: step 0 is NOT fully committed (txNums 1-99 missing)")

	// Even more obviously: if only genesis (txNum=0) is committed, step 5 is not committed at all.
	assert.False(t, stepFullyCommitted(0, 5, stepSize),
		"genesis committed at txNum=0: step 5 has zero commits — must be blocked")
}

// TestDomain_CollationRaceNotTestedByIsolationTest proves that
// TestDomain_CollationIsolatedFromLaterSteps does NOT test the actual race bug
// described in #20169.
//
// The REAL race: step S data arrives in two separate DB commits. If collation opens
// its read tx between those commits, the second batch is invisible. After pruning,
// that data is permanently lost. The PR's single-tx approach alone does not prevent
// this — the committedTxNum guard must also work correctly.
//
// TestDomain_CollationIsolatedFromLaterSteps writes step S and step S+1 data in ONE
// transaction, so the collation always sees both. It tests cross-step filtering (which
// was never broken), not intra-step partial-commit visibility (the actual bug).
//
// This test reproduces the REAL scenario: two separate commits for the same step,
// with collation opening its read tx between them. After pruning, data from the
// second commit is LOST, demonstrating the race is not covered.
func TestDomain_CollationRaceNotTestedByIsolationTest(t *testing.T) {
	logger := log.New()
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.StorageDomain, 16, logger)
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	k1 := []byte("key1")
	k2 := []byte("key2") // written in second batch — will be LOST after the race

	// Batch 1: write k1 at step 0 and commit.
	tx1, err := db.BeginRw(ctx)
	require.NoError(t, err)
	dt := d.BeginFilesRo()
	w := dt.NewWriter()
	require.NoError(t, w.PutWithPrev(k1, []byte("V1"), 2, nil))
	require.NoError(t, w.Flush(ctx, tx1))
	w.Close()
	dt.Close()
	require.NoError(t, tx1.Commit())

	// Open a collation read tx NOW — snapshot sees only batch 1 (no k2 yet).
	// This simulates what buildFiles does when committedTxNum guard is bypassed
	// (e.g. committedTxNum=0 bypass) and collation starts before all step 0 data
	// has been committed.
	collTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer collTx.Rollback()

	// Batch 2: write k2 at step 0 and commit AFTER collation tx was opened.
	tx2, err := db.BeginRw(ctx)
	require.NoError(t, err)
	dt = d.BeginFilesRo()
	w = dt.NewWriter()
	require.NoError(t, w.PutWithPrev(k2, []byte("V2"), 10, nil))
	require.NoError(t, w.Flush(ctx, tx2))
	w.Close()
	dt.Close()
	require.NoError(t, tx2.Commit())

	// Collate step 0 using the pre-batch-2 snapshot: k2 is invisible.
	coll, err := d.collate(ctx, 0, 0, 16, collTx)
	require.NoError(t, err)
	collTx.Rollback()
	defer coll.Close()

	sf, err := d.buildFiles(ctx, 0, coll, background.NewProgressSet())
	require.NoError(t, err)
	defer sf.CleanupOnError()

	// Integrate files and prune step 0 — simulates BuildFilesInBackground + pruning.
	tx3, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx3.Rollback()
	d.integrateDirtyFiles(sf, 0, 16)
	d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())

	dt = d.BeginFilesRo()
	_, err = dt.Prune(ctx, tx3, 0, 0, 16, math.MaxUint64, logEvery)
	dt.Close()
	require.NoError(t, err)

	// k2 was in batch 2 (committed AFTER collation snapshot) → NOT in the file.
	// After pruning step 0 from DB, k2 is permanently lost.
	// This FAILS, proving the race is real and not covered by the isolation test.
	dt = d.BeginFilesRo()
	defer dt.Close()
	v, _, found, err := dt.GetLatest(k2, tx3)
	require.NoError(t, err)
	require.Truef(t, found,
		"k2 (committed in batch 2 after collation snapshot) should be visible — "+
			"but it is LOST because the collation race (#20169) is not prevented when "+
			"committedTxNum=0 bypass allows premature collation. got v=%q", v)
}

func TestAggregator_CommitmentHistoryOnlyMerge(t *testing.T) {
	// Regression: when commitment values are already merged (values.needMerge=false) but history
	// is not, the old aggregator.mergeFiles called commitmentValTransformDomain with a zero
	// MergeRange, causing "failed to create commitment value transformer: file
	// v2.0-storage.0-0.kv was not found".
	stepSize := uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)
	dirs := agg.Dirs()

	// Accounts/storage/code: all files merged {0,2}, no further merge needed.
	generateAccountsFile(t, dirs, []testFileRange{{0, 2}})
	generateStorageFile(t, dirs, []testFileRange{{0, 2}})
	generateCodeFile(t, dirs, []testFileRange{{0, 2}})
	// Commitment KV: merged {0,2} — values.needMerge will be false.
	generateCommitmentFile(t, dirs, []testFileRange{{0, 2}})
	// Commitment history+index: unmerged {0,1},{1,2} — history.needMerge will be true.
	generateCommitmentHistoryAndIndexFiles(t, dirs, []testFileRange{{0, 1}, {1, 2}})

	require.NoError(t, agg.OpenFolder())

	aggTx := agg.BeginFilesRo()
	r := aggTx.findMergeRange(2*stepSize, stepSize, 32)
	aggTx.Close()

	// Precondition: confirm the exact scenario that triggers the bug.
	require.False(t, r.domain[kv.CommitmentDomain].values.needMerge, "commitment values already merged")
	require.True(t, r.domain[kv.CommitmentDomain].history.any(), "commitment history needs merging")
	require.True(t, r.domain[kv.CommitmentDomain].any())

	// Before fix: mergeFiles returned "failed to create commitment value transformer: file
	// v2.0-storage.0-0.kv was not found" because the transformer was called with the
	// zero-value MergeRange from values (needMerge=false, from=0, to=0).
	_, err := agg.mergeLoopStep(t.Context(), 2*stepSize)
	if err != nil {
		assert.NotContains(t, err.Error(), "failed to create commitment value transformer",
			"transformer must not be called when values.needMerge=false")
	}
}

func setupAggSnapRepo(t *testing.T, dirs datadir.Dirs, genRepo func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema)) *SnapshotRepo {
	t.Helper()
	stepSize := uint64(10)
	name, schema := genRepo(stepSize, dirs)

	createConfig := SnapshotCreationConfig{
		RootNumPerStep: stepSize,
		MergeStages:    []uint64{20, 40, 80},
		MinimumSize:    10,
		SafetyMargin:   5,
	}
	d, err := kv.String2Domain(name)
	require.NoError(t, err)
	return NewSnapshotRepo(name, FromDomain(d), &SnapshotConfig{
		SnapshotCreationConfig: &createConfig,
		Schema:                 schema,
	}, log.New())
}
