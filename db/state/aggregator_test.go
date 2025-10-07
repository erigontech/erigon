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
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
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
	collector := etl.NewCollector(BtreeLogPrefix+" genCompress", tb.TempDir(), etl.NewSortableBuffer(bufSize), logger)

	for i := 0; i < keyCount; i++ {
		key := make([]byte, keySize)
		n, err := rnd.Read(key[:])
		require.Equal(tb, keySize, n)
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(i))
		require.NoError(tb, err)

		n, err = rnd.Read(values[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		err = collector.Collect(key, values[:n])
		require.NoError(tb, err)
	}

	writer := seg.NewWriter(comp, compressFlags)

	loader := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		_, err = writer.Write(k)
		require.NoError(tb, err)
		_, err = writer.Write(v)
		require.NoError(tb, err)
		return nil
	}

	err = collector.Load(nil, "", loader, etl.TransformArgs{})
	require.NoError(tb, err)

	collector.Close()

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
	err = BuildBtreeIndexWithDecompressor(IndexFile, r, ps, tb.TempDir(), 777, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence)
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
		n, err := rnd.Read(bk[:])
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
	fmt.Printf("input %x nonce %d balance %d codeHash %d\n", input, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash.Bytes())
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

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

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

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

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

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

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

		kv_versions := agg.d[kv.ReceiptDomain].Version.DataKV
		v_versions := agg.d[kv.ReceiptDomain].Hist.Version.DataV

		require.True(kv_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(kv_versions.MinSupported, version.V1_0)
		require.True(v_versions.Current.Cmp(version.V2_1) >= 0)
		require.Equal(v_versions.MinSupported, version.V1_0)
	})

}

func generateDomainFiles(t *testing.T, name string, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	domainR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone).
			BtIndex().Existence().
			Build()
		return name, schema
	})
	defer domainR.Close()
	populateFiles2(t, dirs, domainR, ranges)

	domainHR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapHistory, name, DataExtensionV, seg.CompressNone).
			Accessor(dirs.SnapAccessors).
			Build()
		return name, schema
	})
	defer domainHR.Close()
	populateFiles2(t, dirs, domainHR, ranges)

	domainII := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (dn string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapIdx, name, DataExtensionEf, seg.CompressNone).
			Accessor(dirs.SnapAccessors).
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
	commitmentR := setupAggSnapRepo(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		name = "commitment"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone).
			Accessor(dirs.SnapDomain).
			Build()
		return name, schema
	})
	defer commitmentR.Close()
	populateFiles2(t, dirs, commitmentR, ranges)
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
