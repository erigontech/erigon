package state

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/datastruct/existence"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	ee "github.com/erigontech/erigon-lib/state/entity_extras"
	"github.com/erigontech/erigon-lib/version"
	"github.com/stretchr/testify/require"
)

// 1. create folder with content; OpenFolder contains all dirtyFiles (check the dirty files)
// 1.1 dirty file integration
// 2. CloseFilesAFterRootNum
// 3. check freezing range logics (different file)
// 4. merge files

func TestOpenFolder_AccountsDomain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	dirs := datadir.New(t.TempDir())
	name, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ee.DataExtensionKv, seg.CompressNone).
			BtIndex().Existence().
			Build()

		return name, schema
	})
	defer repo.Close()
	extensions := repo.cfg.Schema.(*ee.E3SnapSchema).FileExtensions()
	dataCount, btCount, existenceCount, accessorCount := populateFilesFull(t, dirs, name, extensions, dirs.SnapDomain)
	require.Positive(t, dataCount)

	err := repo.OpenFolder()
	require.NoError(t, err)

	// check dirty files
	repo.dirtyFiles.Walk(func(items []*FilesItem) bool {
		for _, item := range items {
			filename := item.decompressor.FileName()
			require.Contains(t, filename, name)
			require.NotContains(t, filename, "torrent")
			dataCount--

			if item.existence != nil {
				existenceCount--
			}

			if item.bindex != nil {
				btCount--
			}

			if item.index != nil {
				accessorCount--
			}
		}

		return true
	})

	require.Equal(t, 0, dataCount)
	require.Equal(t, 0, btCount)
	require.Equal(t, 0, existenceCount)
	require.Equal(t, 0, accessorCount)
}

func TestOpenFolder_CodeII(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	dirs := datadir.New(t.TempDir())
	name, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorHashMap
		name = "code"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapIdx, name, ee.DataExtensionEf, seg.CompressNone).
			Accessor(dirs.SnapAccessors).Build()
		return name, schema
	})
	defer repo.Close()

	extensions := repo.cfg.Schema.(*ee.E3SnapSchema).FileExtensions()
	dataCount, btCount, existenceCount, accessorCount := populateFilesFull(t, dirs, name, extensions, dirs.SnapIdx)

	require.Positive(t, dataCount)

	err := repo.OpenFolder()
	require.NoError(t, err)

	// check dirty files
	repo.dirtyFiles.Walk(func(items []*FilesItem) bool {
		for _, item := range items {
			filename := item.decompressor.FileName()
			require.Contains(t, filename, name)
			require.NotContains(t, filename, "torrent")
			dataCount--

			if item.existence != nil {
				existenceCount--
			}

			if item.bindex != nil {
				btCount--
			}

			if item.index != nil {
				accessorCount--
			}
		}

		return true
	})

	require.Equal(t, 0, dataCount)
	require.Equal(t, 0, btCount)
	require.Equal(t, 0, existenceCount)
	require.Equal(t, 0, accessorCount)
}

func TestIntegrateDirtyFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// setup account
	// add a dirty file
	// check presence of dirty file
	dirs := datadir.New(t.TempDir())
	name, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ee.DataExtensionKv, seg.CompressNone).
			BtIndex().
			Existence().
			Build()

		return name, schema
	})
	defer repo.Close()

	extensions := repo.cfg.Schema.(*ee.E3SnapSchema).FileExtensions()
	dataCount, _, _, _ := populateFilesFull(t, dirs, name, extensions, dirs.SnapDomain)
	require.Positive(t, dataCount)

	err := repo.OpenFolder()
	require.NoError(t, err)

	filesItem := newFilesItemWithSnapConfig(0, 1024, repo.cfg)
	filename := repo.schema.DataFile(version.V1_0, 0, 1024)
	comp, err := seg.NewCompressor(context.Background(), t.Name(), filename, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer comp.Close()
	if err = comp.AddWord([]byte("word")); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, comp.Compress())

	filesItem.decompressor, err = seg.NewDecompressor(filename)
	require.NoError(t, err)
	// add dirty file
	repo.IntegrateDirtyFile(filesItem)
	_, found := repo.dirtyFiles.Get(filesItem)
	require.True(t, found)
}

func TestCloseFilesAfterRootNum(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// setup account
	// set various root numbers and check if the right files are closed
	dirs := datadir.New(t.TempDir())
	name, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ee.DataExtensionKv, seg.CompressNone).
			BtIndex().
			Existence().
			Build()
		return name, schema
	})
	defer repo.Close()

	extensions := repo.cfg.Schema.(*ee.E3SnapSchema).FileExtensions()
	dataCount, _, _, _ := populateFilesFull(t, dirs, name, extensions, dirs.SnapDomain)
	require.Positive(t, dataCount)

	// 0-256, 256-288, 288-296, 296-298

	// all but 1
	require.NoError(t, repo.OpenFolder())
	repo.CloseFilesAfterRootNum(stepToRootNum(t, 10, repo))
	require.Len(t, repo.dirtyFiles.Items(), 1)

	// all but 1
	require.NoError(t, repo.OpenFolder())
	repo.CloseFilesAfterRootNum(stepToRootNum(t, 256, repo))
	require.Len(t, repo.dirtyFiles.Items(), 1)

	// all but 2
	require.NoError(t, repo.OpenFolder())
	repo.CloseFilesAfterRootNum(stepToRootNum(t, 270, repo))
	require.Len(t, repo.dirtyFiles.Items(), 2)

	// all but 2
	require.NoError(t, repo.OpenFolder())
	repo.CloseFilesAfterRootNum(stepToRootNum(t, 288, repo))
	require.Len(t, repo.dirtyFiles.Items(), 2)

	// all but 3
	require.NoError(t, repo.OpenFolder())
	repo.CloseFilesAfterRootNum(stepToRootNum(t, 290, repo))
	require.Len(t, repo.dirtyFiles.Items(), 3)

	// all still open
	require.NoError(t, repo.OpenFolder())
	repo.CloseFilesAfterRootNum(stepToRootNum(t, 297, repo))
	require.Len(t, repo.dirtyFiles.Items(), 4)
}

func TestMergeRangeSnapRepo(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	dirs := datadir.New(t.TempDir())
	name, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ee.DataExtensionKv, seg.CompressNone).
			BtIndex().Existence().
			Build()
		return name, schema
	})
	defer repo.Close()
	stepSize := repo.stepSize

	/// powers of 2
	mergeStages := make([]uint64, 12)
	for i := range mergeStages {
		mergeStages[i] = (1 << (i + 1)) * stepSize
	}

	repo.cfg.SnapshotCreationConfig = &ee.SnapshotCreationConfig{
		RootNumPerStep: 10,
		MergeStages:    mergeStages,
		MinimumSize:    10,
		SafetyMargin:   0,
	}

	testFn := func(ranges []testFileRange, vfCount int, needMerge bool, mergeFromStep, mergeToStep uint64) {
		dataCount, _, _, _ := populateFiles2(t, dirs, name, repo, dirs.SnapDomain, ranges)
		require.Positive(t, dataCount)
		require.NoError(t, repo.OpenFolder())
		repo.RecalcVisibleFiles(RootNum(MaxUint64))
		vf := repo.VisibleFiles()
		require.Len(t, vf, vfCount)

		mr := repo.FindMergeRange(RootNum(vf.EndRootNum()), vf)
		require.Equal(t, mr.needMerge, needMerge)
		if !mr.needMerge {
			require.Equal(t, mr.from, mergeFromStep*stepSize)
			require.Equal(t, mr.to, mergeToStep*stepSize)
		}
		cleanupFiles(t, repo, dirs)
	}

	// 0-1, 1-2 => 0-2
	testFn([]testFileRange{{0, 1}, {1, 2}}, 2, true, 0, 2)

	// 0-1, 1-2, 2-3 => 0-2, 2-3
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}}, 3, true, 0, 2)

	// 0-1, 1-2, 2-3, 3-4 => 0-4
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}}, 4, true, 0, 4)

	// 0-1, 1-2, 2-3, 3-4, 4-5, 5-6, 6-7 => 0-1, 1-2, 2-3, 3-4, 4-6, 6-7
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}}, 7, true, 4, 6)

	// 0-1, 1-2, 2-3, 3-4, 4-6, 6-7 => 0-4, 4-6, 6-7
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 6}, {6, 7}}, 6, true, 0, 4)

	// 0-4, 4-6, 6-7 => same
	testFn([]testFileRange{{0, 4}, {4, 6}, {6, 7}}, 3, false, 0, 0)

	// 0-1, 1-2, 2-3, 3-4, 0-4 => no merge
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {0, 4}}, 1, false, 0, 0)

	// 0-1, 1-2, 2-3, 3-4, 0-2 => 0-4
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {0, 2}}, 3, true, 0, 4)

	// 0-1, 1-2, ..... 14-15 => 0-1....12-13, 13-15
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}, {8, 9}, {9, 10}, {10, 11}, {11, 12}, {12, 13}, {13, 14}, {14, 15}}, 15, true, 13, 15)

	//0-1....12-13, 13-15, 15-16 => 0-16
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}, {8, 9}, {9, 10}, {10, 11}, {11, 12}, {12, 13}, {13, 15}, {15, 16}}, 15, true, 0, 16)
}

// foreign key; commitment <> accounts
func TestReferencingIntegrityChecker(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	_, accountsR := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ee.DataExtensionKv, seg.CompressNone).
			BtIndex().Existence().
			Build()
		return name, schema
	})

	defer accountsR.Close()

	_, commitmentR := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorHashMap
		name = "commitment"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ee.DataExtensionKv, seg.CompressNone).
			Accessor(dirs.SnapDomain).
			Build()
		return name, schema
	})
	defer commitmentR.Close()

	accountsR.cfg.Integrity = ee.NewReferencingIntegrityChecker(commitmentR.cfg.Schema)
	stepSize := accountsR.stepSize

	// setup accounts and commitment files

	// accounts: 0-1; 1-2; 0-2
	// commitment: 0-1; 1-2
	// visibleFiles for accounts (and commitment) should use 0-1, 1-2
	// then cleanAfterMerge should leave infact 0-1, 1-2

	dataCount, _, _, _ := populateFiles2(t, dirs, "accounts", accountsR, dirs.SnapDomain, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	require.Positive(t, dataCount)
	require.NoError(t, accountsR.OpenFolder())
	require.Equal(t, 3, accountsR.dirtyFiles.Len())

	dataCount, _, _, _ = populateFiles2(t, dirs, "commitment", commitmentR, dirs.SnapDomain, []testFileRange{{0, 1}, {1, 2}})
	require.Positive(t, dataCount)
	require.NoError(t, commitmentR.OpenFolder())

	accountsR.RecalcVisibleFiles(RootNum(MaxUint64))
	acf := accountsR.visibleFiles()

	require.Equal(t, uint64(0), acf[0].startTxNum)
	require.Equal(t, 1*stepSize, acf[0].endTxNum)
	require.Equal(t, 1*stepSize, acf[1].startTxNum)
	require.Equal(t, 2*stepSize, acf[1].endTxNum)

	commitmentR.RecalcVisibleFiles(RootNum(MaxUint64))
	ccf := commitmentR.visibleFiles()

	require.Equal(t, uint64(0), ccf[0].startTxNum)
	require.Equal(t, 1*stepSize, ccf[0].endTxNum)
	require.Equal(t, 1*stepSize, ccf[1].startTxNum)

	require.Equal(t, 2*stepSize, ccf[1].endTxNum)

	mergeFile, found := accountsR.dirtyFiles.Get(&FilesItem{startTxNum: 0, endTxNum: 2 * stepSize})
	require.True(t, found)
	require.Equal(t, uint64(0), mergeFile.startTxNum)
	require.Equal(t, 2*stepSize, mergeFile.endTxNum)

	accountsR.CleanAfterMerge(mergeFile, acf)
	fileExistsCheck(t, accountsR, 0, 1, true)
	fileExistsCheck(t, accountsR, 1, 2, true)

	// now let's add merged commitment and do same checks
	dataCount, _, _, _ = populateFiles2(t, dirs, "commitment", commitmentR, dirs.SnapDomain, []testFileRange{{0, 2}})
	require.Positive(t, dataCount)
	require.NoError(t, commitmentR.OpenFolder())

	commitmentR.RecalcVisibleFiles(RootNum(MaxUint64))
	ccf = commitmentR.visibleFiles()

	require.Len(t, ccf, 1)
	require.Equal(t, uint64(0), ccf[0].startTxNum)
	require.Equal(t, 2*stepSize, ccf[0].endTxNum)

	cMergeFile, found := commitmentR.dirtyFiles.Get(&FilesItem{startTxNum: 0, endTxNum: 2 * stepSize})
	require.True(t, found)
	require.Equal(t, uint64(0), cMergeFile.startTxNum)
	require.Equal(t, 2*stepSize, cMergeFile.endTxNum)

	// should remove commitment.0-1,1-2; thus freeing
	// accounts.0-1,1-2 as well
	commitmentR.CleanAfterMerge(cMergeFile, ccf)

	accountsR.RecalcVisibleFiles(RootNum(MaxUint64))
	acf = accountsR.visibleFiles()

	require.Equal(t, uint64(0), acf[0].startTxNum)
	require.Equal(t, 2*stepSize, acf[0].endTxNum)
	accountsR.CleanAfterMerge(mergeFile, acf)
	fileExistsCheck(t, accountsR, 0, 1, false)
	fileExistsCheck(t, accountsR, 1, 2, false)
	fileExistsCheck(t, accountsR, 0, 2, true)
}

func TestRecalcVisibleFilesAfterMerge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	dirs := datadir.New(t.TempDir())
	name, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ee.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ee.DataExtensionKv, seg.CompressNone).
			BtIndex().
			Existence().
			Build()
		return name, schema
	})
	defer repo.Close()
	stepSize := repo.stepSize

	/// powers of 2
	mergeStages := make([]uint64, 12)
	for i := range mergeStages {
		mergeStages[i] = (1 << (i + 1)) * stepSize
	}

	repo.cfg.SnapshotCreationConfig = &ee.SnapshotCreationConfig{
		RootNumPerStep: 10,
		MergeStages:    mergeStages,
		MinimumSize:    10,
		SafetyMargin:   0,
	}

	testFn := func(ranges []testFileRange, needMerge bool, nFilesInRange, nVfAfterMerge, dirtyFilesAfterMerge int) {
		dataCount, _, _, _ := populateFiles2(t, dirs, name, repo, dirs.SnapDomain, ranges)
		require.Positive(t, dataCount)
		require.NoError(t, repo.OpenFolder())
		repo.RecalcVisibleFiles(RootNum(MaxUint64))
		vf := repo.visibleFiles()

		mr := repo.FindMergeRange(RootNum(vf.EndTxNum()), vf.VisibleFiles())
		require.Equal(t, mr.needMerge, needMerge)
		if !mr.needMerge {
			cleanupFiles(t, repo, dirs)
			return
		}

		// add mergeFile
		_, _, _, _ = populateFiles2(t, dirs, name, repo, dirs.SnapDomain, []testFileRange{{mr.from / stepSize, mr.to / stepSize}})

		items := repo.FilesInRange(mr, vf) // vf passed should ideally from rotx, but doesn't matter here
		require.Len(t, items, nFilesInRange)

		merged := newFilesItemWithSnapConfig(mr.from, mr.to, repo.cfg)
		repo.IntegrateDirtyFile(merged)
		require.NoError(t, repo.openDirtyFiles())
		repo.RecalcVisibleFiles(RootNum(MaxUint64))

		vf = repo.visibleFiles()
		require.Len(t, vf, nVfAfterMerge)

		repo.CleanAfterMerge(merged, vf)
		require.Equal(t, repo.dirtyFiles.Len(), dirtyFilesAfterMerge)

		cleanupFiles(t, repo, dirs)
	}

	// 0-1, 1-2 => 0-2
	testFn([]testFileRange{{0, 1}, {1, 2}}, true, 2, 1, 1)

	// 0-1, 1-2, 2-3 => 0-2, 2-3
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}}, true, 2, 2, 2)

	// 0-1, 1-2, 2-3, 3-4 => 0-4
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}}, true, 4, 1, 1)

	// 0-1, 1-2, 2-3, 3-4, 4-5, 5-6, 6-7 => 0-1, 1-2, 2-3, 3-4, 4-6, 6-7
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}}, true, 2, 6, 6)

	// 0-1, 1-2, 2-3, 3-4, 4-6, 6-7 => 0-4, 4-6, 6-7
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 6}, {6, 7}}, true, 4, 3, 3)

	// 0-4, 4-6, 6-7 => same
	testFn([]testFileRange{{0, 4}, {4, 6}, {6, 7}}, false, 0, 0, 0)

	// 0-1, 1-2, 2-3, 3-4, 0-4 => no merge
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {0, 4}}, false, 0, 1, 1)

	// 0-1, 1-2, 2-3, 3-4, 0-2 => 0-4
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {0, 2}}, true, 3, 1, 1)

	// 0-1, 1-2, ..... 14-15 => 0-1....12-13, 13-15
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}, {8, 9}, {9, 10}, {10, 11}, {11, 12}, {12, 13}, {13, 14}, {14, 15}}, true, 2, 14, 14)

	//0-1....12-13, 13-15, 15-16 => 0-16
	testFn([]testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}, {8, 9}, {9, 10}, {10, 11}, {11, 12}, {12, 13}, {13, 15}, {15, 16}}, true, 15, 1, 1)
}

// /////////////////////////////////////// helpers and utils

func cleanupFiles(t *testing.T, repo *SnapshotRepo, dirs datadir.Dirs) {
	t.Helper()
	repo.Close()
	repo.RecalcVisibleFiles(0)
	filepath.Walk(dirs.DataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		os.Remove(path)
		return nil
	})
}

func stepToRootNum(t *testing.T, step uint64, repo *SnapshotRepo) RootNum {
	t.Helper()
	return RootNum(repo.cfg.RootNumPerStep * step)
}

func setupEntity(t *testing.T, dirs datadir.Dirs, genRepo func(stepSize uint64, dirs datadir.Dirs) (name string, schema ee.SnapNameSchema)) (name string, repo *SnapshotRepo) {
	t.Helper()
	stepSize := uint64(10)
	name, schema := genRepo(stepSize, dirs)

	createConfig := ee.SnapshotCreationConfig{
		RootNumPerStep: stepSize,
		MergeStages:    []uint64{20, 40},
		MinimumSize:    10,
		SafetyMargin:   5,
	}
	repo = NewSnapshotRepo(name, &ee.SnapshotConfig{
		SnapshotCreationConfig: &createConfig,
		Schema:                 schema,
	}, log.New())

	return name, repo
}

type dhiiFiles struct {
	domainFiles   []string
	historyFiles  []string
	accessorFiles []string
	idxFiles      []string
	fullPath      bool
}

type testFileRange struct {
	fromStep, toStep uint64
}

func populateFilesFull(t *testing.T, dirs datadir.Dirs, name string, extensions []string, dataFolder string) (dataFileCount, btCount, existenceCount, accessorCount int) {
	t.Helper()
	allFiles := &dhiiFiles{
		domainFiles:   []string{"v1.0-accounts.0-256.bt", "v1.0-accounts.0-256.bt.torrent", "v1.0-accounts.0-256.kv", "v1.0-accounts.0-256.kv.torrent", "v1.0-accounts.0-256.kvei", "v1.0-accounts.0-256.kvei.torrent", "v1.0-accounts.256-288.bt", "v1.0-accounts.256-288.bt.torrent", "v1.0-accounts.256-288.kv", "v1.0-accounts.256-288.kv.torrent", "v1.0-accounts.256-288.kvei", "v1.0-accounts.256-288.kvei.torrent", "v1.0-accounts.288-296.bt", "v1.0-accounts.288-296.bt.torrent", "v1.0-accounts.288-296.kv", "v1.0-accounts.288-296.kv.torrent", "v1.0-accounts.288-296.kvei", "v1.0-accounts.288-296.kvei.torrent", "v1.0-accounts.296-298.bt", "v1.0-accounts.296-298.bt.torrent", "v1.0-accounts.296-298.kv", "v1.0-accounts.296-298.kv.torrent", "v1.0-accounts.296-298.kvei", "v1.0-accounts.296-298.kvei.torrent", "v1.0-code.0-256.bt", "v1.0-code.0-256.bt.torrent", "v1.0-code.0-256.kv", "v1.0-code.0-256.kv.torrent", "v1.0-code.0-256.kvei", "v1.0-code.0-256.kvei.torrent", "v1.0-code.256-288.bt", "v1.0-code.256-288.bt.torrent", "v1.0-code.256-288.kv", "v1.0-code.256-288.kv.torrent", "v1.0-code.256-288.kvei", "v1.0-code.256-288.kvei.torrent", "v1.0-code.288-296.bt", "v1.0-code.288-296.bt.torrent", "v1.0-code.288-296.kv", "v1.0-code.288-296.kv.torrent", "v1.0-code.288-296.kvei", "v1.0-code.288-296.kvei.torrent", "v1.0-code.296-298.bt", "v1.0-code.296-298.bt.torrent", "v1.0-code.296-298.kv", "v1.0-code.296-298.kv.torrent", "v1.0-code.296-298.kvei", "v1.0-code.296-298.kvei.torrent", "v1.0-commitment.0-256.kv", "v1.0-commitment.0-256.kv.torrent", "v1.0-commitment.0-256.kvi", "v1.0-commitment.0-256.kvi.torrent", "v1.0-commitment.256-288.kv", "v1.0-commitment.256-288.kv.torrent", "v1.0-commitment.256-288.kvi", "v1.0-commitment.256-288.kvi.torrent", "v1.0-commitment.288-296.kv", "v1.0-commitment.288-296.kv.torrent", "v1.0-commitment.288-296.kvi", "v1.0-commitment.288-296.kvi.torrent", "v1.0-commitment.296-298.kv", "v1.0-commitment.296-298.kv.torrent", "v1.0-commitment.296-298.kvi", "v1.0-commitment.296-298.kvi.torrent", "v1.0-receipt.0-256.bt", "v1.0-receipt.0-256.bt.torrent", "v1.0-receipt.0-256.kv", "v1.0-receipt.0-256.kv.torrent", "v1.0-receipt.0-256.kvei", "v1.0-receipt.0-256.kvei.torrent", "v1.0-receipt.256-288.bt", "v1.0-receipt.256-288.bt.torrent", "v1.0-receipt.256-288.kv", "v1.0-receipt.256-288.kv.torrent", "v1.0-receipt.256-288.kvei", "v1.0-receipt.256-288.kvei.torrent", "v1.0-receipt.288-296.bt", "v1.0-receipt.288-296.bt.torrent", "v1.0-receipt.288-296.kv", "v1.0-receipt.288-296.kv.torrent", "v1.0-receipt.288-296.kvei", "v1.0-receipt.288-296.kvei.torrent", "v1.0-receipt.296-298.bt", "v1.0-receipt.296-298.bt.torrent", "v1.0-receipt.296-298.kv", "v1.0-receipt.296-298.kv.torrent", "v1.0-receipt.296-298.kvei", "v1.0-receipt.296-298.kvei.torrent", "v1.0-storage.0-256.bt", "v1.0-storage.0-256.bt.torrent", "v1.0-storage.0-256.kv", "v1.0-storage.0-256.kv.torrent", "v1.0-storage.0-256.kvei", "v1.0-storage.0-256.kvei.torrent", "v1.0-storage.256-288.bt", "v1.0-storage.256-288.bt.torrent", "v1.0-storage.256-288.kv", "v1.0-storage.256-288.kv.torrent", "v1.0-storage.256-288.kvei", "v1.0-storage.256-288.kvei.torrent", "v1.0-storage.288-296.bt", "v1.0-storage.288-296.bt.torrent", "v1.0-storage.288-296.kv", "v1.0-storage.288-296.kv.torrent", "v1.0-storage.288-296.kvei", "v1.0-storage.288-296.kvei.torrent", "v1.0-storage.296-298.bt", "v1.0-storage.296-298.bt.torrent", "v1.0-storage.296-298.kv", "v1.0-storage.296-298.kv.torrent", "v1.0-storage.296-298.kvei", "v1.0-storage.296-298.kvei.torrent"},
		historyFiles:  []string{"v1.0-accounts.0-64.v", "v1.0-accounts.0-64.v.torrent", "v1.0-accounts.128-192.v", "v1.0-accounts.128-192.v.torrent", "v1.0-accounts.192-256.v", "v1.0-accounts.192-256.v.torrent", "v1.0-accounts.256-288.v", "v1.0-accounts.256-288.v.torrent", "v1.0-accounts.288-296.v", "v1.0-accounts.288-296.v.torrent", "v1.0-accounts.296-298.v", "v1.0-accounts.296-298.v.torrent", "v1.0-accounts.64-128.v", "v1.0-accounts.64-128.v.torrent", "v1.0-code.0-64.v", "v1.0-code.0-64.v.torrent", "v1.0-code.128-192.v", "v1.0-code.128-192.v.torrent", "v1.0-code.192-256.v", "v1.0-code.192-256.v.torrent", "v1.0-code.256-288.v", "v1.0-code.256-288.v.torrent", "v1.0-code.288-296.v", "v1.0-code.288-296.v.torrent", "v1.0-code.296-298.v", "v1.0-code.296-298.v.torrent", "v1.0-code.64-128.v", "v1.0-code.64-128.v.torrent", "v1.0-receipt.0-64.v", "v1.0-receipt.0-64.v.torrent", "v1.0-receipt.128-192.v", "v1.0-receipt.128-192.v.torrent", "v1.0-receipt.192-256.v", "v1.0-receipt.192-256.v.torrent", "v1.0-receipt.256-288.v", "v1.0-receipt.256-288.v.torrent", "v1.0-receipt.288-296.v", "v1.0-receipt.288-296.v.torrent", "v1.0-receipt.296-298.v", "v1.0-receipt.296-298.v.torrent", "v1.0-receipt.64-128.v", "v1.0-receipt.64-128.v.torrent", "v1.0-storage.0-64.v", "v1.0-storage.0-64.v.torrent", "v1.0-storage.128-192.v", "v1.0-storage.128-192.v.torrent", "v1.0-storage.192-256.v", "v1.0-storage.192-256.v.torrent", "v1.0-storage.256-288.v", "v1.0-storage.256-288.v.torrent", "v1.0-storage.288-296.v", "v1.0-storage.288-296.v.torrent", "v1.0-storage.296-298.v", "v1.0-storage.296-298.v.torrent", "v1.0-storage.64-128.v", "v1.0-storage.64-128.v.torrent"},
		accessorFiles: []string{"v1.0-accounts.0-64.efi", "v1.0-accounts.0-64.efi.torrent", "v1.0-accounts.0-64.vi", "v1.0-accounts.0-64.vi.torrent", "v1.0-accounts.128-192.efi", "v1.0-accounts.128-192.efi.torrent", "v1.0-accounts.128-192.vi", "v1.0-accounts.128-192.vi.torrent", "v1.0-accounts.192-256.efi", "v1.0-accounts.192-256.efi.torrent", "v1.0-accounts.192-256.vi", "v1.0-accounts.192-256.vi.torrent", "v1.0-accounts.256-288.efi", "v1.0-accounts.256-288.efi.torrent", "v1.0-accounts.256-288.vi", "v1.0-accounts.256-288.vi.torrent", "v1.0-accounts.288-296.efi", "v1.0-accounts.288-296.efi.torrent", "v1.0-accounts.288-296.vi", "v1.0-accounts.288-296.vi.torrent", "v1.0-accounts.296-298.efi", "v1.0-accounts.296-298.efi.torrent", "v1.0-accounts.296-298.vi", "v1.0-accounts.296-298.vi.torrent", "v1.0-accounts.64-128.efi", "v1.0-accounts.64-128.efi.torrent", "v1.0-accounts.64-128.vi", "v1.0-accounts.64-128.vi.torrent", "v1.0-code.0-64.efi", "v1.0-code.0-64.efi.torrent", "v1.0-code.0-64.vi", "v1.0-code.0-64.vi.torrent", "v1.0-code.128-192.efi", "v1.0-code.128-192.efi.torrent", "v1.0-code.128-192.vi", "v1.0-code.128-192.vi.torrent", "v1.0-code.192-256.efi", "v1.0-code.192-256.efi.torrent", "v1.0-code.192-256.vi", "v1.0-code.192-256.vi.torrent", "v1.0-code.256-288.efi", "v1.0-code.256-288.efi.torrent", "v1.0-code.256-288.vi", "v1.0-code.256-288.vi.torrent", "v1.0-code.288-296.efi", "v1.0-code.288-296.efi.torrent", "v1.0-code.288-296.vi", "v1.0-code.288-296.vi.torrent", "v1.0-code.296-298.efi", "v1.0-code.296-298.efi.torrent", "v1.0-code.296-298.vi", "v1.0-code.296-298.vi.torrent", "v1.0-code.64-128.efi", "v1.0-code.64-128.efi.torrent", "v1.0-code.64-128.vi", "v1.0-code.64-128.vi.torrent", "v1.0-logaddrs.0-64.efi", "v1.0-logaddrs.0-64.efi.torrent", "v1.0-logaddrs.128-192.efi", "v1.0-logaddrs.128-192.efi.torrent", "v1.0-logaddrs.192-256.efi", "v1.0-logaddrs.192-256.efi.torrent", "v1.0-logaddrs.256-288.efi", "v1.0-logaddrs.256-288.efi.torrent", "v1.0-logaddrs.288-296.efi", "v1.0-logaddrs.288-296.efi.torrent", "v1.0-logaddrs.296-298.efi", "v1.0-logaddrs.296-298.efi.torrent", "v1.0-logaddrs.64-128.efi", "v1.0-logaddrs.64-128.efi.torrent", "v1.0-logtopics.0-64.efi", "v1.0-logtopics.0-64.efi.torrent", "v1.0-logtopics.128-192.efi", "v1.0-logtopics.128-192.efi.torrent", "v1.0-logtopics.192-256.efi", "v1.0-logtopics.192-256.efi.torrent", "v1.0-logtopics.256-288.efi", "v1.0-logtopics.256-288.efi.torrent", "v1.0-logtopics.288-296.efi", "v1.0-logtopics.288-296.efi.torrent", "v1.0-logtopics.296-298.efi", "v1.0-logtopics.296-298.efi.torrent", "v1.0-logtopics.64-128.efi", "v1.0-logtopics.64-128.efi.torrent", "v1.0-receipt.0-64.efi", "v1.0-receipt.0-64.efi.torrent", "v1.0-receipt.0-64.vi", "v1.0-receipt.0-64.vi.torrent", "v1.0-receipt.128-192.efi", "v1.0-receipt.128-192.efi.torrent", "v1.0-receipt.128-192.vi", "v1.0-receipt.128-192.vi.torrent", "v1.0-receipt.192-256.efi", "v1.0-receipt.192-256.efi.torrent", "v1.0-receipt.192-256.vi", "v1.0-receipt.192-256.vi.torrent", "v1.0-receipt.256-288.efi", "v1.0-receipt.256-288.efi.torrent", "v1.0-receipt.256-288.vi", "v1.0-receipt.256-288.vi.torrent", "v1.0-receipt.288-296.efi", "v1.0-receipt.288-296.efi.torrent", "v1.0-receipt.288-296.vi", "v1.0-receipt.288-296.vi.torrent", "v1.0-receipt.296-298.efi", "v1.0-receipt.296-298.efi.torrent", "v1.0-receipt.296-298.vi", "v1.0-receipt.296-298.vi.torrent", "v1.0-receipt.64-128.efi", "v1.0-receipt.64-128.efi.torrent", "v1.0-receipt.64-128.vi", "v1.0-receipt.64-128.vi.torrent", "v1.0-storage.0-64.efi", "v1.0-storage.0-64.efi.torrent", "v1.0-storage.0-64.vi", "v1.0-storage.0-64.vi.torrent", "v1.0-storage.128-192.efi", "v1.0-storage.128-192.efi.torrent", "v1.0-storage.128-192.vi", "v1.0-storage.128-192.vi.torrent", "v1.0-storage.192-256.efi", "v1.0-storage.192-256.efi.torrent", "v1.0-storage.192-256.vi", "v1.0-storage.192-256.vi.torrent", "v1.0-storage.256-288.efi", "v1.0-storage.256-288.efi.torrent", "v1.0-storage.256-288.vi", "v1.0-storage.256-288.vi.torrent", "v1.0-storage.288-296.efi", "v1.0-storage.288-296.efi.torrent", "v1.0-storage.288-296.vi", "v1.0-storage.288-296.vi.torrent", "v1.0-storage.296-298.efi", "v1.0-storage.296-298.efi.torrent", "v1.0-storage.296-298.vi", "v1.0-storage.296-298.vi.torrent", "v1.0-storage.64-128.efi", "v1.0-storage.64-128.efi.torrent", "v1.0-storage.64-128.vi", "v1.0-storage.64-128.vi.torrent", "v1.0-tracesfrom.0-64.efi", "v1.0-tracesfrom.0-64.efi.torrent", "v1.0-tracesfrom.128-192.efi", "v1.0-tracesfrom.128-192.efi.torrent", "v1.0-tracesfrom.192-256.efi", "v1.0-tracesfrom.192-256.efi.torrent", "v1.0-tracesfrom.256-288.efi", "v1.0-tracesfrom.256-288.efi.torrent", "v1.0-tracesfrom.288-296.efi", "v1.0-tracesfrom.288-296.efi.torrent", "v1.0-tracesfrom.296-298.efi", "v1.0-tracesfrom.296-298.efi.torrent", "v1.0-tracesfrom.64-128.efi", "v1.0-tracesfrom.64-128.efi.torrent", "v1.0-tracesto.0-64.efi", "v1.0-tracesto.0-64.efi.torrent", "v1.0-tracesto.128-192.efi", "v1.0-tracesto.128-192.efi.torrent", "v1.0-tracesto.192-256.efi", "v1.0-tracesto.192-256.efi.torrent", "v1.0-tracesto.256-288.efi", "v1.0-tracesto.256-288.efi.torrent", "v1.0-tracesto.288-296.efi", "v1.0-tracesto.288-296.efi.torrent", "v1.0-tracesto.296-298.efi", "v1.0-tracesto.296-298.efi.torrent", "v1.0-tracesto.64-128.efi", "v1.0-tracesto.64-128.efi.torrent"},
		idxFiles:      []string{"v1.0-accounts.0-64.ef", "v1.0-accounts.0-64.ef.torrent", "v1.0-accounts.128-192.ef", "v1.0-accounts.128-192.ef.torrent", "v1.0-accounts.192-256.ef", "v1.0-accounts.192-256.ef.torrent", "v1.0-accounts.256-288.ef", "v1.0-accounts.256-288.ef.torrent", "v1.0-accounts.288-296.ef", "v1.0-accounts.288-296.ef.torrent", "v1.0-accounts.296-298.ef", "v1.0-accounts.296-298.ef.torrent", "v1.0-accounts.64-128.ef", "v1.0-accounts.64-128.ef.torrent", "v1.0-code.0-64.ef", "v1.0-code.0-64.ef.torrent", "v1.0-code.128-192.ef", "v1.0-code.128-192.ef.torrent", "v1.0-code.192-256.ef", "v1.0-code.192-256.ef.torrent", "v1.0-code.256-288.ef", "v1.0-code.256-288.ef.torrent", "v1.0-code.288-296.ef", "v1.0-code.288-296.ef.torrent", "v1.0-code.296-298.ef", "v1.0-code.296-298.ef.torrent", "v1.0-code.64-128.ef", "v1.0-code.64-128.ef.torrent", "v1.0-logaddrs.0-64.ef", "v1.0-logaddrs.0-64.ef.torrent", "v1.0-logaddrs.128-192.ef", "v1.0-logaddrs.128-192.ef.torrent", "v1.0-logaddrs.192-256.ef", "v1.0-logaddrs.192-256.ef.torrent", "v1.0-logaddrs.256-288.ef", "v1.0-logaddrs.256-288.ef.torrent", "v1.0-logaddrs.288-296.ef", "v1.0-logaddrs.288-296.ef.torrent", "v1.0-logaddrs.296-298.ef", "v1.0-logaddrs.296-298.ef.torrent", "v1.0-logaddrs.64-128.ef", "v1.0-logaddrs.64-128.ef.torrent", "v1.0-logtopics.0-64.ef", "v1.0-logtopics.0-64.ef.torrent", "v1.0-logtopics.128-192.ef", "v1.0-logtopics.128-192.ef.torrent", "v1.0-logtopics.192-256.ef", "v1.0-logtopics.192-256.ef.torrent", "v1.0-logtopics.256-288.ef", "v1.0-logtopics.256-288.ef.torrent", "v1.0-logtopics.288-296.ef", "v1.0-logtopics.288-296.ef.torrent", "v1.0-logtopics.296-298.ef", "v1.0-logtopics.296-298.ef.torrent", "v1.0-logtopics.64-128.ef", "v1.0-logtopics.64-128.ef.torrent", "v1.0-receipt.0-64.ef", "v1.0-receipt.0-64.ef.torrent", "v1.0-receipt.128-192.ef", "v1.0-receipt.128-192.ef.torrent", "v1.0-receipt.192-256.ef", "v1.0-receipt.192-256.ef.torrent", "v1.0-receipt.256-288.ef", "v1.0-receipt.256-288.ef.torrent", "v1.0-receipt.288-296.ef", "v1.0-receipt.288-296.ef.torrent", "v1.0-receipt.296-298.ef", "v1.0-receipt.296-298.ef.torrent", "v1.0-receipt.64-128.ef", "v1.0-receipt.64-128.ef.torrent", "v1.0-storage.0-64.ef", "v1.0-storage.0-64.ef.torrent", "v1.0-storage.128-192.ef", "v1.0-storage.128-192.ef.torrent", "v1.0-storage.192-256.ef", "v1.0-storage.192-256.ef.torrent", "v1.0-storage.256-288.ef", "v1.0-storage.256-288.ef.torrent", "v1.0-storage.288-296.ef", "v1.0-storage.288-296.ef.torrent", "v1.0-storage.296-298.ef", "v1.0-storage.296-298.ef.torrent", "v1.0-storage.64-128.ef", "v1.0-storage.64-128.ef.torrent", "v1.0-tracesfrom.0-64.ef", "v1.0-tracesfrom.0-64.ef.torrent", "v1.0-tracesfrom.128-192.ef", "v1.0-tracesfrom.128-192.ef.torrent", "v1.0-tracesfrom.192-256.ef", "v1.0-tracesfrom.192-256.ef.torrent", "v1.0-tracesfrom.256-288.ef", "v1.0-tracesfrom.256-288.ef.torrent", "v1.0-tracesfrom.288-296.ef", "v1.0-tracesfrom.288-296.ef.torrent", "v1.0-tracesfrom.296-298.ef", "v1.0-tracesfrom.296-298.ef.torrent", "v1.0-tracesfrom.64-128.ef", "v1.0-tracesfrom.64-128.ef.torrent", "v1.0-tracesto.0-64.ef", "v1.0-tracesto.0-64.ef.torrent", "v1.0-tracesto.128-192.ef", "v1.0-tracesto.128-192.ef.torrent", "v1.0-tracesto.192-256.ef", "v1.0-tracesto.192-256.ef.torrent", "v1.0-tracesto.256-288.ef", "v1.0-tracesto.256-288.ef.torrent", "v1.0-tracesto.288-296.ef", "v1.0-tracesto.288-296.ef.torrent", "v1.0-tracesto.296-298.ef", "v1.0-tracesto.296-298.ef.torrent", "v1.0-tracesto.64-128.ef", "v1.0-tracesto.64-128.ef.torrent"},
		fullPath:      false,
	}
	return populateFiles(t, dirs, name, extensions, dataFolder, allFiles)
}

func populateFiles2(t *testing.T, dirs datadir.Dirs, name string, repo *SnapshotRepo, dataFolder string, ranges []testFileRange) (dataFileCount, btCount, existenceCount, accessorCount int) {
	t.Helper()
	allFiles := dhiiFiles{fullPath: true}
	extensions := repo.cfg.Schema.(*ee.E3SnapSchema).FileExtensions()
	v := version.V1_0
	acc := repo.schema.AccessorList()
	for _, r := range ranges {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		allFiles.domainFiles = append(allFiles.domainFiles, repo.schema.DataFile(v, from, to))
		if acc.Has(AccessorBTree) {
			f := repo.schema.BtIdxFile(v, from, to)
			allFiles.domainFiles = append(allFiles.domainFiles, f)
		}
		if acc.Has(AccessorExistence) {
			allFiles.domainFiles = append(allFiles.domainFiles, repo.schema.ExistenceFile(v, from, to))
		}
		if acc.Has(AccessorHashMap) {
			if containsSubstring(t, ee.AccessorExtensionKvi.String(), extensions) {
				allFiles.domainFiles = append(allFiles.domainFiles, repo.schema.AccessorIdxFile(v, from, to, 0))
			} else {
				allFiles.accessorFiles = append(allFiles.accessorFiles, repo.schema.AccessorIdxFile(v, from, to, 0))
			}
		}
	}

	return populateFiles(t, dirs, name, extensions, dataFolder, &allFiles)
}

func populateFiles(t *testing.T, dirs datadir.Dirs, name string, extensions []string, dataFolder string, allFiles *dhiiFiles) (dataFileCount, btCount, existenceCount, accessorCount int) {
	t.Helper()

	// populate data files

	// 1. account domain, history and ii

	domainFolder := dirs.SnapDomain
	historyFolder := dirs.SnapHistory
	accessorFolder := dirs.SnapAccessors
	idxFolder := dirs.SnapIdx

	fileGen := func(filename string) {
		if strings.HasSuffix(filename, ".ef") || strings.HasSuffix(filename, ".v") || strings.HasSuffix(filename, ".kv") {
			seg, err := seg.NewCompressor(context.Background(), t.Name(), filename, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
			require.NoError(t, err)
			if err = seg.AddWord([]byte("word")); err != nil {
				t.Fatal(err)
			}
			require.NoError(t, seg.Compress())
			seg.Close()

			if strings.Contains(filename, name) && containsSubstring(t, filename, extensions) && strings.Contains(filename, dataFolder) {
				dataFileCount++
			}

			return
		}

		if strings.HasSuffix(filename, ".bt") {
			seg2, err := seg.NewCompressor(context.Background(), t.Name(), filename+".sample", dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
			require.NoError(t, err)
			if err = seg2.AddWord([]byte("key")); err != nil {
				t.Fatal(err)
			}
			if err = seg2.AddWord([]byte("value")); err != nil {
				t.Fatal(err)
			}
			require.NoError(t, seg2.Compress())
			seg2.Close()
			seg3, err := seg.NewDecompressor(filename + ".sample")
			require.NoError(t, err)

			r := seg.NewReader(seg3.MakeGetter(), seg.CompressNone)
			btindex, err := CreateBtreeIndexWithDecompressor(filename, 128, r, uint32(1), background.NewProgressSet(), dirs.Tmp, log.New(), false, AccessorBTree|AccessorExistence)
			if err != nil {
				t.Fatal(err)
			}
			seg3.Close()
			btindex.Close()

			if strings.Contains(filename, name) && containsSubstring(t, filename, extensions) {
				btCount++
			}

			return
		}

		if strings.HasSuffix(filename, ".kvei") {
			filter, err := existence.NewFilter(0, filename)
			require.NoError(t, err)
			require.NoError(t, filter.Build())
			filter.Close()

			if strings.Contains(filename, name) && containsSubstring(t, filename, extensions) {
				existenceCount++
			}

			return
		}

		if strings.HasSuffix(filename, ".kvi") || strings.HasSuffix(filename, ".vi") || strings.HasSuffix(filename, ".efi") {
			salt := uint32(1)
			rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
				KeyCount:   1,
				BucketSize: 10,
				Salt:       &salt,
				TmpDir:     dirs.Tmp,
				IndexFile:  filename,
				LeafSize:   8,
			}, log.New())
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()

			if err = rs.AddKey([]byte("first_key"), 0); err != nil {
				t.Error(err)
			}
			if err = rs.Build(context.Background()); err != nil {
				t.Errorf("test is expected to fail, too few keys added")
			}
			rs.Close()
			if strings.Contains(filename, name) && containsSubstring(t, filename, extensions) {
				accessorCount++
			}

			return
		}

	}

	touch(t, domainFolder, allFiles.domainFiles, fileGen, allFiles.fullPath)
	touch(t, historyFolder, allFiles.historyFiles, fileGen, allFiles.fullPath)
	touch(t, accessorFolder, allFiles.accessorFiles, fileGen, allFiles.fullPath)
	touch(t, idxFolder, allFiles.idxFiles, fileGen, allFiles.fullPath)

	return dataFileCount, btCount, existenceCount, accessorCount
}

func touch(t *testing.T, folder string, files []string, fileGen func(filename string), fullPath bool) {
	t.Helper()
	for _, f := range files {
		filename := f
		if !fullPath {
			filename = filepath.Join(folder, f)
		}

		// touchFile(t, filename)
		fileGen(filename)
	}
}

func containsSubstring(t *testing.T, str string, list []string) bool {
	t.Helper()
	for _, s := range list {
		if strings.Contains(str, s) {
			return true
		}
	}
	return false
}

func fileExistsCheck(t *testing.T, repo *SnapshotRepo, startStep, endStep uint64, isFound bool) {
	t.Helper()
	stepSize := repo.stepSize
	startTxNum, endTxNum := startStep*stepSize, endStep*stepSize
	_, found := repo.dirtyFiles.Get(&FilesItem{startTxNum: startTxNum, endTxNum: endTxNum})
	require.Equal(t, found, isFound)

	_, err := os.Stat(repo.cfg.Schema.DataFile(version.V1_0, ee.RootNum(startTxNum), ee.RootNum(endTxNum)))
	if isFound {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		require.True(t, os.IsNotExist(err))
	}

}
