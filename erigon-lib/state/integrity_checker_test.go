package state

import (
	"context"
	"fmt"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestDependency(t *testing.T) {
	// shouldn't allow to delete file
	// commitment.0-1, 1-2
	// account.0-1, 1-2, 0-2
	// storage.0-1, 1-2, 0-2

	dirs := datadir.New(t.TempDir())
	logger := log.New()
	dfs := btree.NewBTreeGOptions[*filesItem](filesItemLess, btree.Options{Degree: 128, NoLocks: false})
	df1 := getPopulatedCommitmentFilesItem(t, dirs, 0, 1, false, logger)
	df2 := getPopulatedCommitmentFilesItem(t, dirs, 1, 2, false, logger)
	dfs.Set(df1)
	dfs.Set(df2)
	fg := func() *btree.BTreeG[*filesItem] {
		// only commitment files
		return dfs.Copy()
	}

	dinfo := &DependentInfo{
		domain:      kv.CommitmentDomain,
		filesGetter: fg,
		accessors:   AccessorHashMap,
	}

	checker := NewDependencyIntegrityChecker(dirs, logger)
	checker.AddDependency(kv.AccountsDomain, dinfo)
	// not adding dependency for storage

	assertFn := func(startTxNum, endTxNum uint64, resultC, resultA, resultS bool) {
		require.Equal(t, resultA, checker.CheckAllDependentPresent(kv.AccountsDomain, startTxNum, endTxNum))
		require.Equal(t, resultS, checker.CheckAllDependentPresent(kv.StorageDomain, startTxNum, endTxNum))
		require.Equal(t, resultC, checker.CheckAllDependentPresent(kv.CommitmentDomain, startTxNum, endTxNum))

	}

	assertFn(0, 1, true, true, true)
	assertFn(1, 2, true, true, true)
	assertFn(0, 2, true, false, true)
}

func TestDependency_UnindexedMerged(t *testing.T) {
	// shouldn't allow to delete file
	// commitment.0-1, 1-2, 0-2; but 0-2 is unindexed
	// account.0-1, 1-2, 0-2
	// storage.0-1, 1-2, 0-2

	dirs := datadir.New(t.TempDir())
	logger := log.New()
	dfs := btree.NewBTreeGOptions[*filesItem](filesItemLess, btree.Options{Degree: 128, NoLocks: false})
	df1 := getPopulatedCommitmentFilesItem(t, dirs, 0, 1, false, logger)
	df2 := getPopulatedCommitmentFilesItem(t, dirs, 1, 2, false, logger)
	df3 := getPopulatedCommitmentFilesItem(t, dirs, 0, 2, true, logger)
	dfs.Set(df1)
	dfs.Set(df2)
	dfs.Set(df3)
	fg := func() *btree.BTreeG[*filesItem] {
		// only commitment files
		return dfs.Copy()
	}

	dinfo := &DependentInfo{
		domain:      kv.CommitmentDomain,
		filesGetter: fg,
		accessors:   AccessorHashMap,
	}

	checker := NewDependencyIntegrityChecker(dirs, logger)
	checker.AddDependency(kv.AccountsDomain, dinfo)
	// not adding dependency for storage

	assertFn := func(startTxNum, endTxNum uint64, resultC, resultA, resultS bool) {
		require.Equal(t, resultA, checker.CheckAllDependentPresent(kv.AccountsDomain, startTxNum, endTxNum))
		require.Equal(t, resultS, checker.CheckAllDependentPresent(kv.StorageDomain, startTxNum, endTxNum))
		require.Equal(t, resultC, checker.CheckAllDependentPresent(kv.CommitmentDomain, startTxNum, endTxNum))

	}

	assertFn(0, 1, true, true, true)
	assertFn(1, 2, true, true, true)
	assertFn(0, 2, true, false, true)
}

func getPopulatedCommitmentFilesItem(t *testing.T, dirs datadir.Dirs, startTxNum, endTxNum uint64, noIndex bool, logger log.Logger) *filesItem {
	t.Helper()

	base := fmt.Sprintf(dirs.Snap+"/commitment.%d-%d", startTxNum, endTxNum)
	comp, err := seg.NewCompressor(context.Background(), "", base+"data", dirs.Tmp, seg.DefaultCfg, log.LvlInfo, logger)
	require.NoError(t, err)
	require.NotNil(t, comp)

	err = comp.Compress()
	require.NoError(t, err)

	decomp, err := seg.NewDecompressor(base + "data")
	require.NoError(t, err)
	require.NotNil(t, decomp)

	salt := uint32(1)
	var idx0 *recsplit.Index
	if !noIndex {
		index, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   0,
			BucketSize: 10,
			Salt:       &salt,
			TmpDir:     dirs.Tmp,
			IndexFile:  base + "index",
			LeafSize:   8,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, index)

		require.NoError(t, index.Build(context.Background()))

		idx0 = recsplit.MustOpen(base + "index")
	}

	return &filesItem{decompressor: decomp, index: idx0, startTxNum: startTxNum, endTxNum: endTxNum}
}
