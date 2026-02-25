package state

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestDependency(t *testing.T) {
	// shouldn't pass dependency file not present in dependent
	// commitment.0-1, 1-2 => 0-1, 1-2
	// account.0-1, 1-2, 0-2 => 0-1, 1-2
	// storage.0-1, 1-2, 0-2 => 0-1, 1-2

	dirs := datadir.New(t.TempDir())
	logger := log.New()
	dfs := btree.NewBTreeGOptions(filesItemLess, btree.Options{Degree: 128, NoLocks: false})
	df1 := getPopulatedCommitmentFilesItem(t, dirs, 0, 1, false, logger)
	df2 := getPopulatedCommitmentFilesItem(t, dirs, 1, 2, false, logger)
	dfs.Set(df1)
	dfs.Set(df2)
	fg := func() *btree.BTreeG[*FilesItem] {
		// only commitment files
		return dfs.Copy()
	}

	dinfo := &DependentInfo{
		entity:      CommitmentDomainUniversal,
		filesGetter: fg,
		accessors:   statecfg.AccessorHashMap,
	}

	checker := NewDependencyIntegrityChecker(dirs, logger)
	checker.AddDependency(AccountDomainUniversal, dinfo)
	// not adding dependency for storage

	assertFn := func(startTxNum, endTxNum uint64, resultC, resultA, resultS bool) {
		require.Equal(t, resultA, checker.CheckDependentPresent(AccountDomainUniversal, All, startTxNum, endTxNum))
		require.Equal(t, resultS, checker.CheckDependentPresent(StorageDomainUniversal, All, startTxNum, endTxNum))
		require.Equal(t, resultC, checker.CheckDependentPresent(CommitmentDomainUniversal, All, startTxNum, endTxNum))

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
	dfs := btree.NewBTreeGOptions(filesItemLess, btree.Options{Degree: 128, NoLocks: false})
	df1 := getPopulatedCommitmentFilesItem(t, dirs, 0, 1, false, logger)
	df2 := getPopulatedCommitmentFilesItem(t, dirs, 1, 2, false, logger)
	df3 := getPopulatedCommitmentFilesItem(t, dirs, 0, 2, true, logger)
	dfs.Set(df1)
	dfs.Set(df2)
	dfs.Set(df3)
	fg := func() *btree.BTreeG[*FilesItem] {
		// only commitment files
		return dfs.Copy()
	}

	dinfo := &DependentInfo{
		entity:      CommitmentDomainUniversal,
		filesGetter: fg,
		accessors:   statecfg.AccessorHashMap,
	}

	checker := NewDependencyIntegrityChecker(dirs, logger)
	checker.AddDependency(AccountDomainUniversal, dinfo)
	// not adding dependency for storage

	assertFn := func(startTxNum, endTxNum uint64, resultC, resultA, resultS bool) {
		require.Equal(t, resultA, checker.CheckDependentPresent(AccountDomainUniversal, All, startTxNum, endTxNum))
		require.Equal(t, resultS, checker.CheckDependentPresent(StorageDomainUniversal, All, startTxNum, endTxNum))
		require.Equal(t, resultC, checker.CheckDependentPresent(CommitmentDomainUniversal, All, startTxNum, endTxNum))

	}

	assertFn(0, 1, true, true, true)
	assertFn(1, 2, true, true, true)
	assertFn(0, 2, true, false, true)
}

func TestDependency_DisableInterDomain(t *testing.T) {
	// DisableInterDomain should bypass domain→domain (inter-domain) checks
	// while preserving II→history (intra-domain) checks.
	//
	// Setup:
	//   commitment domain files: 0-1, 1-2
	//   account domain files:    0-1, 1-2, 0-2
	//   history files:           0-1, 1-2 (no 0-2)
	//
	// Inter-domain dep: account domain → commitment domain
	//   account 0-2 has no matching commitment 0-2 → should FAIL normally
	// Intra-domain dep: commitment II → commitment history
	//   II 0-2 has no matching history 0-2 → should FAIL

	dirs := datadir.New(t.TempDir())
	logger := log.New()

	// commitment domain files: 0-1, 1-2
	commitmentFiles := btree.NewBTreeGOptions(filesItemLess, btree.Options{Degree: 128, NoLocks: false})
	commitmentFiles.Set(getPopulatedCommitmentFilesItem(t, dirs, 0, 1, false, logger))
	commitmentFiles.Set(getPopulatedCommitmentFilesItem(t, dirs, 1, 2, false, logger))

	// history files: 0-1, 1-2 (simulating no merged 0-2)
	historyFiles := btree.NewBTreeGOptions(filesItemLess, btree.Options{Degree: 128, NoLocks: false})
	historyFiles.Set(getPopulatedCommitmentFilesItem(t, dirs, 0, 1, false, logger))
	historyFiles.Set(getPopulatedCommitmentFilesItem(t, dirs, 1, 2, false, logger))

	checker := NewDependencyIntegrityChecker(dirs, logger)

	// Inter-domain: account → commitment
	checker.AddDependency(AccountDomainUniversal, &DependentInfo{
		entity:      CommitmentDomainUniversal,
		filesGetter: func() *btree.BTreeG[*FilesItem] { return commitmentFiles.Copy() },
		accessors:   statecfg.AccessorHashMap,
	})

	// Intra-domain: commitment II → commitment history
	commitmentII := FromII(kv.CommitmentHistoryIdx)
	checker.AddDependency(commitmentII, &DependentInfo{
		entity:      commitmentII,
		filesGetter: func() *btree.BTreeG[*FilesItem] { return historyFiles.Copy() },
		accessors:   statecfg.AccessorHashMap,
	})

	// Before DisableInterDomain: both should enforce
	require.True(t, checker.CheckDependentPresent(AccountDomainUniversal, All, 0, 1))
	require.False(t, checker.CheckDependentPresent(AccountDomainUniversal, All, 0, 2)) // no commitment 0-2
	require.True(t, checker.CheckDependentPresent(commitmentII, All, 0, 1))
	require.False(t, checker.CheckDependentPresent(commitmentII, All, 0, 2)) // no history 0-2

	// DisableInterDomain: inter-domain bypassed, intra-domain still active
	checker.DisableInterDomain()
	require.True(t, checker.CheckDependentPresent(AccountDomainUniversal, All, 0, 1))
	require.True(t, checker.CheckDependentPresent(AccountDomainUniversal, All, 0, 2)) // bypassed
	require.True(t, checker.CheckDependentPresent(commitmentII, All, 0, 1))
	require.False(t, checker.CheckDependentPresent(commitmentII, All, 0, 2)) // still enforced

	// EnableInterDomain: both enforce again
	checker.EnableInterDomain()
	require.False(t, checker.CheckDependentPresent(AccountDomainUniversal, All, 0, 2))
	require.False(t, checker.CheckDependentPresent(commitmentII, All, 0, 2))
}

func getPopulatedCommitmentFilesItem(t *testing.T, dirs datadir.Dirs, startTxNum, endTxNum uint64, noIndex bool, logger log.Logger) *FilesItem {
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

	t.Cleanup(func() {
		comp.Close()
		decomp.Close()
		if idx0 != nil {
			idx0.Close()
		}
	})

	return &FilesItem{decompressor: decomp, index: idx0, startTxNum: startTxNum, endTxNum: endTxNum}
}
