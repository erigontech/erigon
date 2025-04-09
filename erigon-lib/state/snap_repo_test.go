package state

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	ae "github.com/erigontech/erigon-lib/state/appendable_extras"
	"github.com/stretchr/testify/require"
)

// 1. create folder with content; OpenFolder contains all dirtyFiles (check the dirty files)
// 1.1 dirty file integration
// 2. CloseFilesAFterRootNum
// 3. check freezing range logics (different file)
// 4. merge files

func TestOpenFolder_AccountsDomain(t *testing.T) {
	name, dirs, repo := setupEntity(t, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ae.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ae.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ae.DataExtensionKv).
			BtIndex(seg.CompressNone).
			Existence().
			Build()

		return name, schema
	})
	defer repo.Close()
	extensions := repo.cfg.Schema.(*ae.E3SnapSchema).FileExtensions()
	dataCount, btCount, existenceCount, accessorCount := populateFiles(t, dirs, name, extensions, dirs.SnapDomain)
	require.Positive(t, dataCount)

	err := repo.OpenFolder()
	require.NoError(t, err)

	// check dirty files
	repo.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			filename := item.decompressor.FileName1
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
	name, dirs, repo := setupEntity(t, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ae.SnapNameSchema) {
		accessors := AccessorHashMap
		name = "code"
		schema = ae.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapIdx, name, ae.DataExtensionEf).
			Accessor(dirs.SnapAccessors, ae.AccessorExtensionEfi).Build()
		return name, schema
	})
	defer repo.Close()

	extensions := repo.cfg.Schema.(*ae.E3SnapSchema).FileExtensions()
	dataCount, btCount, existenceCount, accessorCount := populateFiles(t, dirs, name, extensions, dirs.SnapIdx)

	require.Positive(t, dataCount)

	err := repo.OpenFolder()
	require.NoError(t, err)

	// check dirty files
	repo.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			filename := item.decompressor.FileName1
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
	// setup account
	// add a dirty file
	// check presence of dirty file

	name, dirs, repo := setupEntity(t, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ae.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ae.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ae.DataExtensionKv).
			BtIndex(seg.CompressNone).
			Existence().
			Build()

		return name, schema
	})
	defer repo.Close()

	extensions := repo.cfg.Schema.(*ae.E3SnapSchema).FileExtensions()
	dataCount, _, _, _ := populateFiles(t, dirs, name, extensions, dirs.SnapDomain)
	require.Positive(t, dataCount)

	err := repo.OpenFolder()
	require.NoError(t, err)

	filesItem := newFilesItemWithSnapConfig(0, 1024, repo.cfg)
	filename := repo.parser.DataFile(snaptype.Version(1), 0, 1024)
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
	// setup account
	// set various root numbers and check if the right files are closed

	name, dirs, repo := setupEntity(t, func(stepSize uint64, dirs datadir.Dirs) (name string, schema ae.SnapNameSchema) {
		accessors := AccessorBTree | AccessorExistence
		name = "accounts"
		schema = ae.NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, ae.DataExtensionKv).
			BtIndex(seg.CompressNone).
			Existence().
			Build()
		return name, schema
	})
	defer repo.Close()

	extensions := repo.cfg.Schema.(*ae.E3SnapSchema).FileExtensions()
	dataCount, _, _, _ := populateFiles(t, dirs, name, extensions, dirs.SnapDomain)
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

func stepToRootNum(t *testing.T, step uint64, repo *SnapshotRepo) RootNum {
	t.Helper()
	return RootNum(repo.cfg.RootNumPerStep * step)
}

func setupEntity(t *testing.T, genRepo func(stepSize uint64, dirs datadir.Dirs) (name string, schema ae.SnapNameSchema)) (name string, dirs datadir.Dirs, repo *SnapshotRepo) {
	t.Helper()
	dirs = datadir.New(t.TempDir())
	stepSize := uint64(10)
	name, schema := genRepo(stepSize, dirs)

	createConfig := ae.SnapshotCreationConfig{
		RootNumPerStep: stepSize,
		MergeStages:    []uint64{20, 40},
		MinimumSize:    10,
		SafetyMargin:   5,
	}
	repo = NewSnapshotRepo(name, &ae.SnapshotConfig{
		SnapshotCreationConfig: &createConfig,
		Schema:                 schema,
	}, log.New())

	// t.Cleanup(func() {
	// 	repo.Close()
	// 	//os.RemoveAll(dirs.DataDir)
	// })

	return name, dirs, repo
}

func populateFiles(t *testing.T, dirs datadir.Dirs, name string, extensions []string, dataFolder string) (dataFileCount, btCount, existenceCount, accessorCount int) {
	t.Helper()

	// populate data files

	// 1. account domain, history and ii

	domainFolder := dirs.SnapDomain
	domainFiles := []string{"v1-accounts.0-256.bt", "v1-accounts.0-256.bt.torrent", "v1-accounts.0-256.kv", "v1-accounts.0-256.kv.torrent", "v1-accounts.0-256.kvei", "v1-accounts.0-256.kvei.torrent", "v1-accounts.256-288.bt", "v1-accounts.256-288.bt.torrent", "v1-accounts.256-288.kv", "v1-accounts.256-288.kv.torrent", "v1-accounts.256-288.kvei", "v1-accounts.256-288.kvei.torrent", "v1-accounts.288-296.bt", "v1-accounts.288-296.bt.torrent", "v1-accounts.288-296.kv", "v1-accounts.288-296.kv.torrent", "v1-accounts.288-296.kvei", "v1-accounts.288-296.kvei.torrent", "v1-accounts.296-298.bt", "v1-accounts.296-298.bt.torrent", "v1-accounts.296-298.kv", "v1-accounts.296-298.kv.torrent", "v1-accounts.296-298.kvei", "v1-accounts.296-298.kvei.torrent", "v1-code.0-256.bt", "v1-code.0-256.bt.torrent", "v1-code.0-256.kv", "v1-code.0-256.kv.torrent", "v1-code.0-256.kvei", "v1-code.0-256.kvei.torrent", "v1-code.256-288.bt", "v1-code.256-288.bt.torrent", "v1-code.256-288.kv", "v1-code.256-288.kv.torrent", "v1-code.256-288.kvei", "v1-code.256-288.kvei.torrent", "v1-code.288-296.bt", "v1-code.288-296.bt.torrent", "v1-code.288-296.kv", "v1-code.288-296.kv.torrent", "v1-code.288-296.kvei", "v1-code.288-296.kvei.torrent", "v1-code.296-298.bt", "v1-code.296-298.bt.torrent", "v1-code.296-298.kv", "v1-code.296-298.kv.torrent", "v1-code.296-298.kvei", "v1-code.296-298.kvei.torrent", "v1-commitment.0-256.kv", "v1-commitment.0-256.kv.torrent", "v1-commitment.0-256.kvi", "v1-commitment.0-256.kvi.torrent", "v1-commitment.256-288.kv", "v1-commitment.256-288.kv.torrent", "v1-commitment.256-288.kvi", "v1-commitment.256-288.kvi.torrent", "v1-commitment.288-296.kv", "v1-commitment.288-296.kv.torrent", "v1-commitment.288-296.kvi", "v1-commitment.288-296.kvi.torrent", "v1-commitment.296-298.kv", "v1-commitment.296-298.kv.torrent", "v1-commitment.296-298.kvi", "v1-commitment.296-298.kvi.torrent", "v1-receipt.0-256.bt", "v1-receipt.0-256.bt.torrent", "v1-receipt.0-256.kv", "v1-receipt.0-256.kv.torrent", "v1-receipt.0-256.kvei", "v1-receipt.0-256.kvei.torrent", "v1-receipt.256-288.bt", "v1-receipt.256-288.bt.torrent", "v1-receipt.256-288.kv", "v1-receipt.256-288.kv.torrent", "v1-receipt.256-288.kvei", "v1-receipt.256-288.kvei.torrent", "v1-receipt.288-296.bt", "v1-receipt.288-296.bt.torrent", "v1-receipt.288-296.kv", "v1-receipt.288-296.kv.torrent", "v1-receipt.288-296.kvei", "v1-receipt.288-296.kvei.torrent", "v1-receipt.296-298.bt", "v1-receipt.296-298.bt.torrent", "v1-receipt.296-298.kv", "v1-receipt.296-298.kv.torrent", "v1-receipt.296-298.kvei", "v1-receipt.296-298.kvei.torrent", "v1-storage.0-256.bt", "v1-storage.0-256.bt.torrent", "v1-storage.0-256.kv", "v1-storage.0-256.kv.torrent", "v1-storage.0-256.kvei", "v1-storage.0-256.kvei.torrent", "v1-storage.256-288.bt", "v1-storage.256-288.bt.torrent", "v1-storage.256-288.kv", "v1-storage.256-288.kv.torrent", "v1-storage.256-288.kvei", "v1-storage.256-288.kvei.torrent", "v1-storage.288-296.bt", "v1-storage.288-296.bt.torrent", "v1-storage.288-296.kv", "v1-storage.288-296.kv.torrent", "v1-storage.288-296.kvei", "v1-storage.288-296.kvei.torrent", "v1-storage.296-298.bt", "v1-storage.296-298.bt.torrent", "v1-storage.296-298.kv", "v1-storage.296-298.kv.torrent", "v1-storage.296-298.kvei", "v1-storage.296-298.kvei.torrent"}

	historyFolder := dirs.SnapHistory
	historyFiles := []string{"v1-accounts.0-64.v", "v1-accounts.0-64.v.torrent", "v1-accounts.128-192.v", "v1-accounts.128-192.v.torrent", "v1-accounts.192-256.v", "v1-accounts.192-256.v.torrent", "v1-accounts.256-288.v", "v1-accounts.256-288.v.torrent", "v1-accounts.288-296.v", "v1-accounts.288-296.v.torrent", "v1-accounts.296-298.v", "v1-accounts.296-298.v.torrent", "v1-accounts.64-128.v", "v1-accounts.64-128.v.torrent", "v1-code.0-64.v", "v1-code.0-64.v.torrent", "v1-code.128-192.v", "v1-code.128-192.v.torrent", "v1-code.192-256.v", "v1-code.192-256.v.torrent", "v1-code.256-288.v", "v1-code.256-288.v.torrent", "v1-code.288-296.v", "v1-code.288-296.v.torrent", "v1-code.296-298.v", "v1-code.296-298.v.torrent", "v1-code.64-128.v", "v1-code.64-128.v.torrent", "v1-receipt.0-64.v", "v1-receipt.0-64.v.torrent", "v1-receipt.128-192.v", "v1-receipt.128-192.v.torrent", "v1-receipt.192-256.v", "v1-receipt.192-256.v.torrent", "v1-receipt.256-288.v", "v1-receipt.256-288.v.torrent", "v1-receipt.288-296.v", "v1-receipt.288-296.v.torrent", "v1-receipt.296-298.v", "v1-receipt.296-298.v.torrent", "v1-receipt.64-128.v", "v1-receipt.64-128.v.torrent", "v1-storage.0-64.v", "v1-storage.0-64.v.torrent", "v1-storage.128-192.v", "v1-storage.128-192.v.torrent", "v1-storage.192-256.v", "v1-storage.192-256.v.torrent", "v1-storage.256-288.v", "v1-storage.256-288.v.torrent", "v1-storage.288-296.v", "v1-storage.288-296.v.torrent", "v1-storage.296-298.v", "v1-storage.296-298.v.torrent", "v1-storage.64-128.v", "v1-storage.64-128.v.torrent"}

	accessorFolder := dirs.SnapAccessors
	accessorFiles := []string{"v1-accounts.0-64.efi", "v1-accounts.0-64.efi.torrent", "v1-accounts.0-64.vi", "v1-accounts.0-64.vi.torrent", "v1-accounts.128-192.efi", "v1-accounts.128-192.efi.torrent", "v1-accounts.128-192.vi", "v1-accounts.128-192.vi.torrent", "v1-accounts.192-256.efi", "v1-accounts.192-256.efi.torrent", "v1-accounts.192-256.vi", "v1-accounts.192-256.vi.torrent", "v1-accounts.256-288.efi", "v1-accounts.256-288.efi.torrent", "v1-accounts.256-288.vi", "v1-accounts.256-288.vi.torrent", "v1-accounts.288-296.efi", "v1-accounts.288-296.efi.torrent", "v1-accounts.288-296.vi", "v1-accounts.288-296.vi.torrent", "v1-accounts.296-298.efi", "v1-accounts.296-298.efi.torrent", "v1-accounts.296-298.vi", "v1-accounts.296-298.vi.torrent", "v1-accounts.64-128.efi", "v1-accounts.64-128.efi.torrent", "v1-accounts.64-128.vi", "v1-accounts.64-128.vi.torrent", "v1-code.0-64.efi", "v1-code.0-64.efi.torrent", "v1-code.0-64.vi", "v1-code.0-64.vi.torrent", "v1-code.128-192.efi", "v1-code.128-192.efi.torrent", "v1-code.128-192.vi", "v1-code.128-192.vi.torrent", "v1-code.192-256.efi", "v1-code.192-256.efi.torrent", "v1-code.192-256.vi", "v1-code.192-256.vi.torrent", "v1-code.256-288.efi", "v1-code.256-288.efi.torrent", "v1-code.256-288.vi", "v1-code.256-288.vi.torrent", "v1-code.288-296.efi", "v1-code.288-296.efi.torrent", "v1-code.288-296.vi", "v1-code.288-296.vi.torrent", "v1-code.296-298.efi", "v1-code.296-298.efi.torrent", "v1-code.296-298.vi", "v1-code.296-298.vi.torrent", "v1-code.64-128.efi", "v1-code.64-128.efi.torrent", "v1-code.64-128.vi", "v1-code.64-128.vi.torrent", "v1-logaddrs.0-64.efi", "v1-logaddrs.0-64.efi.torrent", "v1-logaddrs.128-192.efi", "v1-logaddrs.128-192.efi.torrent", "v1-logaddrs.192-256.efi", "v1-logaddrs.192-256.efi.torrent", "v1-logaddrs.256-288.efi", "v1-logaddrs.256-288.efi.torrent", "v1-logaddrs.288-296.efi", "v1-logaddrs.288-296.efi.torrent", "v1-logaddrs.296-298.efi", "v1-logaddrs.296-298.efi.torrent", "v1-logaddrs.64-128.efi", "v1-logaddrs.64-128.efi.torrent", "v1-logtopics.0-64.efi", "v1-logtopics.0-64.efi.torrent", "v1-logtopics.128-192.efi", "v1-logtopics.128-192.efi.torrent", "v1-logtopics.192-256.efi", "v1-logtopics.192-256.efi.torrent", "v1-logtopics.256-288.efi", "v1-logtopics.256-288.efi.torrent", "v1-logtopics.288-296.efi", "v1-logtopics.288-296.efi.torrent", "v1-logtopics.296-298.efi", "v1-logtopics.296-298.efi.torrent", "v1-logtopics.64-128.efi", "v1-logtopics.64-128.efi.torrent", "v1-receipt.0-64.efi", "v1-receipt.0-64.efi.torrent", "v1-receipt.0-64.vi", "v1-receipt.0-64.vi.torrent", "v1-receipt.128-192.efi", "v1-receipt.128-192.efi.torrent", "v1-receipt.128-192.vi", "v1-receipt.128-192.vi.torrent", "v1-receipt.192-256.efi", "v1-receipt.192-256.efi.torrent", "v1-receipt.192-256.vi", "v1-receipt.192-256.vi.torrent", "v1-receipt.256-288.efi", "v1-receipt.256-288.efi.torrent", "v1-receipt.256-288.vi", "v1-receipt.256-288.vi.torrent", "v1-receipt.288-296.efi", "v1-receipt.288-296.efi.torrent", "v1-receipt.288-296.vi", "v1-receipt.288-296.vi.torrent", "v1-receipt.296-298.efi", "v1-receipt.296-298.efi.torrent", "v1-receipt.296-298.vi", "v1-receipt.296-298.vi.torrent", "v1-receipt.64-128.efi", "v1-receipt.64-128.efi.torrent", "v1-receipt.64-128.vi", "v1-receipt.64-128.vi.torrent", "v1-storage.0-64.efi", "v1-storage.0-64.efi.torrent", "v1-storage.0-64.vi", "v1-storage.0-64.vi.torrent", "v1-storage.128-192.efi", "v1-storage.128-192.efi.torrent", "v1-storage.128-192.vi", "v1-storage.128-192.vi.torrent", "v1-storage.192-256.efi", "v1-storage.192-256.efi.torrent", "v1-storage.192-256.vi", "v1-storage.192-256.vi.torrent", "v1-storage.256-288.efi", "v1-storage.256-288.efi.torrent", "v1-storage.256-288.vi", "v1-storage.256-288.vi.torrent", "v1-storage.288-296.efi", "v1-storage.288-296.efi.torrent", "v1-storage.288-296.vi", "v1-storage.288-296.vi.torrent", "v1-storage.296-298.efi", "v1-storage.296-298.efi.torrent", "v1-storage.296-298.vi", "v1-storage.296-298.vi.torrent", "v1-storage.64-128.efi", "v1-storage.64-128.efi.torrent", "v1-storage.64-128.vi", "v1-storage.64-128.vi.torrent", "v1-tracesfrom.0-64.efi", "v1-tracesfrom.0-64.efi.torrent", "v1-tracesfrom.128-192.efi", "v1-tracesfrom.128-192.efi.torrent", "v1-tracesfrom.192-256.efi", "v1-tracesfrom.192-256.efi.torrent", "v1-tracesfrom.256-288.efi", "v1-tracesfrom.256-288.efi.torrent", "v1-tracesfrom.288-296.efi", "v1-tracesfrom.288-296.efi.torrent", "v1-tracesfrom.296-298.efi", "v1-tracesfrom.296-298.efi.torrent", "v1-tracesfrom.64-128.efi", "v1-tracesfrom.64-128.efi.torrent", "v1-tracesto.0-64.efi", "v1-tracesto.0-64.efi.torrent", "v1-tracesto.128-192.efi", "v1-tracesto.128-192.efi.torrent", "v1-tracesto.192-256.efi", "v1-tracesto.192-256.efi.torrent", "v1-tracesto.256-288.efi", "v1-tracesto.256-288.efi.torrent", "v1-tracesto.288-296.efi", "v1-tracesto.288-296.efi.torrent", "v1-tracesto.296-298.efi", "v1-tracesto.296-298.efi.torrent", "v1-tracesto.64-128.efi", "v1-tracesto.64-128.efi.torrent"}

	idxFolder := dirs.SnapIdx
	idxFiles := []string{"v1-accounts.0-64.ef", "v1-accounts.0-64.ef.torrent", "v1-accounts.128-192.ef", "v1-accounts.128-192.ef.torrent", "v1-accounts.192-256.ef", "v1-accounts.192-256.ef.torrent", "v1-accounts.256-288.ef", "v1-accounts.256-288.ef.torrent", "v1-accounts.288-296.ef", "v1-accounts.288-296.ef.torrent", "v1-accounts.296-298.ef", "v1-accounts.296-298.ef.torrent", "v1-accounts.64-128.ef", "v1-accounts.64-128.ef.torrent", "v1-code.0-64.ef", "v1-code.0-64.ef.torrent", "v1-code.128-192.ef", "v1-code.128-192.ef.torrent", "v1-code.192-256.ef", "v1-code.192-256.ef.torrent", "v1-code.256-288.ef", "v1-code.256-288.ef.torrent", "v1-code.288-296.ef", "v1-code.288-296.ef.torrent", "v1-code.296-298.ef", "v1-code.296-298.ef.torrent", "v1-code.64-128.ef", "v1-code.64-128.ef.torrent", "v1-logaddrs.0-64.ef", "v1-logaddrs.0-64.ef.torrent", "v1-logaddrs.128-192.ef", "v1-logaddrs.128-192.ef.torrent", "v1-logaddrs.192-256.ef", "v1-logaddrs.192-256.ef.torrent", "v1-logaddrs.256-288.ef", "v1-logaddrs.256-288.ef.torrent", "v1-logaddrs.288-296.ef", "v1-logaddrs.288-296.ef.torrent", "v1-logaddrs.296-298.ef", "v1-logaddrs.296-298.ef.torrent", "v1-logaddrs.64-128.ef", "v1-logaddrs.64-128.ef.torrent", "v1-logtopics.0-64.ef", "v1-logtopics.0-64.ef.torrent", "v1-logtopics.128-192.ef", "v1-logtopics.128-192.ef.torrent", "v1-logtopics.192-256.ef", "v1-logtopics.192-256.ef.torrent", "v1-logtopics.256-288.ef", "v1-logtopics.256-288.ef.torrent", "v1-logtopics.288-296.ef", "v1-logtopics.288-296.ef.torrent", "v1-logtopics.296-298.ef", "v1-logtopics.296-298.ef.torrent", "v1-logtopics.64-128.ef", "v1-logtopics.64-128.ef.torrent", "v1-receipt.0-64.ef", "v1-receipt.0-64.ef.torrent", "v1-receipt.128-192.ef", "v1-receipt.128-192.ef.torrent", "v1-receipt.192-256.ef", "v1-receipt.192-256.ef.torrent", "v1-receipt.256-288.ef", "v1-receipt.256-288.ef.torrent", "v1-receipt.288-296.ef", "v1-receipt.288-296.ef.torrent", "v1-receipt.296-298.ef", "v1-receipt.296-298.ef.torrent", "v1-receipt.64-128.ef", "v1-receipt.64-128.ef.torrent", "v1-storage.0-64.ef", "v1-storage.0-64.ef.torrent", "v1-storage.128-192.ef", "v1-storage.128-192.ef.torrent", "v1-storage.192-256.ef", "v1-storage.192-256.ef.torrent", "v1-storage.256-288.ef", "v1-storage.256-288.ef.torrent", "v1-storage.288-296.ef", "v1-storage.288-296.ef.torrent", "v1-storage.296-298.ef", "v1-storage.296-298.ef.torrent", "v1-storage.64-128.ef", "v1-storage.64-128.ef.torrent", "v1-tracesfrom.0-64.ef", "v1-tracesfrom.0-64.ef.torrent", "v1-tracesfrom.128-192.ef", "v1-tracesfrom.128-192.ef.torrent", "v1-tracesfrom.192-256.ef", "v1-tracesfrom.192-256.ef.torrent", "v1-tracesfrom.256-288.ef", "v1-tracesfrom.256-288.ef.torrent", "v1-tracesfrom.288-296.ef", "v1-tracesfrom.288-296.ef.torrent", "v1-tracesfrom.296-298.ef", "v1-tracesfrom.296-298.ef.torrent", "v1-tracesfrom.64-128.ef", "v1-tracesfrom.64-128.ef.torrent", "v1-tracesto.0-64.ef", "v1-tracesto.0-64.ef.torrent", "v1-tracesto.128-192.ef", "v1-tracesto.128-192.ef.torrent", "v1-tracesto.192-256.ef", "v1-tracesto.192-256.ef.torrent", "v1-tracesto.256-288.ef", "v1-tracesto.256-288.ef.torrent", "v1-tracesto.288-296.ef", "v1-tracesto.288-296.ef.torrent", "v1-tracesto.296-298.ef", "v1-tracesto.296-298.ef.torrent", "v1-tracesto.64-128.ef", "v1-tracesto.64-128.ef.torrent"}

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

			btindex, err := CreateBtreeIndexWithDecompressor(filename, 128, seg3, seg.CompressNone, uint32(1), background.NewProgressSet(), dirs.Tmp, log.New(), false)
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
			filter, err := NewExistenceFilter(0, filename)
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

	touch(t, domainFolder, domainFiles, fileGen)
	touch(t, historyFolder, historyFiles, fileGen)
	touch(t, accessorFolder, accessorFiles, fileGen)
	touch(t, idxFolder, idxFiles, fileGen)

	return dataFileCount, btCount, existenceCount, accessorCount
}

func touch(t *testing.T, folder string, files []string, fileGen func(filename string)) {
	t.Helper()
	for _, f := range files {
		filename := filepath.Join(folder, f)
		// touchFile(t, filename)
		fileGen(filename)
	}
}

// func touchFile(t *testing.T, filename string) {
// 	t.Helper()
// 	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
// 	if err != nil {
// 		t.Fatalf("failed to open file %s: %v", filename, err)
// 	}
// 	defer file.Close()
// }

func containsSubstring(t *testing.T, str string, list []string) bool {
	t.Helper()
	for _, s := range list {
		if strings.Contains(str, s) {
			return true
		}
	}
	return false
}
