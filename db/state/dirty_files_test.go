package state

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/mvcc"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func TestStepRange(t *testing.T) {
	t.Parallel()
	stepSize := uint64(4)

	t.Run("simple range", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 1,
			endTxNum:   10,
		}

		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(0), startStep)
		require.Equal(t, kv.Step(2), endStep)
		require.Equal(t, uint64(2), f.StepCount(4))
	})

	t.Run("inner boundaries", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 4,
			endTxNum:   7,
		}

		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(1), startStep)
		require.Equal(t, kv.Step(1), endStep)
		require.Equal(t, uint64(0), f.StepCount(stepSize))
	})

	t.Run("outer boundaries", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 3,
			endTxNum:   8,
		}
		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(0), startStep)
		require.Equal(t, kv.Step(2), endStep)
		require.Equal(t, uint64(2), f.StepCount(stepSize))
	})
}

func TestFileItemWithMissedAccessor(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	// filesItem
	f1 := &FilesItem{
		startTxNum: 1,
		endTxNum:   10,
	}
	f2 := &FilesItem{
		startTxNum: 11,
		endTxNum:   20,
	}
	f3 := &FilesItem{
		startTxNum: 31,
		endTxNum:   40,
	}
	aggStep := uint64(10)

	df := newDirtyFiles()
	df.Set(f1)
	df.Set(f2)
	df.Set(f3)

	accessorFor := func(fromStep, toStep kv.Step) []string {
		return []string{
			filepath.Join(tmp, fmt.Sprintf("testacc_%d_%d.bin", fromStep, toStep)),
			filepath.Join(tmp, fmt.Sprintf("testacc2_%d_%d.bin", fromStep, toStep)),
		}
	}

	// create accesssor files for f1, f2
	for _, fname := range accessorFor(f1.StepRange(aggStep)) {
		require.NoError(t, os.WriteFile(fname, []byte("test"), 0o644))
		f := fname
		t.Cleanup(func() { _ = dir.RemoveFile(f) })
	}

	for _, fname := range accessorFor(f2.StepRange(aggStep)) {
		require.NoError(t, os.WriteFile(fname, []byte("test"), 0o644))
		f := fname
		t.Cleanup(func() { _ = dir.RemoveFile(f) })
	}

	fileItems := fileItemsWithMissedAccessors(df.Items(), aggStep, accessorFor)
	require.Len(t, fileItems, 1)
	require.Equal(t, f3, fileItems[0])
}

func TestVisibleFileVersion(t *testing.T) {
	t.Parallel()
	vf := visibleFile{src: &FilesItem{version: version.V2_1}}
	require.Equal(t, version.V2_1, vf.Version())
}

func TestOpenDirtyFileHelpersLogInvalidMask(t *testing.T) {
	t.Parallel()
	const mask = "[invalid"
	dirEntries := []string{"v1.0-file"}

	tests := []struct {
		name string
		open func(t *testing.T, logger log.Logger)
	}{
		{
			name: "data",
			open: func(t *testing.T, logger log.Logger) {
				require.False(t, openDirtyDataFile(&FilesItem{}, mask, dirEntries, t.TempDir(), version.Versions{}, "test", logger))
			},
		},
		{
			name: "accessor",
			open: func(t *testing.T, logger log.Logger) {
				openDirtyAccessor(mask, dirEntries, t.TempDir(), version.Versions{}, func(string, version.Version) error { return nil }, "test", logger)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, output := newBufferedTestLogger()

			test.open(t, logger)

			require.Contains(t, output.String(), "f="+mask)
			require.Contains(t, output.String(), "lvl=dbug")
		})
	}
}

func TestOpenDirtyDataFileDoesNotLogNonCorruptionOpenError(t *testing.T) {
	t.Parallel()
	logger, output := newBufferedTestLogger()
	fileName := "v1.0-accounts.0-1.kv"
	versions := version.Versions{Current: version.V1_0, MinSupported: version.V1_0}

	require.False(t, openDirtyDataFile(&FilesItem{}, "*-accounts.0-1.kv", []string{fileName}, t.TempDir(), versions, "test", logger))
	require.Empty(t, output.String())
}

func TestOpenDirtyAccessorLogsOpenErrorAtDebug(t *testing.T) {
	t.Parallel()
	logger, output := newBufferedTestLogger()
	fileName := "v1.0-accounts.0-1.kvi"
	versions := version.Versions{Current: version.V1_0, MinSupported: version.V1_0}

	openDirtyAccessor("*-accounts.0-1.kvi", []string{fileName}, t.TempDir(), versions, func(string, version.Version) error {
		return errors.New("open failed")
	}, "test", logger)

	require.Contains(t, output.String(), "lvl=dbug")
	require.Contains(t, output.String(), `err="open failed"`)
}

func newBufferedTestLogger() (log.Logger, *bytes.Buffer) {
	output := new(bytes.Buffer)
	logger := log.New()
	logger.SetHandler(log.StreamHandler(output, log.LogfmtFormat()))
	return logger, output
}

// openDirtyFiles must accept a mixed-version file set (v1.0/v2.0/v2.1) without tripping the
// MustSupport version-acceptance check, opening one dirty item per range from disk.
func TestOpenDirtyFilesAcceptsMixedVersions(t *testing.T) {
	t.Parallel()
	logger := log.New()
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)
	// Bump this (accounts) domain's read ceiling to v2.1 so the mixed-version fixtures open without
	// tripping MustSupport; unrelated to commitment-domain versioning.
	d.FileVersion.DataKV = version.Versions{Current: version.V2_1, MinSupported: version.V1_0}

	tmp := t.TempDir()
	cases := []struct {
		name string
		rng  string
	}{
		{"v1.0-accounts.0-1.kv", "0-1"},
		{"v2.0-accounts.1-2.kv", "1-2"},
		{"v2.1-accounts.2-3.kv", "2-3"},
	}
	fileNames := make([]string, 0, len(cases))
	for _, c := range cases {
		writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, c.name), tmp, logger)
		fileNames = append(fileNames, c.name)
	}

	d.scanDirtyFiles(fileNames)
	require.NoError(t, d.openDirtyFiles(t.Context(), fileNames))

	opened := make(map[string]bool)
	d.dirtyFiles.Scan(func(it *FilesItem) bool {
		from, to := it.StepRange(d.stepSize)
		require.NotNil(t, it.decompressor, "dirty file %d-%d must be opened", from, to)
		opened[fmt.Sprintf("%d-%d", from, to)] = true
		return true
	})

	for _, c := range cases {
		require.True(t, opened[c.rng], "range %s must open from %s", c.rng, c.name)
	}
}

// Two same-range commitment files of different versions collapse to one dirty item
// resolved to the highest version (v2.1); the lower v2.0 twin is left on disk, never opened.
func TestOpenDirtyFilesSameRangePrefersNewestVersion(t *testing.T) {
	t.Parallel()
	for _, order := range [][]string{
		{"v2.0-commitment.0-2.kv", "v2.1-commitment.0-2.kv"},
		{"v2.1-commitment.0-2.kv", "v2.0-commitment.0-2.kv"},
	} {
		t.Run(strings.Join(order, ","), func(t *testing.T) {
			logger := log.New()
			_, d := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, 16, logger)
			tmp := t.TempDir()
			for _, name := range order {
				writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, name), tmp, logger)
			}

			d.scanDirtyFiles(order)
			require.NoError(t, d.openDirtyFiles(t.Context(), order))

			require.Equal(t, 1, d.dirtyFiles.Len(), "same-range duplicate must collapse to one dirty file")
			var opened *FilesItem
			d.dirtyFiles.Scan(func(it *FilesItem) bool { opened = it; return true })
			require.NotNil(t, opened)
			require.Equal(t, "v2.1-commitment.0-2.kv", filepath.Base(opened.decompressor.FilePath()), "newest version wins")

			_, err := os.Stat(filepath.Join(d.dirs.SnapDomain, "v2.0-commitment.0-2.kv"))
			require.NoError(t, err, "lower-version twin stays on disk, just unopened")
		})
	}
}

func writeTestKVFile(t *testing.T, path, tmp string, logger log.Logger) {
	t.Helper()
	comp, err := seg.NewCompressor(t.Context(), "test", path, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer comp.Close()
	require.NoError(t, comp.AddWord([]byte("k")))
	require.NoError(t, comp.AddWord([]byte("v")))
	require.NoError(t, comp.Compress())
}

func TestRetireSkipsAlreadyDetachedItems(t *testing.T) {
	t.Parallel()
	dirty := newDirtyFiles()
	item := &FilesItem{startTxNum: 0, endTxNum: 16}
	dirty.Set(item)

	require.Equal(t, []*FilesItem{item}, retire(mvcc.RetireReasonWasDeletedFromDisk, dirty, []*FilesItem{item}, "test", log.New()))
	require.Empty(t, retire(mvcc.RetireReasonAged, dirty, []*FilesItem{item}, "test", log.New()))
	require.False(t, item.canDelete.Load())

	reopened := &FilesItem{startTxNum: 0, endTxNum: 16}
	dirty.Set(reopened)
	require.Empty(t, retire(mvcc.RetireReasonAged, dirty, []*FilesItem{item}, "test", log.New()))
	_, ok := dirty.Get(reopened)
	require.True(t, ok, "a stale item must not detach the reopened file of the same range")
}

func TestCheckForVisibilityRequiresAccessors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		acc  statecfg.Accessors
		open func(*FilesItem)
	}{
		{"btree", statecfg.AccessorBTree, func(it *FilesItem) { it.bindex = &btindex.BtIndex{} }},
		{"hashmap", statecfg.AccessorHashMap, func(it *FilesItem) { it.index = &recsplit.Index{} }},
		{"existence", statecfg.AccessorExistence, func(it *FilesItem) { it.existence = &existence.Filter{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			it := visibleItem(0, 16)
			require.False(t, checkForVisibility(it, tc.acc, false))
			tc.open(it)
			require.True(t, checkForVisibility(it, tc.acc, false))
		})
	}
}

func TestScanDirtyFilesIgnoresUnrelatedNames(t *testing.T) {
	t.Parallel()
	ii := emptyTestInvertedIndex(t, 1)
	ii.scanDirtyFiles([]string{
		"v1.0-accounts.0-1.ef",
		"v1.0-accounts.1-2.efi",
		"v1.0-accounts.1-2.wrongext",
		"v1.0-storage.1-2.ef",
		"accounts.1-2.ef",
		"README.txt",
	})
	require.Equal(t, 1, ii.dirtyFiles.Len())
}

func TestDirtyFilesEndTxNumMinimax(t *testing.T) {
	t.Parallel()
	ii := emptyTestInvertedIndex(t, 1)
	require.Zero(t, ii.dirtyFilesEndTxNumMinimax())
	ii.scanDirtyFiles([]string{"v1.0-accounts.0-1.ef", "v1.0-accounts.1-2.ef", "v1.0-accounts.2-4.ef"})
	require.Equal(t, uint64(4), ii.dirtyFilesEndTxNumMinimax())
}

func TestOpenDirtyFilesKeepsDataOnlyFile(t *testing.T) {
	t.Parallel()
	logger := log.New()
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)
	names := []string{"v1.0-accounts.0-1.kv"}
	writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, names[0]), t.TempDir(), logger)

	d.scanDirtyFiles(names)
	require.NoError(t, d.openDirtyFiles(t.Context(), names))

	require.Equal(t, 1, d.dirtyFiles.Len())
	d.dirtyFiles.Scan(func(it *FilesItem) bool {
		require.NotNil(t, it.decompressor)
		require.Nil(t, it.bindex)
		require.Nil(t, it.existence)
		return true
	})
}

func TestOpenDirtyFilesDropsEmptyDataFile(t *testing.T) {
	t.Parallel()
	logger := log.New()
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)
	names := []string{"v1.0-accounts.0-1.kv", "v1.0-accounts.1-2.kv"}
	writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, names[0]), t.TempDir(), logger)
	require.NoError(t, os.WriteFile(filepath.Join(d.dirs.SnapDomain, names[1]), nil, 0o644))

	d.scanDirtyFiles(names)
	require.NoError(t, d.openDirtyFiles(t.Context(), names))

	require.Equal(t, 1, d.dirtyFiles.Len())
	_, found := d.dirtyFiles.Get(&FilesItem{startTxNum: 16, endTxNum: 32})
	require.False(t, found)
}

func touchFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o644))
	}
}

func stepRanges(t *testing.T, items []*FilesItem, stepSize uint64) []string {
	t.Helper()
	res := make([]string, len(items))
	for i, it := range items {
		from, to := it.StepRange(stepSize)
		res[i] = fmt.Sprintf("%d-%d", from, to)
	}
	return res
}

func TestDomainMissedBtreeAccessorsNeedsBtAndExistence(t *testing.T) {
	t.Parallel()
	d := emptyTestDomain(t, 1)
	require.True(t, d.Accessors.Has(statecfg.AccessorBTree))
	d.scanDirtyFiles([]string{"v1.0-accounts.0-1.kv", "v1.0-accounts.1-2.kv", "v1.0-accounts.2-3.kv"})
	touchFiles(t, d.dirs.SnapDomain,
		"v1.0-accounts.0-1.bt", "v1.0-accounts.0-1.kvei",
		"v1.0-accounts.1-2.bt",
		"v1.0-accounts.2-3.kvei",
	)

	missed := d.missedBtreeAccessors(d.dirtyFiles.Items(), readDirNames(d.dirs.SnapDomain))
	require.Equal(t, []string{"1-2", "2-3"}, stepRanges(t, missed, d.stepSize))
}

func TestMissedMapAccessors(t *testing.T) {
	t.Parallel()
	t.Run("domain kvi", func(t *testing.T) {
		d := emptyTestDomain(t, 1)
		d.Accessors = statecfg.AccessorHashMap
		d.scanDirtyFiles([]string{"v1.0-accounts.0-1.kv", "v1.0-accounts.1-2.kv"})
		touchFiles(t, d.dirs.SnapDomain, "v1.0-accounts.0-1.kvi")
		missed := d.missedMapAccessors(d.dirtyFiles.Items(), readDirNames(d.dirs.SnapDomain))
		require.Equal(t, []string{"1-2"}, stepRanges(t, missed, d.stepSize))
	})
	t.Run("history vi", func(t *testing.T) {
		h := emptyTestDomain(t, 1).History
		h.Accessors = statecfg.AccessorHashMap
		h.scanDirtyFiles([]string{"v1.0-accounts.0-1.v", "v1.0-accounts.1-2.v"})
		touchFiles(t, h.dirs.SnapAccessors, "v1.0-accounts.0-1.vi")
		missed := h.missedMapAccessors(h.dirtyFiles.Items(), readDirNames(h.dirs.SnapAccessors))
		require.Equal(t, []string{"1-2"}, stepRanges(t, missed, h.stepSize))
	})
	t.Run("inverted index efi", func(t *testing.T) {
		ii := emptyTestInvertedIndex(t, 1)
		ii.Accessors = statecfg.AccessorHashMap
		ii.scanDirtyFiles([]string{"v1.0-accounts.0-1.ef", "v1.0-accounts.1-2.ef"})
		touchFiles(t, ii.dirs.SnapAccessors, "v1.0-accounts.0-1.efi")
		missed := ii.missedMapAccessors(ii.dirtyFiles.Items(), readDirNames(ii.dirs.SnapAccessors))
		require.Equal(t, []string{"1-2"}, stepRanges(t, missed, ii.stepSize))
	})
}
