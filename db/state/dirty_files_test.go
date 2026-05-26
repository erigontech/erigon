package state

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
	}

	for _, fname := range accessorFor(f2.StepRange(aggStep)) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
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

func TestOpenDirtyFilesPopulatesVersion(t *testing.T) {
	t.Parallel()
	logger := log.New()
	_, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, logger)
	// Accept v2.1 reads here so the mixed-version scan does not panic in MustSupport
	// before the global commitment ceiling is raised in a later task.
	d.FileVersion.DataKV = version.Versions{Current: version.V2_1, MinSupported: version.V1_0}

	tmp := t.TempDir()
	cases := []struct {
		name string
		rng  string
		ver  version.Version
	}{
		{"v1.0-accounts.0-1.kv", "0-1", version.V1_0},
		{"v2.0-accounts.1-2.kv", "1-2", version.V2_0},
		{"v2.1-accounts.2-3.kv", "2-3", version.V2_1},
	}
	fileNames := make([]string, 0, len(cases))
	for _, c := range cases {
		writeTestKVFile(t, filepath.Join(d.dirs.SnapDomain, c.name), tmp, logger)
		fileNames = append(fileNames, c.name)
	}

	d.scanDirtyFiles(fileNames)
	require.NoError(t, d.openDirtyFiles(fileNames))

	gotDirty := make(map[string]version.Version)
	gotVisible := make(map[string]version.Version)
	d.dirtyFiles.Scan(func(it *FilesItem) bool {
		from, to := it.StepRange(d.stepSize)
		key := fmt.Sprintf("%d-%d", from, to)
		gotDirty[key] = it.version
		gotVisible[key] = visibleFile{src: it}.Version()
		return true
	})

	for _, c := range cases {
		require.Equal(t, c.ver, gotDirty[c.rng], "dirty file version for %s", c.name)
		require.Equal(t, c.ver, gotVisible[c.rng], "visibleFile.Version() for %s", c.name)
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
