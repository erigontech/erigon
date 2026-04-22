package state

// Tests focused on making file creation less fragile.
// File creation is rare; bugs are hard to detect in prod and re-gen is expensive.
// These tests catch issues as early as possible.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// TestFilesWithMissedAccessors_BTree verifies that when a .kv data file exists
// but its .bt accessor is missing, FilesWithMissedAccessors reports it.
// Partial file creation (data written, accessor not yet written) must be detected early.
func TestFilesWithMissedAccessors_BTree(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})

	// Populate only data files (no .bt or .kvei accessor files)
	ranges := []testFileRange{{0, 1}, {1, 2}}
	for _, r := range ranges {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
		// Only create the data file, not the accessors
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}
	require.NoError(t, repo.OpenFolder())

	missed := repo.FilesWithMissedAccessors()
	btMissed := missed.Get(statecfg.AccessorBTree)
	require.NotEmpty(t, btMissed, "BTree accessor missing should be detected")
	require.Len(t, btMissed, 2)
}

// TestFilesWithMissedAccessors_Existence verifies that when a .kv data file exists
// but its .kvei existence filter is missing, it is detected.
func TestFilesWithMissedAccessors_Existence(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})

	ranges := []testFileRange{{0, 1}}
	for _, r := range ranges {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}
	require.NoError(t, repo.OpenFolder())

	missed := repo.FilesWithMissedAccessors()
	existMissed := missed.Get(statecfg.AccessorExistence)
	require.NotEmpty(t, existMissed, "Existence filter missing should be detected")
}

// TestFilesWithMissedAccessors_HashMap verifies that when a .kv data file exists
// but its .kvi HashMap accessor is missing, it is detected.
func TestFilesWithMissedAccessors_HashMap(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		name = "commitment"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			Accessor(dirs.SnapDomain, ver).
			Build()
		return name, schema
	})

	ranges := []testFileRange{{0, 1}, {1, 2}}
	for _, r := range ranges {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}
	require.NoError(t, repo.OpenFolder())

	missed := repo.FilesWithMissedAccessors()
	hashMissed := missed.Get(statecfg.AccessorHashMap)
	require.NotEmpty(t, hashMissed, "HashMap accessor missing should be detected")
	require.Len(t, hashMissed, 2)
}

// TestFilesWithMissedAccessors_AllPresent verifies that when all accessor files
// are present, FilesWithMissedAccessors returns nothing.
func TestFilesWithMissedAccessors_AllPresent(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})

	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, repo.OpenFolder())

	missed := repo.FilesWithMissedAccessors()
	require.Empty(t, missed.Get(statecfg.AccessorBTree), "No BTree accessors should be missing")
	require.Empty(t, missed.Get(statecfg.AccessorExistence), "No existence filters should be missing")
}

// TestFileNamingRoundTrip_Domain verifies that file paths generated by DataFile/BtIdxFile/ExistenceFile
// can be parsed back by Parse() and yield matching from/to/ext values.
// A mismatch here would mean files created at one path can't be found at another — a hard-to-debug prod issue.
func TestFileNamingRoundTrip_Domain(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Existence(ver).Build()

	cases := []struct{ from, to uint64 }{
		{0, 1000},
		{1000, 2000},
		{0, 256000},
		{256000, 512000},
	}
	for _, c := range cases {
		from, to := RootNum(c.from), RootNum(c.to)

		// Data file round-trip
		dataPath, err := schema.DataFile(ver.Current, from, to)
		require.NoError(t, err)
		info, ok := schema.Parse(filepath.Base(dataPath))
		require.True(t, ok, "data file name must be parseable: %s", dataPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(DataExtensionKv), info.Ext)

		// BTree index round-trip
		btPath, err := schema.BtIdxFile(ver.Current, from, to)
		require.NoError(t, err)
		info, ok = schema.Parse(filepath.Base(btPath))
		require.True(t, ok, "bt file name must be parseable: %s", btPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, ".bt", info.Ext)

		// Existence file round-trip
		exPath, err := schema.ExistenceFile(ver.Current, from, to)
		require.NoError(t, err)
		info, ok = schema.Parse(filepath.Base(exPath))
		require.True(t, ok, "existence file name must be parseable: %s", exPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, ".kvei", info.Ext)
	}
}

// TestFileNamingRoundTrip_History verifies history file naming round-trip.
func TestFileNamingRoundTrip_History(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapHistory, "accounts", DataExtensionV, seg.CompressNone, ver).
		Accessor(dirs.SnapAccessors, ver).Build()

	cases := []struct{ from, to uint64 }{{0, 64000}, {64000, 128000}, {128000, 256000}}
	for _, c := range cases {
		from, to := RootNum(c.from), RootNum(c.to)

		dataPath, err := schema.DataFile(ver.Current, from, to)
		require.NoError(t, err)
		info, ok := schema.Parse(filepath.Base(dataPath))
		require.True(t, ok, "history data file name must be parseable: %s", dataPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(DataExtensionV), info.Ext)

		accPath, err := schema.AccessorIdxFile(ver.Current, from, to, 0)
		require.NoError(t, err)
		info, ok = schema.Parse(filepath.Base(accPath))
		require.True(t, ok, "history accessor file name must be parseable: %s", accPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(AccessorExtensionVi), info.Ext)
	}
}

// TestFileNamingRoundTrip_II verifies inverted index file naming round-trip.
func TestFileNamingRoundTrip_II(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapIdx, "logaddrs", DataExtensionEf, seg.CompressNone, ver).
		Accessor(dirs.SnapAccessors, ver).Build()

	cases := []struct{ from, to uint64 }{{0, 64000}, {64000, 128000}}
	for _, c := range cases {
		from, to := RootNum(c.from), RootNum(c.to)

		dataPath, err := schema.DataFile(ver.Current, from, to)
		require.NoError(t, err)
		info, ok := schema.Parse(filepath.Base(dataPath))
		require.True(t, ok, "II data file name must be parseable: %s", dataPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(DataExtensionEf), info.Ext)

		accPath, err := schema.AccessorIdxFile(ver.Current, from, to, 0)
		require.NoError(t, err)
		info, ok = schema.Parse(filepath.Base(accPath))
		require.True(t, ok, "II accessor file name must be parseable: %s", accPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(AccessorExtensionEfi), info.Ext)
	}
}

// TestSnapshotConfigValidation verifies that invalid SnapshotCreationConfig panics early.
// This prevents misconfigured nodes from silently creating files with wrong ranges.
func TestSnapshotConfigValidation(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	schema := NewE2SnapSchema(dirs, "bodies", NewE2SnapSchemaVersion(ver, ver))

	// MergeStages not divisible by RootNumPerStep must panic
	require.Panics(t, func() {
		NewSnapshotConfig(&SnapshotCreationConfig{
			RootNumPerStep: 1000,
			MergeStages:    []uint64{1500}, // 1500 % 1000 != 0
			MinimumSize:    1000,
		}, schema)
	})

	// MinimumSize not divisible by RootNumPerStep must panic
	require.Panics(t, func() {
		NewSnapshotConfig(&SnapshotCreationConfig{
			RootNumPerStep: 1000,
			MergeStages:    []uint64{2000},
			MinimumSize:    1500, // 1500 % 1000 != 0
		}, schema)
	})

	// Valid config must not panic
	require.NotPanics(t, func() {
		NewSnapshotConfig(&SnapshotCreationConfig{
			RootNumPerStep: 1000,
			MergeStages:    []uint64{2000, 10000},
			MinimumSize:    1000,
		}, schema)
	})
}

// TestE3SnapSchemaBuilder_MismatchedAccessors verifies that the builder panics
// when an accessor is declared in `accessors` but its Build* method is not called,
// or vice versa. This catches configuration mistakes at startup rather than at runtime.
func TestE3SnapSchemaBuilder_MismatchedAccessors(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)

	// BTree declared in accessors but BtIndex() not called → panic on Build()
	require.Panics(t, func() {
		NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
			Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
			Build() // BtIndex not called
	})

	// HashMap declared in accessors but Accessor() not called → panic on Build()
	require.Panics(t, func() {
		NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
			Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
			Build() // Accessor not called
	})

	// Existence declared in accessors but Existence() not called → panic on Build()
	require.Panics(t, func() {
		NewE3SnapSchemaBuilder(statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
			Build() // Existence not called
	})

	// BtIndex() called but BTree not in accessors → panic on Build()
	require.Panics(t, func() {
		NewE3SnapSchemaBuilder(statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
			Existence(ver).
			BtIndex(ver). // BTree not in accessors
			Build()
	})
}

// TestDirtyFilesWithNoBtreeAccessors verifies detection of data files missing .bt index.
// This is the key early-warning for incomplete file creation.
func TestDirtyFilesWithNoBtreeAccessors(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})

	// Create data files only (no .bt accessors)
	for _, r := range []testFileRange{{0, 1}, {1, 2}} {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}
	require.NoError(t, repo.OpenFolder())
	require.Equal(t, 2, repo.dirtyFiles.Len())

	missing := repo.DirtyFilesWithNoBtreeAccessors()
	require.Len(t, missing, 2, "both data files should report missing BTree accessor")
}

// TestDirtyFilesWithNoHashAccessors verifies detection of data files missing .kvi index.
func TestDirtyFilesWithNoHashAccessors(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorHashMap
		name = "commitment"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			Accessor(dirs.SnapDomain, ver).
			Build()
		return name, schema
	})

	for _, r := range []testFileRange{{0, 1}, {1, 2}} {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}
	require.NoError(t, repo.OpenFolder())

	missing := repo.DirtyFilesWithNoHashAccessors()
	require.Len(t, missing, 2, "both data files should report missing HashMap accessor")
}

// TestDirtyFilesWithAccessors_NoneWhenPresent verifies that when accessors exist,
// the missing-accessor detection functions return empty.
func TestDirtyFilesWithAccessors_NoneWhenPresent(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})

	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, repo.OpenFolder())

	require.Empty(t, repo.DirtyFilesWithNoBtreeAccessors())
}

// TestOpenFolder_PartialFiles_StillOpens verifies that OpenFolder succeeds even when
// some data files have no accessors. This is critical: a node shouldn't crash if file
// creation was partially completed (data written, accessor build was interrupted).
func TestOpenFolder_PartialFiles_StillOpens(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})

	// First file: fully created (data + all accessors)
	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}})
	// Second file: only data, no accessors (simulates interrupted file creation)
	from, to := RootNum(1*repo.stepSize), RootNum(2*repo.stepSize)
	dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
	populateFiles(t, dirs, repo.schema, []string{dataFile})

	// OpenFolder must not fail — partial file state is recoverable
	require.NoError(t, repo.OpenFolder())
	require.Equal(t, 2, repo.dirtyFiles.Len(), "both files (full and partial) should be in dirty files")

	// The fully-formed file's accessors should be loaded
	// The partial file's accessors should be nil (not panic)
	count := 0
	repo.dirtyFiles.Walk(func(items []*FilesItem) bool {
		for _, item := range items {
			require.NotNil(t, item.decompressor, "decompressor must be open for all dirty files")
			count++
		}
		return true
	})
	require.Equal(t, 2, count)
}

// TestSnapInfoIsDataFile verifies that IsDataFile correctly classifies all valid data extensions.
// Misclassifying a data extension means files get skipped during recovery.
func TestSnapInfoIsDataFile(t *testing.T) {
	t.Parallel()
	dataExts := []string{".kv", ".v", ".ef", ".seg"}
	for _, ext := range dataExts {
		info := &SnapInfo{Ext: ext}
		require.True(t, info.IsDataFile(), "extension %q must be a data file", ext)
	}

	// Accessor/index extensions are NOT data files
	nonDataExts := []string{".bt", ".kvi", ".vi", ".efi", ".kvei", ".idx", "", ".dat"}
	for _, ext := range nonDataExts {
		info := &SnapInfo{Ext: ext}
		require.False(t, info.IsDataFile(), "extension %q must NOT be a data file", ext)
	}
}

// TestRealFileCreation_DomainDataFile creates a real .kv data file using the proper
// compressor path and verifies it can be decompressed after creation.
// This catches bugs in the file creation path that only manifest with real I/O.
func TestRealFileCreation_DomainDataFile(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Existence(ver).Build()

	from, to := RootNum(0), RootNum(stepSize)
	dataPath, err := schema.DataFile(ver.Current, from, to)
	require.NoError(t, err)

	// Create the file using the same compressor used in production
	comp, err := seg.NewCompressor(t.Context(), t.Name(), dataPath, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	comp.DisableFsync()
	require.NoError(t, comp.AddWord([]byte("key1")))
	require.NoError(t, comp.AddWord([]byte("val1")))
	require.NoError(t, comp.Compress())
	comp.Close()

	// Verify: file must be decompressible
	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	defer decomp.Close()
	require.Equal(t, 2, decomp.Count())

	// Verify: filename parses back correctly
	info, ok := schema.Parse(filepath.Base(dataPath))
	require.True(t, ok)
	require.Equal(t, uint64(from), info.From)
	require.Equal(t, uint64(to), info.To)
}

// TestFilesWithMissedAccessors_PartialBTree verifies that when some files have
// BTree accessors and some don't, only the ones without are reported as missing.
func TestFilesWithMissedAccessors_PartialBTree(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		accessors := statecfg.AccessorBTree | statecfg.AccessorExistence
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(accessors, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).
			Build()
		return name, schema
	})

	// First range: complete (data + bt + kvei)
	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}})
	// Second range: only data (missing bt + kvei)
	from2, to2 := RootNum(1*repo.stepSize), RootNum(2*repo.stepSize)
	dataFile2, _ := repo.schema.DataFile(version.V1_0, from2, to2)
	populateFiles(t, dirs, repo.schema, []string{dataFile2})

	require.NoError(t, repo.OpenFolder())
	require.Equal(t, 2, repo.dirtyFiles.Len())

	missed := repo.FilesWithMissedAccessors()
	btMissed := missed.Get(statecfg.AccessorBTree)
	require.Len(t, btMissed, 1, "only the second file should be missing BTree accessor")
	require.Equal(t, uint64(from2), btMissed[0].startTxNum)
	require.Equal(t, uint64(to2), btMissed[0].endTxNum)
}

// TestFileNamingRoundTrip_AllStepSizes verifies that file naming works correctly
// across different step sizes. A step-size bug would cause files to be generated
// at wrong paths and never found on re-open.
func TestFileNamingRoundTrip_AllStepSizes(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart

	stepSizes := []uint64{100, 1000, 4096, 10000}
	for _, stepSize := range stepSizes {
		schema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
			Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Build()

		from := RootNum(0)
		to := RootNum(stepSize * 256)

		dataPath, err := schema.DataFile(ver.Current, from, to)
		require.NoError(t, err)

		info, ok := schema.Parse(filepath.Base(dataPath))
		require.True(t, ok, "stepSize=%d: data file name must parse correctly", stepSize)
		require.Equal(t, uint64(from), info.From, "stepSize=%d: from mismatch", stepSize)
		require.Equal(t, uint64(to), info.To, "stepSize=%d: to mismatch", stepSize)
	}
}

// TestE3ParseRejectsWrongTag verifies that Parse rejects filenames with a different
// entity name. Files of different entities must not be confused with each other.
func TestE3ParseRejectsWrongTag(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)

	accountsSchema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Build()

	// A storage file must not be parseable by the accounts schema
	_, ok := accountsSchema.Parse("v1.0-storage.0-256.kv")
	require.False(t, ok, "accounts schema must reject storage filenames")

	_, ok = accountsSchema.Parse("v1.0-accounts.0-256.kv")
	require.True(t, ok, "accounts schema must accept accounts filenames")

	// Wrong extension
	_, ok = accountsSchema.Parse("v1.0-accounts.0-256.v")
	require.False(t, ok, "accounts .kv schema must reject .v files")
}

// TestMissedFilesMap_IsEmpty verifies that IsEmpty correctly detects when all
// accessors are present vs when some are missing. If IsEmpty is wrong, the system
// may skip building missing accessors and leave files in a broken state.
func TestMissedFilesMap_IsEmpty(t *testing.T) {
	t.Parallel()
	// Empty map: nothing missing
	empty := MissedFilesMap{}
	require.True(t, empty.IsEmpty())

	// Map with empty slice: nothing missing
	emptySlice := MissedFilesMap{statecfg.AccessorBTree: {}}
	require.True(t, emptySlice.IsEmpty())

	// Map with non-empty slice: something is missing
	withItem := MissedFilesMap{statecfg.AccessorBTree: {&FilesItem{}}}
	require.False(t, withItem.IsEmpty())

	// Mixed: one accessor has items, another doesn't
	mixed := MissedFilesMap{
		statecfg.AccessorBTree:     {&FilesItem{}},
		statecfg.AccessorExistence: {},
	}
	require.False(t, mixed.IsEmpty())
}

// TestProductionSchemas_ValidNamesAndRoundTrip verifies that production-level schemas
// (domain, history, II) generate parseable filenames. Schema changes that break
// filename compatibility cause prod nodes to fail to find their existing data files.
func TestProductionSchemas_ValidNamesAndRoundTrip(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)

	type schemaCase struct {
		name   string
		schema SnapNameSchema
	}

	cases := []schemaCase{
		{
			"accounts-domain",
			NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
				Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
				BtIndex(ver).Existence(ver).Build(),
		},
		{
			"commitment-domain",
			NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
				Data(dirs.SnapDomain, "commitments", DataExtensionKv, seg.CompressNone, ver).
				Accessor(dirs.SnapDomain, ver).Build(),
		},
		{
			"accounts-history",
			NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
				Data(dirs.SnapHistory, "accounts", DataExtensionV, seg.CompressNone, ver).
				Accessor(dirs.SnapAccessors, ver).Build(),
		},
		{
			"logaddrs-ii",
			NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
				Data(dirs.SnapIdx, "logaddrs", DataExtensionEf, seg.CompressNone, ver).
				Accessor(dirs.SnapAccessors, ver).Build(),
		},
	}

	rangeCases := []struct{ from, to uint64 }{{0, 1000}, {1000, 2000}, {256000, 512000}}

	for _, sc := range cases {
		for _, rc := range rangeCases {
			from, to := RootNum(rc.from), RootNum(rc.to)

			dataPath, err := sc.schema.DataFile(ver.Current, from, to)
			require.NoError(t, err, "%s: DataFile must not error", sc.name)
			require.NotEmpty(t, dataPath, "%s: DataFile must return non-empty path", sc.name)

			info, ok := sc.schema.Parse(filepath.Base(dataPath))
			require.True(t, ok, "%s: generated data filename must parse correctly: %s", sc.name, dataPath)
			require.Equal(t, rc.from, info.From, "%s: from mismatch", sc.name)
			require.Equal(t, rc.to, info.To, "%s: to mismatch", sc.name)
			require.True(t, info.IsDataFile(), "%s: data file must be classified as data file", sc.name)
		}
	}
}

// TestFilesWithMissedAccessors_EmptyRepo verifies that FilesWithMissedAccessors
// returns empty for an empty repo (no dirty files). This is a boundary case
// that must not panic.
func TestFilesWithMissedAccessors_EmptyRepo(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	missed := repo.FilesWithMissedAccessors()
	require.NotNil(t, missed)
	require.True(t, missed.IsEmpty(), "empty repo must report no missed accessors")
}

// TestOpenFolder_UnrelatedFilesIgnored verifies that files with wrong names or
// extensions in the snapshot directory are silently ignored. This prevents
// accidental files (temp files, README, etc.) from causing parse panics.
func TestOpenFolder_UnrelatedFilesIgnored(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	// Create valid data files
	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}})

	// Create garbage files in the same directory — should be silently ignored
	garbageFiles := []string{
		dirs.SnapDomain + "/README.txt",
		dirs.SnapDomain + "/v1.0-accounts.0-1.wrongext",
		dirs.SnapDomain + "/not-a-snapshot-file.kv",
		dirs.SnapDomain + "/v1.0-storage.0-1.kv", // wrong entity name
	}
	for _, f := range garbageFiles {
		// Create zero-byte placeholder
		if fp, err := os.Create(f); err == nil {
			fp.Close()
		}
	}

	require.NoError(t, repo.OpenFolder())
	// Only the valid account file should be loaded
	require.Equal(t, 1, repo.dirtyFiles.Len(), "unrelated files must be ignored by OpenFolder")
}

// TestFindFilesBySearchVersion verifies that DataFile with SearchVersion correctly
// finds the highest-version file on disk within the supported range.
// This function is used to re-open snapshot files after restart. A bug here means
// a node restart fails to find its existing files → falls back to full re-download.
func TestFindFilesBySearchVersion(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Build()

	from, to := RootNum(0), RootNum(stepSize)

	// Create v1.0 file on disk using a real compressor
	v10Path, err := schema.DataFile(version.V1_0, from, to)
	require.NoError(t, err)
	comp, err := seg.NewCompressor(t.Context(), t.Name(), v10Path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	comp.DisableFsync()
	require.NoError(t, comp.AddWord([]byte("key1")))
	require.NoError(t, comp.Compress())
	comp.Close()

	// SearchVersion must find the file
	foundPath, err := schema.DataFile(version.SearchVersion, from, to)
	require.NoError(t, err, "SearchVersion must find v1.0 file on disk")
	require.Equal(t, v10Path, foundPath, "SearchVersion must return the v1.0 file path")

	// SearchVersion with no file on disk must error
	_, err = schema.DataFile(version.SearchVersion, RootNum(stepSize), RootNum(2*stepSize))
	require.Error(t, err, "SearchVersion must error when no file exists for that range")
}

// TestOpenFolder_CorruptedDataFile verifies that OpenFolder handles a corrupted
// (empty/truncated) data file gracefully: the file is excluded from dirty files
// instead of crashing the node. If a file creation was interrupted before content
// was written, the node must still start up correctly.
func TestOpenFolder_CorruptedDataFile(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	// Create a valid file and a corrupted (empty) file
	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}})

	// Add a corrupted data file for range 1-2 (empty file, not a valid seg)
	corruptPath, _ := repo.schema.DataFile(version.V1_0, RootNum(repo.stepSize), RootNum(2*repo.stepSize))
	f, err := os.Create(corruptPath)
	require.NoError(t, err)
	f.Close()

	// OpenFolder must not crash on corrupted file
	err = repo.OpenFolder()
	require.NoError(t, err, "OpenFolder must succeed even with a corrupted data file")

	// Only the valid file should remain in dirty files
	require.Equal(t, 1, repo.dirtyFiles.Len(),
		"corrupted file must be excluded from dirty files; only valid file should remain")
}

// TestFindFilesBySearchVersion_VersionRangeFiltering verifies that SearchVersion
// selects the highest-version file within [MinSupported, Current] and rejects files
// outside that range. If an unsupported version is selected, data can be misread.
func TestFindFilesBySearchVersion_VersionRangeFiltering(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	stepSize := uint64(1000)

	// Schema supports V1_1 as current, V1_0 as min supported
	// → V1_0 and V1_1 files should be found; V1_2 file (above current) should be ignored
	ver := version.V1_1_standart
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Build()

	from, to := RootNum(0), RootNum(stepSize)

	makeFile := func(v version.Version) {
		path, err := schema.DataFile(v, from, to)
		require.NoError(t, err)
		comp, err := seg.NewCompressor(t.Context(), t.Name(), path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		comp.DisableFsync()
		require.NoError(t, comp.AddWord([]byte("word")))
		require.NoError(t, comp.Compress())
		comp.Close()
	}

	// Create v1.0 and v1.1 files — both are within [V1_0, V1_1] range
	makeFile(version.V1_0)
	makeFile(version.V1_1)

	// SearchVersion must return the highest version (V1_1)
	foundPath, err := schema.DataFile(version.SearchVersion, from, to)
	require.NoError(t, err, "SearchVersion must find a file in the supported range")

	info, ok := schema.Parse(filepath.Base(foundPath))
	require.True(t, ok)
	require.Equal(t, version.V1_1, info.Version,
		"SearchVersion must select the highest version within the supported range")
}

// TestFindFilesByStrictSearchVersion verifies that StrictSearchVersion:
// - returns a match when exactly one file exists
// - errors when multiple files exist (strict = no ambiguity)
func TestFindFilesByStrictSearchVersion(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	schema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Build()

	from, to := RootNum(0), RootNum(stepSize)

	makeFile := func(v version.Version) {
		path, err := schema.DataFile(v, from, to)
		require.NoError(t, err)
		comp, err := seg.NewCompressor(t.Context(), t.Name(), path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		comp.DisableFsync()
		require.NoError(t, comp.AddWord([]byte("word")))
		require.NoError(t, comp.Compress())
		comp.Close()
	}

	// Exactly one file: strict search must succeed
	makeFile(version.V1_0)
	_, err := schema.DataFile(version.StrictSearchVersion, from, to)
	require.NoError(t, err, "StrictSearchVersion must succeed when exactly one file exists")

	// Two files: strict search must fail (ambiguous)
	// Note: V1_1_standart schema needed to allow two different versions
	ver2 := version.V1_1_standart
	schema2 := NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
		Data(dirs.SnapDomain, "storage", DataExtensionKv, seg.CompressNone, ver2).
		BtIndex(ver2).Build()
	makeFileFn := func(v version.Version) {
		path, err := schema2.DataFile(v, from, to)
		require.NoError(t, err)
		comp, err := seg.NewCompressor(t.Context(), t.Name(), path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		comp.DisableFsync()
		require.NoError(t, comp.AddWord([]byte("word")))
		require.NoError(t, comp.Compress())
		comp.Close()
	}
	makeFileFn(version.V1_0)
	makeFileFn(version.V1_1)

	_, err = schema2.DataFile(version.StrictSearchVersion, from, to)
	require.Error(t, err, "StrictSearchVersion must error when multiple files exist for the same range")
}

// TestFilesWithMissedAccessors_LargeRepo verifies that missed accessor detection
// scales correctly — only files with missing accessors are reported, even in a
// larger set with many complete files.
func TestFilesWithMissedAccessors_LargeRepo(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	// 5 complete files + 2 incomplete (data only)
	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}})
	for _, r := range []testFileRange{{5, 6}, {6, 7}} {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}

	require.NoError(t, repo.OpenFolder())
	require.Equal(t, 7, repo.dirtyFiles.Len())

	missed := repo.FilesWithMissedAccessors()
	btMissed := missed.Get(statecfg.AccessorBTree)
	require.Len(t, btMissed, 2, "only the 2 incomplete files must be reported as missing BTree accessor")

	// Verify the reported missing files are the correct ones
	for _, item := range btMissed {
		require.GreaterOrEqual(t, item.startTxNum, uint64(5*repo.stepSize),
			"missing files must be from the incomplete ranges (5-6 and 6-7)")
	}
}

// TestFindAccessorFilesBySearchVersion verifies that BtIdxFile, ExistenceFile, and
// AccessorIdxFile work correctly with SearchVersion. The restart path must be able
// to find all file types on disk, not just the data (.kv) file.
func TestFindAccessorFilesBySearchVersion(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	from, to := RootNum(0), RootNum(stepSize)

	// Domain schema (BTree + Existence) — create all files for range 0-1000
	domainSchema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Existence(ver).Build()

	btFile, _ := domainSchema.BtIdxFile(version.V1_0, from, to)
	exFile, _ := domainSchema.ExistenceFile(version.V1_0, from, to)
	kvFile, _ := domainSchema.DataFile(version.V1_0, from, to)
	populateFiles(t, dirs, domainSchema, []string{kvFile, btFile, exFile})

	btPath, err := domainSchema.BtIdxFile(version.SearchVersion, from, to)
	require.NoError(t, err, "SearchVersion must find .bt accessor file on disk")
	require.Contains(t, filepath.Base(btPath), ".bt")

	exPath, err := domainSchema.ExistenceFile(version.SearchVersion, from, to)
	require.NoError(t, err, "SearchVersion must find .kvei existence file on disk")
	require.Contains(t, filepath.Base(exPath), ".kvei")

	// II schema (HashMap → .efi)
	iiSchema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapIdx, "logaddrs", DataExtensionEf, seg.CompressNone, ver).
		Accessor(dirs.SnapAccessors, ver).Build()

	efFile, _ := iiSchema.DataFile(version.V1_0, from, to)
	efiFile, _ := iiSchema.AccessorIdxFile(version.V1_0, from, to, 0)
	populateFiles(t, dirs, iiSchema, []string{efFile, efiFile})

	efiPath, err := iiSchema.AccessorIdxFile(version.SearchVersion, from, to, 0)
	require.NoError(t, err, "SearchVersion must find .efi accessor file on disk")
	require.Contains(t, filepath.Base(efiPath), ".efi")
}

// TestFileNamingRoundTrip_AllAccessorTypes verifies round-trip for all accessor
// file types (.kvi, .vi, .efi, .bt, .kvei). A naming bug in any of these causes
// the accessor to not be found on restart → full rebuild required.
func TestFileNamingRoundTrip_AllAccessorTypes(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	stepSize := uint64(1000)
	from, to := RootNum(0), RootNum(stepSize)

	// BTree (.bt) + Existence (.kvei)
	domainSchema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
		BtIndex(ver).Existence(ver).Build()

	btPath, _ := domainSchema.BtIdxFile(ver.Current, from, to)
	info, ok := domainSchema.Parse(filepath.Base(btPath))
	require.True(t, ok, ".bt file must parse correctly")
	require.Equal(t, ".bt", info.Ext)
	require.Equal(t, uint64(from), info.From)

	exPath, _ := domainSchema.ExistenceFile(ver.Current, from, to)
	info, ok = domainSchema.Parse(filepath.Base(exPath))
	require.True(t, ok, ".kvei file must parse correctly")
	require.Equal(t, ".kvei", info.Ext)

	// HashMap accessor (.kvi for domain)
	commitSchema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapDomain, "commitments", DataExtensionKv, seg.CompressNone, ver).
		Accessor(dirs.SnapDomain, ver).Build()
	kviPath, _ := commitSchema.AccessorIdxFile(ver.Current, from, to, 0)
	info, ok = commitSchema.Parse(filepath.Base(kviPath))
	require.True(t, ok, ".kvi file must parse correctly")
	require.Equal(t, string(AccessorExtensionKvi), info.Ext)

	// HashMap accessor (.vi for history)
	histSchema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapHistory, "accounts", DataExtensionV, seg.CompressNone, ver).
		Accessor(dirs.SnapAccessors, ver).Build()
	viPath, _ := histSchema.AccessorIdxFile(ver.Current, from, to, 0)
	info, ok = histSchema.Parse(filepath.Base(viPath))
	require.True(t, ok, ".vi file must parse correctly")
	require.Equal(t, string(AccessorExtensionVi), info.Ext)

	// HashMap accessor (.efi for inverted index)
	iiSchema := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapIdx, "logaddrs", DataExtensionEf, seg.CompressNone, ver).
		Accessor(dirs.SnapAccessors, ver).Build()
	efiPath, _ := iiSchema.AccessorIdxFile(ver.Current, from, to, 0)
	info, ok = iiSchema.Parse(filepath.Base(efiPath))
	require.True(t, ok, ".efi file must parse correctly")
	require.Equal(t, string(AccessorExtensionEfi), info.Ext)
}

// TestVisibleFiles_RequiresCompleteAccessors verifies the critical safety property:
// a data file is NOT included in visible files unless ALL its required accessors are present.
// This prevents incomplete file creation from causing incorrect state reads in prod.
// A file created without its accessor must be invisible to readers — it cannot be used
// until the accessor is built, even if the data file is valid.
func TestVisibleFiles_RequiresCompleteAccessors(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	// Create a data file for 0-1 WITHOUT its .bt and .kvei accessors
	from, to := RootNum(0), RootNum(repo.stepSize)
	dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
	populateFiles(t, dirs, repo.schema, []string{dataFile})
	require.NoError(t, repo.OpenFolder())
	require.Equal(t, 1, repo.dirtyFiles.Len(), "data file must be in dirty files")

	// RecalcVisibleFiles must NOT include the file — it's missing its BTree accessor
	maxRootNum := repo.RecalcVisibleFiles(RootNum(MaxUint64))
	vf := repo.VisibleFiles()
	require.Empty(t, vf, "file with missing BTree accessor must NOT be visible")
	require.Equal(t, RootNum(0), maxRootNum, "max root num must be 0 when no visible files")

	// Now add the complete accessors
	btFile, _ := repo.schema.BtIdxFile(version.V1_0, from, to)
	exFile, _ := repo.schema.ExistenceFile(version.V1_0, from, to)
	populateFiles(t, dirs, repo.schema, []string{btFile, exFile})

	// Re-open to load the newly created accessor files
	require.NoError(t, repo.OpenFolder())
	maxRootNum = repo.RecalcVisibleFiles(RootNum(MaxUint64))
	vf = repo.VisibleFiles()
	require.Len(t, vf, 1, "file with all accessors present must be visible")
	require.Greater(t, uint64(maxRootNum), uint64(0), "max root num must advance once file is visible")
}

// TestVisibleFiles_HashMap verifies the safety property for HashMap accessor.
func TestVisibleFiles_HashMap(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "commitment"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			Accessor(dirs.SnapDomain, ver).Build()
		return name, schema
	})

	from, to := RootNum(0), RootNum(repo.stepSize)
	dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
	populateFiles(t, dirs, repo.schema, []string{dataFile})
	require.NoError(t, repo.OpenFolder())

	// File must be invisible: HashMap accessor (.kvi) is missing
	repo.RecalcVisibleFiles(RootNum(MaxUint64))
	require.Empty(t, repo.VisibleFiles(), "file with missing .kvi accessor must NOT be visible")

	// Add the accessor, file must become visible
	kviFile, _ := repo.schema.AccessorIdxFile(version.V1_0, from, to, 0)
	populateFiles(t, dirs, repo.schema, []string{kviFile})
	require.NoError(t, repo.OpenFolder())
	repo.RecalcVisibleFiles(RootNum(MaxUint64))
	require.Len(t, repo.VisibleFiles(), 1, "file becomes visible once its accessor is present")
}

// TestFileCreationLifecycle_FullCycle tests the complete file creation lifecycle:
// create data file → detected as missing accessor → invisible to readers →
// create accessor → no longer missing → visible to readers.
// This is the key end-to-end flow for catching file creation bugs early.
func TestFileCreationLifecycle_FullCycle(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	from, to := RootNum(0), RootNum(repo.stepSize)
	v := version.V1_0

	// Phase 1: Only data file created (simulates interrupted file creation)
	dataFile, _ := repo.schema.DataFile(v, from, to)
	populateFiles(t, dirs, repo.schema, []string{dataFile})
	require.NoError(t, repo.OpenFolder())

	require.Equal(t, 1, repo.dirtyFiles.Len(), "data file must be tracked in dirty files")
	require.Len(t, repo.DirtyFilesWithNoBtreeAccessors(), 1, "BTree accessor must be detected as missing")
	missedPhase1 := repo.FilesWithMissedAccessors()
	require.NotEmpty(t, missedPhase1.Get(statecfg.AccessorBTree), "must report missing BTree")
	require.NotEmpty(t, missedPhase1.Get(statecfg.AccessorExistence), "must report missing Existence")

	repo.RecalcVisibleFiles(RootNum(MaxUint64))
	require.Empty(t, repo.VisibleFiles(), "file with missing accessors must NOT be visible to readers")

	// Phase 2: Add accessor files (simulates completing interrupted file creation)
	btFile, _ := repo.schema.BtIdxFile(v, from, to)
	exFile, _ := repo.schema.ExistenceFile(v, from, to)
	populateFiles(t, dirs, repo.schema, []string{btFile, exFile})
	require.NoError(t, repo.OpenFolder())

	require.Empty(t, repo.DirtyFilesWithNoBtreeAccessors(), "no missing BTree accessors after creation")
	require.True(t, repo.FilesWithMissedAccessors().IsEmpty(), "no missed accessors after all files present")

	repo.RecalcVisibleFiles(RootNum(MaxUint64))
	vf := repo.VisibleFiles()
	require.Len(t, vf, 1, "complete file must now be visible to readers")
	require.Equal(t, uint64(from), vf[0].StartRootNum())
	require.Equal(t, uint64(to), vf[0].EndRootNum())
}

// TestDirtyFilesMaxRootNum verifies that DirtyFilesMaxRootNum correctly tracks the
// end boundary of the last file. This value is used to determine how far file
// creation has progressed — a wrong value causes gaps or duplicate file creation.
func TestDirtyFilesMaxRootNum(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	// Empty repo: max root num must be 0
	require.Equal(t, RootNum(0), repo.DirtyFilesMaxRootNum())

	// Add files for ranges 0-1, 1-2, 2-3 (only data files, no accessors needed for DirtyFilesMaxRootNum)
	for _, r := range []testFileRange{{0, 1}, {1, 2}, {2, 3}} {
		from, to := RootNum(r.fromStep*repo.stepSize), RootNum(r.toStep*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(version.V1_0, from, to)
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}
	require.NoError(t, repo.OpenFolder())

	maxRootNum := repo.DirtyFilesMaxRootNum()
	require.Equal(t, RootNum(3*repo.stepSize), maxRootNum,
		"DirtyFilesMaxRootNum must return end of the last file (3*stepSize)")
}

// TestNodeRestart_PartialFileCreation simulates the hard-to-detect prod scenario:
// a node was building new snapshot files when it crashed. On restart, the repo
// must correctly distinguish between:
// (a) previously-completed files that are safe to serve to readers, and
// (b) newly-started files whose accessor build was interrupted.
// Without this detection, a restarted node might serve data from files with
// missing accessors (causing reads to silently fail or return wrong results).
func TestNodeRestart_PartialFileCreation(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	v := version.V1_0

	// Simulate "previous successful runs": 3 complete files (data + all accessors)
	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}, {1, 2}, {2, 3}})

	// Simulate "interrupted run": data file created, accessor build crashed
	// This is the most common prod scenario for file creation fragility
	interruptedFrom, interruptedTo := RootNum(3*repo.stepSize), RootNum(4*repo.stepSize)
	dataOnlyFile, _ := repo.schema.DataFile(v, interruptedFrom, interruptedTo)
	populateFiles(t, dirs, repo.schema, []string{dataOnlyFile})

	// Node restart: OpenFolder
	require.NoError(t, repo.OpenFolder(), "restart must succeed even with interrupted file creation")
	require.Equal(t, 4, repo.dirtyFiles.Len(), "all files (complete + interrupted) must be tracked")

	// Only the interrupted file should be flagged as needing accessor build
	missed := repo.FilesWithMissedAccessors()
	require.False(t, missed.IsEmpty(), "interrupted file must be detected as needing accessor build")

	btMissed := missed.Get(statecfg.AccessorBTree)
	require.Len(t, btMissed, 1, "only the interrupted file should need BTree accessor")
	require.Equal(t, uint64(interruptedFrom), btMissed[0].startTxNum)
	require.Equal(t, uint64(interruptedTo), btMissed[0].endTxNum)

	// The 3 complete files must be visible; the interrupted file must NOT be visible
	repo.RecalcVisibleFiles(RootNum(MaxUint64))
	vf := repo.VisibleFiles()
	require.Len(t, vf, 3, "only complete files must be visible; interrupted file must be invisible")
	for _, f := range vf {
		require.LessOrEqual(t, f.EndRootNum(), uint64(interruptedFrom),
			"interrupted file range must not appear in visible files")
		require.Less(t, f.StartRootNum(), uint64(interruptedFrom),
			"visible files must be from completed ranges only")
	}
}

// TestAllFilesDataOnly_NoneVisible tests the worst-case prod scenario:
// an entire snapshot directory was populated with data files only (all accessor builds
// failed or haven't started yet). In this state, NO files should be visible to readers.
// This prevents serving incorrect state from files without their lookup indexes.
func TestAllFilesDataOnly_NoneVisible(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (name string, schema SnapNameSchema) {
		name = "accounts"
		schema = NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, name, DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return name, schema
	})

	v := version.V1_0

	// Create 5 data files, no accessors at all
	numFiles := 5
	for i := 0; i < numFiles; i++ {
		from, to := RootNum(uint64(i)*repo.stepSize), RootNum(uint64(i+1)*repo.stepSize)
		dataFile, _ := repo.schema.DataFile(v, from, to)
		populateFiles(t, dirs, repo.schema, []string{dataFile})
	}
	require.NoError(t, repo.OpenFolder())
	require.Equal(t, numFiles, repo.dirtyFiles.Len())

	// All files must be detected as missing accessors
	missed := repo.FilesWithMissedAccessors()
	require.Equal(t, numFiles, len(missed.Get(statecfg.AccessorBTree)),
		"all files must be flagged as missing BTree accessor")
	require.Equal(t, numFiles, len(missed.Get(statecfg.AccessorExistence)),
		"all files must be flagged as missing Existence accessor")
	require.Equal(t, numFiles, len(repo.DirtyFilesWithNoBtreeAccessors()),
		"all dirty files must be reported as lacking BTree accessor")

	// Zero files visible — entire snapshot is unusable until accessors are built
	repo.RecalcVisibleFiles(RootNum(MaxUint64))
	require.Empty(t, repo.VisibleFiles(),
		"no files must be visible when all accessor builds are missing")
}

// TestE2Schema_FileNamingRoundTrip verifies that E2 schema generates parseable
// filenames. E2 schemas are used for block-level snapshots; a bug here would
// prevent block headers/bodies from being found after they're snapshotted.
func TestE2Schema_FileNamingRoundTrip(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := NewE2SnapSchemaVersion(version.V1_0_standart, version.V1_0_standart)
	stepSize := uint64(1000)

	bodies := NewE2SnapSchemaWithStep(dirs, "bodies", []string{"bodies"}, stepSize, ver)
	headers := NewE2SnapSchemaWithStep(dirs, "headers", []string{"headers"}, stepSize, ver)
	txns := NewE2SnapSchemaWithIndexTag(dirs, "transactions",
		[]string{"transactions", "transactions-to-block"}, ver)

	cases := []struct {
		schema   SnapNameSchema
		name     string
		from, to uint64
	}{
		{bodies, "bodies", 0, 500000},
		{headers, "headers", 500000, 1000000},
		{txns, "transactions", 0, 500000},
	}

	for _, c := range cases {
		dataPath, err := c.schema.DataFile(version.V1_0, RootNum(c.from), RootNum(c.to))
		require.NoError(t, err, "%s: DataFile must not error", c.name)

		info, ok := c.schema.Parse(filepath.Base(dataPath))
		require.True(t, ok, "%s: generated filename must parse correctly", c.name)
		require.Equal(t, c.from, info.From, "%s: from mismatch", c.name)
		require.Equal(t, c.to, info.To, "%s: to mismatch", c.name)

		// Accessor file round-trip for index 0
		accPath, err := c.schema.AccessorIdxFile(version.V1_0, RootNum(c.from), RootNum(c.to), 0)
		require.NoError(t, err, "%s: AccessorIdxFile must not error", c.name)
		info, ok = c.schema.Parse(filepath.Base(accPath))
		require.True(t, ok, "%s: accessor filename must parse correctly", c.name)
		require.Equal(t, c.from, info.From, "%s accessor: from mismatch", c.name)
		require.Equal(t, c.to, info.To, "%s accessor: to mismatch", c.name)
	}
}

// TestVisibleFiles_MergedFilePrefersOverConstituents verifies a critical correctness
// property of calcVisibleFiles: when both a merged file (large range) AND its constituent
// small files all have complete accessors, only the merged file appears in the visible set.
// This prevents readers from seeing the same data twice, which would cause silent duplication.
//
// The btree ordering places (10-20) before (0-20) [same endTxNum, higher start wins].
// When (0-20) is processed, it removes both (0-10) and (10-20) from newVisibleFiles via
// the isProperSubsetOf check — this is the critical subset-removal invariant.
func TestVisibleFiles_MergedFilePrefersOverConstituents(t *testing.T) {
	t.Parallel()
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	_, repo := setupEntity(t, dirs, func(stepSize uint64, dirs datadir.Dirs) (string, SnapNameSchema) {
		schema := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
			Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressNone, ver).
			BtIndex(ver).Existence(ver).Build()
		return "accounts", schema
	})

	// Create three files: two small + one merged covering same range, all with complete accessors.
	// This simulates the state between merge completion and old-file cleanup.
	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}, {1, 2}, {0, 2}})
	require.NoError(t, repo.OpenFolder())
	require.Equal(t, 3, repo.dirtyFiles.Len(), "all 3 files must be tracked as dirty files")

	repo.RecalcVisibleFiles(RootNum(MaxUint64))
	visible := repo.VisibleFiles()

	// Only the merged file (0-2*stepSize) must appear — NOT the two small files.
	// If this fails, the system would serve the same data twice to readers.
	require.Len(t, visible, 1, "merged file must replace constituent small files in visible set")
	require.Equal(t, uint64(0), visible[0].StartRootNum(), "merged file must start at 0")
	require.Equal(t, uint64(2*repo.stepSize), visible[0].EndRootNum(), "merged file must cover full range")
}
