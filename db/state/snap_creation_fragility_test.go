package state

// Tests focused on making file creation less fragile.
// File creation is rare; bugs are hard to detect in prod and re-gen is expensive.
// These tests catch issues as early as possible.

import (
	"context"
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
	defer repo.Close()

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
	defer repo.Close()

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
	defer repo.Close()

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
	defer repo.Close()

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
		info, ok := schema.Parse(fileBaseName(dataPath))
		require.True(t, ok, "data file name must be parseable: %s", dataPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(DataExtensionKv), info.Ext)

		// BTree index round-trip
		btPath, err := schema.BtIdxFile(ver.Current, from, to)
		require.NoError(t, err)
		info, ok = schema.Parse(fileBaseName(btPath))
		require.True(t, ok, "bt file name must be parseable: %s", btPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, ".bt", info.Ext)

		// Existence file round-trip
		exPath, err := schema.ExistenceFile(ver.Current, from, to)
		require.NoError(t, err)
		info, ok = schema.Parse(fileBaseName(exPath))
		require.True(t, ok, "existence file name must be parseable: %s", exPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, ".kvei", info.Ext)
	}
}

// TestFileNamingRoundTrip_History verifies history file naming round-trip.
func TestFileNamingRoundTrip_History(t *testing.T) {
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
		info, ok := schema.Parse(fileBaseName(dataPath))
		require.True(t, ok, "history data file name must be parseable: %s", dataPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(DataExtensionV), info.Ext)

		accPath, err := schema.AccessorIdxFile(ver.Current, from, to, 0)
		require.NoError(t, err)
		info, ok = schema.Parse(fileBaseName(accPath))
		require.True(t, ok, "history accessor file name must be parseable: %s", accPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(AccessorExtensionVi), info.Ext)
	}
}

// TestFileNamingRoundTrip_II verifies inverted index file naming round-trip.
func TestFileNamingRoundTrip_II(t *testing.T) {
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
		info, ok := schema.Parse(fileBaseName(dataPath))
		require.True(t, ok, "II data file name must be parseable: %s", dataPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(DataExtensionEf), info.Ext)

		accPath, err := schema.AccessorIdxFile(ver.Current, from, to, 0)
		require.NoError(t, err)
		info, ok = schema.Parse(fileBaseName(accPath))
		require.True(t, ok, "II accessor file name must be parseable: %s", accPath)
		require.Equal(t, c.from, info.From)
		require.Equal(t, c.to, info.To)
		require.Equal(t, string(AccessorExtensionEfi), info.Ext)
	}
}

// TestSnapshotConfigValidation verifies that invalid SnapshotCreationConfig panics early.
// This prevents misconfigured nodes from silently creating files with wrong ranges.
func TestSnapshotConfigValidation(t *testing.T) {
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
	defer repo.Close()

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
	defer repo.Close()

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
	defer repo.Close()

	populateFiles2(t, dirs, repo, []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, repo.OpenFolder())

	require.Empty(t, repo.DirtyFilesWithNoBtreeAccessors())
}

// TestOpenFolder_PartialFiles_StillOpens verifies that OpenFolder succeeds even when
// some data files have no accessors. This is critical: a node shouldn't crash if file
// creation was partially completed (data written, accessor build was interrupted).
func TestOpenFolder_PartialFiles_StillOpens(t *testing.T) {
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
	defer repo.Close()

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

// TestSnapshotConfig_StepsInFrozenFile verifies StepsInFrozenFile returns correct values.
// A wrong value here means files are frozen too early or too late.
func TestSnapshotConfig_StepsInFrozenFile(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	schema := NewE2SnapSchema(dirs, "bodies", NewE2SnapSchemaVersion(ver, ver))

	// With MergeStages: last stage is 100_000, stepSize=1000 → 100 steps per frozen file
	cfg := NewSnapshotConfig(&SnapshotCreationConfig{
		RootNumPerStep: 1000,
		MergeStages:    []uint64{10_000, 100_000},
		MinimumSize:    1000,
	}, schema)
	require.Equal(t, uint64(100), cfg.StepsInFrozenFile())

	// With empty MergeStages: falls back to MinimumSize / RootNumPerStep
	cfg2 := NewSnapshotConfig(&SnapshotCreationConfig{
		RootNumPerStep: 1000,
		MergeStages:    []uint64{},
		MinimumSize:    5000,
	}, schema)
	require.Equal(t, uint64(5), cfg2.StepsInFrozenFile())
}

// TestE3SnapCreationConfig_IsValid verifies that E3SnapCreationConfig produces a valid config.
// This is the default production config; it must not panic on validation.
func TestE3SnapCreationConfig_IsValid(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	ver := version.V1_0_standart
	schema := NewE2SnapSchema(dirs, "bodies", NewE2SnapSchemaVersion(ver, ver))

	stepSizes := []uint64{100, 500, 1000, 4096}
	for _, stepSize := range stepSizes {
		cfg := E3SnapCreationConfig(stepSize)
		require.NotPanics(t, func() {
			NewSnapshotConfig(cfg, schema)
		}, "E3SnapCreationConfig(%d) must produce a valid config", stepSize)
		// Verify basic invariants
		require.Equal(t, stepSize, cfg.RootNumPerStep)
		require.Equal(t, stepSize, cfg.MinimumSize)
		require.NotEmpty(t, cfg.MergeStages)
		// All merge stages must be divisible by stepSize
		for i, stage := range cfg.MergeStages {
			require.Zero(t, stage%stepSize, "MergeStages[%d]=%d not divisible by stepSize=%d", i, stage, stepSize)
		}
	}
}

// TestRealFileCreation_DomainDataFile creates a real .kv data file using the proper
// compressor path and verifies it can be decompressed after creation.
// This catches bugs in the file creation path that only manifest with real I/O.
func TestRealFileCreation_DomainDataFile(t *testing.T) {
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
	comp, err := seg.NewCompressor(context.Background(), t.Name(), dataPath, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
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
	info, ok := schema.Parse(fileBaseName(dataPath))
	require.True(t, ok)
	require.Equal(t, uint64(from), info.From)
	require.Equal(t, uint64(to), info.To)
}

// TestFilesWithMissedAccessors_PartialBTree verifies that when some files have
// BTree accessors and some don't, only the ones without are reported as missing.
func TestFilesWithMissedAccessors_PartialBTree(t *testing.T) {
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
	defer repo.Close()

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

		info, ok := schema.Parse(fileBaseName(dataPath))
		require.True(t, ok, "stepSize=%d: data file name must parse correctly", stepSize)
		require.Equal(t, uint64(from), info.From, "stepSize=%d: from mismatch", stepSize)
		require.Equal(t, uint64(to), info.To, "stepSize=%d: to mismatch", stepSize)
	}
}

// TestE3ParseRejectsWrongTag verifies that Parse rejects filenames with a different
// entity name. Files of different entities must not be confused with each other.
func TestE3ParseRejectsWrongTag(t *testing.T) {
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

// fileBaseName returns the base name from a full file path.
func fileBaseName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[i+1:]
		}
	}
	return path
}
