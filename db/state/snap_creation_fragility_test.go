package state

// Tests focused on making file creation less fragile.
// File creation is rare; bugs are hard to detect in prod and re-gen is expensive.
// These tests catch issues as early as possible.

import (
	"testing"

	"github.com/stretchr/testify/require"

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

// fileBaseName returns the base name from a full file path.
func fileBaseName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[i+1:]
		}
	}
	return path
}
