package datadir

import (
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/dir"
	"github.com/stretchr/testify/require"
)

// mustExist asserts that a regular file exists
func mustExist(t *testing.T, p string) {
	t.Helper()
	exists, err := dir.FileExist(p)
	require.NoError(t, err)
	require.True(t, exists)
}

// mustNotExist asserts that a regular file does not exist
func mustNotExist(t *testing.T, p string) {
	t.Helper()
	exists, err := dir.FileExist(p)
	require.NoError(t, err)
	require.False(t, exists)
}

// mustDirExist asserts that a directory exists
func mustDirExist(t *testing.T, p string) {
	t.Helper()
	exists, err := dir.Exist(p)
	require.NoError(t, err)
	require.True(t, exists)
}

// mustDirNotExist asserts that a directory does not exist
func mustDirNotExist(t *testing.T, p string) {
	t.Helper()
	exists, err := dir.Exist(p)
	require.NoError(t, err)
	require.False(t, exists)
}

// helper to create an empty file
func touch(t *testing.T, p string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	f, err := os.Create(p)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func Test_RenameNewVersions(t *testing.T) {
	base := t.TempDir()
	d := New(base)
	bridgeDir := filepath.Join(d.DataDir, dbcfg.PolygonBridgeDB)
	heimdallDir := filepath.Join(d.DataDir, dbcfg.HeimdallDB)
	touch(t, bridgeDir)
	touch(t, heimdallDir)

	// 1) v1.0- file should be renamed to v1-
	oldName := filepath.Join(d.Snap, "v1.0-000001-000002-headers.seg")
	newName := filepath.Join(d.Snap, "v1-000001-000002-headers.seg")
	touch(t, oldName)

	// 2) commitment file in SnapIdx should be removed (not renamed)
	oldName2 := filepath.Join(d.SnapDomain, "v1.0-accounts.3596-3597.kv")
	newName2 := filepath.Join(d.SnapDomain, "v1-accounts.3596-3597.kv")
	touch(t, oldName2)

	// Erigon3.0 supports only v1 versions. expect remove v2 files
	unsupported := filepath.Join(d.SnapHistory, "v2.0-000001-000002-headers.idx")
	touch(t, unsupported)

	// Sanity preconditions
	mustExist(t, oldName)
	mustExist(t, oldName2)
	mustExist(t, unsupported)

	require.NoError(t, d.RenameNewVersions())

	mustNotExist(t, oldName)
	mustNotExist(t, oldName2)
	mustExist(t, newName)
	mustExist(t, newName2)

	mustNotExist(t, unsupported)

	mustDirNotExist(t, bridgeDir)
	mustDirNotExist(t, heimdallDir)
	mustDirNotExist(t, d.Chaindata)
}
