package datadir

import (
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
	bridgeDir := filepath.Join(d.DataDir, "polygon-bridge")
	heimdallDir := filepath.Join(d.DataDir, "heimdall")
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

func Test_RenameVersions_SkipCaplinSidecarDirs(t *testing.T) {
	tests := []struct {
		name string
		walk func(d *Dirs) error
		// sidecar goes into CaplinBlobs and CaplinColumnData and must survive
		sidecar string
		// control goes into SnapCaplin and must become controlAfter, or be
		// deleted when controlAfter is empty
		control      string
		controlAfter string
	}{
		{
			name:         "old_versions_rename_pass",
			walk:         func(d *Dirs) error { return d.RenameOldVersions(false) },
			sidecar:      "v1-something.seg",
			control:      "v1-000001-000002-headers.seg",
			controlAfter: "v1.0-000001-000002-headers.seg",
		},
		{
			name:         "new_versions_rename_pass",
			walk:         func(d *Dirs) error { return d.RenameNewVersions() },
			sidecar:      "v1.0-something.seg",
			control:      "v1.0-000001-000002-headers.seg",
			controlAfter: "v1-000001-000002-headers.seg",
		},
		{
			name:    "new_versions_delete_pass",
			walk:    func(d *Dirs) error { return d.RenameNewVersions() },
			sidecar: "v2.0-something.seg",
			control: "v2.0-000001-000002-headers.seg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := New(t.TempDir())

			blobFile := filepath.Join(d.CaplinBlobs, tt.sidecar)
			columnFile := filepath.Join(d.CaplinColumnData, tt.sidecar)
			controlFile := filepath.Join(d.SnapCaplin, tt.control)
			touch(t, blobFile)
			touch(t, columnFile)
			touch(t, controlFile)

			require.NoError(t, tt.walk(&d))

			mustExist(t, blobFile)
			mustExist(t, columnFile)

			mustNotExist(t, controlFile)
			if tt.controlAfter != "" {
				mustExist(t, filepath.Join(d.SnapCaplin, tt.controlAfter))
			}
		})
	}
}

// The commitment special case keys off the directory a file sits in, not the
// entry the walk started from — d.Snap is walked recursively and the Snap*
// subdirs are no longer separate roots.
func Test_RenameVersions_CommitmentInIndexDirs(t *testing.T) {
	tests := []struct {
		name string
		walk func(d *Dirs) error
		// removed lands in SnapIdx, SnapHistory and SnapAccessors; removedAfter
		// is the name it would carry had it been renamed instead
		removed      string
		removedAfter string
		// kept lands in SnapDomain, which the special case excludes
		kept      string
		keptAfter string
	}{
		{
			name:         "old_versions",
			walk:         func(d *Dirs) error { return d.RenameOldVersions(false) },
			removed:      "v1-commitment.0-1024.efi",
			removedAfter: "v1.0-commitment.0-1024.efi",
			kept:         "v1-commitment.0-1024.kv",
			keptAfter:    "v1.0-commitment.0-1024.kv",
		},
		{
			name:         "new_versions",
			walk:         func(d *Dirs) error { return d.RenameNewVersions() },
			removed:      "v1.0-commitment.0-1024.efi",
			removedAfter: "v1-commitment.0-1024.efi",
			kept:         "v1.0-commitment.0-1024.kv",
			keptAfter:    "v1-commitment.0-1024.kv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := New(t.TempDir())

			var removedFiles []string
			for _, indexDir := range []string{d.SnapIdx, d.SnapHistory, d.SnapAccessors} {
				p := filepath.Join(indexDir, tt.removed)
				touch(t, p)
				removedFiles = append(removedFiles, p)
			}
			keptFile := filepath.Join(d.SnapDomain, tt.kept)
			touch(t, keptFile)

			require.NoError(t, tt.walk(&d))

			for _, p := range removedFiles {
				mustNotExist(t, p)
				mustNotExist(t, filepath.Join(filepath.Dir(p), tt.removedAfter))
			}
			mustNotExist(t, keptFile)
			mustExist(t, filepath.Join(d.SnapDomain, tt.keptAfter))
		})
	}
}

// WalkDir roots on Lstat, so a symlinked snapshots/ is descended only because the
// Snap* subdirs are named in VersionedDirs. db/datadir/reset handles that layout
// deliberately, so collapsing them onto d.Snap would silently skip the whole tree.
func Test_RenameOldVersions_SymlinkedSnapshotsDir(t *testing.T) {
	base := t.TempDir()
	external := t.TempDir()
	require.NoError(t, os.Symlink(external, filepath.Join(base, SnapDir)))

	d := New(base)
	require.NoError(t, os.MkdirAll(d.SnapDomain, 0o755))

	oldName := filepath.Join(d.SnapDomain, "v1-accounts.0-1.kv")
	newName := filepath.Join(d.SnapDomain, "v1.0-accounts.0-1.kv")
	touch(t, oldName)

	require.NoError(t, d.RenameOldVersions(false))

	mustNotExist(t, oldName)
	mustExist(t, newName)
}

// A stale v1-*.torrent with nothing to rename still has to reset the downloader,
// or its DB keeps indexing the torrents that were just deleted.
func Test_RenameOldVersions_TorrentOnlyResetsDownloader(t *testing.T) {
	t.Run("torrent_only", func(t *testing.T) {
		d := New(t.TempDir())
		torrent := filepath.Join(d.Downloader, "v1-000001-000002-headers.seg.torrent")
		touch(t, torrent)

		require.NoError(t, d.RenameOldVersions(false))

		mustNotExist(t, torrent)
		mustDirNotExist(t, d.Downloader)
	})

	t.Run("nothing_stale", func(t *testing.T) {
		d := New(t.TempDir())
		keep := filepath.Join(d.Snap, "v1.0-000001-000002-headers.seg")
		touch(t, keep)

		require.NoError(t, d.RenameOldVersions(false))

		mustExist(t, keep)
		mustDirExist(t, d.Downloader)
	})
}
