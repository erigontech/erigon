package downloader

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/p2p/enr"
)

func TestChainTomlPath(t *testing.T) {
	// Use filepath.Join for the input too so the test is portable across OSes —
	// ChainTomlPath uses filepath.Join internally, which emits platform separators.
	dir := filepath.Join("data", "snapshots")
	assert.Equal(t, filepath.Join(dir, "chain.toml"), ChainTomlPath(dir))
}

func TestGenerateChainToml_EmptyDir(t *testing.T) {
	snapDir := t.TempDir()
	toml, err := GenerateChainToml(snapDir, nil)
	require.NoError(t, err)
	assert.Empty(t, toml)
}

func TestGenerateChainToml_WithTorrents(t *testing.T) {
	snapDir := t.TempDir()

	// Create two fake data files and their torrents.
	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")
	createTestTorrent(t, snapDir, "v1-000000-000500-bodies.seg")

	toml, err := GenerateChainToml(snapDir, nil)
	require.NoError(t, err)

	tomlStr := string(toml)
	// Should contain both entries.
	assert.Contains(t, tomlStr, "v1-000000-000500-headers.seg")
	assert.Contains(t, tomlStr, "v1-000000-000500-bodies.seg")
	// Should be sorted (bodies before headers).
	bodiesIdx := strings.Index(tomlStr, "bodies")
	headersIdx := strings.Index(tomlStr, "headers")
	assert.Less(t, bodiesIdx, headersIdx, "entries should be sorted alphabetically")
}

func TestGenerateChainToml_IncludesStateSubdirs(t *testing.T) {
	snapDir := t.TempDir()
	for _, sub := range chainTomlScanSubdirs {
		require.NoError(t, os.MkdirAll(filepath.Join(snapDir, sub), 0o755))
	}

	// Top-level block torrent.
	createTestTorrent(t, snapDir, "v1.1-000000-000500-headers.seg")
	// State torrents in their canonical subdirs (mirroring Erigon's
	// snapshot layout — see snapshot.SubdirForName).
	createTestTorrentInSubdir(t, snapDir, "domain", "v2.0-accounts.0-256.kv")
	createTestTorrentInSubdir(t, snapDir, "history", "v2.0-accounts.0-256.v")
	createTestTorrentInSubdir(t, snapDir, "idx", "v3.0-accounts.0-256.ef")
	createTestTorrentInSubdir(t, snapDir, "accessor", "v1.1-code.0-256.vi")

	toml, err := GenerateChainToml(snapDir, nil)
	require.NoError(t, err)
	tomlStr := string(toml)

	// Block torrent emitted bare (no subdir prefix).
	assert.Contains(t, tomlStr, `"v1.1-000000-000500-headers.seg"`)
	// State torrents emitted with subdir prefix.
	assert.Contains(t, tomlStr, `"domain/v2.0-accounts.0-256.kv"`)
	assert.Contains(t, tomlStr, `"history/v2.0-accounts.0-256.v"`)
	assert.Contains(t, tomlStr, `"idx/v3.0-accounts.0-256.ef"`)
	assert.Contains(t, tomlStr, `"accessor/v1.1-code.0-256.vi"`)
}

func TestGenerateChainToml_MissingSubdirIsNotAnError(t *testing.T) {
	snapDir := t.TempDir()
	// No subdirs created — only top-level. Used to emulate a fresh
	// datadir on a consumer-only node that hasn't retired any state.
	createTestTorrent(t, snapDir, "v1.1-000000-000500-headers.seg")

	toml, err := GenerateChainToml(snapDir, nil)
	require.NoError(t, err,
		"missing state subdirs must not error — fresh consumer datadir is a valid case")
	assert.Contains(t, string(toml), "v1.1-000000-000500-headers.seg")
}

// TestGenerateChainToml_ServableGate: when a servable filter is
// passed, only entries whose info-hash is in the set get into the
// output. This is the "validate before advertise" gate — it stops
// the publisher from announcing files in chain.toml that haven't
// yet been loaded into the torrent client (and therefore can't be
// served to peers).
func TestGenerateChainToml_ServableGate(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()

	// Three torrents on disk; only one will be in the servable set.
	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")
	createTestTorrent(t, snapDir, "v1-000000-000500-bodies.seg")
	createTestTorrent(t, snapDir, "v1-000500-001000-headers.seg")

	// Build a "servable" set containing only the second file's hash.
	miPath := filepath.Join(snapDir, "v1-000000-000500-bodies.seg.torrent")
	mi, err := metainfo.LoadFromFile(miPath)
	require.NoError(t, err)
	servable := map[metainfo.Hash]struct{}{
		mi.HashInfoBytes(): {},
	}

	toml, err := GenerateChainToml(snapDir, servable)
	require.NoError(t, err)
	tomlStr := string(toml)

	// Only the bodies entry should appear; headers entries should be
	// filtered out because their info-hashes aren't in the servable set.
	assert.Contains(t, tomlStr, `"v1-000000-000500-bodies.seg"`)
	assert.NotContains(t, tomlStr, `"v1-000000-000500-headers.seg"`,
		"file with disk-side .torrent but NOT in servable set must be excluded")
	assert.NotContains(t, tomlStr, `"v1-000500-001000-headers.seg"`,
		"file with disk-side .torrent but NOT in servable set must be excluded")
}

// TestGenerateChainToml_EmptyServableSetFiltersAll: a non-nil but
// empty servable set means "the torrent client has nothing loaded
// yet" — chain.toml should be empty even if disk has .torrent files.
func TestGenerateChainToml_EmptyServableSetFiltersAll(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")
	createTestTorrent(t, snapDir, "v1-000000-000500-bodies.seg")

	servable := map[metainfo.Hash]struct{}{} // non-nil but empty
	toml, err := GenerateChainToml(snapDir, servable)
	require.NoError(t, err)
	assert.Empty(t, toml,
		"empty servable set means no advertisable files; chain.toml must be empty")
}

func TestGenerateChainToml_Deterministic(t *testing.T) {
	snapDir := t.TempDir()

	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")
	createTestTorrent(t, snapDir, "v1-000000-000500-bodies.seg")

	toml1, err := GenerateChainToml(snapDir, nil)
	require.NoError(t, err)

	toml2, err := GenerateChainToml(snapDir, nil)
	require.NoError(t, err)

	assert.Equal(t, toml1, toml2, "GenerateChainToml should produce identical output for same inputs")
}

func TestSaveAndLoadChainToml(t *testing.T) {
	snapDir := t.TempDir()
	content := []byte(`"test.seg" = "abcdef0123456789abcdef0123456789abcdef01"` + "\n")

	err := SaveChainToml(snapDir, content)
	require.NoError(t, err)

	loaded, err := LoadChainToml(snapDir)
	require.NoError(t, err)
	assert.Equal(t, content, loaded)
}

func TestLoadChainToml_NotExists(t *testing.T) {
	snapDir := t.TempDir()
	loaded, err := LoadChainToml(snapDir)
	require.NoError(t, err)
	assert.Nil(t, loaded)
}

func TestSaveChainToml_Overwrite(t *testing.T) {
	snapDir := t.TempDir()

	err := SaveChainToml(snapDir, []byte("version1"))
	require.NoError(t, err)

	err = SaveChainToml(snapDir, []byte("version2"))
	require.NoError(t, err)

	loaded, err := LoadChainToml(snapDir)
	require.NoError(t, err)
	assert.Equal(t, []byte("version2"), loaded)
}

func TestSaveChainToml_ReadOnlyOverwrite(t *testing.T) {
	snapDir := t.TempDir()

	// Write initial content, then make it read-only (simulates torrent client behavior).
	err := SaveChainToml(snapDir, []byte("initial"))
	require.NoError(t, err)
	require.NoError(t, os.Chmod(ChainTomlPath(snapDir), 0o444))

	// SaveChainToml should still succeed despite the read-only file.
	err = SaveChainToml(snapDir, []byte("updated"))
	require.NoError(t, err)

	data, err := os.ReadFile(ChainTomlPath(snapDir))
	require.NoError(t, err)
	assert.Equal(t, []byte("updated"), data)
}

func TestBuildChainTomlTorrent(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)

	// Write a chain.toml file.
	content := []byte(`"test.seg" = "abcdef0123456789abcdef0123456789abcdef01"` + "\n")
	err := SaveChainToml(snapDir, content)
	require.NoError(t, err)

	// Build the torrent.
	infoHash, err := BuildChainTomlTorrent(snapDir, torrentFS)
	require.NoError(t, err)
	assert.NotEqual(t, metainfo.Hash{}, infoHash)

	// Verify the .torrent file was created.
	torrentPath := filepath.Join(snapDir, ChainTomlFileName+".torrent")
	_, err = os.Stat(torrentPath)
	require.NoError(t, err)
}

func TestBuildChainTomlTorrent_Rebuild(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)

	// Build with content v1.
	err := SaveChainToml(snapDir, []byte("v1 content"))
	require.NoError(t, err)
	hash1, err := BuildChainTomlTorrent(snapDir, torrentFS)
	require.NoError(t, err)

	// Rebuild with content v2.
	err = SaveChainToml(snapDir, []byte("v2 content"))
	require.NoError(t, err)
	hash2, err := BuildChainTomlTorrent(snapDir, torrentFS)
	require.NoError(t, err)

	// Info-hashes should differ because content changed.
	assert.NotEqual(t, hash1, hash2, "different content should produce different info-hashes")
}

func TestPublishChainToml(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)

	// Create a test torrent file to be included in chain.toml.
	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")

	var receivedENR enr.ChainToml
	updater := func(ct enr.ChainToml) {
		receivedENR = ct
	}

	err := PublishChainToml(snapDir, torrentFS, "", false, nil, updater)
	require.NoError(t, err)

	// Verify chain.toml was created.
	loaded, err := LoadChainToml(snapDir)
	require.NoError(t, err)
	assert.NotEmpty(t, loaded)

	// Verify ENR updater was called (no chainName → AuthoritativeBlocks/KnownBlocks = 0).
	assert.Equal(t, uint64(0), receivedENR.AuthoritativeBlocks)
	assert.Equal(t, uint64(0), receivedENR.KnownBlocks)
	assert.NotEqual(t, [20]byte{}, receivedENR.InfoHash)
}

func TestPublishChainToml_NilUpdater(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)
	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")

	// Should not panic with nil updater.
	err := PublishChainToml(snapDir, torrentFS, "", false, nil, nil)
	require.NoError(t, err)
}

// createTestTorrent creates a fake data file and its .torrent in snapDir.
func createTestTorrent(t *testing.T, snapDir, name string) {
	t.Helper()

	// Create the data file.
	dataPath := filepath.Join(snapDir, name)
	err := os.WriteFile(dataPath, []byte("test data for "+name), 0o644)
	require.NoError(t, err)

	// Build the .torrent file.
	tf := NewAtomicTorrentFS(snapDir)
	_, err = BuildTorrentIfNeed(t.Context(), name, snapDir, tf)
	require.NoError(t, err)
}

// createTestTorrentInSubdir creates a fake data file and its .torrent
// inside subdir of snapDir (e.g. "domain/foo.kv"). Used by tests that
// exercise the subdir-aware GenerateChainToml path.
func createTestTorrentInSubdir(t *testing.T, snapDir, subdir, name string) {
	t.Helper()
	relName := subdir + "/" + name
	dataPath := filepath.Join(snapDir, subdir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(dataPath), 0o755))
	require.NoError(t, os.WriteFile(dataPath, []byte("test data for "+relName), 0o644))
	tf := NewAtomicTorrentFS(snapDir)
	_, err := BuildTorrentIfNeed(t.Context(), relName, snapDir, tf)
	require.NoError(t, err)
}

// TestClassifyRetiredEntries verifies the publisher-republish
// reconciliation: when files in old aren't in new, they're classified
// as MergedOut (a new entry of same type covers their range as a
// superset) or Removed (no superset exists).
func TestClassifyRetiredEntries(t *testing.T) {
	t.Parallel()

	t.Run("merged_out_state_files", func(t *testing.T) {
		// Three small commitment step files in old, replaced by one
		// merged file covering all three steps in new.
		old := map[string]string{
			"v2.0-commitment.0-1.kv":         "aa",
			"v2.0-commitment.1-2.kv":         "bb",
			"v2.0-commitment.2-3.kv":         "cc",
			"v2.0-accounts.0-3.kv":           "dd", // unchanged
			"v1.1-000000-000500-headers.seg": "ee", // unchanged block
		}
		newM := map[string]string{
			"v2.0-commitment.0-3.kv":         "ff", // merged
			"v2.0-accounts.0-3.kv":           "dd",
			"v1.1-000000-000500-headers.seg": "ee",
		}
		got := ClassifyRetiredEntries(old, newM)
		require.Equal(t, []string{
			"v2.0-commitment.0-1.kv",
			"v2.0-commitment.1-2.kv",
			"v2.0-commitment.2-3.kv",
		}, got.MergedOut)
		require.Empty(t, got.Removed)
	})

	t.Run("removed_no_superset", func(t *testing.T) {
		// A file simply dropped — no merged file in new covers its
		// range.
		old := map[string]string{
			"v2.0-commitment.0-1.kv": "aa",
			"v2.0-accounts.0-1.kv":   "bb",
		}
		newM := map[string]string{
			"v2.0-accounts.0-1.kv": "bb",
		}
		got := ClassifyRetiredEntries(old, newM)
		require.Empty(t, got.MergedOut)
		require.Equal(t, []string{"v2.0-commitment.0-1.kv"}, got.Removed)
	})

	t.Run("unchanged_entries_not_classified", func(t *testing.T) {
		old := map[string]string{"v2.0-commitment.0-1.kv": "aa"}
		newM := map[string]string{"v2.0-commitment.0-1.kv": "aa"}
		got := ClassifyRetiredEntries(old, newM)
		require.Empty(t, got.MergedOut)
		require.Empty(t, got.Removed)
	})

	t.Run("only_in_new_ignored", func(t *testing.T) {
		// New entries that weren't in old are not part of the
		// classification — they're handled by the existing
		// "applied new entries" path.
		old := map[string]string{}
		newM := map[string]string{"v2.0-commitment.0-1.kv": "aa"}
		got := ClassifyRetiredEntries(old, newM)
		require.Empty(t, got.MergedOut)
		require.Empty(t, got.Removed)
	})

	t.Run("unparseable_name_treated_as_removed", func(t *testing.T) {
		// A name we can't parse loses the merge claim
		// conservatively — without range info we can't promise a
		// merged file covers it. Defaults to Removed.
		old := map[string]string{"random-config.toml": "aa"}
		newM := map[string]string{}
		got := ClassifyRetiredEntries(old, newM)
		require.Empty(t, got.MergedOut)
		require.Equal(t, []string{"random-config.toml"}, got.Removed)
	})
}
