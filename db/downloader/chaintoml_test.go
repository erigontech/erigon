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
	assert.Equal(t, "/data/snapshots/chain.toml", ChainTomlPath("/data/snapshots"))
}

func TestGenerateChainToml_EmptyDir(t *testing.T) {
	snapDir := t.TempDir()
	toml, err := GenerateChainToml(snapDir)
	require.NoError(t, err)
	assert.Empty(t, toml)
}

func TestGenerateChainToml_WithTorrents(t *testing.T) {
	snapDir := t.TempDir()

	// Create two fake data files and their torrents.
	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")
	createTestTorrent(t, snapDir, "v1-000000-000500-bodies.seg")

	toml, err := GenerateChainToml(snapDir)
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

func TestGenerateChainToml_Deterministic(t *testing.T) {
	snapDir := t.TempDir()

	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")
	createTestTorrent(t, snapDir, "v1-000000-000500-bodies.seg")

	toml1, err := GenerateChainToml(snapDir)
	require.NoError(t, err)

	toml2, err := GenerateChainToml(snapDir)
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

	err := PublishChainToml(snapDir, torrentFS, 42, updater)
	require.NoError(t, err)

	// Verify chain.toml was created.
	loaded, err := LoadChainToml(snapDir)
	require.NoError(t, err)
	assert.NotEmpty(t, loaded)

	// Verify ENR updater was called.
	assert.Equal(t, uint64(42), receivedENR.FrozenTx)
	assert.NotEqual(t, [20]byte{}, receivedENR.InfoHash)
}

func TestPublishChainToml_NilUpdater(t *testing.T) {
	snapDir := t.TempDir()
	torrentFS := NewAtomicTorrentFS(snapDir)
	createTestTorrent(t, snapDir, "v1-000000-000500-headers.seg")

	// Should not panic with nil updater.
	err := PublishChainToml(snapDir, torrentFS, 0, nil)
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
