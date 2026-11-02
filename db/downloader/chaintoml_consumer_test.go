package downloader

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadChainTomlByInfoHashPreservesSnapshotFiles(t *testing.T) {
	snapDir := t.TempDir()
	settingsPath := filepath.Join(snapDir, "erigondb.toml")
	settings := []byte("step_size = 1562500\n")
	require.NoError(t, os.WriteFile(settingsPath, settings, 0o644))

	cfg := newTestClientConfig(t, snapDir)
	cfg.Slogger = slog.Default()
	store, client, err := newTorrentClient(snapDir, cfg)
	require.NoError(t, err)
	defer store.Close()
	defer client.Close()

	infoBytes, err := bencode.Marshal(metainfo.Info{
		Name:        "erigondb.toml",
		PieceLength: 256 * 1024,
	})
	require.NoError(t, err)
	infoHash := metainfo.Hash{1}
	require.NotEqual(t, infoHash, (&metainfo.MetaInfo{InfoBytes: infoBytes}).HashInfoBytes())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	var downloadErr error
	go func() {
		defer close(done)
		_, downloadErr = DownloadChainTomlByInfoHash(ctx, client, infoHash, snapDir, nil)
	}()
	defer func() {
		cancel()
		<-done
	}()

	var manifest *torrent.Torrent
	require.Eventually(t, func() bool {
		var ok bool
		manifest, ok = client.Torrent(infoHash)
		return ok
	}, 5*time.Second, time.Millisecond)
	if err = manifest.SetInfoBytes(infoBytes); err != nil {
		cancel()
	}
	<-done
	require.Error(t, downloadErr)

	got, err := os.ReadFile(settingsPath)
	require.NoError(t, err)
	assert.Equal(t, settings, got)
	assert.Empty(t, client.Torrents())
	entries, err := os.ReadDir(snapDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "erigondb.toml", entries[0].Name())
}

func TestDownloadChainTomlByInfoHashKeepsExistingTorrent(t *testing.T) {
	snapDir := t.TempDir()
	client, err := torrent.NewClient(newTestClientConfig(t, snapDir))
	require.NoError(t, err)
	defer client.Close()
	infoHash := metainfo.Hash{1}
	existing, _ := client.AddTorrentInfoHash(infoHash)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = DownloadChainTomlByInfoHash(ctx, client, infoHash, snapDir, nil)
	require.Error(t, err)
	got, ok := client.Torrent(infoHash)
	require.True(t, ok, "the existing torrent must remain registered")
	assert.Same(t, existing, got)
	entries, err := filepath.Glob(filepath.Join(snapDir, ".chaintoml-download-*"))
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestDownloadChainTomlByInfoHashUsesSeededManifest(t *testing.T) {
	snapDir := t.TempDir()
	data := BuildTomlFromMap(map[string]string{"snapshot.seg": "abc123"})
	require.NoError(t, os.WriteFile(ChainTomlPath(snapDir), data, 0o644))
	client, err := torrent.NewClient(newTestClientConfig(t, snapDir))
	require.NoError(t, err)
	defer client.Close()
	mi := buildTestMetainfo(t, snapDir, ChainTomlFileName)
	existing, err := client.AddTorrent(&mi)
	require.NoError(t, err)
	require.True(t, existing.Complete().Bool())

	got, err := DownloadChainTomlByInfoHash(t.Context(), client, mi.HashInfoBytes(), snapDir, nil)
	require.NoError(t, err)
	assert.Equal(t, data, got)
	registered, ok := client.Torrent(mi.HashInfoBytes())
	require.True(t, ok)
	assert.Same(t, existing, registered)
	entries, err := filepath.Glob(filepath.Join(snapDir, ".chaintoml-download-*"))
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestParseChainToml(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		input := []byte(`"v1-000000-000100-headers.seg" = "abc123"
"v1-000100-000200-headers.seg" = "def456"
`)
		m, err := ParseChainToml(input)
		require.NoError(t, err)
		assert.Len(t, m, 2)
		assert.Equal(t, "abc123", m["v1-000000-000100-headers.seg"])
		assert.Equal(t, "def456", m["v1-000100-000200-headers.seg"])
	})

	t.Run("empty", func(t *testing.T) {
		m, err := ParseChainToml([]byte{})
		require.NoError(t, err)
		assert.Empty(t, m)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := ParseChainToml([]byte("not valid toml [[["))
		assert.Error(t, err)
	})
}

func TestBuildTomlFromMap(t *testing.T) {
	m := map[string]string{
		"b-file.seg": "hash2",
		"a-file.seg": "hash1",
		"c-file.seg": "hash3",
	}
	result := BuildTomlFromMap(m)

	// Should be sorted by key
	expected := `"a-file.seg" = "hash1"
"b-file.seg" = "hash2"
"c-file.seg" = "hash3"
`
	assert.Equal(t, expected, string(result))
}

func TestParseAndBuildRoundtrip(t *testing.T) {
	original := map[string]string{
		"v1-000000-000100-headers.seg": "abc123",
		"v1-000100-000200-headers.seg": "def456",
	}

	tomlBytes := BuildTomlFromMap(original)
	parsed, err := ParseChainToml(tomlBytes)
	require.NoError(t, err)
	assert.Equal(t, original, parsed)
}

func TestMergeChainToml_NoConflict(t *testing.T) {
	existing := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"c.seg": "hash3",
		"d.seg": "hash4",
	}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 2, newCount)
	assert.Len(t, merged, 4)
	assert.Equal(t, "hash1", merged["a.seg"])
	assert.Equal(t, "hash3", merged["c.seg"])
}

func TestMergeChainToml_ExistingWins(t *testing.T) {
	existing := map[string]string{
		"a.seg": "existing-hash",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"a.seg": "different-hash", // conflict — existing should win
		"c.seg": "hash3",
	}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 1, newCount) // only c.seg is new
	assert.Equal(t, "existing-hash", merged["a.seg"])
	assert.Equal(t, "hash3", merged["c.seg"])
}

func TestMergeChainToml_EmptyExisting(t *testing.T) {
	existing := map[string]string{}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 2, newCount)
	assert.Len(t, merged, 2)
}

func TestMergeChainToml_EmptyDiscovered(t *testing.T) {
	existing := map[string]string{
		"a.seg": "hash1",
	}
	discovered := map[string]string{}

	merged, newCount := MergeChainToml(existing, discovered)
	assert.Equal(t, 0, newCount)
	assert.Len(t, merged, 1)
}

func TestCompareChainToml_Matching(t *testing.T) {
	local := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}

	matching, newEntries, mismatches := CompareChainToml(local, discovered)
	assert.Equal(t, 2, matching)
	assert.Equal(t, 0, newEntries)
	assert.Empty(t, mismatches)
}

func TestCompareChainToml_NewEntries(t *testing.T) {
	local := map[string]string{
		"a.seg": "hash1",
	}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
		"c.seg": "hash3",
	}

	matching, newEntries, mismatches := CompareChainToml(local, discovered)
	assert.Equal(t, 1, matching)
	assert.Equal(t, 2, newEntries)
	assert.Empty(t, mismatches)
}

func TestCompareChainToml_HashMismatch(t *testing.T) {
	local := map[string]string{
		"a.seg": "hash1",
		"b.seg": "hash2",
	}
	discovered := map[string]string{
		"a.seg": "hash1",
		"b.seg": "different-hash",
	}

	matching, newEntries, mismatches := CompareChainToml(local, discovered)
	assert.Equal(t, 1, matching)
	assert.Equal(t, 0, newEntries)
	assert.Equal(t, []string{"b.seg"}, mismatches)
}
