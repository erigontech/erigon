package downloader

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func TestChainTomlDiscoveryUsesTempDir(t *testing.T) {
	for _, tc := range []struct {
		name     string
		discover func(*Downloader, context.Context, string, NodeSource)
	}{
		{name: "acquire", discover: (*Downloader).acquireChainToml},
		{name: "verify", discover: (*Downloader).verifyChainToml},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.Dirs{Snap: t.TempDir(), Tmp: t.TempDir()}
			cfg := newTestClientConfig(t, dirs.Snap)
			cfg.Slogger = slog.Default()
			store, client, err := newTorrentClient(dirs.Snap, cfg)
			require.NoError(t, err)
			defer store.Close()
			defer client.Close()
			d := &Downloader{
				cfg:           &downloadercfg.Cfg{Dirs: dirs},
				torrentClient: client,
				logger:        log.New(),
			}
			infoHash := metainfo.Hash{1}
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			node := makeTestNode(t, key, &enr.ChainToml{KnownBlocks: 1, InfoHash: infoHash})
			ns := &mockNodeSource{nodes: []*enode.Node{node}}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			done := make(chan struct{})
			go func() {
				defer close(done)
				tc.discover(d, ctx, "testnet", ns)
			}()
			defer func() {
				cancel()
				<-done
			}()
			require.Eventually(t, func() bool {
				_, ok := client.Torrent(infoHash)
				return ok
			}, 5*time.Second, time.Millisecond)

			entries, err := os.ReadDir(dirs.Tmp)
			require.NoError(t, err)
			require.Len(t, entries, 1, "downloads must use the directory cleaned on startup")
			assert.True(t, entries[0].IsDir())
			entries, err = os.ReadDir(dirs.Snap)
			require.NoError(t, err)
			assert.Empty(t, entries)

			cancel()
			<-done
			entries, err = os.ReadDir(dirs.Tmp)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestDownloadChainTomlByInfoHashPreservesSnapshotFiles(t *testing.T) {
	for _, hashMatches := range []bool{true, false} {
		name := "matching_hash"
		if !hashMatches {
			name = "mismatched_hash"
		}
		t.Run(name, func(t *testing.T) {
			snapDir := t.TempDir()
			tmpDir := t.TempDir()
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
			infoHash := (&metainfo.MetaInfo{InfoBytes: infoBytes}).HashInfoBytes()
			if !hashMatches {
				infoHash[0] ^= 0xff
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			done := make(chan struct{})
			var downloadErr error
			go func() {
				defer close(done)
				_, downloadErr = DownloadChainTomlByInfoHash(ctx, client, infoHash, snapDir, tmpDir, nil)
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
			metadataErr := manifest.SetInfoBytes(infoBytes)
			if !hashMatches {
				// The torrent library may reject mismatched metadata before our validation.
				cancel()
			}
			<-done
			require.Error(t, metadataErr)
			if hashMatches {
				require.ErrorContains(t, metadataErr, `chain.toml torrent names "erigondb.toml"`)
				require.ErrorContains(t, downloadErr, `chain.toml torrent names "erigondb.toml"`)
			}

			got, err := os.ReadFile(settingsPath)
			require.NoError(t, err)
			assert.Equal(t, settings, got)
			assert.Empty(t, client.Torrents())
			entries, err := os.ReadDir(snapDir)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			assert.Equal(t, "erigondb.toml", entries[0].Name())
			entries, err = os.ReadDir(tmpDir)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestDownloadChainTomlByInfoHashKeepsExistingTorrent(t *testing.T) {
	snapDir := t.TempDir()
	tmpDir := t.TempDir()
	client, err := torrent.NewClient(newTestClientConfig(t, snapDir))
	require.NoError(t, err)
	defer client.Close()
	infoHash := metainfo.Hash{1}
	existing, _ := client.AddTorrentInfoHash(infoHash)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = DownloadChainTomlByInfoHash(ctx, client, infoHash, snapDir, tmpDir, nil)
	require.EqualError(t, err, "requested torrent already exists and is incomplete")
	got, ok := client.Torrent(infoHash)
	require.True(t, ok, "the existing torrent must remain registered")
	assert.Same(t, existing, got)
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestDownloadChainTomlByInfoHashUsesSeededManifest(t *testing.T) {
	data := BuildTomlFromMap(map[string]string{"snapshot.seg": "abc123"})
	for _, tc := range []struct {
		name        string
		replacement []byte
		err         string
	}{
		{name: "unchanged"},
		{name: "rewritten", replacement: BuildTomlFromMap(map[string]string{"snapshot.seg": "def456"}), err: "chain.toml does not match torrent"},
		{name: "shorter", replacement: []byte{}, err: "chain.toml does not match torrent"},
		{name: "longer", replacement: BuildTomlFromMap(map[string]string{"snapshot.seg": "abc123", "extra.seg": "def456"}), err: "chain.toml does not match torrent"},
		{name: "oversized", replacement: make([]byte, maxChainTomlSize+1), err: "chain.toml exceeds size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapDir := t.TempDir()
			tmpDir := t.TempDir()
			require.NoError(t, SaveChainToml(snapDir, data))
			client, err := torrent.NewClient(newTestClientConfig(t, snapDir))
			require.NoError(t, err)
			defer client.Close()
			mi := buildTestMetainfo(t, snapDir, ChainTomlFileName)
			existing, err := client.AddTorrent(&mi)
			require.NoError(t, err)
			require.True(t, existing.Complete().Bool())
			if tc.replacement != nil {
				require.NoError(t, SaveChainToml(snapDir, tc.replacement))
			}

			got, err := DownloadChainTomlByInfoHash(t.Context(), client, mi.HashInfoBytes(), snapDir, tmpDir, nil)
			if tc.replacement == nil {
				require.NoError(t, err)
				assert.Equal(t, data, got)
			} else {
				require.ErrorContains(t, err, tc.err)
				assert.Nil(t, got)
				onDisk, readErr := os.ReadFile(ChainTomlPath(snapDir))
				require.NoError(t, readErr)
				assert.Equal(t, tc.replacement, onDisk)
			}
			registered, ok := client.Torrent(mi.HashInfoBytes())
			require.True(t, ok)
			assert.Same(t, existing, registered)
			entries, err := os.ReadDir(tmpDir)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestDownloadChainTomlByInfoHashRejectsZeroPieceLength(t *testing.T) {
	snapDir := t.TempDir()
	tmpDir := t.TempDir()
	client, err := torrent.NewClient(newTestClientConfig(t, snapDir))
	require.NoError(t, err)
	defer client.Close()
	infoBytes, err := bencode.Marshal(metainfo.Info{Name: ChainTomlFileName, PieceLength: 0})
	require.NoError(t, err)
	mi := metainfo.MetaInfo{InfoBytes: infoBytes}
	existing, err := client.AddTorrent(&mi)
	require.NoError(t, err)
	require.True(t, existing.Complete().Bool())

	_, err = DownloadChainTomlByInfoHash(t.Context(), client, mi.HashInfoBytes(), snapDir, tmpDir, nil)
	require.EqualError(t, err, "invalid chain.toml torrent piece length: 0")
}

func TestDownloadChainTomlByInfoHashLimitsMetadataLength(t *testing.T) {
	for _, length := range []int64{maxChainTomlSize - 1, maxChainTomlSize, maxChainTomlSize + 1} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			snapDir, tmpDir := t.TempDir(), t.TempDir()
			client, err := torrent.NewClient(newTestClientConfig(t, snapDir))
			require.NoError(t, err)
			defer client.Close()
			infoBytes, err := bencode.Marshal(metainfo.Info{
				Name: ChainTomlFileName, Length: length, PieceLength: length, Pieces: make([]byte, 20),
			})
			require.NoError(t, err)
			infoHash := (&metainfo.MetaInfo{InfoBytes: infoBytes}).HashInfoBytes()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			done := make(chan struct{})
			var downloadErr error
			go func() {
				defer close(done)
				_, downloadErr = DownloadChainTomlByInfoHash(ctx, client, infoHash, snapDir, tmpDir, nil)
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
			metadataErr := manifest.SetInfoBytes(infoBytes)
			if length <= maxChainTomlSize || metadataErr == nil {
				cancel()
			}
			<-done
			if length <= maxChainTomlSize {
				require.NoError(t, metadataErr)
			} else {
				require.ErrorContains(t, metadataErr, "chain.toml exceeds size limit")
				require.ErrorContains(t, downloadErr, "chain.toml exceeds size limit")
				assert.Nil(t, manifest.Info(), "oversized metadata must be rejected before storage opens")
			}
			assert.Empty(t, client.Torrents())
			entries, err := os.ReadDir(tmpDir)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestChainTomlStorageRejectsInvalidMetadataBeforeOpeningFiles(t *testing.T) {
	for _, tc := range []struct {
		name string
		info metainfo.Info
		err  string
	}{
		{name: "wrong_name", info: metainfo.Info{Name: "erigondb.toml"}, err: "torrent names"},
		{name: "utf8_name", info: metainfo.Info{NameUtf8: "erigondb.toml"}, err: "torrent names"},
		{name: "multiple_files", info: metainfo.Info{Files: []metainfo.FileInfo{{Path: []string{"part"}}}}, err: "multi-file"},
		{name: "v2_length", info: metainfo.Info{MetaVersion: 2, FileTree: metainfo.FileTree{
			File: metainfo.FileTreeFile{Length: maxChainTomlSize + 1, PiecesRoot: string(make([]byte, 32))},
		}}, err: "expected v1"},
		{name: "negative_length", info: metainfo.Info{Length: -1}, err: "invalid chain.toml torrent length"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.info.Name == "" {
				tc.info.Name = ChainTomlFileName
			}
			tc.info.PieceLength = 256 * 1024
			downloadDir := t.TempDir()
			store := chainTomlStorage{
				ClientImplCloser: storage.NewFileOpts(storage.NewFileClientOpts{ClientBaseDir: downloadDir}),
				rejected:         make(chan error, 1),
			}
			defer store.Close()
			torrentStore, err := store.OpenTorrent(t.Context(), &tc.info, metainfo.Hash{})
			if err == nil {
				require.NoError(t, torrentStore.Close())
			}
			assert.ErrorContains(t, err, tc.err)
			entries, err := os.ReadDir(downloadDir)
			require.NoError(t, err)
			assert.Empty(t, entries, "rejected metadata must not create files")
		})
	}
}

func TestReadChainTomlLimitsSize(t *testing.T) {
	for _, size := range []int{0, maxChainTomlSize - 1, maxChainTomlSize, maxChainTomlSize + 2} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			r := bytes.NewReader(make([]byte, size))
			data, err := readChainToml(r)
			if size <= maxChainTomlSize {
				require.NoError(t, err)
				assert.Len(t, data, size)
			} else {
				assert.ErrorContains(t, err, "chain.toml exceeds size limit")
				assert.Zero(t, len(data))
				assert.Equal(t, 1, r.Len(), "read only one byte beyond the limit to detect excess data")
			}
		})
	}
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
