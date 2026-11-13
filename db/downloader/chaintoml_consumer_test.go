package downloader

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/snapcfg"
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

func TestChainTomlDiscoveryPreservesConcurrentSeed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		discover func(*Downloader, context.Context, string, NodeSource)
	}{
		{name: "acquire", discover: (*Downloader).acquireChainToml},
		{name: "verify", discover: (*Downloader).verifyChainToml},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.Dirs{Snap: t.TempDir(), Tmp: t.TempDir()}
			require.NoError(t, SaveChainToml(dirs.Snap, BuildTomlFromMap(map[string]string{"snapshot.seg": "abc123"})))
			torrentFS := &AtomicTorrentFS{dir: dirs.Snap}
			infoHash, err := BuildChainTomlTorrent(dirs.Snap, torrentFS)
			require.NoError(t, err)
			cfg := newTestClientConfig(t, dirs.Snap)
			cfg.Slogger = slog.Default()
			store, client, err := newTorrentClient(dirs.Snap, cfg)
			require.NoError(t, err)
			defer store.Close()
			defer client.Close()
			d := &Downloader{
				ctx:           t.Context(),
				cfg:           &downloadercfg.Cfg{Dirs: dirs},
				torrentClient: client,
				torrentFS:     torrentFS,
				logger:        log.New(),
			}
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

			seedStarted := make(chan struct{})
			seedDone := make(chan struct{})
			var seedErr error
			go func() {
				defer close(seedDone)
				close(seedStarted)
				seedErr = d.seedChainTomlTorrent()
			}()
			defer func() {
				cancel()
				<-seedDone
			}()
			<-seedStarted
			select {
			case <-seedDone:
				t.Fatalf("seeding returned before discovery cleanup: %v", seedErr)
			case <-time.After(100 * time.Millisecond):
			}
			cancel()
			<-seedDone
			require.NoError(t, seedErr)
			seed, registered := client.Torrent(infoHash)
			require.True(t, registered)
			assert.True(t, seed.Complete().Bool(), "the local manifest must be available for seeding")

			<-done
			registeredSeed, registered := client.Torrent(infoHash)
			require.True(t, registered, "download cleanup must preserve the local seed")
			assert.Same(t, seed, registeredSeed)
			entries, err := os.ReadDir(dirs.Tmp)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestChainTomlDiscoveryDoesNotBlockDownloadBatches(t *testing.T) {
	for _, tc := range []struct {
		name     string
		discover func(*Downloader, context.Context, string, NodeSource)
	}{
		{name: "acquire", discover: (*Downloader).acquireChainToml},
		{name: "verify", discover: (*Downloader).verifyChainToml},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := newDownloaderTest(t)
			d := test.downloader
			d.EnableP2PManifest()
			require.NoError(t, os.WriteFile(filepath.Join(test.dirs.Snap, "snapshot.seg"), []byte("snapshot data"), 0o644))
			require.NoError(t, d.AddNewSeedableFile(t.Context(), "snapshot.seg"))
			snapshotSpec, err := d.torrentFS.LoadByName("snapshot.seg")
			require.NoError(t, err)
			require.NoError(t, PublishChainToml(d.snapDir(), d.torrentFS, "", nil))
			manifestSpec, err := d.torrentFS.LoadByName(ChainTomlFileName)
			require.NoError(t, err)
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			node := makeTestNode(t, key, &enr.ChainToml{KnownBlocks: 1, InfoHash: manifestSpec.InfoHash})
			ns := &mockNodeSource{nodes: []*enode.Node{node}}
			ctx, cancel := context.WithCancel(t.Context())
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
				_, ok := d.torrentClient.Torrent(manifestSpec.InfoHash)
				return ok
			}, 5*time.Second, time.Millisecond)

			batchesDone := make(chan struct{})
			var batchErr error
			go func() {
				defer close(batchesDone)
				items := []preverifiedSnapshot{{InfoHash: snapshotSpec.InfoHash, Name: "snapshot.seg"}}
				for range 2 {
					if batchErr = d.DownloadSnapshots(t.Context(), items, "completed batch"); batchErr != nil {
						return
					}
				}
			}()
			defer func() {
				cancel()
				<-batchesDone
			}()
			select {
			case <-batchesDone:
				require.NoError(t, batchErr)
			case <-time.After(5 * time.Second):
				t.Fatal("completed batches must not wait for peer manifest downloads")
			}
			pending, registered := d.torrentClient.Torrent(manifestSpec.InfoHash)
			require.True(t, registered)
			require.False(t, pending.Complete().Bool(), "discovery must still be waiting for the peer")

			cancel()
			<-done
			require.Eventually(t, func() bool {
				seed, registered := d.torrentClient.Torrent(manifestSpec.InfoHash)
				return registered && seed.Complete().Bool()
			}, 5*time.Second, time.Millisecond, "download cleanup must preserve the local seed")
			entries, err := os.ReadDir(test.dirs.Tmp)
			require.NoError(t, err)
			assert.Empty(t, entries)
		})
	}
}

func TestChainTomlSeedStopsAfterClose(t *testing.T) {
	d := newDownloaderTest(t).downloader
	d.chainTomlTorrentLock.Lock()
	unlock := sync.OnceFunc(d.chainTomlTorrentLock.Unlock)
	defer unlock()

	require.NoError(t, d.PublishLocalChainToml())
	d.lockingClose()
	unlock()
	d.wg.Wait()
	require.Empty(t, d.torrentClient.Torrents(), "pending seeding must not add torrents after shutdown")
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

func TestDownloadChainTomlByInfoHashRejectsMismatchedMetadata(t *testing.T) {
	snapDir := t.TempDir()
	client, err := torrent.NewClient(newTestClientConfig(t, snapDir))
	require.NoError(t, err)
	defer client.Close()
	infoBytes, err := bencode.Marshal(metainfo.Info{Name: ChainTomlFileName, PieceLength: 256 * 1024})
	require.NoError(t, err)
	mi := metainfo.MetaInfo{InfoBytes: infoBytes}
	advertisedHash := mi.HashInfoBytes()
	advertisedHash[0] ^= 0xff
	manifest, _ := client.AddTorrentOpt(torrent.AddTorrentOpts{InfoHash: advertisedHash, InfoBytes: infoBytes})
	require.Nil(t, manifest.Info(), "the torrent client must reject info bytes that do not match the info hash")

	data, err := DownloadChainTomlByInfoHash(t.Context(), client, advertisedHash, snapDir, t.TempDir(), nil)
	require.Error(t, err)
	assert.Nil(t, data)
}

func TestChainTomlCacheRejectsInvalidToml(t *testing.T) {
	test := newDownloaderTest(t)
	d := test.downloader
	require.NoError(t, SaveChainToml(test.dirs.Snap, []byte("invalid TOML")))
	mi := buildTestMetainfo(t, test.dirs.Snap, ChainTomlFileName)
	manifest, err := d.torrentClient.AddTorrent(&mi)
	require.NoError(t, err)
	require.True(t, manifest.Complete().Bool())
	peer := &ChainTomlPeer{ChainToml: enr.ChainToml{InfoHash: mi.HashInfoBytes()}}

	data, err := d.downloadChainToml(t.Context(), peer)
	require.ErrorContains(t, err, "parsing chain.toml")
	assert.Nil(t, data)

	require.NoError(t, SaveChainToml(test.dirs.Snap, []byte("different data")))
	_, err = d.downloadChainToml(t.Context(), peer)
	require.ErrorContains(t, err, "chain.toml does not match torrent", "a failed parse must not populate the cache")
}

func TestAcquireChainTomlReusesCacheAfterLocalChanges(t *testing.T) {
	const network = "sepolia"
	previous := append([]byte{}, snapcfg.GetToml(network)...)
	previousCfg, _ := snapcfg.KnownCfg(network)
	t.Cleanup(func() { require.NoError(t, snapcfg.SetToml(network, previous, previousCfg.Local)) })
	require.NoError(t, snapcfg.SetToml(network, []byte{}, false))
	test := newDownloaderTest(t)
	d := test.downloader
	data := BuildTomlFromMap(map[string]string{"domain/v1-accounts.0-64.kv": strings.Repeat("a", 40)})
	require.NoError(t, SaveChainToml(test.dirs.Snap, data))
	mi := buildTestMetainfo(t, test.dirs.Snap, ChainTomlFileName)
	manifest, err := d.torrentClient.AddTorrent(&mi)
	require.NoError(t, err)
	require.True(t, manifest.Complete().Bool())
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	node := makeTestNode(t, key, &enr.ChainToml{KnownBlocks: 1, InfoHash: mi.HashInfoBytes()})
	ns := &mockNodeSource{nodes: []*enode.Node{node}}

	d.acquireChainToml(t.Context(), network, ns)
	require.Equal(t, data, snapcfg.GetToml(network))
	manifest.Drop()
	require.NoError(t, dir.RemoveFile(ChainTomlPath(test.dirs.Snap)))
	require.NoError(t, snapcfg.SetToml(network, []byte{}, false))

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	d.acquireChainToml(ctx, network, ns)
	require.Equal(t, data, snapcfg.GetToml(network), "cached peer entries must be applied to the changed local registry")
	got, err := LoadChainToml(test.dirs.Snap)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestDownloadChainTomlByInfoHashUsesSeededManifest(t *testing.T) {
	data := BuildTomlFromMap(map[string]string{"snapshot.seg": "abc123"})
	for _, tc := range []struct {
		name        string
		replacement []byte
		err         string
	}{
		{name: "unchanged"},
		{
			name:        "rewritten",
			replacement: BuildTomlFromMap(map[string]string{"snapshot.seg": "def456"}),
			err:         "chain.toml does not match torrent",
		},
		{
			name:        "shorter",
			replacement: []byte{},
			err:         "chain.toml does not match torrent",
		},
		{
			name:        "longer",
			replacement: append(bytes.Clone(data), '\n'),
			err:         "chain.toml does not match torrent length",
		},
		{
			name:        "oversized",
			replacement: make([]byte, maxChainTomlSize+1),
			err:         "chain.toml does not match torrent length",
		},
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

func TestValidateChainTomlInfoAcceptsSizeLimit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		length int64
	}{
		{name: "below_limit", length: maxChainTomlSize - 1},
		{name: "at_limit", length: maxChainTomlSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := metainfo.Info{
				Name:        ChainTomlFileName,
				Length:      tc.length,
				PieceLength: tc.length,
				Pieces:      make([]byte, 20),
			}
			require.NoError(t, validateChainTomlInfo(&info))
		})
	}
}

func TestChainTomlStorageRejectsInvalidMetadataBeforeOpeningFiles(t *testing.T) {
	for _, tc := range []struct {
		name string
		info metainfo.Info
		err  string
	}{
		{
			name: "wrong_name",
			info: metainfo.Info{Name: "erigondb.toml"},
			err:  "torrent names",
		},
		{
			name: "utf8_name",
			info: metainfo.Info{NameUtf8: "erigondb.toml"},
			err:  "torrent names",
		},
		{
			name: "multiple_files",
			info: metainfo.Info{Files: []metainfo.FileInfo{{Path: []string{"part"}}}},
			err:  "multi-file",
		},
		{
			name: "v2_length",
			info: metainfo.Info{
				MetaVersion: 2,
				FileTree: metainfo.FileTree{
					File: metainfo.FileTreeFile{
						Length:     maxChainTomlSize + 1,
						PiecesRoot: string(make([]byte, 32)),
					},
				},
			},
			err: "expected v1",
		},
		{
			name: "negative_length",
			info: metainfo.Info{Length: -1},
			err:  "invalid chain.toml torrent length",
		},
		{
			name: "above_limit",
			info: metainfo.Info{
				Length:      maxChainTomlSize + 1,
				PieceLength: maxChainTomlSize + 1,
				Pieces:      make([]byte, 20),
			},
			err: "exceeds size limit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.info.Name == "" {
				tc.info.Name = ChainTomlFileName
			}
			if tc.info.PieceLength == 0 {
				tc.info.PieceLength = 256 * 1024
			}
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
