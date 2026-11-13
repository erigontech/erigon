package downloader

import (
	"context"
	"crypto/ecdsa"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// TestBTPeerDiscovery_AddPeersFromENR downloads a manifest from an ENR-advertised peer
// without DHT or trackers and preserves the local manifest.
func TestBTPeerDiscovery_AddPeersFromENR(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	// Create temp dirs for seeder and leecher.
	seederDir := t.TempDir()
	leecherDir := t.TempDir()
	tmpDir := t.TempDir()

	// Create a test file to share.
	testData := BuildTomlFromMap(map[string]string{"snapshot.seg": "abc123"})
	testFile := ChainTomlFileName
	require.NoError(t, os.WriteFile(filepath.Join(seederDir, testFile), testData, 0o644))
	localData := []byte("existing local manifest")
	require.NoError(t, os.WriteFile(ChainTomlPath(leecherDir), localData, 0o644))

	// Start seeder torrent client.
	seederCfg := newTestClientConfig(t, seederDir)
	seederCfg.Seed = true

	seeder, err := torrent.NewClient(seederCfg)
	require.NoError(t, err)
	defer seeder.Close()

	// Build the torrent metainfo for the test file.
	mi := buildTestMetainfo(t, seederDir, testFile)
	infoHash := mi.HashInfoBytes()

	// Add the torrent to the seeder.
	seederTorrent, err := seeder.AddTorrent(&mi)
	require.NoError(t, err)
	require.True(t, seederTorrent.Complete().Bool(), "seeder should have the complete file")

	// Build a fake ENR for the seeder with chain-toml + bt entries.
	seederKey, _ := crypto.GenerateKey()
	seederNode := makeNodeWithBT(t, seederKey, net.IPv4(127, 0, 0, 1),
		&enr.ChainToml{KnownBlocks: 100, InfoHash: [20]byte(infoHash)},
		enr.BT(uint16(seeder.LocalPort())))

	// Simulate discovery: leecher finds the seeder's ENR.
	ns := &mockNodeSource{nodes: []*enode.Node{seederNode}}
	best := DiscoverChainToml(ns)
	require.NotNil(t, best)
	assert.Equal(t, [20]byte(infoHash), best.ChainToml.InfoHash)

	// Start leecher torrent client.
	leecherCfg := newTestClientConfig(t, leecherDir)
	leecherCfg.Slogger = slog.Default()
	store, leecher, err := newTorrentClient(leecherDir, leecherCfg)
	require.NoError(t, err)
	defer store.Close()
	defer leecher.Close()
	d := &Downloader{
		cfg:           &downloadercfg.Cfg{Dirs: datadir.Dirs{Snap: leecherDir, Tmp: tmpDir}},
		torrentClient: leecher,
	}

	downloaded, err := d.downloadChainToml(ctx, best)
	require.NoError(t, err)
	assert.Equal(t, testData, downloaded)
	stats := seederTorrent.Stats()
	uploaded := stats.BytesWrittenData.Int64()
	require.Positive(t, uploaded)
	for range 2 {
		downloaded, err = d.downloadChainToml(ctx, best)
		require.NoError(t, err)
		assert.Equal(t, testData, downloaded)
		stats = seederTorrent.Stats()
		assert.Equal(t, uploaded, stats.BytesWrittenData.Int64(), "an unchanged manifest must not be downloaded again")
	}

	seederTorrent.Drop()
	updatedData := BuildTomlFromMap(map[string]string{"snapshot.seg": "def456"})
	require.NoError(t, SaveChainToml(seederDir, updatedData))
	updatedInfo := buildTestMetainfo(t, seederDir, testFile)
	updatedTorrent, err := seeder.AddTorrent(&updatedInfo)
	require.NoError(t, err)
	require.True(t, updatedTorrent.Complete().Bool())
	updatedEntry := enr.ChainToml{KnownBlocks: 101, InfoHash: updatedInfo.HashInfoBytes()}
	updatedPeer := &ChainTomlPeer{
		ChainToml: updatedEntry,
		Node:      makeNodeWithBT(t, seederKey, net.IPv4(127, 0, 0, 1), &updatedEntry, enr.BT(uint16(seeder.LocalPort()))),
	}
	downloaded, err = d.downloadChainToml(ctx, updatedPeer)
	require.NoError(t, err)
	assert.Equal(t, updatedData, downloaded, "a changed hash must fetch the new manifest")

	expiredPeerCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	_, err = d.downloadChainToml(expiredPeerCtx, best)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the previous cache entry must be evicted")
	stats = updatedTorrent.Stats()
	uploaded = stats.BytesWrittenData.Int64()
	downloaded, err = d.downloadChainToml(ctx, updatedPeer)
	require.NoError(t, err)
	assert.Equal(t, updatedData, downloaded)
	stats = updatedTorrent.Stats()
	assert.Equal(t, uploaded, stats.BytesWrittenData.Int64(), "a failed download must retain the successful cache entry")

	got, err := os.ReadFile(ChainTomlPath(leecherDir))
	require.NoError(t, err)
	assert.Equal(t, localData, got)
	assert.Empty(t, leecher.Torrents())
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// A peer can advertise the same chain.toml the node seeds itself; reading it must not drop the seeded torrent.
func TestDownloadChainTomlKeepsSeededTorrent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, SaveChainToml(dir, []byte("local manifest")))
	cfg := newTestClientConfig(t, dir)
	cfg.Seed = true
	client, err := torrent.NewClient(cfg)
	require.NoError(t, err)
	defer client.Close()
	mi := buildTestMetainfo(t, dir, ChainTomlFileName)
	_, err = client.AddTorrent(&mi)
	require.NoError(t, err)

	_, err = DownloadChainTomlByInfoHash(t.Context(), client, mi.HashInfoBytes(), dir, t.TempDir(), nil)
	require.NoError(t, err)
	_, ok := client.Torrent(mi.HashInfoBytes())
	require.True(t, ok)
}

// TestBTPeerDiscovery_NoBTPort verifies that when a peer doesn't advertise
// a BT port, addTorrentPeerFromENR is a no-op (no panic, no peers added).
func TestBTPeerDiscovery_NoBTPort(t *testing.T) {
	key, _ := crypto.GenerateKey()
	ct := &enr.ChainToml{KnownBlocks: 50, InfoHash: [20]byte{1}}
	node := makeTestNode(t, key, ct)

	peer := &ChainTomlPeer{ChainToml: *ct, Node: node}

	client, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	require.NoError(t, err)
	defer client.Close()

	torr, _ := client.AddTorrentInfoHash([20]byte{1})
	defer torr.Drop()

	// Should not panic and should not add any peers.
	addTorrentPeerFromENR(torr, peer)
	assert.Zero(t, torr.Stats().TotalPeers)
}

// newTestClientConfig returns a client that talks to nobody it was not handed
// directly. DisableUTP also keeps the listener off UDP, which the OS may refuse
// on the dynamic port the TCP listener already took.
func newTestClientConfig(t *testing.T, dataDir string) *torrent.ClientConfig {
	t.Helper()
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dataDir
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.DisableUTP = true
	return cfg
}

func buildTestMetainfo(t *testing.T, dir, filename string) metainfo.MetaInfo {
	t.Helper()
	info := metainfo.Info{
		PieceLength: 256 * 1024,
	}
	err := info.BuildFromFilePath(filepath.Join(dir, filename))
	require.NoError(t, err)

	var mi metainfo.MetaInfo
	mi.InfoBytes, err = bencode.Marshal(info)
	require.NoError(t, err)
	return mi
}

func makeNodeWithBT(t *testing.T, key *ecdsa.PrivateKey, ip net.IP, ct *enr.ChainToml, bt enr.BT) *enode.Node {
	t.Helper()
	var r enr.Record
	if ct != nil {
		r.Set(*ct)
	}
	r.Set(bt)
	r.Set(enr.IP(ip))
	require.NoError(t, enode.SignV4(&r, key))
	n, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)
	return n
}
