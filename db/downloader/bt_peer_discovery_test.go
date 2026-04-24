package downloader

import (
	"context"
	"crypto/ecdsa"
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
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// TestBTPeerDiscovery_AddPeersFromENR verifies that a leecher can discover
// and download from a seeder using only the BT port from the seeder's ENR.
// This is the core mechanism for decentralized snapshot distribution:
// no DHT, no tracker — just direct peer injection from ENR records.
func TestBTPeerDiscovery_AddPeersFromENR(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Create temp dirs for seeder and leecher.
	seederDir := t.TempDir()
	leecherDir := t.TempDir()

	// Create a test file to share.
	testData := []byte("hello from the seeder — this is a snapshot manifest")
	testFile := "test-chain.toml"
	require.NoError(t, os.WriteFile(filepath.Join(seederDir, testFile), testData, 0o644))

	// Start seeder torrent client.
	seederCfg := torrent.NewDefaultClientConfig()
	seederCfg.DataDir = seederDir
	seederCfg.ListenPort = 0 // OS-assigned port
	seederCfg.NoDHT = true
	seederCfg.DisableTrackers = true
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
	leecherCfg := torrent.NewDefaultClientConfig()
	leecherCfg.DataDir = leecherDir
	leecherCfg.ListenPort = 0
	leecherCfg.NoDHT = true
	leecherCfg.DisableTrackers = true

	leecher, err := torrent.NewClient(leecherCfg)
	require.NoError(t, err)
	defer leecher.Close()

	// Add the torrent by info-hash (leecher doesn't have the metainfo yet).
	leecherTorrent, _ := leecher.AddTorrentInfoHash(infoHash)

	// Use the ENR BT port to add the seeder as a direct peer.
	addTorrentPeerFromENR(leecherTorrent, best)

	// Wait for metainfo exchange.
	select {
	case <-leecherTorrent.GotInfo():
	case <-ctx.Done():
		t.Fatal("timed out waiting for torrent info from seeder")
	}

	// Download the file.
	leecherTorrent.DownloadAll()
	select {
	case <-leecherTorrent.Complete().On():
	case <-ctx.Done():
		t.Fatal("timed out downloading from seeder")
	}

	// Verify the downloaded content matches.
	downloaded, err := os.ReadFile(filepath.Join(leecherDir, testFile))
	require.NoError(t, err)
	assert.Equal(t, testData, downloaded)
}

// TestBTPeerDiscovery_NoBTPort verifies that when a peer doesn't advertise
// a BT port, addTorrentPeerFromENR is a no-op (no panic, no peers added).
func TestBTPeerDiscovery_NoBTPort(t *testing.T) {
	key, _ := crypto.GenerateKey()
	ct := &enr.ChainToml{KnownBlocks: 50, InfoHash: [20]byte{1}}
	node := makeTestNode(t, key, ct)

	peer := &ChainTomlPeer{ChainToml: *ct, Node: node}

	// Create a minimal torrent client for the test.
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.ListenPort = 0
	cfg.NoDHT = true
	client, err := torrent.NewClient(cfg)
	require.NoError(t, err)
	defer client.Close()

	torr, _ := client.AddTorrentInfoHash([20]byte{1})
	defer torr.Drop()

	// Should not panic and should not add any peers.
	addTorrentPeerFromENR(torr, peer)
	assert.Zero(t, torr.Stats().TotalPeers)
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
