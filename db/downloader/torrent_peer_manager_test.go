package downloader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anacrolix/torrent"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func newTorrentPeerManagerTestClient(t *testing.T) *torrent.Client {
	t.Helper()
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DialForPeerConns = false
	client, err := torrent.NewClient(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

func addTorrentPeerManagerTestTorrent(t *testing.T, client *torrent.Client, hash byte) *torrent.Torrent {
	t.Helper()
	var infoHash [20]byte
	infoHash[0] = hash
	torrent, _ := client.AddTorrentInfoHash(infoHash)
	return torrent
}

func TestTorrentPeerManager_AddAndRemove(t *testing.T) {
	client, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Create two peers: one with BT port, one without.
	key1, _ := crypto.GenerateKey()
	key2, _ := crypto.GenerateKey()

	peerWithBT := makeNodeWithBT(t, key1, []byte{127, 0, 0, 1},
		&enr.ChainToml{KnownBlocks: 100}, enr.BT(42069))
	peerWithoutBT := makeTestNode(t, key2, &enr.ChainToml{KnownBlocks: 50})

	source := &mockNodeSource{nodes: []*enode.Node{peerWithBT, peerWithoutBT}}

	logger := log.New()

	m := NewTorrentPeerManager(client, func() NodeSource { return source }, logger)

	// First sync: should add only the peer with BT port.
	m.sync()
	assert.Equal(t, 1, m.PeerCount())

	// Second sync with same peers: no change.
	m.sync()
	assert.Equal(t, 1, m.PeerCount())

	// Remove the BT peer from the source.
	source.nodes = []*enode.Node{peerWithoutBT}
	m.sync()
	assert.Equal(t, 0, m.PeerCount(), "peer should be removed when it leaves DevP2P")

	// Add it back.
	source.nodes = []*enode.Node{peerWithBT, peerWithoutBT}
	m.sync()
	assert.Equal(t, 1, m.PeerCount(), "peer should be re-added when it rejoins DevP2P")
}

func TestTorrentPeerManager_NilNodeSource(t *testing.T) {
	client, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	logger := log.New()
	m := NewTorrentPeerManager(client, func() NodeSource { return nil }, logger)

	// Should not panic when node source returns nil.
	m.sync()
	assert.Equal(t, 0, m.PeerCount())
}

func TestTorrentPeerManager_AddsKnownPeerToNewTorrent(t *testing.T) {
	client := newTorrentPeerManagerTestClient(t)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	peer := makeNodeWithBT(t, key, []byte{127, 0, 0, 1}, &enr.ChainToml{KnownBlocks: 100}, enr.BT(42069))
	source := &mockNodeSource{nodes: []*enode.Node{peer}}
	m := NewTorrentPeerManager(client, func() NodeSource { return source }, log.New())

	firstTorrent := addTorrentPeerManagerTestTorrent(t, client, 1)
	m.sync()
	assert.Equal(t, []string{"127.0.0.1:42069"}, torrentPeerAddresses(firstTorrent))

	newTorrent := addTorrentPeerManagerTestTorrent(t, client, 2)
	m.sync()
	assert.Equal(t, []string{"127.0.0.1:42069"}, torrentPeerAddresses(newTorrent))
}

func TestTorrentPeerManager_RefreshesChangedPeerEndpoint(t *testing.T) {
	client := newTorrentPeerManagerTestClient(t)
	tor := addTorrentPeerManagerTestTorrent(t, client, 3)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	peer := makeNodeWithBT(t, key, []byte{127, 0, 0, 1}, &enr.ChainToml{KnownBlocks: 100}, enr.BT(42069))
	source := &mockNodeSource{nodes: []*enode.Node{peer}}
	m := NewTorrentPeerManager(client, func() NodeSource { return source }, log.New())

	m.sync()
	assert.Equal(t, []string{"127.0.0.1:42069"}, torrentPeerAddresses(tor))

	updatedPeer := makeNodeWithBT(t, key, []byte{127, 0, 0, 2}, &enr.ChainToml{KnownBlocks: 100}, enr.BT(42070))
	source.nodes = []*enode.Node{updatedPeer}
	m.sync()
	assert.Contains(t, torrentPeerAddresses(tor), "127.0.0.2:42070")
}

func torrentPeerAddresses(tor *torrent.Torrent) []string {
	peers := tor.KnownSwarm()
	addresses := make([]string, 0, len(peers))
	for _, peer := range peers {
		addresses = append(addresses, peer.Addr.String())
	}
	return addresses
}
