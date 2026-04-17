package downloader

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anacrolix/torrent"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func TestTorrentPeerManager_AddAndRemove(t *testing.T) {
	// Create a minimal torrent client.
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.ListenPort = 0
	cfg.NoDHT = true
	client, err := torrent.NewClient(cfg)
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
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.ListenPort = 0
	cfg.NoDHT = true
	client, err := torrent.NewClient(cfg)
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
