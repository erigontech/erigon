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

// TestTorrentPeerManager_LateSetSelfIPUpgradesAddrs pins Bug D's fix: a
// peer discovered before SetSelfIP arrives only carries its ENR-IP in
// addrs (no 127.0.0.1 loopback fallback). On a subsequent sync after
// SetSelfIP records a matching self-IP, the peer's addrs must be
// recomputed and grow the loopback candidate — otherwise the same-host
// case (two erigon instances on one box, no hairpin NAT) never
// recovers.
func TestTorrentPeerManager_LateSetSelfIPUpgradesAddrs(t *testing.T) {
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.ListenPort = 0
	cfg.NoDHT = true
	client, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	peerIP := []byte{198, 51, 100, 7} // a non-loopback IP
	key, _ := crypto.GenerateKey()
	peer := makeNodeWithBT(t, key, peerIP,
		&enr.ChainToml{KnownBlocks: 100}, enr.BT(42069))
	source := &mockNodeSource{nodes: []*enode.Node{peer}}

	m := NewTorrentPeerManager(client, func() NodeSource { return source }, log.New())

	// Discovery sync runs BEFORE SetSelfIP — addrs has just the peer IP.
	m.sync()
	assert.Equal(t, 1, m.PeerCount())
	m.mu.Lock()
	got := m.knownPeers[peer.ID()].addrs
	m.mu.Unlock()
	assert.Len(t, got, 1, "pre-SetSelfIP: addrs should contain only the ENR-IP")

	// SetSelfIP arrives late with a matching self-IP. Next sync must
	// recompute addrs and add the 127.0.0.1 fallback.
	m.SetSelfIP(peerIP)
	m.sync()
	m.mu.Lock()
	got = m.knownPeers[peer.ID()].addrs
	m.mu.Unlock()
	assert.Len(t, got, 2, "post-SetSelfIP: addrs should contain ENR-IP plus 127.0.0.1 fallback")
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
