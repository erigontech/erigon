package downloader

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/anacrolix/torrent"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// TorrentPeerManager synchronizes the torrent client's peer set with the
// DevP2P peer set. It periodically scans known DevP2P peers for "bt" ENR
// entries and adds them as torrent peers. When a DevP2P peer disconnects
// (disappears from the NodeSource), the torrent client handles the connection
// drop naturally — there is no explicit "remove peer" in the torrent API.
type TorrentPeerManager struct {
	client       *torrent.Client
	nodeSourceFn func() NodeSource
	logger       log.Logger

	mu         sync.Mutex
	knownPeers map[enode.ID]btPeerInfo       // peers we've added to the torrent client
	torrents   map[*torrent.Torrent]struct{} // torrents that already have every known peer
}

type btPeerInfo struct {
	addr torrent.PeerInfo
}

func NewTorrentPeerManager(client *torrent.Client, nodeSourceFn func() NodeSource, logger log.Logger) *TorrentPeerManager {
	return &TorrentPeerManager{
		client:       client,
		nodeSourceFn: nodeSourceFn,
		logger:       logger,
		knownPeers:   make(map[enode.ID]btPeerInfo),
	}
}

// Run starts the peer management loop. It should be called in a goroutine.
func (m *TorrentPeerManager) Run(ctx context.Context) {
	// Initial delay to let P2P establish connections.
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		m.sync()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sync scans the current DevP2P peer set and updates the torrent peer set.
func (m *TorrentPeerManager) sync() {
	ns := m.nodeSourceFn()
	if ns == nil {
		return
	}

	currentPeers := make(map[enode.ID]btPeerInfo)

	for _, node := range ns.AllNodes() {
		var bt enr.BT
		if err := node.Record().Load(&bt); err != nil || bt == 0 {
			continue
		}

		ip := node.IP()
		if ip == nil {
			continue
		}

		currentPeers[node.ID()] = btPeerInfo{addr: torrent.PeerInfo{Addr: ipPortAddr{IP: ip, Port: int(bt)}, Trusted: true}}
	}

	// A new or moved peer goes to every torrent; a torrent new since the last sync gets every peer.
	var changed, all []torrent.PeerInfo
	m.mu.Lock()
	for id, p := range currentPeers {
		all = append(all, p.addr)
		if old, ok := m.knownPeers[id]; !ok || old.addr.Addr.String() != p.addr.Addr.String() {
			changed = append(changed, p.addr)
			m.logger.Debug("[bt-peers] added torrent peer from ENR", "node", id.TerminalString(), "addr", p.addr.Addr)
		}
	}
	m.knownPeers = currentPeers
	known := m.torrents
	m.mu.Unlock()

	seen := make(map[*torrent.Torrent]struct{})
	for _, t := range m.client.Torrents() {
		seen[t] = struct{}{}
		if _, ok := known[t]; !ok {
			t.AddPeers(all)
		} else if len(changed) > 0 {
			t.AddPeers(changed)
		}
	}
	m.mu.Lock()
	m.torrents = seen
	m.mu.Unlock()
}

// PeerCount returns the number of tracked torrent peers.
func (m *TorrentPeerManager) PeerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.knownPeers)
}

// ipPortAddr is defined in chaintoml_consumer.go; adding a compile-time
// check to ensure it implements net.Addr (used by torrent.PeerInfo.Addr).
var _ net.Addr = ipPortAddr{}
