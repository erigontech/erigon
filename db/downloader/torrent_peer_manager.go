package downloader

import (
	"context"
	"maps"
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

	mu           sync.Mutex
	knownPeers   map[enode.ID]btPeerInfo
	torrentPeers map[*torrent.Torrent]map[enode.ID]string
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
		torrentPeers: make(map[*torrent.Torrent]map[enode.ID]string),
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

		id := node.ID()
		pi := torrent.PeerInfo{
			Addr:    ipPortAddr{IP: ip, Port: int(bt)},
			Trusted: true,
		}
		currentPeers[id] = btPeerInfo{addr: pi}
	}

	torrents := m.client.Torrents()
	activeTorrents := make(map[*torrent.Torrent]struct{}, len(torrents))
	for _, t := range torrents {
		activeTorrents[t] = struct{}{}
	}

	m.mu.Lock()
	for id := range m.knownPeers {
		if _, exists := currentPeers[id]; !exists {
			delete(m.knownPeers, id)
			for _, peers := range m.torrentPeers {
				delete(peers, id)
			}
			m.logger.Debug("[bt-peers] peer left DevP2P, removed from tracking",
				"node", id.TerminalString())
		}
	}
	maps.Copy(m.knownPeers, currentPeers)
	for t := range m.torrentPeers {
		if _, exists := activeTorrents[t]; !exists {
			delete(m.torrentPeers, t)
		}
	}
	var pendingAdds []torrentPeerAdd
	for _, t := range torrents {
		processedPeers := m.torrentPeers[t]
		if processedPeers == nil {
			processedPeers = make(map[enode.ID]string)
			m.torrentPeers[t] = processedPeers
		}
		for id, peer := range m.knownPeers {
			processed, exists := processedPeers[id]
			if exists && processed == peer.addr.Addr.String() {
				continue
			}
			processedPeers[id] = peer.addr.Addr.String()
			pendingAdds = append(pendingAdds, torrentPeerAdd{torrent: t, id: id, peer: peer.addr})
		}
	}
	m.mu.Unlock()

	for _, add := range pendingAdds {
		add.torrent.AddPeers([]torrent.PeerInfo{add.peer})
		m.logger.Debug("[bt-peers] added torrent peer from ENR",
			"node", add.id.TerminalString(),
			"addr", add.peer.Addr)
	}
}

type torrentPeerAdd struct {
	torrent *torrent.Torrent
	id      enode.ID
	peer    torrent.PeerInfo
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
