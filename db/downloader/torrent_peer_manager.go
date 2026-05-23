package downloader

import (
	"context"
	"fmt"
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
	knownPeers map[enode.ID]btPeerInfo // peers we've added to the torrent client
	selfIP     net.IP                  // our external IP, for same-host loopback fallback
}

type btPeerInfo struct {
	// addrs is the set of dial candidates for a peer — usually one (the
	// ENR-advertised address), but two when the peer is on the same
	// host: ENR-IP + 127.0.0.1 fallback, so a setup without hairpin NAT
	// can still reach the local BT port.
	addrs []torrent.PeerInfo
}

func NewTorrentPeerManager(client *torrent.Client, nodeSourceFn func() NodeSource, logger log.Logger) *TorrentPeerManager {
	return &TorrentPeerManager{
		client:       client,
		nodeSourceFn: nodeSourceFn,
		logger:       logger,
		knownPeers:   make(map[enode.ID]btPeerInfo),
	}
}

// SetSelfIP records this node's externally-advertised IP. When a peer's
// ENR-IP matches it, sync also installs a 127.0.0.1:<bt> dial candidate
// for that peer so a same-host (no-hairpin-NAT) setup can reach it.
// Pass nil to clear.
func (m *TorrentPeerManager) SetSelfIP(ip net.IP) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.selfIP = ip
}

// Run starts the peer management loop. It should be called in a goroutine.
func (m *TorrentPeerManager) Run(ctx context.Context) {
	// Initial delay to let P2P establish connections.
	// Tightened from 10s to 2s — the forced discv5 ping-on-connect
	// makes ENR records (with bt port) available within sub-second
	// of devp2p handshake.
	select {
	case <-ctx.Done():
		return
	case <-time.After(2 * time.Second):
	}

	// 5s ticker (was 30s) — when a new peer joins, we want them added
	// to all torrents fast, especially with sparse fleets where one
	// new peer could be the only source for some files.
	ticker := time.NewTicker(5 * time.Second)
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
//
// On each cycle we (1) record any newly-discovered peers, (2) drop any
// peers that have left the DevP2P set, and (3) re-publish the entire
// known-peer list to ALL currently-tracked torrents. The re-publish step
// matters because torrents are added to the client at different times
// (chain.toml is applied incrementally, post-tip retire keeps producing
// new torrents). A peer discovered at sync N must still get added to a
// torrent that joins the client at sync N+M; tracking only "new peer"
// transitions misses the symmetric "new torrent" case. anacrolix
// dedupes on (peer-id, torrent) pairs, so re-publishing existing peers
// is idempotent — no extra connections, no double-counting.
func (m *TorrentPeerManager) sync() {
	ns := m.nodeSourceFn()
	if ns == nil {
		return
	}

	currentPeers := make(map[enode.ID]struct{})

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
		currentPeers[id] = struct{}{}

		m.mu.Lock()
		_, already := m.knownPeers[id]
		// Always (re-)compute addrs so a late SetSelfIP propagates to peers
		// discovered before it landed — otherwise same-host peers stay stuck
		// without the 127.0.0.1 fallback forever once cached.
		dialIPs := PeerDialIPs(ip, m.selfIP)
		addrs := make([]torrent.PeerInfo, 0, len(dialIPs))
		for _, dialIP := range dialIPs {
			addrs = append(addrs, torrent.PeerInfo{
				Addr:    ipPortAddr{IP: dialIP, Port: int(bt)},
				Trusted: true,
			})
		}
		m.knownPeers[id] = btPeerInfo{addrs: addrs}
		m.mu.Unlock()

		if !already {
			m.logger.Debug("[bt-peers] added torrent peer from ENR",
				"node", id.TerminalString(),
				"addr", fmt.Sprintf("%s:%d", ip, bt),
				"candidates", len(addrs))
		}
	}

	// Remove peers that are no longer in the DevP2P peer set.
	m.mu.Lock()
	for id := range m.knownPeers {
		if _, exists := currentPeers[id]; !exists {
			delete(m.knownPeers, id)
			m.logger.Debug("[bt-peers] peer left DevP2P, removed from tracking",
				"node", id.TerminalString())
		}
	}

	// Re-publish every still-known peer to every current torrent. Picks
	// up torrents added since each peer's first-discovery sync.
	pis := make([]torrent.PeerInfo, 0, len(m.knownPeers))
	for _, info := range m.knownPeers {
		pis = append(pis, info.addrs...)
	}
	m.mu.Unlock()

	if len(pis) > 0 {
		for _, t := range m.client.Torrents() {
			t.AddPeers(pis)
		}
	}
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
