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
// entries and adds a peer to every torrent once its BT endpoint answers; an
// endpoint that does not answer is checked again later. When a DevP2P peer
// disconnects (disappears from the NodeSource), the torrent client handles the
// connection drop naturally — there is no explicit "remove peer" in the torrent API.
type TorrentPeerManager struct {
	client       *torrent.Client
	nodeSourceFn func() NodeSource
	logger       log.Logger

	// probe, inject and now default to dialing over the client's sockets, adding the peer to every
	// torrent and the wall clock; tests replace them.
	probe  func(ctx context.Context, addr string) bool
	inject func(torrent.PeerInfo)
	now    func() time.Time

	// knownPeers holds the DevP2P peers with a BT endpoint, reachable or not. Only sync touches it, from Run's one
	// goroutine, so it needs no lock.
	knownPeers map[enode.ID]btPeerInfo
}

const (
	// btPeerProbeTimeout bounds the one dial that checks a peer's BT endpoint.
	btPeerProbeTimeout = 10 * time.Second
	// An unreachable endpoint is checked again after btPeerRetryMin, doubling up to btPeerRetryMax.
	btPeerRetryMin = time.Minute
	btPeerRetryMax = 30 * time.Minute
)

type btPeerInfo struct {
	addr      torrent.PeerInfo
	reachable bool          // added to the torrents
	retry     time.Duration // wait before the next check of an unreachable endpoint
	retryAt   time.Time
}

func NewTorrentPeerManager(client *torrent.Client, nodeSourceFn func() NodeSource, logger log.Logger) *TorrentPeerManager {
	m := &TorrentPeerManager{
		client:       client,
		nodeSourceFn: nodeSourceFn,
		logger:       logger,
		knownPeers:   make(map[enode.ID]btPeerInfo),
		now:          time.Now,
	}
	m.probe = m.dialable
	m.inject = m.addPeerToAllTorrents
	return m
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
		m.sync(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sync scans the current DevP2P peer set and adds the peers whose BT endpoint answers to every torrent.
// The torrent client dials a peer once per torrent and keeps no memory of failures across torrents, so an
// endpoint that does not answer (NAT, firewall) would cost thousands of dials, each waiting for its timeout.
// Each endpoint is checked with one dial first; an unreachable one is checked again later.
func (m *TorrentPeerManager) sync(ctx context.Context) {
	ns := m.nodeSourceFn()
	if ns == nil {
		return
	}

	nodes := ns.AllNodes() // may resolve each peer over the network, so the clock is read after it
	now := m.now()
	currentPeers := make(map[enode.ID]struct{})
	var due []enode.ID

	for _, node := range nodes {
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
		addr := ipPortAddr{IP: ip, Port: int(bt)}
		p, known := m.knownPeers[id]
		if !known || p.addr.Addr.String() != addr.String() {
			m.knownPeers[id] = btPeerInfo{addr: torrent.PeerInfo{Addr: addr, Trusted: true}}
			due = append(due, id)
		} else if !p.reachable && !now.Before(p.retryAt) {
			due = append(due, id)
		}
	}
	// Forget peers that left the DevP2P peer set; they are checked again if they come back.
	for id := range m.knownPeers {
		if _, exists := currentPeers[id]; !exists {
			delete(m.knownPeers, id)
			m.logger.Debug("[bt-peers] peer left DevP2P, removed from tracking", "node", id.TerminalString())
		}
	}
	reachable := make([]bool, len(due))
	var probes sync.WaitGroup
	for i := range due {
		probes.Go(func() { reachable[i] = m.probe(ctx, m.knownPeers[due[i]].addr.Addr.String()) }) // reads only
	}
	probes.Wait()
	checked := m.now() // a failed check can take its whole timeout; the wait starts after it

	var toInject []torrent.PeerInfo
	for i, id := range due {
		p := m.knownPeers[id]
		if reachable[i] {
			p.reachable = true
			toInject = append(toInject, p.addr)
			m.logger.Debug("[bt-peers] added torrent peer from ENR", "node", id.TerminalString(), "addr", p.addr.Addr)
		} else {
			p.retry = min(max(2*p.retry, btPeerRetryMin), btPeerRetryMax)
			p.retryAt = checked.Add(p.retry)
			m.logger.Debug("[bt-peers] BT endpoint from ENR does not answer", "node", id.TerminalString(), "addr", p.addr.Addr, "retryIn", p.retry)
		}
		m.knownPeers[id] = p
	}
	for _, pi := range toInject {
		m.inject(pi)
	}
}

// dialable reports whether addr accepts a connection over the torrent client's own sockets (uTP and TCP,
// as configured), the way the client would dial it.
func (m *TorrentPeerManager) dialable(ctx context.Context, addr string) bool {
	var dialers []torrent.Dialer
	for _, l := range m.client.Listeners() {
		if d, ok := l.(torrent.Dialer); ok {
			dialers = append(dialers, d)
		}
	}
	// torrent.DialFirst cannot be used: it never makes its result channel, so it blocks forever.
	ctx, cancel := context.WithTimeout(ctx, btPeerProbeTimeout)
	defer cancel()
	conns := make(chan net.Conn, len(dialers))
	for _, d := range dialers {
		go func() {
			conn, err := d.Dial(ctx, addr)
			if err != nil {
				conn = nil
			}
			conns <- conn
		}()
	}
	answered := false
	for range dialers {
		if conn := <-conns; conn != nil {
			answered = true
			cancel() // the other dials are no longer needed
			_ = conn.Close()
		}
	}
	return answered
}

// addPeerToAllTorrents adds the given peer to all currently active torrents.
func (m *TorrentPeerManager) addPeerToAllTorrents(pi torrent.PeerInfo) {
	for _, t := range m.client.Torrents() {
		t.AddPeers([]torrent.PeerInfo{pi})
	}
}

// PeerCount returns the number of tracked torrent peers. Like sync, it must not run alongside Run.
func (m *TorrentPeerManager) PeerCount() int {
	return len(m.knownPeers)
}

// ipPortAddr is defined in chaintoml_consumer.go; adding a compile-time
// check to ensure it implements net.Addr (used by torrent.PeerInfo.Addr).
var _ net.Addr = ipPortAddr{}
