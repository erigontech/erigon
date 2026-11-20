package p2p

import (
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	duplicateConnGrace       = 5 * time.Second
	simultaneousDialWindow   = 15 * time.Second
	duplicateConnIdlePoll    = 200 * time.Millisecond
	duplicateConnMaxIdleWait = 10 * time.Second
	duplicateLossTolerance   = 10 * time.Second
)

// connDeduper keeps one connection per peer when two peers dial each other, possibly over different
// transports. Of the connections to a peer, those dialled by the peer with the lower ID are preferred.
// Both peers apply the same rule, so they keep the same connection without coordination. It never
// rejects a connection and never closes a preferred one, so a dead connection cannot block a new one
// and a peer is not left with no connection.
type connDeduper struct {
	local         peer.ID
	net           network.Network
	grace         time.Duration // a preferred connection must stay open this long before the others close
	window        time.Duration // connections opened this close together come from a simultaneous dial
	idlePoll      time.Duration
	maxIdleWait   time.Duration
	lossTolerance time.Duration

	mu   sync.Mutex
	lost map[peer.ID]time.Time // when a peer last lost a connection while another one stayed open
}

func newConnDeduper(local peer.ID, n network.Network) *connDeduper {
	return &connDeduper{
		local:         local,
		net:           n,
		grace:         duplicateConnGrace,
		window:        simultaneousDialWindow,
		idlePoll:      duplicateConnIdlePoll,
		maxIdleWait:   duplicateConnMaxIdleWait,
		lossTolerance: duplicateLossTolerance,
		lost:          make(map[peer.ID]time.Time),
	}
}

// preferred reports whether c was dialled by the peer with the lower ID.
func (d *connDeduper) preferred(c network.Conn) bool {
	return (c.Stat().Direction == network.DirOutbound) == (d.local < c.RemotePeer())
}

// resolve closes the duplicates that the arrival of c makes redundant. A new preferred connection
// replaces the others. A new non-preferred one is closed only by the peer that dialled it, and only
// when a preferred connection was opened shortly before it: the dialler would not have dialled a
// peer it was connected to, so that preferred connection came from a simultaneous dial. On the
// other side the preferred connection may be half-open and the new one a reconnection, so it stays.
func (d *connDeduper) resolve(c network.Conn) {
	conns := d.net.ConnsToPeer(c.RemotePeer())
	if d.preferred(c) {
		for _, other := range conns {
			if other != c && !d.preferred(other) {
				d.closeWhenIdle(other, c)
			}
		}
		return
	}
	if c.Stat().Direction != network.DirOutbound {
		return
	}
	opened := c.Stat().Opened
	for _, other := range conns {
		if !d.preferred(other) {
			continue
		}
		if age := opened.Sub(other.Stat().Opened); age >= 0 && age <= d.window {
			d.closeWhenIdle(c, other)
			return
		}
	}
}

// closeWhenIdle closes dup once it carries no request streams, or after maxIdleWait, unless keep
// has closed in the meantime. Gossipsub streams do not count: gossipsub reopens them on the
// remaining connection.
func (d *connDeduper) closeWhenIdle(dup, keep network.Conn) {
	deadline := time.Now().Add(d.maxIdleWait)
	for hasRequestStreams(dup) && time.Now().Before(deadline) {
		time.Sleep(d.idlePoll)
	}
	if keep.IsClosed() || dup.IsClosed() {
		return
	}
	_ = dup.Close()
}

func hasRequestStreams(c network.Conn) bool {
	for _, s := range c.GetStreams() {
		if !strings.HasPrefix(string(s.Protocol()), "/meshsub/") {
			return true
		}
	}
	return false
}

// LostDuplicateRecently reports whether pid lost a connection while another one stayed open, within
// the tolerance. Requests and handshakes cut by that close are not the peer's fault.
func (d *connDeduper) LostDuplicateRecently(pid peer.ID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	at, ok := d.lost[pid]
	if ok && time.Since(at) > d.lossTolerance {
		delete(d.lost, pid)
		return false
	}
	return ok
}

func (d *connDeduper) Connected(_ network.Network, c network.Conn) {
	time.AfterFunc(d.grace, func() { d.resolve(c) })
}

func (d *connDeduper) Disconnected(n network.Network, c network.Conn) {
	pid := c.RemotePeer()
	if len(n.ConnsToPeer(pid)) == 0 {
		d.mu.Lock()
		delete(d.lost, pid)
		d.mu.Unlock()
		return
	}
	d.mu.Lock()
	d.lost[pid] = time.Now()
	d.mu.Unlock()
}

func (d *connDeduper) Listen(network.Network, ma.Multiaddr)      {}
func (d *connDeduper) ListenClose(network.Network, ma.Multiaddr) {}
