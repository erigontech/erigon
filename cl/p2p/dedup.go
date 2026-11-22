package p2p

import (
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
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
	network.NoopNotifiee

	local         peer.ID
	net           network.Network
	grace         time.Duration // a preferred connection must stay open this long before the others close
	idlePoll      time.Duration
	maxIdleWait   time.Duration
	lossTolerance time.Duration

	mu    sync.Mutex
	conns map[peer.ID][]*trackedConn
}

// trackedConn is a connection between its Connected notification and lossTolerance after its
// Disconnected one.
type trackedConn struct {
	conn     network.Conn
	closedAt time.Time
	excused  bool
}

func newConnDeduper(local peer.ID, n network.Network) *connDeduper {
	return &connDeduper{
		local:         local,
		net:           n,
		grace:         duplicateConnGrace,
		idlePoll:      duplicateConnIdlePoll,
		maxIdleWait:   duplicateConnMaxIdleWait,
		lossTolerance: duplicateLossTolerance,
		conns:         make(map[peer.ID][]*trackedConn),
	}
}

// preferred reports whether c was dialled by the peer with the lower ID.
func (d *connDeduper) preferred(c network.Conn) bool {
	return (c.Stat().Direction == network.DirOutbound) == (d.local < c.RemotePeer())
}

// duplicateOf reports whether resolve closes dup to keep keep, on the side that applies the rule:
// a non-preferred connection opened before a preferred one, or shortly after it.
func (d *connDeduper) duplicateOf(dup, keep network.Conn) bool {
	return !d.preferred(dup) && d.preferred(keep) && dup.Stat().Opened.Sub(keep.Stat().Opened) <= simultaneousDialWindow
}

// resolve closes the duplicates that the arrival of c makes redundant. A new preferred connection
// replaces the older non-preferred ones. A non-preferred connection is closed only by the peer that
// dialled it, and only when a preferred connection was opened shortly before it: the dialler would
// not have dialled a peer it was connected to, so that preferred connection came from a simultaneous
// dial. Elsewhere a newer non-preferred connection may be a reconnection that replaces a half-open
// preferred one, so it stays.
func (d *connDeduper) resolve(c network.Conn) {
	conns := d.net.ConnsToPeer(c.RemotePeer())
	if d.preferred(c) {
		opened := c.Stat().Opened
		for _, other := range conns {
			if other != c && !d.preferred(other) && !other.Stat().Opened.After(opened) {
				d.closeWhenIdle(other, c)
			}
		}
		return
	}
	if c.Stat().Direction != network.DirOutbound {
		return
	}
	for _, other := range conns {
		if !other.Stat().Opened.After(c.Stat().Opened) && d.duplicateOf(c, other) {
			d.closeWhenIdle(c, other)
			return
		}
	}
}

// closeWhenIdle closes dup once it carries no request streams, unless keep closes first. A duplicate
// that stays busy for maxIdleWait is left open. Gossipsub streams do not count: gossipsub reopens
// them on the remaining connection.
func (d *connDeduper) closeWhenIdle(dup, keep network.Conn) {
	deadline := time.Now().Add(d.maxIdleWait)
	for hasRequestStreams(dup) {
		if keep.IsClosed() || !time.Now().Before(deadline) {
			return
		}
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

// ConsumeDuplicateLoss reports whether a failure with pid may come from closing one of its duplicate
// connections, and uses that up: each duplicate pair excuses one failure. A request or handshake cut
// by that close is not the peer's fault. The pair counts from when both connections registered, not
// from the close, because a stream on a connection the remote closes can fail before this side sees
// the close.
func (d *connDeduper) ConsumeDuplicateLoss(pid peer.ID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	conns := d.conns[pid]
	for _, dup := range conns {
		if dup.excused || d.expired(dup, now) {
			continue
		}
		for _, keep := range conns {
			if !d.expired(keep, now) && d.duplicateOf(dup.conn, keep.conn) {
				dup.excused = true
				return true
			}
		}
	}
	return false
}

func (d *connDeduper) expired(c *trackedConn, now time.Time) bool {
	return !c.closedAt.IsZero() && now.Sub(c.closedAt) > d.lossTolerance
}

func (d *connDeduper) Connected(_ network.Network, c network.Conn) {
	d.mu.Lock()
	pid := c.RemotePeer()
	d.conns[pid] = append(d.conns[pid], &trackedConn{conn: c})
	d.mu.Unlock()
	time.AfterFunc(d.grace, func() { d.resolve(c) })
}

func (d *connDeduper) Disconnected(n network.Network, c network.Conn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	pid := c.RemotePeer()
	// Checked under the lock, so of the connections closing concurrently the last one sees none left.
	if len(n.ConnsToPeer(pid)) == 0 {
		delete(d.conns, pid)
		return
	}
	now := time.Now()
	kept := d.conns[pid][:0]
	for _, t := range d.conns[pid] {
		if t.conn == c {
			t.closedAt = now
		}
		if !d.expired(t, now) {
			kept = append(kept, t)
		}
	}
	d.conns[pid] = kept
}
