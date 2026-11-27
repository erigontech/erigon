package p2p

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

type fakeStream struct {
	network.Stream
	proto protocol.ID
}

func (s fakeStream) Protocol() protocol.ID { return s.proto }

type fakeConn struct {
	network.Conn
	remote  peer.ID
	dir     network.Direction
	opened  time.Time
	closed  atomic.Bool
	streams atomic.Pointer[[]network.Stream]
}

func (c *fakeConn) RemotePeer() peer.ID { return c.remote }
func (c *fakeConn) Stat() network.ConnStats {
	return network.ConnStats{Stats: network.Stats{Direction: c.dir, Opened: c.opened}}
}
func (c *fakeConn) IsClosed() bool { return c.closed.Load() }
func (c *fakeConn) Close() error   { c.closed.Store(true); return nil }
func (c *fakeConn) GetStreams() []network.Stream {
	if s := c.streams.Load(); s != nil {
		return *s
	}
	return nil
}

func (c *fakeConn) setStreams(protos ...protocol.ID) {
	streams := make([]network.Stream, len(protos))
	for i, p := range protos {
		streams[i] = fakeStream{proto: p}
	}
	c.streams.Store(&streams)
}

type fakeNetwork struct {
	network.Network
	conns []network.Conn
}

func (n *fakeNetwork) ConnsToPeer(peer.ID) []network.Conn { return n.conns }

const (
	lowID  = peer.ID("a")
	highID = peer.ID("b")
)

// newDedupFixture returns a deduper for the low ID peer and its connections to the high ID peer: the
// preferred one it dialled and the non-preferred one the remote dialled the given time later.
func newDedupFixture(after time.Duration) (*connDeduper, *fakeConn, *fakeConn) {
	t0 := time.Now()
	preferred := &fakeConn{remote: highID, dir: network.DirOutbound, opened: t0}
	other := &fakeConn{remote: highID, dir: network.DirInbound, opened: t0.Add(after)}
	d := newConnDeduper(lowID, &fakeNetwork{conns: []network.Conn{preferred, other}})
	d.idlePoll = time.Millisecond
	d.maxIdleWait = time.Second
	return d, preferred, other
}

// newDialerDedupFixture is newDedupFixture from the high ID peer: the preferred connection was
// dialled by the remote, and the non-preferred one by this peer the given time later.
func newDialerDedupFixture(after time.Duration) (*connDeduper, *fakeConn, *fakeConn) {
	d, preferred, other := newDedupFixture(after)
	d.local = highID
	preferred.remote, preferred.dir = lowID, network.DirInbound
	other.remote, other.dir = lowID, network.DirOutbound
	return d, preferred, other
}

func TestDedupPrefersTheConnectionDialledByTheLowerID(t *testing.T) {
	low := newConnDeduper(lowID, nil)
	high := newConnDeduper(highID, nil)
	dialledByLow := &fakeConn{remote: highID, dir: network.DirOutbound}
	sameConnAtHigh := &fakeConn{remote: lowID, dir: network.DirInbound}
	require.True(t, low.preferred(dialledByLow))
	require.True(t, high.preferred(sameConnAtHigh))
	require.False(t, low.preferred(&fakeConn{remote: highID, dir: network.DirInbound}))
	require.False(t, high.preferred(&fakeConn{remote: lowID, dir: network.DirOutbound}))
}

func TestDedupClosesTheOtherConnectionWhenThePreferredOneArrives(t *testing.T) {
	d, preferred, other := newDedupFixture(-time.Second)
	d.resolve(preferred)
	require.True(t, other.IsClosed())
	require.False(t, preferred.IsClosed())
}

func TestDedupNeverClosesAPreferredConnection(t *testing.T) {
	d, preferred, other := newDedupFixture(-time.Second)
	secondPreferred := &fakeConn{remote: highID, dir: network.DirOutbound, opened: preferred.opened}
	d.net.(*fakeNetwork).conns = append(d.net.(*fakeNetwork).conns, secondPreferred)
	d.resolve(preferred)
	require.True(t, other.IsClosed())
	require.False(t, secondPreferred.IsClosed(), "both connections were dialled by the lower ID")
}

func TestDedupClosesANewConnectionFromASimultaneousDial(t *testing.T) {
	d, preferred, other := newDialerDedupFixture(time.Second)
	d.resolve(other)
	require.True(t, other.IsClosed())
	require.False(t, preferred.IsClosed())
}

// Only the dialler knows that the preferred connection registered while it was dialling. On the
// other side the preferred connection may be half-open and the new one the remote's reconnection.
func TestDedupKeepsANewConnectionTheRemoteDialled(t *testing.T) {
	d, preferred, other := newDedupFixture(time.Second)
	d.resolve(other)
	require.False(t, other.IsClosed())
	require.False(t, preferred.IsClosed())
}

// A preferred connection older than a simultaneous dial may be dead: the remote dials again after it
// lost the connection, so the new one must stay.
func TestDedupKeepsANewConnectionWhenThePreferredOneIsOld(t *testing.T) {
	d, preferred, other := newDialerDedupFixture(time.Minute)
	d.resolve(other)
	require.False(t, other.IsClosed())
	require.False(t, preferred.IsClosed())
}

// A preferred connection newer than this one closes it from its own timer, once its grace period
// shows the remote kept it.
func TestDedupLeavesAnOlderConnectionToTheNewerPreferredOne(t *testing.T) {
	d, preferred, other := newDialerDedupFixture(-time.Second)
	d.resolve(other)
	require.False(t, other.IsClosed())
	require.False(t, preferred.IsClosed())
}

func TestDedupKeepsTheOtherConnectionWhenThePreferredOneClosed(t *testing.T) {
	d, preferred, other := newDialerDedupFixture(time.Second)
	preferred.closed.Store(true)
	d.resolve(other)
	require.False(t, other.IsClosed())

	d, preferred, other = newDedupFixture(-time.Second)
	preferred.closed.Store(true)
	d.resolve(preferred)
	require.False(t, other.IsClosed(), "the remote may have rejected the preferred connection")
}

func TestDedupWaitsForRequestStreamsBeforeClosing(t *testing.T) {
	d, preferred, other := newDedupFixture(-time.Second)
	d.maxIdleWait = time.Minute
	other.setStreams("/meshsub/1.2.0", "/eth2/beacon_chain/req/status/2/ssz_snappy")
	done := make(chan struct{})
	go func() { d.resolve(preferred); close(done) }()
	time.Sleep(20 * time.Millisecond)
	require.False(t, other.IsClosed(), "a request stream is still open")

	other.setStreams("/meshsub/1.2.0")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("gossipsub streams must not hold the duplicate open")
	}
	require.True(t, other.IsClosed())
}

func TestDedupKeepsTheOtherConnectionWhenThePreferredOneClosesWhileWaiting(t *testing.T) {
	d, preferred, other := newDedupFixture(-time.Second)
	d.maxIdleWait = time.Minute
	other.setStreams("/eth2/beacon_chain/req/status/2/ssz_snappy")
	done := make(chan struct{})
	go func() { d.resolve(preferred); close(done) }()
	time.Sleep(20 * time.Millisecond)
	preferred.closed.Store(true)
	other.setStreams()
	<-done
	require.False(t, other.IsClosed())
}

// A reconnection opened after the preferred connection may replace a half-open one, so the preferred
// connection's timer must not close it; its dialler closes it if it came from a simultaneous dial.
func TestDedupKeepsAReconnectionOpenedAfterThePreferredConnection(t *testing.T) {
	d, preferred, reconn := newDedupFixture(time.Second)
	d.resolve(reconn)
	d.resolve(preferred)
	require.False(t, reconn.IsClosed())
	require.False(t, preferred.IsClosed())
}

// A duplicate that stays busy is left open: closing it would cut more requests than one excuse covers.
func TestDedupLeavesABusyDuplicateOpen(t *testing.T) {
	d, preferred, other := newDedupFixture(-time.Second)
	d.maxIdleWait = 20 * time.Millisecond
	other.setStreams("/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy")
	d.resolve(preferred)
	require.False(t, other.IsClosed())
}

func TestDedupStopsWaitingWhenThePreferredConnectionCloses(t *testing.T) {
	d, preferred, other := newDedupFixture(-time.Second)
	d.maxIdleWait = time.Minute
	other.setStreams("/eth2/beacon_chain/req/status/2/ssz_snappy")
	done := make(chan struct{})
	go func() { d.resolve(preferred); close(done) }()
	preferred.closed.Store(true)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting for a duplicate of a connection that closed")
	}
	require.False(t, other.IsClosed())
}

// trackedDeduper is newDedupFixture with both connections registered and resolve not scheduled.
func trackedDeduper(after time.Duration) (*connDeduper, *fakeConn, *fakeConn) {
	d, preferred, other := newDedupFixture(after)
	d.grace = time.Hour
	d.Connected(d.net, preferred)
	d.Connected(d.net, other)
	return d, preferred, other
}

func TestDedupExcusesAClosedDuplicateUntilItExpires(t *testing.T) {
	d, preferred, other := trackedDeduper(time.Second)
	n := d.net.(*fakeNetwork)
	n.conns = []network.Conn{preferred}
	d.Disconnected(n, other)
	require.True(t, d.ConsumeDuplicateLoss(highID), "a closed duplicate still excuses a failure")

	d, preferred, other = trackedDeduper(time.Second)
	n = d.net.(*fakeNetwork)
	n.conns = []network.Conn{preferred}
	d.lossTolerance = 0
	d.Disconnected(n, other)
	time.Sleep(time.Millisecond)
	require.False(t, d.ConsumeDuplicateLoss(highID), "the excuse expires after the close")
}

func TestDedupDoesNotExcuseADuplicateLongAfterThePreferredConnectionClosed(t *testing.T) {
	d, preferred, _ := trackedDeduper(time.Second)
	n := d.net.(*fakeNetwork)
	n.conns = []network.Conn{d.conns[highID][1].conn}
	d.lossTolerance = 0
	d.Disconnected(n, preferred)
	time.Sleep(time.Millisecond)
	require.False(t, d.ConsumeDuplicateLoss(highID))
}

// Closing a duplicate cuts all of its streams at once, so the failures that follow the first
// excused one closely are excused too.
func TestDedupExcusesTheFailuresOfOneClose(t *testing.T) {
	d, _, _ := trackedDeduper(time.Second)
	d.lossBurst = time.Hour
	require.True(t, d.ConsumeDuplicateLoss(highID))
	excused := d.conns[highID][1]
	excused.excusedAt = excused.excusedAt.Add(-time.Minute)
	first := excused.excusedAt
	require.True(t, d.ConsumeDuplicateLoss(highID), "a second stream cut by the same close")
	require.Equal(t, first, excused.excusedAt, "the burst counts from the first excused failure")
	excused.excusedAt = excused.excusedAt.Add(-2 * d.lossBurst)
	require.False(t, d.ConsumeDuplicateLoss(highID), "a failure after the burst is the peer's")
}

// The preferred connection replaces an older non-preferred one of any age, so that close is excused.
// A non-preferred connection opened well after the preferred one is kept, so it excuses nothing.
func TestDedupExcusesOnlyConnectionsItWouldClose(t *testing.T) {
	d, _, _ := trackedDeduper(-time.Hour)
	require.True(t, d.ConsumeDuplicateLoss(highID))

	d, _, _ = trackedDeduper(simultaneousDialWindow + time.Second)
	require.False(t, d.ConsumeDuplicateLoss(highID))

	d, preferred, _ := trackedDeduper(time.Second)
	d.conns[highID] = d.conns[highID][:1]
	d.Connected(d.net, &fakeConn{remote: highID, dir: network.DirOutbound, opened: preferred.opened})
	require.False(t, d.ConsumeDuplicateLoss(highID), "two preferred connections are never deduplicated")
}

func TestDedupForgetsAPeerWithNoConnectionLeft(t *testing.T) {
	d, preferred, other := trackedDeduper(time.Second)
	n := d.net.(*fakeNetwork)
	n.conns = nil
	d.Disconnected(n, preferred)
	require.Empty(t, d.conns)
	d.Disconnected(n, other)
	require.Empty(t, d.conns, "a later notification of a concurrent close adds nothing back")

	d, preferred, other = trackedDeduper(-time.Hour)
	n = d.net.(*fakeNetwork)
	n.conns = []network.Conn{preferred}
	d.lossTolerance = 0
	d.Disconnected(n, other)
	time.Sleep(time.Millisecond)
	d.Disconnected(n, &fakeConn{remote: highID})
	require.Len(t, d.conns[highID], 1, "expired connections are dropped")
}

// barrierGater holds each secured checkpoint until the host has reached it for both its outbound
// dial and the remote's inbound one, so two dials started together are both in flight before either
// connection registers.
type barrierGater struct {
	allowAll
	in, out         chan struct{}
	inOnce, outOnce sync.Once
}

func newBarrierGater() *barrierGater {
	return &barrierGater{in: make(chan struct{}), out: make(chan struct{})}
}

func (g *barrierGater) InterceptSecured(dir network.Direction, _ peer.ID, _ network.ConnMultiaddrs) bool {
	if dir == network.DirInbound {
		g.inOnce.Do(func() { close(g.in) })
	} else {
		g.outOnce.Do(func() { close(g.out) })
	}
	timeout := time.After(5 * time.Second)
	for _, reached := range []chan struct{}{g.in, g.out} {
		select {
		case <-reached:
		case <-timeout:
			return true
		}
	}
	return true
}

type allowAll struct{}

func (allowAll) InterceptPeerDial(peer.ID) bool                                  { return true }
func (allowAll) InterceptAddrDial(peer.ID, ma.Multiaddr) bool                    { return true }
func (allowAll) InterceptAccept(network.ConnMultiaddrs) bool                     { return true }
func (allowAll) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) { return true, 0 }

type dedupHost struct {
	host.Host
	deduper             *connDeduper
	maxConns, zeroConns atomic.Int32
}

// newDedupHost dials TCP from ephemeral ports: with reuseport, two hosts that dial each other's
// listen ports at once share one TCP 4-tuple, and both security handshakes fail before either
// connection exists.
func newDedupHost(t *testing.T) *dedupHost {
	t.Helper()
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1"),
		libp2p.Transport(tcp.NewTCPTransport, tcp.DisableReuseport()),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.DefaultMuxers,
		libp2p.Security(noise.ID, noise.New),
		libp2p.Ping(false),
		libp2p.ConnectionGater(newBarrierGater()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	d := newConnDeduper(h.ID(), h.Network())
	d.grace = 200 * time.Millisecond
	d.idlePoll = 10 * time.Millisecond
	d.maxIdleWait = 2 * time.Second
	dh := &dedupHost{Host: h, deduper: d}
	h.Network().Notify(d)
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, c network.Conn) {
			count := int32(len(n.ConnsToPeer(c.RemotePeer())))
			for old := dh.maxConns.Load(); count > old && !dh.maxConns.CompareAndSwap(old, count); old = dh.maxConns.Load() {
			}
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			if len(n.ConnsToPeer(c.RemotePeer())) == 0 {
				dh.zeroConns.Add(1)
			}
		},
	})
	return dh
}

func (h *dedupHost) addrInfo(t *testing.T, transport string) peer.AddrInfo {
	t.Helper()
	for _, a := range h.Addrs() {
		if _, err := a.ValueForProtocol(map[string]int{"tcp": ma.P_TCP, "quic": ma.P_QUIC_V1}[transport]); err == nil {
			return peer.AddrInfo{ID: h.ID(), Addrs: []ma.Multiaddr{a}}
		}
	}
	t.Fatalf("no %s address", transport)
	return peer.AddrInfo{}
}

func dialEachOther(t *testing.T, a, b *dedupHost, transports [2]string) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() { <-start; errs <- a.Connect(t.Context(), b.addrInfo(t, transports[0])) }()
	go func() { <-start; errs <- b.Connect(t.Context(), a.addrInfo(t, transports[1])) }()
	close(start)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
}

func TestDedupSimultaneousDialKeepsExactlyOneConnection(t *testing.T) {
	for _, transports := range [][2]string{{"tcp", "tcp"}, {"quic", "quic"}, {"tcp", "quic"}, {"quic", "tcp"}} {
		t.Run(transports[0]+"-"+transports[1], func(t *testing.T) {
			for range 3 {
				a, b := newDedupHost(t), newDedupHost(t)
				dialEachOther(t, a, b, transports)

				require.Eventually(t, func() bool {
					return len(a.Network().ConnsToPeer(b.ID())) == 1 && len(b.Network().ConnsToPeer(a.ID())) == 1
				}, 5*time.Second, 10*time.Millisecond)
				require.Equal(t, int32(2), a.maxConns.Load(), "both dials must have connected")
				ca, cb := a.Network().ConnsToPeer(b.ID())[0], b.Network().ConnsToPeer(a.ID())[0]
				require.True(t, a.deduper.preferred(ca))
				require.True(t, b.deduper.preferred(cb))
				require.True(t, ca.LocalMultiaddr().Equal(cb.RemoteMultiaddr()), "both sides keep the same connection")
				require.Zero(t, a.zeroConns.Load()+b.zeroConns.Load(), "a peer must never be left without a connection")
			}
		})
	}
}

// When the remote closes a duplicate, a stream on it fails before the Disconnected notification, and
// on QUIC before the connection reports closed, so the failure must be excused without either.
func TestDedupExcusesAStreamCutByTheRemoteClosingADuplicate(t *testing.T) {
	for _, transports := range [][2]string{{"tcp", "tcp"}, {"quic", "quic"}, {"tcp", "quic"}, {"quic", "tcp"}} {
		t.Run(transports[0]+"-"+transports[1], func(t *testing.T) {
			for range 5 {
				a, b := newDedupHost(t), newDedupHost(t)
				a.deduper.grace, b.deduper.grace = time.Hour, time.Hour
				dialEachOther(t, a, b, transports)
				require.Eventually(t, func() bool {
					return len(a.Network().ConnsToPeer(b.ID())) == 2 && len(b.Network().ConnsToPeer(a.ID())) == 2
				}, 5*time.Second, 10*time.Millisecond)

				var dupA, dupB network.Conn
				for _, c := range a.Network().ConnsToPeer(b.ID()) {
					if !a.deduper.preferred(c) {
						dupA = c
					}
				}
				for _, c := range b.Network().ConnsToPeer(a.ID()) {
					if !b.deduper.preferred(c) {
						dupB = c
					}
				}
				s, err := dupA.NewStream(t.Context())
				require.NoError(t, err)
				excused := make(chan bool, 1)
				go func() {
					// The remote's protocol negotiation can write to the stream before the close.
					_, _ = io.Copy(io.Discard, s)
					excused <- a.deduper.ConsumeDuplicateLoss(b.ID())
				}()
				require.NoError(t, dupB.Close())
				require.True(t, <-excused)
			}
		})
	}
}
