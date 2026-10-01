package p2p

import (
	"net"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var privateCIDRList = []string{
	// https://tools.ietf.org/html/rfc1918
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	// https://tools.ietf.org/html/rfc6598
	"100.64.0.0/10",
	// https://tools.ietf.org/html/rfc3927
	"169.254.0.0/16",
}

type Gater struct {
	filter *multiaddr.Filters
	host   atomic.Pointer[host.Host]
}

func NewGater(cfg *P2PConfig) (g *Gater, err error) {
	g = &Gater{}
	g.filter, err = configureFilter(cfg)
	if err != nil {
		return nil, err
	}
	return g, nil
}

// SetHost lets the gater see live connections once the host exists. buildOptions
// registers the gater before libp2p.New returns the host it gates, so InterceptSecured
// fails open (allow) until this is called - and libp2p.New can itself start accepting
// connections before it returns, so one or more can complete and register during that
// same window, before this method has even run.
//
// It registers a Connected notifee for connections that complete afterward, and
// reconciles every peer already present in the host's connection list for ones that
// slipped through during the startup window - otherwise a redundant pair that both
// registered before this point would never be observed by anything and would never
// converge.
func (g *Gater) SetHost(h host.Host) {
	g.host.Store(&h)
	h.Network().Notify(&network.NotifyBundle{ConnectedF: g.onConnected})
	for _, p := range h.Network().Peers() {
		g.reconcilePeer(h.Network(), p)
	}
}

// onConnected closes a peer's redundant non-QUIC connection once QUIC is known to also
// be connected. Connected fires strictly after its own connection is added to the
// swarm's connection map, so whichever of two racing connections (admitted
// concurrently by InterceptSecured before either registered) registers second is
// guaranteed to see both in ConnsToPeer here - the swarm's connection map serializes
// the two registrations even when the admission checks raced.
func (g *Gater) onConnected(net network.Network, conn network.Conn) {
	g.reconcilePeer(net, conn.RemotePeer())
}

func (g *Gater) reconcilePeer(net network.Network, p peer.ID) {
	conns := net.ConnsToPeer(p)
	if len(conns) < 2 {
		return
	}
	hasQUIC := false
	for _, c := range conns {
		if _, err := c.RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1); err == nil {
			hasQUIC = true
			break
		}
	}
	if !hasQUIC {
		return
	}
	for _, c := range conns {
		if _, err := c.RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1); err != nil {
			_ = c.Close()
		}
	}
}

// InterceptPeerDial tests whether we're permitted to Dial the specified peer.
// This is called by the network.Network implementation when dialling a peer.
func (g *Gater) InterceptPeerDial(p peer.ID) (allow bool) {
	return true
}

// InterceptAddrDial tests whether we're permitted to dial the specified
// multiaddr for the given peer.
//
// This is called by the network.Network implementation after it has
// resolved the peer's addrs, and prior to dialling each.
func (g *Gater) InterceptAddrDial(_ peer.ID, n multiaddr.Multiaddr) (allow bool) {
	return filterConnections(g.filter, n)
}

// InterceptAccept tests whether an incipient inbound connection is allowed.
//
// This is called by the upgrader, or by the transport directly (e.g. QUIC,
// Bluetooth), straight after it has accepted a connection from its socket.
func (g *Gater) InterceptAccept(n network.ConnMultiaddrs) (allow bool) {
	return filterConnections(g.filter, n.RemoteMultiaddr())
}

// InterceptSecured tests whether a given connection, now authenticated,
// is allowed.
//
// This is called by the upgrader, after it has performed the security
// handshake, and before it negotiates the muxer, or by the directly by the
// transport, at the exact same checkpoint.
//
// Two peers that discover each other via discv5 can each independently dial the
// other around the same time, one over TCP and one over QUIC: go-libp2p does not
// deduplicate connections across transports (swarm.addConn appends unconditionally),
// so both dials succeed and the peer ends up with two live connections that never
// converge on their own. Rejecting a new non-preferred (TCP) connection here, when a
// QUIC connection to the same peer is already registered, is cheaper than admitting it
// and closing it afterwards and — because "is this address QUIC" is a fact both sides
// compute identically — always converges on keeping the same connection (the QUIC one)
// rather than racing on timestamps that the two peers could disagree on.
//
// This runs before the muxer negotiates (see the doc comment above), so a QUIC
// connection admitted here can still fail to fully upgrade afterward. Closing an
// existing, healthy TCP connection on its behalf at this point would risk leaving the
// peer with no connection at all if that happens. So this only ever rejects a new,
// redundant TCP attempt against an already-registered QUIC connection; it never closes
// anything itself. A QUIC connection that arrives while a TCP one is already registered
// is simply admitted here, and reconcilePeer - invoked from the Connected notifee,
// which only fires once a connection is fully registered - closes the stale TCP
// connection once QUIC has actually, successfully gone live. This also means arrival
// order and direction don't matter: whichever order the two legs reach this point in,
// and regardless of who dialed whom, the same two rules (reject redundant TCP, let
// reconcilePeer clean up after a successful QUIC registration) converge on the same
// outcome.
func (g *Gater) InterceptSecured(_ network.Direction, p peer.ID, addrs network.ConnMultiaddrs) (allow bool) {
	hostPtr := g.host.Load()
	if hostPtr == nil {
		return true
	}
	if _, err := addrs.RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1); err == nil {
		return true
	}
	for _, conn := range (*hostPtr).Network().ConnsToPeer(p) {
		if _, err := conn.RemoteMultiaddr().ValueForProtocol(multiaddr.P_QUIC_V1); err == nil {
			return false
		}
	}
	return true
}

// InterceptUpgraded tests whether a fully capable connection is allowed.
//
// At this point, the connection a multiplexer has been selected.
// When rejecting a connection, the gater can return a DisconnectReason.
// Refer to the godoc on the ConnectionGater type for more information.
//
// NOTE: the go-libp2p implementation currently IGNORES the disconnect reason.
func (g *Gater) InterceptUpgraded(_ network.Conn) (allow bool, reason control.DisconnectReason) {
	return true, 0
}

func filterConnections(f *multiaddr.Filters, a multiaddr.Multiaddr) bool {
	acceptedNets := f.FiltersForAction(multiaddr.ActionAccept)
	restrictConns := len(acceptedNets) != 0

	// If we have an allow list added in, we by default reject all
	// connection attempts except for those coming in from the
	// appropriate ip subnets.
	if restrictConns {
		ip, err := manet.ToIP(a)
		if err != nil {
			return false
		}
		found := false
		for _, ipnet := range acceptedNets {
			if ipnet.Contains(ip) {
				found = true
				break
			}
		}
		return found
	}
	return !f.AddrBlocked(a)
}

func configureFilter(cfg *P2PConfig) (*multiaddr.Filters, error) {
	var err error
	addrFilter := multiaddr.NewFilters()
	if !cfg.LocalDiscovery {
		addrFilter, err = privateCIDRFilter(addrFilter, multiaddr.ActionDeny)
		if err != nil {
			return nil, err
		}
	}
	return addrFilter, nil
}

// helper function to either accept or deny all private addresses
// if a new rule for a private address is in conflict with a previous one, log a warning
func privateCIDRFilter(addrFilter *multiaddr.Filters, action multiaddr.Action) (*multiaddr.Filters, error) {
	for _, privCidr := range privateCIDRList {
		_, ipnet, err := net.ParseCIDR(privCidr)
		if err != nil {
			return nil, err
		}
		//	curAction, _ := addrFilter.ActionForFilter(*ipnet)
		//	switch {
		//	case action == multiaddr.ActionAccept:
		//		if curAction == multiaddr.ActionDeny {
		//			// rule conflict
		//		}
		//	case action == multiaddr.ActionDeny:
		//		if curAction == multiaddr.ActionAccept {
		//			// rule conflict
		//		}
		//	}
		addrFilter.AddFilter(*ipnet, action)
	}
	return addrFilter, nil
}
