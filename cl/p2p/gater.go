package p2p

import (
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/erigontech/erigon/common/log/v3"
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
	filter      *multiaddr.Filters
	rateLimiter *ipRateLimiter
	poolLimiter *peerPoolLimiter
}

func NewGater(cfg *P2PConfig, logger log.Logger) (g *Gater, err error) {
	g = &Gater{}
	g.filter, err = configureFilter(cfg)
	if err != nil {
		return nil, err
	}
	g.rateLimiter = newIPRateLimiter(defaultIPRateLimiterConfig(), logger, time.Now)
	g.poolLimiter = newPeerPoolLimiter(cfg.MaxPeerCount)
	return g, nil
}

// SetHost lets the gater see live connections once the host exists. buildOptions
// registers the gater before libp2p.New returns the host it gates, so the peer-pool
// occupancy check fails open (allow) until this is called.
func (g *Gater) SetHost(h host.Host) {
	g.poolLimiter.setHost(hostConns{h})
}

// hostConns adapts a live libp2p host.Host to liveConnsSource.
type hostConns struct {
	host host.Host
}

func (h hostConns) remoteIPs() []net.IP {
	conns := h.host.Network().Conns()
	ips := make([]net.IP, 0, len(conns))
	for _, conn := range conns {
		if ip, err := manet.ToIP(conn.RemoteMultiaddr()); err == nil {
			ips = append(ips, ip)
		}
	}
	return ips
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
// Bluetooth), straight after it has accepted a connection from its socket. It is the
// cheapest point to reject abusive traffic: no crypto handshake has run yet, for
// either transport.
func (g *Gater) InterceptAccept(n network.ConnMultiaddrs) (allow bool) {
	if !filterConnections(g.filter, n.RemoteMultiaddr()) {
		return false
	}
	ip, err := manet.ToIP(n.RemoteMultiaddr())
	if err != nil {
		return true
	}
	if !g.rateLimiter.allow(ip) {
		return false
	}
	return g.poolLimiter.allow(ip)
}

// InterceptSecured tests whether a given connection, now authenticated,
// is allowed.
//
// This is called by the upgrader, after it has performed the security
// handshake, and before it negotiates the muxer, or by the directly by the
// transport, at the exact same checkpoint.
func (g *Gater) InterceptSecured(_ network.Direction, _ peer.ID, _ network.ConnMultiaddrs) (allow bool) {
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
