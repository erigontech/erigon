// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sentinel

import (
	"net"

	"github.com/libp2p/go-libp2p/core/control"
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
}

func NewGater(cfg *SentinelConfig) (g *Gater, err error) {
	g = &Gater{}
	g.filter, err = configureFilter(cfg)
	if err != nil {
		return nil, err
	}
	return g, nil
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

func configureFilter(cfg *SentinelConfig) (*multiaddr.Filters, error) {
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
