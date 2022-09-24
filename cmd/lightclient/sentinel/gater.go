package sentinel

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/peers"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// InterceptPeerDial tests whether we're permitted to Dial the specified peer.
func (_ *Sentinel) InterceptPeerDial(_ peer.ID) (allow bool) {
	return true
}

// InterceptAddrDial tests whether we're permitted to dial the specified
// multiaddr for the given peer.
func (s *Sentinel) InterceptAddrDial(pid peer.ID, m multiaddr.Multiaddr) (allow bool) {
	return !s.peers.IsBadPeer(pid)
}

// InterceptAccept checks whether the incidental inbound connection is allowed.
func (s *Sentinel) InterceptAccept(n network.ConnMultiaddrs) (allow bool) {
	return len(s.host.Network().Peers()) < peers.DefaultMaxPeers
}

// InterceptSecured tests whether a given connection, now authenticated,
// is allowed.
func (_ *Sentinel) InterceptSecured(_ network.Direction, _ peer.ID, _ network.ConnMultiaddrs) (allow bool) {
	return true
}

// InterceptUpgraded tests whether a fully capable connection is allowed.
func (_ *Sentinel) InterceptUpgraded(_ network.Conn) (allow bool, reason control.DisconnectReason) {
	return true, 0
}
