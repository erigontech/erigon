/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sentinel

import (
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
	return !s.HasTooManyPeers()
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
