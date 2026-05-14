// Copyright 2026 The Erigon Authors
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

package downloader

import "net"

// PeerDialIPs returns the IP addresses a torrent peer should be dialed
// at, given the peer's ENR-advertised IP and this node's own external
// IP (nil if not yet known).
//
// Normally the result is just the advertised IP. The exception is the
// same-host case: when the peer's advertised IP equals our own external
// IP — two erigon instances on one box without hairpin NAT — that
// address isn't dialable from here even though the peer is reachable on
// loopback, so 127.0.0.1 is appended as an extra dial candidate. This
// is a no-op (single IP) in any normal deployment; it only kicks in for
// the single-host topology our local multi-node tests use.
func PeerDialIPs(peerIP, selfIP net.IP) []net.IP {
	if peerIP == nil {
		return nil
	}
	out := []net.IP{peerIP}
	if selfIP != nil && !peerIP.IsLoopback() && peerIP.Equal(selfIP) {
		out = append(out, net.IPv4(127, 0, 0, 1))
	}
	return out
}
