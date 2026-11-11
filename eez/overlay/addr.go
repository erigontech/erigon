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

package overlay

import (
	"errors"
	"fmt"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// ParseListen parses a listen address: a TCP multiaddr with a fixed port,
// since the node record advertises the port before the listener binds.
func ParseListen(s string) (ma.Multiaddr, error) {
	addr, err := ma.NewMultiaddr(strings.TrimSpace(s))
	if err != nil {
		return nil, err
	}
	port, err := ListenPort(addr)
	if err != nil {
		return nil, err
	}
	if port == 0 {
		return nil, fmt.Errorf("%s has TCP port 0; set a fixed port", addr)
	}
	return addr, nil
}

// ListenPort returns the TCP port of addr.
func ListenPort(addr ma.Multiaddr) (uint16, error) {
	s, err := addr.ValueForProtocol(ma.P_TCP)
	if err != nil {
		return 0, fmt.Errorf("%s is not a TCP multiaddr", addr)
	}
	var port uint16
	if _, err := fmt.Sscan(s, &port); err != nil {
		return 0, fmt.Errorf("%s: TCP port %q: %w", addr, s, err)
	}
	return port, nil
}

// ParsePeers parses a comma-separated list of static peers, each an IP
// multiaddr ending in /p2p/<id>.
func ParsePeers(s string) ([]peer.AddrInfo, error) {
	var peers []peer.AddrInfo
	for raw := range strings.SplitSeq(s, ",") {
		if raw = strings.TrimSpace(raw); raw == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", raw, err)
		}
		if _, last := ma.SplitLast(addr); last == nil || last.Protocol().Code != ma.P_P2P {
			return nil, fmt.Errorf("%s does not end in /p2p/<id>", raw)
		}
		// The transport resolves no names, so such a peer would fail every
		// dial.
		if !hasIP(addr) {
			return nil, fmt.Errorf("%s: %w", raw, errNoIP)
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", raw, err)
		}
		peers = append(peers, *info)
	}
	return peers, nil
}

var errNoIP = errors.New("DNS multiaddrs are not supported; use an IP")

func hasIP(addr ma.Multiaddr) bool {
	for _, c := range addr {
		if code := c.Protocol().Code; code == ma.P_IP4 || code == ma.P_IP6 {
			return true
		}
	}
	return false
}
