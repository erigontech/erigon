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
	"fmt"
	"net/netip"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/erigontech/erigon/p2p/forkid"
)

// Entry is the eez entry of a node record: the overlay's TCP port.
type Entry uint16

// ENRKey implements enr.Entry.
func (Entry) ENRKey() string { return "eez" }

// ethEntry reads the eth entry of a node record, its fork id.
type ethEntry struct {
	ForkID forkid.ID
	Rest   []rlp.RawValue `rlp:"tail"`
}

func (ethEntry) ENRKey() string { return "eth" }

// discoveryV5 supplies overlay peers from the discv5 table of srv: the
// entries that carry an eez entry and the local node's fork id.
func discoveryV5(srv *p2p.Server) func() []peer.AddrInfo {
	return func() []peer.AddrInfo {
		d := srv.DiscV5()
		if d == nil {
			return nil
		}
		var ours ethEntry
		if srv.LocalNode().Node().Load(&ours) != nil {
			return nil
		}
		var found []peer.AddrInfo
		for _, n := range d.AllNodes() {
			if p, ok := overlayAddr(n, ours.ForkID); ok {
				found = append(found, p)
			}
		}
		return found
	}
}

// overlayAddr returns the overlay address of n if its record carries an eez
// entry, an ip entry and the fork id fork.
func overlayAddr(n *enode.Node, fork forkid.ID) (peer.AddrInfo, bool) {
	var theirs ethEntry
	if n.Load(&theirs) != nil || theirs.ForkID != fork {
		return peer.AddrInfo{}, false
	}
	var port Entry
	if n.Load(&port) != nil {
		return peer.AddrInfo{}, false
	}
	// The ip entry, not the endpoint discovery prefers, which can be IPv6.
	var ip enr.IPv4Addr
	if n.Load(&ip) != nil {
		return peer.AddrInfo{}, false
	}
	id, err := PeerID(n.Pubkey())
	if err != nil {
		return peer.AddrInfo{}, false
	}
	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", netip.Addr(ip), uint16(port)))
	if err != nil {
		return peer.AddrInfo{}, false
	}
	return peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{addr}}, true
}
