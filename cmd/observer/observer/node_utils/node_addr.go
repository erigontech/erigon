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

package node_utils

import (
	"fmt"
	"net"

	"github.com/erigontech/erigon/v3/cmd/observer/database"
	"github.com/erigontech/erigon/v3/cmd/observer/utils"
	"github.com/erigontech/erigon/v3/p2p/enode"
	"github.com/erigontech/erigon/v3/p2p/enr"
)

func MakeNodeAddr(node *enode.Node) database.NodeAddr {
	var addr database.NodeAddr

	var ipEntry enr.IPv4
	if node.Load(&ipEntry) == nil {
		addr.IP = net.IP(ipEntry)
	}

	var ipV6Entry enr.IPv6
	if node.Load(&ipV6Entry) == nil {
		addr.IPv6.IP = net.IP(ipEntry)
	}

	var portDiscEntry enr.UDP
	if (addr.IP != nil) && (node.Load(&portDiscEntry) == nil) {
		addr.PortDisc = uint16(portDiscEntry)
	}

	var ipV6PortDiscEntry enr.UDP6
	if (addr.IPv6.IP != nil) && (node.Load(&ipV6PortDiscEntry) == nil) {
		addr.IPv6.PortDisc = uint16(ipV6PortDiscEntry)
	}

	var portRLPxEntry enr.TCP
	if (addr.IP != nil) && (node.Load(&portRLPxEntry) == nil) {
		addr.PortRLPx = uint16(portRLPxEntry)
	}

	var ipV6PortRLPxEntry enr.TCP
	if (addr.IPv6.IP != nil) && (node.Load(&ipV6PortRLPxEntry) == nil) {
		addr.IPv6.PortRLPx = uint16(ipV6PortRLPxEntry)
	}

	return addr
}

func MakeNodeFromAddr(id database.NodeID, addr database.NodeAddr) (*enode.Node, error) {
	rec := new(enr.Record)

	pubkey, err := utils.ParseHexPublicKey(string(id))
	if err != nil {
		return nil, err
	}
	rec.Set((*enode.Secp256k1)(pubkey))

	if addr.IP != nil {
		rec.Set(enr.IP(addr.IP))
	}
	if addr.IPv6.IP != nil {
		rec.Set(enr.IPv6(addr.IPv6.IP))
	}
	if addr.PortDisc != 0 {
		rec.Set(enr.UDP(addr.PortDisc))
	}
	if addr.PortRLPx != 0 {
		rec.Set(enr.TCP(addr.PortRLPx))
	}
	if addr.IPv6.PortDisc != 0 {
		rec.Set(enr.UDP6(addr.IPv6.PortDisc))
	}
	if addr.IPv6.PortRLPx != 0 {
		rec.Set(enr.TCP6(addr.IPv6.PortRLPx))
	}

	rec.Set(enr.ID("unsigned"))
	node, err := enode.New(enr.SchemeMap{"unsigned": noSignatureIDScheme{}}, rec)
	if err != nil {
		return nil, fmt.Errorf("failed to make a node: %w", err)
	}
	return node, nil
}

type noSignatureIDScheme struct {
	enode.V4ID
}

func (noSignatureIDScheme) Verify(_ *enr.Record, _ []byte) error {
	return nil
}
