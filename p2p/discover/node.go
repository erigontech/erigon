// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package discover

import (
	"net"
	"time"

	"github.com/erigontech/erigon/p2p/enode"
)

// node represents a host on the network.
// The fields of Node may not be modified.
type node struct {
	enode.Node
	addedAt        time.Time // time when the node was added to the table
	livenessChecks uint      // how often liveness was checked
}

func wrapNode(n *enode.Node) *node {
	return &node{Node: *n}
}

func wrapNodes(ns []*enode.Node) []*node {
	result := make([]*node, len(ns))
	for i, n := range ns {
		result[i] = wrapNode(n)
	}
	return result
}

func unwrapNode(n *node) *enode.Node {
	return &n.Node
}

func unwrapNodes(ns []*node) []*enode.Node {
	result := make([]*enode.Node, len(ns))
	for i, n := range ns {
		result[i] = unwrapNode(n)
	}
	return result
}

func (n *node) addr() *net.UDPAddr {
	return &net.UDPAddr{IP: n.IP(), Port: n.UDP()}
}

func (n *node) String() string {
	return n.Node.String()
}
