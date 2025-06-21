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

package discover

import (
	"math/rand"
	"net"
	"runtime"
	"sort"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/discover/v5wire"
	"github.com/erigontech/erigon/p2p/enode"
)

// This test checks that lookup works.
func TestUDPv5_lookup(t *testing.T) {
	t.Skip("issue #6223")
	t.Parallel()
	logger := log.New()
	test := newUDPV5Test(t, logger)
	t.Cleanup(test.close)

	// Lookup on empty table returns no nodes.
	if results := test.udp.Lookup(lookupTestnet.target.ID()); len(results) > 0 {
		t.Fatalf("lookup on empty table returned %d results: %#v", len(results), results)
	}

	// Ensure the tester knows all nodes in lookupTestnet by IP.
	for d, nn := range lookupTestnet.dists {
		for i, key := range nn {
			n := lookupTestnet.node(d, i)
			test.getNode(key, &net.UDPAddr{IP: n.IP(), Port: n.UDP()}, logger)
		}
	}

	// Seed table with initial node.
	initialNode := lookupTestnet.node(256, 0)
	fillTable(test.table, []*node{wrapNode(initialNode)})

	// Start the lookup.
	resultC := make(chan []*enode.Node, 1)
	go func() {
		resultC <- test.udp.Lookup(lookupTestnet.target.ID())
		test.close()
	}()

	// Answer lookup packets.
	asked := make(map[enode.ID]bool)
	for done := false; !done; {
		done = test.waitPacketOut(func(p v5wire.Packet, to *net.UDPAddr, _ v5wire.Nonce) {
			recipient, key := lookupTestnet.nodeByAddr(to)
			switch p := p.(type) {
			case *v5wire.Ping:
				test.packetInFrom(key, to, &v5wire.Pong{ReqID: p.ReqID}, logger)
			case *v5wire.Findnode:
				if asked[recipient.ID()] {
					t.Error("Asked node", recipient.ID(), "twice")
				}
				asked[recipient.ID()] = true
				nodes := lookupTestnet.neighborsAtDistances(recipient, p.Distances, 16)
				t.Logf("Got FINDNODE for %v, returning %d nodes", p.Distances, len(nodes))
				for _, resp := range packNodes(p.ReqID, nodes) {
					test.packetInFrom(key, to, resp, logger)
				}
			}
		})
	}

	// Verify result nodes.
	results := <-resultC
	checkLookupResults(t, lookupTestnet, results)
}

// Real sockets, real crypto: this test checks end-to-end connectivity for UDPv5.
func TestUDPv5_lookupE2E(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fix me on win please")
	}
	t.Parallel()
	logger := log.New()

	bootNode := startLocalhostV5(t, Config{}, logger)
	bootNodeRec := bootNode.Self()

	const N = 5
	nodes := []*UDPv5{bootNode}
	for len(nodes) < N {
		cfg := Config{
			Bootnodes: []*enode.Node{bootNodeRec},
		}
		node := startLocalhostV5(t, cfg, logger)
		nodes = append(nodes, node)
	}

	defer func() {
		for _, node := range nodes {
			node.Close()
		}
	}()

	last := nodes[N-1]
	target := nodes[rand.Intn(N-2)].Self()

	// It is expected that all nodes can be found.
	expectedResult := make([]*enode.Node, len(nodes))
	for i := range nodes {
		expectedResult[i] = nodes[i].Self()
	}
	sort.Slice(expectedResult, func(i, j int) bool {
		return enode.DistCmp(target.ID(), expectedResult[i].ID(), expectedResult[j].ID()) < 0
	})

	// Do the lookup.
	results := last.Lookup(target.ID())
	if err := checkNodesEqual(results, expectedResult); err != nil {
		t.Fatalf("lookup returned wrong results: %v", err)
	}
}
