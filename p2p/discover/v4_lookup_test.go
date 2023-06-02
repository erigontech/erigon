// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

//go:build integration

package discover

import (
	"context"
	"crypto/ecdsa"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/discover/v4wire"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/log/v3"
)

func TestUDPv4_Lookup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	t.Parallel()
	logger := log.New()

	ctx := context.Background()
	ctx = contextWithReplyTimeout(ctx, time.Second)

	test := newUDPTestContext(ctx, t, logger)
	defer test.close()

	// Lookup on empty table returns no nodes.
	targetKey, _ := v4wire.DecodePubkey(crypto.S256(), v4wire.Pubkey(lookupTestnet.target))
	if results := test.udp.LookupPubkey(targetKey); len(results) > 0 {
		t.Fatalf("lookup on empty table returned %d results: %#v", len(results), results)
	}

	// Seed table with initial node.
	fillTable(test.table, []*node{wrapNode(lookupTestnet.node(256, 0))})

	// Answer lookup packets.
	go serveTestnet(test, lookupTestnet)

	// Start the lookup.
	results := test.udp.LookupPubkey(targetKey)

	// Verify result nodes.
	t.Logf("results:")
	for _, e := range results {
		t.Logf("  ld=%d, %x", enode.LogDist(lookupTestnet.target.ID(), e.ID()), e.ID().Bytes())
	}
	if len(results) != bucketSize {
		t.Errorf("wrong number of results: got %d, want %d", len(results), bucketSize)
	}
	checkLookupResults(t, lookupTestnet, results)
}

func TestUDPv4_LookupIterator(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	t.Parallel()
	logger := log.New()

	// Set up RandomNodes() to use expected keys instead of generating random ones.
	testNetPrivateKeys := lookupTestnet.privateKeys()
	testNetPrivateKeyIndex := -1
	privateKeyGenerator := func() (*ecdsa.PrivateKey, error) {
		testNetPrivateKeyIndex = (testNetPrivateKeyIndex + 1) % len(testNetPrivateKeys)
		return testNetPrivateKeys[testNetPrivateKeyIndex], nil
	}
	ctx := context.Background()
	ctx = contextWithReplyTimeout(ctx, time.Second)
	ctx = contextWithPrivateKeyGenerator(ctx, privateKeyGenerator)

	test := newUDPTestContext(ctx, t, logger)
	defer test.close()

	// Seed table with initial nodes.
	bootnodes := make([]*node, len(lookupTestnet.dists[256]))
	for i := range lookupTestnet.dists[256] {
		bootnodes[i] = wrapNode(lookupTestnet.node(256, i))
	}
	fillTable(test.table, bootnodes)
	go serveTestnet(test, lookupTestnet)

	// Create the iterator and collect the nodes it yields.
	iter := test.udp.RandomNodes()
	seen := make(map[enode.ID]*enode.Node)
	for limit := lookupTestnet.len(); iter.Next() && len(seen) < limit; {
		seen[iter.Node().ID()] = iter.Node()
	}
	iter.Close()

	// Check that all nodes in lookupTestnet were seen by the iterator.
	results := make([]*enode.Node, 0, len(seen))
	for _, n := range seen {
		results = append(results, n)
	}
	sortByID(results)
	want := lookupTestnet.nodes()
	if err := checkNodesEqual(results, want); err != nil {
		t.Fatal(err)
	}
}

// TestUDPv4_LookupIteratorClose checks that lookupIterator ends when its Close
// method is called.
func TestUDPv4_LookupIteratorClose(t *testing.T) {
	t.Parallel()
	logger := log.New()
	test := newUDPTest(t, logger)
	defer test.close()

	// Seed table with initial nodes.
	bootnodes := make([]*node, len(lookupTestnet.dists[256]))
	for i := range lookupTestnet.dists[256] {
		bootnodes[i] = wrapNode(lookupTestnet.node(256, i))
	}
	fillTable(test.table, bootnodes)
	go serveTestnet(test, lookupTestnet)

	it := test.udp.RandomNodes()
	if ok := it.Next(); !ok || it.Node() == nil {
		t.Fatalf("iterator didn't return any node")
	}

	it.Close()

	ncalls := 0
	for ; ncalls < 100 && it.Next(); ncalls++ {
		if it.Node() == nil {
			t.Error("iterator returned Node() == nil node after Next() == true")
		}
	}
	t.Logf("iterator returned %d nodes after close", ncalls)
	if it.Next() {
		t.Errorf("Next() == true after close and %d more calls", ncalls)
	}
	if n := it.Node(); n != nil {
		t.Errorf("iterator returned non-nil node after close and %d more calls", ncalls)
	}
}

func serveTestnet(test *udpTest, testnet *preminedTestnet) {
	for done := false; !done; {
		done = test.waitPacketOut(func(p v4wire.Packet, to *net.UDPAddr, hash []byte) {
			n, key := testnet.nodeByAddr(to)
			switch p.(type) {
			case *v4wire.Ping:
				test.packetInFrom(nil, key, to, &v4wire.Pong{Expiration: futureExp, ReplyTok: hash})
			case *v4wire.Findnode:
				dist := enode.LogDist(n.ID(), testnet.target.ID())
				nodes := testnet.nodesAtDistance(dist - 1)
				test.packetInFrom(nil, key, to, &v4wire.Neighbors{Expiration: futureExp, Nodes: nodes})
			}
		})
	}
}
