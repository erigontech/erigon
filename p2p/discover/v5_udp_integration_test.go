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
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/discover/v5wire"
)

// This test checks that pending calls are re-sent when a handshake happens.
func TestUDPv5_callResend(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fix me on win please")
	}
	t.Parallel()
	logger := log.New()
	test := newUDPV5Test(t, logger)
	t.Cleanup(test.close)

	remote := test.getNode(test.remotekey, test.remoteaddr, logger).Node()
	done := make(chan error, 2)
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()
	go func() {
		_, err := test.udp.ping(remote)
		done <- err
	}()

	// Ping answered by WHOAREYOU.
	test.waitPacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, nonce v5wire.Nonce) {
		test.packetIn(&v5wire.Whoareyou{Nonce: nonce}, logger)
	})
	// Ping should be re-sent.
	test.waitPacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {
		test.packetIn(&v5wire.Pong{ReqID: p.ReqID}, logger)
	})
	// Answer the other ping.
	test.waitPacketOut(func(p *v5wire.Ping, addr *net.UDPAddr, _ v5wire.Nonce) {
		test.packetIn(&v5wire.Pong{ReqID: p.ReqID}, logger)
	})
	if err := <-done; err != nil {
		t.Fatalf("unexpected ping error: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("unexpected ping error: %v", err)
	}
}

// This test checks that calls with n replies may take up to n * respTimeout.
func TestUDPv5_callTimeoutReset(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fix me on win please")
	}
	t.Parallel()
	logger := log.New()

	replyTimeout := respTimeoutV5
	// This must be significantly lower than replyTimeout to not get "RPC timeout" error.
	singleReplyDelay := replyTimeout / (totalNodesResponseLimit - 1)
	if singleReplyDelay*totalNodesResponseLimit < replyTimeout {
		t.Fatalf("The total delay of all replies must exceed an individual reply timeout.")
	}
	if replyTimeout-singleReplyDelay < 50*time.Millisecond {
		t.Errorf("50ms is sometimes not enough on a slow CI to process a reply.")
	}

	ctx := context.Background()
	ctx = contextWithReplyTimeout(ctx, replyTimeout)

	test := newUDPV5TestContext(ctx, t, logger)
	t.Cleanup(test.close)

	// Launch the request:
	var (
		distance = uint(230)
		remote   = test.getNode(test.remotekey, test.remoteaddr, logger).Node()
		nodes    = nodesAtDistance(remote.ID(), int(distance), totalNodesResponseLimit)
		done     = make(chan error, 1)
	)
	go func() {
		_, err := test.udp.findnode(remote, []uint{distance})
		done <- err
	}()

	// Serve two responses, slowly.
	test.waitPacketOut(func(p *v5wire.Findnode, addr *net.UDPAddr, _ v5wire.Nonce) {
		for i := 0; i < totalNodesResponseLimit; i++ {
			time.Sleep(singleReplyDelay)
			test.packetIn(&v5wire.Nodes{
				ReqID: p.ReqID,
				Total: totalNodesResponseLimit,
				Nodes: nodesToRecords(nodes[i : i+1]),
			}, logger)
		}
	})
	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %q", err)
	}
}
