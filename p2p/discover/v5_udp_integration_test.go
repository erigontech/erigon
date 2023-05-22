//go:build integration

package discover

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/p2p/discover/v5wire"
	"github.com/ledgerwatch/log/v3"
)

// This test checks that calls with n replies may take up to n * respTimeout.
func TestUDPv5_callTimeoutReset(t *testing.T) {
	if runtime.GOOS == "windows" {
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
