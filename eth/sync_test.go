// Copyright 2015 The go-ethereum Authors
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

package eth

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
)

func TestFastSyncDisabling64(t *testing.T) { testFastSyncDisabling(t, 64) }
func TestFastSyncDisabling65(t *testing.T) { testFastSyncDisabling(t, 65) }

// Tests that fast sync gets disabled as soon as a real block is successfully
// imported into the blockchain.
func testFastSyncDisabling(t *testing.T, protocol int) {
	t.Skip("should be restored. skipped for turbo-geth")

	// Create a pristine protocol manager, check that fast sync is left enabled
	pmEmpty, clear := newTestProtocolManagerMust(t, downloader.FastSync, 0, nil, nil)
	defer clear()
	if atomic.LoadUint32(&empty.handler.fastSync) == 0 {
		t.Fatalf("fast sync disabled on pristine blockchain")
	}
	defer empty.close()

	// Create a full protocol manager, check that fast sync gets disabled
	pmFull, clearFull := newTestProtocolManagerMust(t, downloader.FastSync, 1024, nil, nil)
	defer clearFull()
	if atomic.LoadUint32(&full.handler.fastSync) == 1 {
		t.Fatalf("fast sync not disabled on non-empty blockchain")
	}
	defer full.close()

	// Sync up the two handlers
	emptyPipe, fullPipe := p2p.MsgPipe()
	defer emptyPipe.Close()
	defer fullPipe.Close()

	go pmFull.handle(pmFull.newPeer(protocol, p2p.NewPeer(enode.ID{}, "empty", nil), io2, pmFull.txpool.Get))   //nolint:errcheck
	go pmEmpty.handle(pmEmpty.newPeer(protocol, p2p.NewPeer(enode.ID{}, "full", nil), io1, pmEmpty.txpool.Get)) //nolint:errcheck
	defer emptyPeer.Close()
	defer fullPeer.Close()

	go empty.handler.runEthPeer(emptyPeer, func(peer *eth.Peer) error {
		return eth.Handle((*ethHandler)(empty.handler), peer)
	})
	go full.handler.runEthPeer(fullPeer, func(peer *eth.Peer) error {
		return eth.Handle((*ethHandler)(full.handler), peer)
	})
	// Wait a bit for the above handlers to start
	time.Sleep(250 * time.Millisecond)

	// Check that fast sync was disabled
	op := peerToSyncOp(downloader.FastSync, empty.handler.peers.peerWithHighestTD())
	if err := empty.handler.doSync(op); err != nil {
		t.Fatal("sync failed:", err)
	}
	if atomic.LoadUint32(&empty.handler.fastSync) == 1 {
		t.Fatalf("fast sync not disabled after successful synchronisation")
	}
}
