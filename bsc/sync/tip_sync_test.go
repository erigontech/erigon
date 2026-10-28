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

package bscsync

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

type tipHarness struct {
	t        *testing.T
	local    []*types.Block
	chainRW  *fakeChainRW
	p2p      *fakeP2P
	reg      *fakeTipRegistrar
	te       *TipEvents
	verifier *fakeVerifier
	network  *fakeNetwork
	ts       *tipSync
	runErr   chan error
}

// newTipHarness stores blocks 0..100 as the local canonical chain and starts the tip events.
// Call start to run the tip loop.
func newTipHarness(t *testing.T) *tipHarness {
	local := fakeChain(101)
	h := &tipHarness{
		t:        t,
		local:    local,
		chainRW:  newFakeChainRW(local),
		p2p:      &fakeP2P{peers: 2},
		reg:      newFakeTipRegistrar(),
		verifier: newFakeVerifier(),
		network:  newFakeNetwork(local),
		runErr:   make(chan error, 1),
	}
	logger := testlog.Logger(t, log.LvlCrit)
	h.te = NewTipEvents(logger, h.reg)
	ctx, cancel := context.WithCancel(t.Context())
	teErr := make(chan error, 1)
	go func() { teErr <- h.te.Run(ctx) }()
	for range 3 {
		<-h.reg.registered
	}
	h.ts = newTipSync(logger, h.chainRW, h.p2p, h.network.fetchBackwards, h.te, fakeParliaDB{h.chainRW}, h.verifier)
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, <-teErr, context.Canceled)
	})
	return h
}

func (h *tipHarness) start() {
	ctx, cancel := context.WithCancel(h.t.Context())
	go func() { h.runErr <- h.ts.run(ctx, h.chainRW.CurrentHeader(ctx)) }()
	h.t.Cleanup(func() {
		cancel()
		synctest.Wait()
	})
	synctest.Wait()
}

func (h *tipHarness) extend(parent *types.Block, n int, salt byte) []*types.Block {
	var blocks []*types.Block
	for range n {
		b := fakeBlock(parent.HeaderNoCopy(), salt)
		blocks = append(blocks, b)
		parent = b
	}
	h.network.add(blocks...)
	return blocks
}

func (h *tipHarness) announce(peer uint64, blocks ...*types.Block) {
	for _, b := range blocks {
		h.reg.sendBlock(peer, b)
		synctest.Wait()
	}
}

func (h *tipHarness) requireHead(b *types.Block) {
	h.t.Helper()
	require.Equal(h.t, b.Hash(), h.chainRW.lastFcu().head)
	require.Equal(h.t, b.Hash(), h.chainRW.CurrentHeader(context.Background()).Hash())
}

func TestTipLinearExtension(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		blocks := h.extend(h.local[100], 3, 0)

		h.announce(1, blocks[0])
		h.requireHead(blocks[0])
		h.announce(1, blocks[1], blocks[2])
		h.requireHead(blocks[2])
		require.Equal(t, 3, h.chainRW.numFcus())
		require.Equal(t, 3, h.chainRW.numInserted())
		require.Empty(t, h.p2p.penalties())
	})
}

func TestTipCoalescesQueuedEvents(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		blocks := h.extend(h.local[100], 5, 0)
		for _, b := range blocks {
			h.reg.sendBlock(1, b)
		}
		h.start()

		h.requireHead(blocks[4])
		require.Equal(t, 1, h.chainRW.numFcus())
		require.Equal(t, 5, h.chainRW.numInserted())
	})
}

func TestTipBackwardFillFromNewBlock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		blocks := h.extend(h.local[100], 10, 0)

		h.announce(1, blocks[9])
		h.requireHead(blocks[9])
		require.Equal(t, 10, h.chainRW.numInserted())
	})
}

func TestTipBackwardFillFromNewBlockHashes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		blocks := h.extend(h.local[100], 10, 0)

		pkt := eth.NewBlockHashesPacket{{Hash: blocks[9].Hash(), Number: blocks[9].NumberU64()}}
		h.reg.sendHashes(1, pkt)
		h.reg.sendHashes(2, pkt)
		synctest.Wait()
		h.requireHead(blocks[9])
		require.Equal(t, 10, h.chainRW.numInserted())
	})
}

func TestTipOneBlockReorg(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		outOfTurn := fakeBlockWithDifficulty(h.local[100].HeaderNoCopy(), 1, 1)
		inTurn := fakeBlockWithDifficulty(h.local[100].HeaderNoCopy(), 2, 2)
		h.network.add(outOfTurn, inTurn)

		h.announce(1, outOfTurn)
		h.requireHead(outOfTurn)
		h.announce(2, inTurn)
		h.requireHead(inTurn)

		h.announce(1, outOfTurn)
		h.requireHead(inTurn)
		require.Equal(t, 2, h.chainRW.numFcus())
	})
}

func TestTipInvalidSealIsRejectedAndPenalized(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		blocks := h.extend(h.local[100], 1, 0)
		h.verifier.invalid[blocks[0].Hash()] = true

		h.announce(3, blocks[0])
		require.Zero(t, h.chainRW.numFcus())
		require.Zero(t, h.chainRW.numInserted())
		require.Equal(t, []*p2p.PeerId{p2p.PeerIdFromUint64(3)}, h.p2p.penalties())
	})
}

func justify(v *fakeVerifier, at *types.Block, source, target *types.Block) {
	v.justified[at.Hash()] = &parlia.VoteData{
		SourceNumber: source.NumberU64(), SourceHash: source.Hash(),
		TargetNumber: target.NumberU64(), TargetHash: target.Hash(),
	}
}

func TestTipPrunesToFinalizedAfterCommit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		initRoot := h.ts.ccb.Root().Hash()
		blocks := h.extend(h.local[100], 3, 0)
		justify(h.verifier, blocks[1], blocks[0], blocks[1])
		justify(h.verifier, blocks[2], blocks[1], blocks[2])

		h.chainRW.setFcuStatus(func(common.Hash) execmodule.ExecutionStatus { return execmodule.ExecutionStatusBusy })
		h.announce(1, blocks[0], blocks[1])
		require.Equal(t, initRoot, h.ts.ccb.Root().Hash())

		h.chainRW.setFcuStatus(nil)
		h.announce(1, blocks[2])
		h.requireHead(blocks[2])
		require.Equal(t, fcuCall{head: blocks[2].Hash(), safe: blocks[2].Hash(), finalized: blocks[1].Hash()}, h.chainRW.lastFcu())
		require.Equal(t, blocks[1].Hash(), h.ts.ccb.Root().Hash())
	})
}

func TestTipBadBlock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		blocks := h.extend(h.local[100], 3, 0)
		bad := blocks[1].Hash()
		h.chainRW.setFcuStatus(func(head common.Hash) execmodule.ExecutionStatus {
			if head == bad {
				return execmodule.ExecutionStatusBadBlock
			}
			return execmodule.ExecutionStatusSuccess
		})

		h.announce(1, blocks[0])
		h.requireHead(blocks[0])
		h.announce(2, blocks[1])
		require.Equal(t, blocks[0].Hash(), h.ts.ccb.Tip().Hash())
		require.False(t, h.ts.ccb.ContainsHash(bad))
		require.Equal(t, []*p2p.PeerId{p2p.PeerIdFromUint64(2)}, h.p2p.penalties())

		fcus := h.chainRW.numFcus()
		h.announce(3, blocks[1], blocks[2])
		require.Equal(t, fcus, h.chainRW.numFcus())
		require.Equal(t, blocks[0].Hash(), h.chainRW.CurrentHeader(context.Background()).Hash())
		require.Len(t, h.p2p.penalties(), 3)
	})
}

func TestTipRetriesBusyAndTooFarAway(t *testing.T) {
	t.Parallel()
	for _, status := range []execmodule.ExecutionStatus{execmodule.ExecutionStatusBusy, execmodule.ExecutionStatusTooFarAway} {
		t.Run(status.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newTipHarness(t)
				h.start()
				blocks := h.extend(h.local[100], 1, 0)
				h.chainRW.setFcuStatus(func(common.Hash) execmodule.ExecutionStatus { return status })

				h.announce(1, blocks[0])
				require.Equal(t, h.local[100].Hash(), h.chainRW.CurrentHeader(context.Background()).Hash())

				h.chainRW.setFcuStatus(nil)
				time.Sleep(2 * tipRetryInterval)
				synctest.Wait()
				h.requireHead(blocks[0])
				require.Empty(t, h.runErr)
			})
		})
	}
}

func TestTipFallsBackOnUnexpectedForkChoiceStatus(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()
		blocks := h.extend(h.local[100], 1, 0)
		h.chainRW.setFcuStatus(func(common.Hash) execmodule.ExecutionStatus { return execmodule.ExecutionStatusMissingSegment })

		h.announce(1, blocks[0])
		require.ErrorIs(t, h.waitRunErr(), errFallBack)
	})
}

func (h *tipHarness) waitRunErr() error {
	select {
	case err := <-h.runErr:
		return err
	case <-time.After(time.Minute):
		require.FailNow(h.t, "tip loop did not return")
		return nil
	}
}

func (h *tipHarness) announceHead(num uint64, peers ...uint64) {
	pkt := eth.NewBlockHashesPacket{{Hash: common.Hash{0xaa, byte(num >> 8), byte(num)}, Number: num}}
	for _, peer := range peers {
		h.reg.sendHashes(peer, pkt)
	}
	synctest.Wait()
}

func TestTipFallsBackWhenFarBehind(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()

		h.announceHead(100+tipFallbackDistance, 1, 2)
		time.Sleep(2 * tipRetryInterval)
		synctest.Wait()
		require.Empty(t, h.runErr)

		h.announceHead(100+tipFallbackDistance+1, 1, 2)
		time.Sleep(2 * tipRetryInterval)
		synctest.Wait()
		require.ErrorIs(t, h.waitRunErr(), errFallBack)
	})
}

func TestTipFallsBackWhenStalled(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newTipHarness(t)
		h.start()

		time.Sleep(2 * tipStallTimeout)
		synctest.Wait()
		require.Empty(t, h.runErr, "no fallback while no peer announces a higher head")

		blocks := h.extend(h.local[100], 1, 0)
		h.announce(1, blocks[0])
		h.requireHead(blocks[0])
		h.announceHead(110, 1, 2)
		time.Sleep(tipStallTimeout - 2*tipRetryInterval)
		synctest.Wait()
		require.Empty(t, h.runErr)

		time.Sleep(3 * tipRetryInterval)
		synctest.Wait()
		require.ErrorIs(t, h.waitRunErr(), errFallBack)
	})
}
