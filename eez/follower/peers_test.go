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

package follower_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
)

// composeBatches posts n batches of size blocks each, every Sync block
// holding a system transaction.
func composeBatches(t *testing.T, c *derivertest.Composer, n, size int) []*derivertest.Batch {
	t.Helper()
	var out []*derivertest.Batch
	for range n {
		b := composeBatch(c, size)
		c.Post(derivertest.Posting{Batch: b})
		out = append(out, b)
	}
	return out
}

// composeBatch composes a batch of size blocks, its Sync block holding a
// system transaction.
func composeBatch(c *derivertest.Composer, size int) *derivertest.Batch {
	blocks := make([]derivertest.Block, size)
	for i := range blocks {
		blocks[i] = derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}}
	}
	blocks[0].Txs = [][]byte{c.Transfer(bob, 1)}
	return c.Compose([]da.Action{{
		SourceRollupID: protocol.MainnetRollupID, TargetRollupID: 1, SourceAddress: bob, TargetAddress: alice,
		Data: []byte{}, Success: true, ReturnData: []byte{},
	}}, blocks...)
}

func blocksOf(batches ...*derivertest.Batch) []*types.Block {
	var out []*types.Block
	for _, b := range batches {
		out = append(out, b.Blocks...)
	}
	return out
}

// finalizeHead finalizes the L1 head and returns it.
func finalizeHead(t *testing.T, c *derivertest.Composer) *l1.Header {
	t.Helper()
	head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.NoError(t, c.L1.Finalize(head.Number))
	return head
}

// logsFrom records the first block of every log query.
type logsFrom struct {
	*recording.MemoryChain
	mu    sync.Mutex
	froms []uint64
}

func (r *logsFrom) Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	r.mu.Lock()
	r.froms = append(r.froms, from)
	r.mu.Unlock()
	return r.MemoryChain.Logs(ctx, from, to, address, topics)
}

func (r *logsFrom) first() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Min(r.froms)
}

// finalizedAt is an L1 whose finalized block is f, which the chain itself
// leaves unfinalized so that it can reorg below it.
type finalizedAt struct {
	*recording.MemoryChain
	f l1.Header
}

func (r finalizedAt) Finalized(context.Context) (*l1.Header, error) {
	h := r.f
	return &h, nil
}

// forkChoices records the fork choices made on a chain.
type forkChoices struct {
	deriver.Chain
	mu    sync.Mutex
	calls [][3]common.Hash
}

func (c *forkChoices) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	c.mu.Lock()
	c.calls = append(c.calls, [3]common.Hash{head, safe, finalized})
	c.mu.Unlock()
	return c.Chain.ForkChoice(ctx, head, safe, finalized)
}

func (c *forkChoices) made() [][3]common.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

// peersMode puts a follower in peers mode, fetching through f.
func peersMode(f *follower.Fetcher) func(*follower.Config) {
	return func(c *follower.Config) {
		c.Peers, c.LoopBlockLimit = f, 500
	}
}

// newPeersNode returns a node in peers mode at genesis, reading L1 from
// reader, whose one L2 peer, connected or not, serves the composer's chain.
func newPeersNode(t *testing.T, c *derivertest.Composer, reader l1.ChainReader, connected bool) (*node, *peerSentry, *forkChoices) {
	t.Helper()
	m, chain := derivertest.NewL2(t)
	fcs := &forkChoices{Chain: chain}
	s := newPeerSentry(c.L2, connected)
	n := (&node{m: m, dir: t.TempDir()}).restart(t, reader, fcs, peersMode(newFetcher(t, s, chain)))
	return n, s, fcs
}

func (n *node) logged(substrings ...string) bool {
	for line := range strings.SplitSeq(n.logs.String(), "\n") {
		if !slices.ContainsFunc(substrings, func(s string) bool { return !strings.Contains(line, s) }) {
			return true
		}
	}
	return false
}

// TestBootFromPeers: in peers mode a follower syncs the history below the
// root L1's finalized block F holds from its L2 peer, seeds derivation at F,
// and derives only the later settlements, scanning L1 from F, not from the
// registry's deploy block. It keeps no checkpoint.
func TestBootFromPeers(t *testing.T) {
	c := derivertest.NewComposer(t)
	synced := composeBatches(t, c, 2, 4)
	f := finalizeHead(t, c)
	derived := composeBatches(t, c, 1, 3)
	reader := &logsFrom{MemoryChain: c.L1}
	n, s, _ := newPeersNode(t, c, reader, true)

	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, blocksOf(append(synced, derived...)...)...)
	require.NotEmpty(t, s.sent())
	require.Equal(t, len(derived[0].Blocks), n.chain.Built(), "only the settlement above F is derived")
	require.Equal(t, f.Number, reader.first())
	status := n.follower.Status()
	require.Equal(t, synced[1].Blocks[len(synced[1].Blocks)-1].NumberU64(), status.Finalized)
	require.Equal(t, derived[0].Blocks[len(derived[0].Blocks)-1].NumberU64(), status.Safe)
	require.True(t, n.logged("eez.node.sync.mode", "mode=peers"))
	require.True(t, n.logged("eez.node.sync.seeded"))
	cp, err := deriver.LoadCheckpoint(n.dir)
	require.NoError(t, err)
	require.Nil(t, cp, "peers mode writes no checkpoint")
}

// TestBootFromPeersUnregistered: a rollup L1's finalized block does not
// list has no history before it; the follower asks its peers for nothing and
// derives from that block.
func TestBootFromPeersUnregistered(t *testing.T) {
	c := derivertest.NewComposer(t)
	c.Advance(3)
	f := finalizeHead(t, c)
	derived := composeBatches(t, c, 1, 3)
	reader := &logsFrom{MemoryChain: c.L1}
	n, s, _ := newPeersNode(t, c, reader, true)

	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, derived[0].Blocks...)
	require.Empty(t, s.sent())
	require.Equal(t, f.Number, reader.first())
}

// TestBootFromPeersWaits: in peers mode a follower waits at genesis, saying
// why, while L1 has no finalized block and then while no L2 peer is
// connected; it derives nothing from L1 meanwhile.
func TestBootFromPeersWaits(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	n, s, _ := newPeersNode(t, c, c.L1, false)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- n.follower.Boot(ctx) }()

	require.Eventually(t, func() bool { return n.logged("eez.node.sync.waiting", "no finalized block") }, 10*time.Second, 5*time.Millisecond)
	// Boot waits in place, saying why at most every 30 s.
	require.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return strings.Count(n.logs.String(), "event=eez.node.sync.waiting ") > 1
		}
	}, 100*time.Millisecond, 5*time.Millisecond)
	f := finalizeHead(t, c)
	require.Eventually(t, func() bool {
		return n.logged("eez.node.sync.waiting_for_peers", "peers=0", "head=0", "l1Finalized="+strconv.FormatUint(f.Number, 10))
	}, 10*time.Second, 5*time.Millisecond)
	head, err := n.chain.Head(t.Context())
	require.NoError(t, err)
	require.Zero(t, head.Number.Uint64())
	require.Zero(t, n.chain.Built())
	require.Empty(t, s.sent())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// TestBootFromPeersRestart: a follower restarted more than 1,024 blocks below
// the root L1 has finalized since rewinds to its own finalized block, with
// that block's parent finalized so that a settled sibling may replace it,
// syncs from its L2 peer again, executing and finalizing what it fetched
// every LoopBlockLimit blocks, and seeds at the new finalized L1 block.
func TestBootFromPeersRestart(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := composeBatches(t, c, 1, 3)
	finalizeHead(t, c)
	above := composeBatches(t, c, 1, 2)
	n, _, fcs := newPeersNode(t, c, c.L1, true)
	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, above[0].Blocks...)
	finalized := first[0].Blocks[len(first[0].Blocks)-1]

	more := composeBatches(t, c, 6, 200)
	f2 := finalizeHead(t, c)
	root := more[5].Blocks[len(more[5].Blocks)-1]
	require.Greater(t, root.NumberU64(), above[0].Blocks[1].NumberU64()+1024)
	before := len(fcs.made())
	again := n.restart(t, c.L1, fcs, peersMode(newFetcher(t, newPeerSentry(c.L2, true), fcs.Chain)))
	require.NoError(t, again.follower.Boot(t.Context()))

	calls := fcs.made()[before:]
	f, p := finalized.Hash(), finalized.ParentHash()
	require.Equal(t, [3]common.Hash{f, f, f}, calls[0], "rewound to the local finalized block")
	require.Equal(t, [3]common.Hash{f, p, p}, calls[1], "its parent finalized")
	at := slices.Index(calls, [3]common.Hash{root.Hash(), root.Hash(), root.Hash()})
	require.Positive(t, at, "fork choice to the root")
	for _, call := range calls[2:at] {
		require.Equal(t, [3]common.Hash{call[0], call[0], call[0]}, call, "fetched blocks are finalized as they execute")
	}
	require.Len(t, calls[2:at], 2, "a fork choice every 500 of 1,200 blocks")
	again.requireBlocks(t, blocksOf(append(append(first, above...), more...)...)...)
	require.Equal(t, root.NumberU64(), again.follower.Status().Finalized)
	r, ok := again.follower.Deriver().Last()
	require.True(t, ok)
	require.Equal(t, f2.Number, r.L1Block)
}

// TestBootFromPeersUnresponsiveSentry: a sentry that never answers a peer
// count holds neither a boot waiting for L2 peers nor its cancellation.
func TestBootFromPeersUnresponsiveSentry(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	finalizeHead(t, c)
	n, s, _ := newPeersNode(t, c, c.L1, false)
	s.holdCounts(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- n.follower.Boot(ctx) }()
	require.Eventually(t, func() bool { return n.logged("syncing L2 history from peers", "peers=0") }, 10*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("boot still running after its context ended")
	}
}

// TestBootFromPeersSeedFloor: an L1 reorg below the L1 block a follower in
// peers mode seeded at stops it: L1 no longer holds the evidence of the
// history its peers served.
func TestBootFromPeersSeedFloor(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 2, 3)
	head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	n, _, _ := newPeersNode(t, c, finalizedAt{c.L1, *head}, true)
	require.NoError(t, n.follower.Boot(t.Context()))

	c.ReorgL1(head.Number - 1)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.ErrorIs(t, n.follower.Run(ctx), follower.ErrSeedReorged)
}

// TestBootFromPeersNotOnL1Chain: a follower whose finalized block is not on
// the chain of the root L1 finalized stops, whether its peer serves that
// chain or it stores it already.
func TestBootFromPeersNotOnL1Chain(t *testing.T) {
	c := derivertest.NewComposer(t)
	// Longer than LoopBlockLimit: none of it may execute.
	settled := composeBatches(t, c, 3, 200)
	finalizeHead(t, c)
	other := derivertest.NewComposer(t)
	stray := other.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte("stray"), Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte("stray"), Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte("stray"), Txs: [][]byte{}},
	)
	tip := stray.Blocks[len(stray.Blocks)-1].Hash()
	for _, stored := range []bool{false, true} {
		n, s, fcs := newPeersNode(t, c, c.L1, true)
		if stored {
			require.NoError(t, fcs.Chain.Insert(t.Context(), blocksOf(settled...)))
		}
		require.NoError(t, fcs.Chain.Insert(t.Context(), stray.Blocks))
		require.NoError(t, fcs.Chain.ForkChoice(t.Context(), tip, tip, tip))
		before := len(fcs.made())

		require.ErrorIs(t, n.follower.Boot(t.Context()), follower.ErrNotOnL1Chain, "stored %t", stored)
		require.False(t, n.logged("boot catch-up failed"), "boot stops without retrying")
		for _, call := range fcs.made()[before:] {
			require.False(t, slices.ContainsFunc(blocksOf(settled...), func(b *types.Block) bool { return b.Hash() == call[0] }),
				"fork choice to %x of the root's chain", call[0])
		}
		if stored {
			require.Empty(t, s.sent())
		}
	}
}

// TestBootFromPeersChildOfFinalized: a root one block above the local
// finalized block, which the downloader starts its blocks from.
func TestBootFromPeersChildOfFinalized(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := composeBatches(t, c, 1, 3)
	finalizeHead(t, c)
	n, _, fcs := newPeersNode(t, c, c.L1, true)
	require.NoError(t, n.follower.Boot(t.Context()))
	f := first[0].Blocks[len(first[0].Blocks)-1]

	next := composeBatches(t, c, 1, 1)
	require.Equal(t, f.Hash(), next[0].Blocks[0].ParentHash())
	finalizeHead(t, c)
	again := n.restart(t, c.L1, fcs, peersMode(newFetcher(t, newPeerSentry(c.L2, true), fcs.Chain)))
	require.NoError(t, again.follower.Boot(t.Context()))
	again.requireBlocks(t, next[0].Blocks...)
}

// TestBootFromPeersResumedFinalized: a local finalized block that L1 later
// replaced by a sibling, a resume of a partly applied batch, does not stop
// a sync from peers: only its parent must stay on the chain.
func TestBootFromPeersResumedFinalized(t *testing.T) {
	c := derivertest.NewComposer(t)
	inbound := func(to common.Address, value uint64) da.Action {
		return da.Action{
			SourceRollupID: protocol.MainnetRollupID, TargetRollupID: 1, SourceAddress: common.HexToAddress("0x11"), TargetAddress: to,
			Value: *uint256.NewInt(value), Data: []byte{}, Success: true, ReturnData: []byte{},
		}
	}
	rival := c.Compose([]da.Action{inbound(alice, 7), inbound(bob, 8)},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 1)}},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}},
	)
	resumed := c.Resume(rival, 2, derivertest.Block{Coinbase: bob, Extra: []byte("resumed"), Txs: [][]byte{c.Transfer(alice, 9)}})
	c.Post(derivertest.Posting{Batch: rival, Applied: []int{0, 1}})
	finalizeHead(t, c)
	n, _, fcs := newPeersNode(t, c, c.L1, true)
	require.NoError(t, n.follower.Boot(t.Context()))
	fin, err := fcs.Chain.Finalized(t.Context())
	require.NoError(t, err)
	require.Equal(t, rival.Syncs[1].Hash(), fin, "the partly applied Sync block is finalized")

	c.Post(derivertest.Posting{Batch: resumed, Applied: []int{2}})
	finalizeHead(t, c)
	again := n.restart(t, c.L1, fcs, peersMode(newFetcher(t, newPeerSentry(c.L2, true), fcs.Chain)))
	require.NoError(t, again.follower.Boot(t.Context()))
	again.requireBlocks(t, resumed.Blocks...)
}

// cancelAfterMove cancels its context after the first fork choice of a sync
// from peers, as a node killed mid-sync.
type cancelAfterMove struct {
	deriver.Chain
	rewound map[common.Hash]bool
	cancel  context.CancelFunc
}

func (c *cancelAfterMove) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	err := c.Chain.ForkChoice(ctx, head, safe, finalized)
	if err == nil && !c.rewound[head] {
		c.cancel()
	}
	return err
}

// TestBootFromPeersInterrupted: a follower killed after a sync from peers
// executed more blocks than the execution module can unwind boots again.
func TestBootFromPeersInterrupted(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := composeBatches(t, c, 1, 3)
	finalizeHead(t, c)
	n, _, fcs := newPeersNode(t, c, c.L1, true)
	require.NoError(t, n.follower.Boot(t.Context()))
	f := first[0].Blocks[len(first[0].Blocks)-1]

	more := composeBatches(t, c, 6, 200)
	finalizeHead(t, c)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	killed := n.restart(t, c.L1, &cancelAfterMove{Chain: fcs.Chain, rewound: map[common.Hash]bool{f.Hash(): true}, cancel: cancel},
		peersMode(newFetcher(t, newPeerSentry(c.L2, true), fcs.Chain)))
	require.ErrorIs(t, killed.follower.Boot(ctx), context.Canceled)
	head, err := fcs.Chain.Head(t.Context())
	require.NoError(t, err)
	require.Greater(t, head.Number.Uint64(), f.NumberU64()+96, "past the blocks the module keeps changesets for")

	again := n.restart(t, c.L1, fcs, peersMode(newFetcher(t, newPeerSentry(c.L2, true), fcs.Chain)))
	require.NoError(t, again.follower.Boot(t.Context()))
	again.requireBlocks(t, blocksOf(more...)...)
}

// TestBootFromPeersSeedGenesisFinalized: a rollup L1's finalized block does
// not list on a datadir that finalized blocks of another deployment stops:
// the execution module cannot replay below them.
func TestBootFromPeersSeedGenesisFinalized(t *testing.T) {
	c1 := derivertest.NewComposer(t)
	composeBatches(t, c1, 1, 3)
	finalizeHead(t, c1)
	n, _, fcs := newPeersNode(t, c1, c1.L1, true)
	require.NoError(t, n.follower.Boot(t.Context()))

	c2 := derivertest.NewComposer(t)
	require.NoError(t, c2.L1.Finalize(derivertest.Config().L2.(*config.L2Config).RegistryDeployBlock-1))
	c2.Post(derivertest.Posting{Batch: c2.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte("other"), Txs: [][]byte{}})})
	again := n.restart(t, c2.L1, fcs, peersMode(newFetcher(t, newPeerSentry(c2.L2, true), fcs.Chain)))
	require.ErrorIs(t, again.follower.Boot(t.Context()), follower.ErrNotOnL1Chain)
	require.False(t, again.logged("boot catch-up failed"), "boot stops without retrying")
}

// TestBootFromPeersFinalizedGenesis: L1's finalized block is its genesis,
// where the rollup is not registered; derivation starts after it.
func TestBootFromPeersFinalizedGenesis(t *testing.T) {
	c := derivertest.NewComposer(t)
	derived := composeBatches(t, c, 1, 3)
	g, err := c.L1.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	n, _, _ := newPeersNode(t, c, finalizedAt{c.L1, *g}, true)
	require.NoError(t, n.follower.Boot(t.Context()))
	n.requireBlocks(t, derived[0].Blocks...)
}

// refusing refuses every fork choice to another head than genesis.
type refusing struct {
	deriver.Chain
	genesis common.Hash
}

func (c refusing) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	if head == c.genesis {
		return c.Chain.ForkChoice(ctx, head, safe, finalized)
	}
	return fmt.Errorf("%w: fork choice %x: BadBlock", deriver.ErrForkChoiceRefused, head)
}

// TestBootFromPeersRefusedForkChoice: a sync from peers whose blocks the
// execution module refuses stops boot, at the root or while it fetches: no
// peer can mend it.
func TestBootFromPeersRefusedForkChoice(t *testing.T) {
	c := derivertest.NewComposer(t)
	composeBatches(t, c, 1, 3)
	finalizeHead(t, c)
	for _, limit := range []uint64{500, 2} {
		m, chain := derivertest.NewL2(t)
		n := (&node{m: m, dir: t.TempDir()}).restart(t, c.L1, refusing{chain, m.Genesis.Hash()},
			peersMode(newFetcher(t, newPeerSentry(c.L2, true), chain)), func(c *follower.Config) { c.LoopBlockLimit = limit })
		err := n.follower.Boot(t.Context())
		require.ErrorIs(t, err, deriver.ErrForkChoiceRefused, "limit %d", limit)
		require.ErrorIs(t, err, follower.ErrPeerHistoryRefused, "limit %d", limit)
		require.False(t, n.logged("boot catch-up failed"), "boot stops without retrying")
		require.False(t, n.logged("eez.node.sync.waiting_for_peers"), "limit %d", limit)
	}
}
