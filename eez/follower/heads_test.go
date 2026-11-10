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
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/overlay"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

var sequencerAddr = common.HexToAddress("0x5e9")

// signedHead is the head of b from the source (sequencerAddr, epochMs),
// carrying b when withBlock is set.
func signedHead(b *types.Block, epochMs, counter uint64, withBlock bool) *seqsig.VerifiedHead {
	h := &seqsig.VerifiedHead{Signer: sequencerAddr, Head: seqsig.Head{
		RollupID:   1,
		Number:     b.NumberU64(),
		BlockHash:  b.Hash(),
		ParentHash: b.ParentHash(),
		EpochMs:    epochMs,
		Counter:    counter,
	}}
	if withBlock {
		h.Block = b
	}
	return h
}

// headsOptions configure a headsNode.
type headsOptions struct {
	// peers gives the node its L2 peers: a fetcher of blocks and their count.
	peers func(t *testing.T, chain deriver.Chain) follower.BlockFetcher
	// wrap, when set, wraps the node's chain.
	wrap func(deriver.Chain) deriver.Chain
	// heads, when set, delivers the node's signed heads instead of a channel
	// the test sends on.
	heads  <-chan *seqsig.VerifiedHead
	config func(*follower.Config)
	// noBoot leaves the follower unbooted, for Run to boot.
	noBoot bool
}

// headsNode is a booted follower whose head follows signed heads.
type headsNode struct {
	m        *execmoduletester.ExecModuleTester
	chain    *deriver.ExecChain
	follower *follower.Follower
	heads    chan *seqsig.VerifiedHead
	logs     *syncBuffer
}

func newHeadsNode(t *testing.T, reader l1.ChainReader, o headsOptions) *headsNode {
	t.Helper()
	m, chain := derivertest.NewL2(t)
	n := &headsNode{m: m, chain: chain, heads: make(chan *seqsig.VerifiedHead, 64), logs: &syncBuffer{}}
	cfg := testConfig
	cfg.Dir = t.TempDir()
	cfg.Heads = n.heads
	if o.heads != nil {
		cfg.Heads = o.heads
	}
	if o.peers != nil {
		cfg.Fetcher = o.peers(t, chain)
	}
	if o.config != nil {
		o.config(&cfg)
	}
	var local deriver.Chain = chain
	if o.wrap != nil {
		local = o.wrap(chain)
	}
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(n.logs, log.LogfmtFormat())))
	f, err := follower.New(reader, local, derivertest.Config(), cfg, logger)
	require.NoError(t, err)
	if !o.noBoot {
		require.NoError(t, f.Boot(t.Context()))
	}
	n.follower = f
	return n
}

// run runs the node's follower until the test ends.
func (n *headsNode) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.follower.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		if t.Failed() {
			t.Log(n.logs.String())
		}
	})
}

// awaitHead waits for the node's head to be want.
func (n *headsNode) awaitHead(t *testing.T, want *types.Block) {
	t.Helper()
	require.Eventually(t, func() bool {
		head, err := n.chain.Head(context.Background())
		return err == nil && head.Hash() == want.Hash()
	}, 30*time.Second, 5*time.Millisecond, "head %d", want.NumberU64())
}

// step sends heads and steps the node's unsafe head.
func (n *headsNode) step(t *testing.T, heads ...*seqsig.VerifiedHead) {
	t.Helper()
	for _, h := range heads {
		n.heads <- h
	}
	require.NoError(t, n.follower.StepOverlay(t.Context()))
}

func (n *headsNode) forkChoice(t *testing.T) (head, safe, finalized common.Hash) {
	t.Helper()
	fc, err := n.m.ExecModule.GetForkChoice(t.Context())
	require.NoError(t, err)
	return fc.HeadHash, fc.SafeHash, fc.FinalizedHash
}

func (n *headsNode) requireHead(t *testing.T, want *types.Block) {
	t.Helper()
	head, err := n.chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, want.Hash(), head.Hash(), "head %d, want %d", head.Number.Uint64(), want.NumberU64())
}

// stored reports whether the node stores b, canonical or not.
func (n *headsNode) stored(t *testing.T, b *types.Block) bool {
	t.Helper()
	h, err := n.chain.HeaderByHash(t.Context(), b.Hash())
	require.NoError(t, err)
	return h != nil
}

// events returns the node's log lines of event.
func (n *headsNode) events(event string) []string {
	var out []string
	for line := range strings.Lines(n.logs.String()) {
		if strings.Contains(line, " event="+event+" ") || strings.HasSuffix(strings.TrimSpace(line), " event="+event) {
			out = append(out, line)
		}
	}
	return out
}

// mapFetcher serves the blocks it holds as connected L2 peers would, and
// records the hashes fetched.
type mapFetcher struct {
	chain  deriver.Chain
	blocks map[common.Hash]*types.Block
	// hook, when set, runs first; its error fails the fetch.
	hook func(ctx context.Context) error

	mu      sync.Mutex
	fetched []common.Hash
	opts    []follower.FetchOptions
}

func newMapFetcher(chain deriver.Chain, blocks ...*types.Block) *mapFetcher {
	f := &mapFetcher{chain: chain, blocks: make(map[common.Hash]*types.Block)}
	for _, b := range blocks {
		f.blocks[b.Hash()] = b
	}
	return f
}

func (f *mapFetcher) Fetch(ctx context.Context, hash common.Hash, opts follower.FetchOptions) (*types.Header, error) {
	f.mu.Lock()
	f.fetched = append(f.fetched, hash)
	f.opts = append(f.opts, opts)
	hook := f.hook
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	var missing []*types.Block
	for h := hash; ; {
		local, err := f.chain.HeaderByHash(ctx, h)
		if err != nil {
			return nil, err
		}
		if local != nil {
			break
		}
		b, ok := f.blocks[h]
		if !ok {
			return nil, fmt.Errorf("block %x not served", h)
		}
		missing = append(missing, b)
		h = b.ParentHash()
	}
	slices.Reverse(missing)
	if len(missing) > 0 {
		if err := f.chain.Insert(ctx, missing); err != nil {
			return nil, err
		}
		if opts.Inserted != nil {
			if err := opts.Inserted(ctx, missing); err != nil {
				return nil, err
			}
		}
	}
	return f.chain.HeaderByHash(ctx, hash)
}

func (f *mapFetcher) Peers() uint64 { return 1 }

func (f *mapFetcher) hashes() []common.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.fetched)
}

func (f *mapFetcher) options() []follower.FetchOptions {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.opts)
}

func (f *mapFetcher) setHook(hook func(ctx context.Context) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hook = hook
}

func mapPeers(blocks ...*types.Block) (*mapFetcher, func(*testing.T, deriver.Chain) follower.BlockFetcher) {
	f := &mapFetcher{}
	return f, func(_ *testing.T, chain deriver.Chain) follower.BlockFetcher {
		*f = *newMapFetcher(chain, blocks...)
		return f
	}
}

// forks is a composer whose block 1 is settled, with two unsettled branches
// on it: A, a2 and a3, and B, b2 and b3, each block its own batch.
type forks struct {
	c              *derivertest.Composer
	settled        *types.Block
	a2, a3, b2, b3 *derivertest.Batch
}

func newForks(t *testing.T) *forks {
	t.Helper()
	c := derivertest.NewComposer(t)
	f := &forks{c: c}
	s := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	c.Post(derivertest.Posting{Batch: s})
	f.settled = s.Blocks[0]
	block := func(coinbase common.Address, extra string) derivertest.Block {
		return derivertest.Block{Coinbase: coinbase, Extra: []byte(extra), Txs: [][]byte{}}
	}
	// Each batch starts from the block of the one before.
	f.a2 = c.Compose(nil, block(alice, "a2"))
	c.Settle(f.a2.Blocks[0].Header())
	f.a3 = c.Compose(nil, block(alice, "a3"))
	c.Rewind(f.settled.Header())
	f.b2 = c.Compose(nil, block(bob, "b2"))
	c.Settle(f.b2.Blocks[0].Header())
	f.b3 = c.Compose(nil, block(bob, "b3"))
	c.Settle(f.b3.Blocks[0].Header())
	return f
}

func (f *forks) blocks() []*types.Block {
	return []*types.Block{f.a2.Blocks[0], f.a3.Blocks[0], f.b2.Blocks[0], f.b3.Blocks[0]}
}

// newHead applies a new L1 head to n's follower.
func (f *forks) newHead(t *testing.T, n *headsNode) error {
	t.Helper()
	head, err := f.c.L1.Head(t.Context())
	require.NoError(t, err)
	return n.follower.Handle(t.Context(), l1.NewHead{Head: *head})
}

// failingExtend fails to build block number while fail is set.
type failingExtend struct {
	deriver.Chain
	number uint64
	fail   atomic.Bool
}

var errBuild = errors.New("build failed")

func (c *failingExtend) Extend(ctx context.Context, parent *types.Header, in deriver.HeaderInputs, txs []types.Transaction, safe, finalized common.Hash) (*types.BlockWithReceipts, error) {
	if c.fail.Load() && in.Number == c.number {
		return nil, errBuild
	}
	return c.Chain.Extend(ctx, parent, in, txs, safe, finalized)
}

// countingForkChoice counts the fork choices to each head, and fails the
// first failures of them to failHead.
type countingForkChoice struct {
	deriver.Chain
	failHead common.Hash
	failures int

	mu    sync.Mutex
	heads map[common.Hash]int
}

var errForkChoice = errors.New("fork choice failed")

func (c *countingForkChoice) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	c.mu.Lock()
	if c.heads == nil {
		c.heads = make(map[common.Hash]int)
	}
	c.heads[head]++
	fail := head == c.failHead && c.heads[head] <= c.failures
	c.mu.Unlock()
	if fail {
		return errForkChoice
	}
	return c.Chain.ForkChoice(ctx, head, safe, finalized)
}

func (c *countingForkChoice) count(head common.Hash) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.heads[head]
}

// TestSignedHeadForkChoice is T-HEAD-2 on the follower loop: the head follows
// the highest-version signed head that descends from safe, fetching its
// missing ancestors, and safe and finalized stay derivation's.
func TestSignedHeadForkChoice(t *testing.T) {
	t.Run("highest version first", func(t *testing.T) {
		f := newForks(t)
		fetcher, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})
		_, safe, finalized := n.forkChoice(t)

		// B arrives first, A from a later epoch after it.
		n.step(t, signedHead(f.b3.Blocks[0], 100, 2, true), signedHead(f.a3.Blocks[0], 200, 2, true))
		n.requireHead(t, f.a3.Blocks[0])
		require.Equal(t, []common.Hash{f.a2.Blocks[0].Hash()}, fetcher.hashes(), "only the missing ancestor")
		head, gotSafe, gotFinalized := n.forkChoice(t)
		require.Equal(t, f.a3.Blocks[0].Hash(), head)
		require.Equal(t, safe, gotSafe, "safe stays derivation's")
		require.Equal(t, finalized, gotFinalized, "finalized stays derivation's")
		require.Len(t, n.events("eez.node.follower.head.advanced"), 1)
		require.Contains(t, n.events("eez.node.follower.head.advanced")[0], "imported=2")

		// A replay of an older A head is ignored.
		n.step(t, signedHead(f.a2.Blocks[0], 200, 1, true))
		n.requireHead(t, f.a3.Blocks[0])

		// A newer A head on a block the node has moves the head back to it.
		n.step(t, signedHead(f.a2.Blocks[0], 200, 3, true))
		n.requireHead(t, f.a2.Blocks[0])
		require.Len(t, fetcher.hashes(), 1)
	})

	t.Run("fetched blocks are adopted", func(t *testing.T) {
		f := newForks(t)
		fetcher, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})

		// The head comes without its block: one fetch inserts the target and
		// its ancestor, which the next decision finds local.
		n.step(t, signedHead(f.a3.Blocks[0], 200, 2, false))
		n.requireHead(t, f.a3.Blocks[0])
		require.Equal(t, []common.Hash{f.a3.Blocks[0].Hash()}, fetcher.hashes())
		require.Contains(t, n.events("eez.node.follower.head.advanced")[0], "imported=2")
		n.step(t)
		require.Len(t, fetcher.hashes(), 1, "no fetch for the head the node follows")
	})

	t.Run("a provable conflict with safe is skipped", func(t *testing.T) {
		f := newForks(t)
		f.c.Post(derivertest.Posting{Batch: f.b2})
		_, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})
		require.Equal(t, uint64(2), n.follower.Deriver().Safe())

		a3 := signedHead(f.a3.Blocks[0], 200, 2, true)
		n.step(t, a3, signedHead(f.b3.Blocks[0], 100, 2, true))
		n.requireHead(t, f.b3.Blocks[0])
		n.step(t)
		rejected := n.events("eez.node.follower.head.rejected")
		require.Len(t, rejected, 1, "a head retried every tick warns once")
		require.Contains(t, rejected[0], a3.Head.BlockHash.Hex())
		require.Contains(t, rejected[0], seqsig.ErrConflictsSafe.Error())
	})

	t.Run("failover when L1 settles a block the preferred chain lacks", func(t *testing.T) {
		f := newForks(t)
		_, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})
		n.step(t, signedHead(f.a3.Blocks[0], 200, 2, true), signedHead(f.b3.Blocks[0], 100, 2, true))
		n.requireHead(t, f.a3.Blocks[0])

		f.c.Post(derivertest.Posting{Batch: f.b2})
		require.NoError(t, f.newHead(t, n))
		require.Equal(t, uint64(2), n.follower.Deriver().Safe())
		n.step(t)
		n.requireHead(t, f.b3.Blocks[0])
		_, safe, _ := n.forkChoice(t)
		require.Equal(t, f.b2.Blocks[0].Hash(), safe)
	})

	t.Run("held while derivation has indexed a batch above safe", func(t *testing.T) {
		f := newForks(t)
		b4 := f.c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte("b4"), Txs: [][]byte{}})
		fetcher, peers := mapPeers(append(f.blocks(), b4.Blocks[0])...)
		var chain *failingExtend
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers, wrap: func(c deriver.Chain) deriver.Chain {
			chain = &failingExtend{Chain: c, number: 3}
			return chain
		}})

		// b2 and b3 settle in one L1 block; b3 fails to build, leaving b2
		// indexed above safe.
		f.c.Post(derivertest.Posting{Batch: f.b2}, derivertest.Posting{Batch: f.b3})
		chain.fail.Store(true)
		require.ErrorIs(t, f.newHead(t, n), errBuild)
		require.Equal(t, uint64(2), n.follower.Deriver().Cursor())
		require.Equal(t, uint64(1), n.follower.Deriver().Safe())
		n.requireHead(t, f.b2.Blocks[0])

		n.step(t, signedHead(b4.Blocks[0], 100, 3, true))
		n.requireHead(t, f.b2.Blocks[0])
		require.Empty(t, fetcher.hashes(), "a held follower fetches nothing")
		require.NotEmpty(t, n.events("eez.node.follower.head.held"))

		chain.fail.Store(false)
		require.NoError(t, n.follower.Sync(t.Context()))
		require.Equal(t, uint64(3), n.follower.Deriver().Safe())
		n.step(t)
		n.requireHead(t, b4.Blocks[0])
		require.Empty(t, fetcher.hashes())
	})

	t.Run("a target more than 1,024 blocks ahead waits for derivation", func(t *testing.T) {
		f := newForks(t)
		fetcher, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})
		ahead := signedHead(types.NewBlock(&types.Header{Number: *uint256.NewInt(1 + seqsig.MaxAncestryWalk + 1), ParentHash: common.Hash{1}}, nil, nil, nil, nil, nil), 300, 1, false)

		n.step(t, ahead, signedHead(f.a2.Blocks[0], 200, 1, true))
		n.step(t)
		n.requireHead(t, f.settled)
		require.Empty(t, fetcher.hashes())
		further := signedHead(types.NewBlock(&types.Header{Number: *uint256.NewInt(1 + seqsig.MaxAncestryWalk + 2), ParentHash: ahead.Head.BlockHash}, nil, nil, nil, nil, nil), 300, 2, false)
		n.step(t, further)
		n.requireHead(t, f.settled)
		waiting := n.events("eez.node.follower.head.ahead")
		require.Len(t, waiting, 2, "once per head")
		require.Contains(t, waiting[0], ahead.Head.BlockHash.Hex())
		require.Contains(t, waiting[0], "lvl=info", "on entering the wait")
		require.Contains(t, waiting[1], further.Head.BlockHash.Hex())
		require.Contains(t, waiting[1], "lvl=dbug", "while waiting")
	})

	t.Run("a head that fails to import is not executed again", func(t *testing.T) {
		f := newForks(t)
		header := f.a2.Blocks[0].Header()
		header.Root = common.Hash{1}
		bad := f.a2.Blocks[0].WithSeal(header)
		var chain *countingForkChoice
		n := newHeadsNode(t, f.c.L1, headsOptions{wrap: func(c deriver.Chain) deriver.Chain {
			chain = &countingForkChoice{Chain: c}
			return chain
		}})

		n.step(t, signedHead(bad, 200, 1, true))
		n.step(t)
		n.requireHead(t, f.settled)
		require.Equal(t, 1, chain.count(bad.Hash()))
		require.Len(t, n.events("eez.node.follower.head.rejected"), 1)
	})

	t.Run("a head whose import fails for another cause is retried", func(t *testing.T) {
		f := newForks(t)
		a2 := f.a2.Blocks[0]
		var chain *countingForkChoice
		n := newHeadsNode(t, f.c.L1, headsOptions{wrap: func(c deriver.Chain) deriver.Chain {
			chain = &countingForkChoice{Chain: c, failHead: a2.Hash(), failures: 1}
			return chain
		}})

		n.step(t, signedHead(a2, 200, 1, true))
		n.requireHead(t, f.settled)
		require.Len(t, n.events("eez.node.follower.head.rejected"), 1)
		n.step(t)
		n.requireHead(t, a2)
		require.Equal(t, 2, chain.count(a2.Hash()))
	})

	t.Run("unknown ancestry is not a conflict", func(t *testing.T) {
		f := newForks(t)
		fetcher, peers := mapPeers(f.b2.Blocks[0], f.b3.Blocks[0])
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})

		// A's blocks are served by nobody; B's head is in hand on a local
		// parent.
		n.step(t, signedHead(f.a3.Blocks[0], 200, 2, false), signedHead(f.b2.Blocks[0], 100, 1, true))
		n.step(t)
		n.requireHead(t, f.settled)
		require.Equal(t, []common.Hash{f.a3.Blocks[0].Hash(), f.a3.Blocks[0].Hash()}, fetcher.hashes())
		require.Len(t, n.events("eez.node.follower.head.rejected"), 1)
	})

	t.Run("a failed fetch is retried", func(t *testing.T) {
		f := newForks(t)
		fetcher, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})
		failures := []error{errors.New("peer went away"), errors.New("request timed out")}
		fetcher.setHook(func(context.Context) error {
			if len(failures) == 0 {
				return nil
			}
			err := failures[0]
			failures = failures[1:]
			return err
		})

		a3 := signedHead(f.a3.Blocks[0], 200, 2, false)
		n.step(t, a3)
		n.step(t)
		n.requireHead(t, f.settled)
		rejected := n.events("eez.node.follower.head.rejected")
		require.Len(t, rejected, 2, "once per reason")
		require.Contains(t, rejected[0], "peer went away")
		require.Contains(t, rejected[1], "request timed out")
		n.step(t)
		n.requireHead(t, f.a3.Blocks[0])
	})

	t.Run("a fetch cut by shutdown is not a rejection", func(t *testing.T) {
		f := newForks(t)
		fetcher, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers})
		ctx, cancel := context.WithCancel(t.Context())
		fetcher.setHook(func(ctx context.Context) error {
			cancel()
			return ctx.Err()
		})

		n.heads <- signedHead(f.a3.Blocks[0], 200, 2, false)
		require.ErrorIs(t, n.follower.StepOverlay(ctx), context.Canceled)
		require.Empty(t, n.events("eez.node.follower.head.rejected"))
	})

	t.Run("requests are sized to the gap and bounded", func(t *testing.T) {
		f := newForks(t)
		fetcher, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers, config: func(cfg *follower.Config) { cfg.HeadRequest = 3 * time.Second }})

		n.step(t, signedHead(f.a3.Blocks[0], 200, 2, false))
		n.requireHead(t, f.a3.Blocks[0])
		opts := fetcher.options()
		require.Len(t, opts, 1)
		require.Equal(t, uint64(2), opts[0].Batch, "the gap to the local head")
		require.Equal(t, 3*time.Second, opts[0].Timeout)
		require.Equal(t, uint64(seqsig.MaxAncestryWalk), opts[0].Limit)
	})
}

// newPeering returns the L2 peers of a follower whose one peer, connected or
// not, is node, over the mock sentry.
func newPeering(node *execmoduletester.ExecModuleTester, connected bool) (*peerSentry, func(*testing.T, deriver.Chain) follower.BlockFetcher) {
	s := newPeerSentry(node, connected)
	return s, func(t *testing.T, chain deriver.Chain) follower.BlockFetcher { return newFetcher(t, s, chain) }
}

// TestSignedHeadTailFetch is T-P2P-3: a follower missing the blocks below a
// signed head fetches them by hash from its L2 peer over devp2p, imports them
// and moves only its head.
func TestSignedHeadTailFetch(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	c.Post(derivertest.Posting{Batch: settled})
	// The tip is a Sync block with a system transaction.
	unsettled := c.Compose([]da.Action{{
		SourceRollupID: protocol.MainnetRollupID, TargetRollupID: 1, SourceAddress: bob, TargetAddress: alice,
		Data: []byte{}, Success: true, ReturnData: []byte{},
	}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	tip := unsettled.Blocks[2]
	require.Equal(t, 1, countSystemTxs([]*types.Block{tip}))

	for _, withBlock := range []bool{false, true} {
		t.Run(fmt.Sprintf("by hash, block attached %v", withBlock), func(t *testing.T) {
			s, peers := newPeering(c.L2, true)
			n := newHeadsNode(t, c.L1, headsOptions{peers: peers})
			_, safe, finalized := n.forkChoice(t)
			require.Equal(t, settled.Blocks[0].Hash(), safe)

			n.step(t, signedHead(tip, 100, 1, withBlock))
			derivertest.RequireChain(t, n.chain, unsettled.Blocks...)
			head, gotSafe, gotFinalized := n.forkChoice(t)
			require.Equal(t, tip.Hash(), head)
			require.Equal(t, safe, gotSafe, "safe stays derivation's")
			require.Equal(t, finalized, gotFinalized, "finalized stays derivation's")
			require.Contains(t, s.sent(), sentryproto.MessageId_GET_BLOCK_BODIES_66)
			advanced := n.events("eez.node.follower.head.advanced")
			require.Len(t, advanced, 1)
			require.Contains(t, advanced[0], "imported=3")

			requests := len(s.sent())
			n.step(t)
			require.Len(t, s.sent(), requests, "the head it follows needs no request")
		})
	}

	t.Run("body not matching its header", func(t *testing.T) {
		s, peers := newPeering(c.L2, true)
		s.tamper = func(id sentryproto.MessageId, data []byte) []byte {
			if id != sentryproto.MessageId_BLOCK_BODIES_66 {
				return data
			}
			return dropSystemTxs(t, data)
		}
		n := newHeadsNode(t, c.L1, headsOptions{peers: peers})
		n.step(t, signedHead(tip, 100, 1, false))
		n.requireHead(t, settled.Blocks[0])
		require.Positive(t, s.penalized())
		for _, b := range unsettled.Blocks {
			require.False(t, n.stored(t, b), "block %d", b.NumberU64())
		}
		require.Len(t, n.events("eez.node.follower.head.rejected"), 1)
	})

	t.Run("no connected peer", func(t *testing.T) {
		s, peers := newPeering(c.L2, false)
		n := newHeadsNode(t, c.L1, headsOptions{peers: peers})
		n.step(t, signedHead(unsettled.Blocks[1], 100, 1, false))
		n.step(t)
		n.step(t, signedHead(tip, 100, 2, false))
		n.requireHead(t, settled.Blocks[0])
		require.Empty(t, s.sent(), "no request without a connected peer")
		rejected := n.events("eez.node.follower.head.rejected")
		require.Len(t, rejected, 2, "once per head")
		require.Contains(t, rejected[0], seqsig.ErrNoPeers.Error())
		require.Contains(t, rejected[0], "lvl=warn", "on entering the wait")
		require.Contains(t, rejected[1], tip.Hash().Hex())
		require.Contains(t, rejected[1], "lvl=dbug", "while waiting")
	})
}

// dropSystemTxs drops the trailing system transaction of every body in a
// BlockBodies message.
func dropSystemTxs(t *testing.T, data []byte) []byte {
	t.Helper()
	var p eth.BlockBodiesPacket66
	require.NoError(t, rlp.DecodeBytes(data, &p))
	for _, b := range p.BlockBodiesPacket {
		if n := len(b.Transactions); n > 0 && b.Transactions[n-1].Type() == protocol.SystemTxType {
			b.Transactions = b.Transactions[:n-1]
		}
	}
	out, err := rlp.EncodeToBytes(&p)
	require.NoError(t, err)
	return out
}

// TestSignedHeadWalkBound is T-P2P-3's bounds over the mock sentry: a target
// whose missing ancestry is longer than 1,024 blocks leaves the head where it
// is, and one more than 1,024 blocks ahead is not fetched.
func TestSignedHeadWalkBound(t *testing.T) {
	empty := func(n int, extra string) []derivertest.Block {
		blocks := make([]derivertest.Block, n)
		for i := range blocks {
			blocks[i] = derivertest.Block{Coinbase: alice, Extra: []byte(extra), Txs: [][]byte{}}
		}
		return blocks
	}
	// The peer's chain has 1,600 blocks on the settled block, the
	// follower's own branch 600.
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, empty(1, "")...)
	c.Post(derivertest.Posting{Batch: settled})
	peer := c.Compose(nil, empty(1600, "peer")...)
	tip := peer.Blocks[len(peer.Blocks)-1]
	branch := derivertest.NewComposer(t)
	require.Equal(t, settled.Blocks[0].Hash(), branch.Compose(nil, empty(1, "")...).Blocks[0].Hash())
	own := branch.Compose(nil, empty(600, "own")...)

	t.Run("a walk past 1,024 blocks", func(t *testing.T) {
		s, peers := newPeering(c.L2, true)
		n := newHeadsNode(t, c.L1, headsOptions{peers: peers, config: func(cfg *follower.Config) { cfg.HeadRequest = time.Minute }})
		require.NoError(t, n.chain.Insert(t.Context(), own.Blocks))
		require.NoError(t, n.follower.Deriver().ForkChoiceTo(t.Context(), own.Blocks[len(own.Blocks)-1].Hash()))
		local := own.Blocks[len(own.Blocks)-1]
		require.LessOrEqual(t, tip.NumberU64()-local.NumberU64(), uint64(seqsig.MaxAncestryWalk), "within reach of the head")

		n.step(t, signedHead(tip, 100, 1, false))
		n.requireHead(t, local)
		require.NotEmpty(t, s.sent())
		require.False(t, n.stored(t, tip))
		require.False(t, n.stored(t, peer.Blocks[0]))
		rejected := n.events("eez.node.follower.head.rejected")
		require.Len(t, rejected, 1)
		require.Contains(t, rejected[0], seqsig.ErrUnverifiable.Error())
	})

	t.Run("a target more than 1,024 blocks ahead", func(t *testing.T) {
		s, peers := newPeering(c.L2, true)
		n := newHeadsNode(t, c.L1, headsOptions{peers: peers})
		n.step(t, signedHead(tip, 100, 1, false))
		n.requireHead(t, settled.Blocks[0])
		require.Empty(t, s.sent())
		require.Len(t, n.events("eez.node.follower.head.ahead"), 1)
	})
}

// startOverlay starts an overlay node that accepts the heads signer signs
// for domain, with static peers.
func startOverlay(t *testing.T, domain seqsig.Domain, signer common.Address, peers ...peer.AddrInfo) (*overlay.Overlay, peer.AddrInfo) {
	t.Helper()
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listen := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", l.Addr().(*net.TCPAddr).Port))
	require.NoError(t, l.Close())
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	o, err := overlay.New(overlay.Config{Listen: listen, Peers: peers, Key: key, Domain: domain, Signer: signer}, log.New())
	require.NoError(t, err)
	require.NoError(t, o.Start())
	t.Cleanup(func() { require.NoError(t, o.Stop()) })
	return o, peer.AddrInfo{ID: o.ID(), Addrs: []ma.Multiaddr{listen}}
}

// publishHead publishes the head of b, signed by key for domain, with a new
// counter every interval until ctx ends: the overlay forms its mesh after
// the first publications.
func publishHead(ctx context.Context, o *overlay.Overlay, domain seqsig.Domain, key *ecdsa.PrivateKey, epochMs *uint64, counter *uint64, b *types.Block) error {
	enc, err := rlp.EncodeToBytes(b)
	if err != nil {
		return err
	}
	for {
		*counter++
		h := seqsig.Head{RollupID: domain.RollupID, Number: b.NumberU64(), BlockHash: b.Hash(), ParentHash: b.ParentHash(), EpochMs: *epochMs, Counter: *counter}
		sig, err := seqsig.Sign(h, domain, key)
		if err != nil {
			return err
		}
		if err := o.Publish(ctx, seqsig.Envelope{Head: h, Signature: sig, Block: enc}); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestSignedHeadOverlay: heads the sequencer signs reach the follower over
// the overlay, and its latest block follows them while its safe block
// follows L1.
func TestSignedHeadOverlay(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	c.Post(derivertest.Posting{Batch: settled})
	unsettled := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	tip := unsettled.Blocks[1]

	cc := derivertest.Config()
	l2 := cc.L2.(*config.L2Config)
	domain := seqsig.Domain{ChainID: cc.ChainID.Uint64(), Registry: l2.Registry, RollupID: l2.RollupID}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := crypto.PubkeyToAddress(key.PublicKey)
	sequencer, sequencerInfo := startOverlay(t, domain, signer)
	follows, _ := startOverlay(t, domain, signer, sequencerInfo)

	_, peers := newPeering(c.L2, true)
	n := newHeadsNode(t, c.L1, headsOptions{peers: peers, heads: follows.Heads(), config: func(cfg *follower.Config) { cfg.HeadRetry = 20 * time.Millisecond }})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.follower.Run(ctx) }()
	defer func() {
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	}()

	epochMs, counter := uint64(time.Now().UnixMilli()), uint64(0)
	publish := func(b *types.Block) (stop func()) {
		ctx, cancel := context.WithCancel(t.Context())
		errc := make(chan error, 1)
		go func() { errc <- publishHead(ctx, sequencer, domain, key, &epochMs, &counter, b) }()
		return func() {
			cancel()
			require.NoError(t, <-errc)
		}
	}
	await := func(head, safe *types.Block) {
		t.Helper()
		require.Eventually(t, func() bool {
			fc, err := n.m.ExecModule.GetForkChoice(context.Background())
			return err == nil && fc.HeadHash == head.Hash() && fc.SafeHash == safe.Hash()
		}, time.Minute, 10*time.Millisecond, "head %d, safe %d", head.NumberU64(), safe.NumberU64())
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Log(n.logs.String())
		}
	})
	stop := publish(tip)
	await(tip, settled.Blocks[0])
	stop()
	derivertest.RequireChain(t, n.chain, unsettled.Blocks...)

	c.Post(derivertest.Posting{Batch: unsettled})
	await(tip, tip)

	next := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}})
	stop = publish(next.Blocks[0])
	await(next.Blocks[0], tip)
	stop()
}

// TestSignedHeadRequests: each devp2p request of a fetch is bounded, not the
// fetch, and a fetch that fails keeps the blocks it inserted for the next.
func TestSignedHeadRequests(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}})
	c.Post(derivertest.Posting{Batch: settled})
	blocks := make([]derivertest.Block, 200)
	for i := range blocks {
		blocks[i] = derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}}
	}
	unsettled := c.Compose(nil, blocks...)
	tip := unsettled.Blocks[len(unsettled.Blocks)-1]
	request := func(timeout time.Duration) func(*follower.Config) {
		return func(cfg *follower.Config) { cfg.HeadRequest = timeout }
	}

	t.Run("a slow peer within the request bound", func(t *testing.T) {
		s, peers := newPeering(c.L2, true)
		// Three requests, the target's header, one header batch and one body
		// batch, each within the bound and together beyond it.
		s.hold = func(sentryproto.MessageId) time.Duration { return 900 * time.Millisecond }
		n := newHeadsNode(t, c.L1, headsOptions{peers: peers, config: request(2 * time.Second)})

		target := unsettled.Blocks[59]
		n.step(t, signedHead(target, 100, 1, false))
		n.requireHead(t, target)
		require.Contains(t, n.events("eez.node.follower.head.advanced")[0], "imported=60")
	})

	t.Run("a peer that never answers bodies", func(t *testing.T) {
		s, peers := newPeering(c.L2, true)
		var silent atomic.Bool
		silent.Store(true)
		s.hold = func(id sentryproto.MessageId) time.Duration {
			if id == sentryproto.MessageId_GET_BLOCK_BODIES_66 && silent.Load() {
				return -1
			}
			return 0
		}
		n := newHeadsNode(t, c.L1, headsOptions{peers: peers, config: request(200 * time.Millisecond)})

		start := time.Now()
		n.step(t, signedHead(tip, 100, 1, false))
		require.Less(t, time.Since(start), 10*time.Second, "bounded per request, not by the downloader's default")
		n.requireHead(t, settled.Blocks[0])
		require.False(t, n.stored(t, unsettled.Blocks[0]))
		require.Len(t, n.events("eez.node.follower.head.rejected"), 1)

		silent.Store(false)
		n.step(t)
		n.requireHead(t, tip)
	})

	t.Run("a retry resumes from the blocks inserted", func(t *testing.T) {
		s, peers := newPeering(c.L2, true)
		var bodies atomic.Int32
		var silent atomic.Bool
		silent.Store(true)
		s.hold = func(id sentryproto.MessageId) time.Duration {
			if id == sentryproto.MessageId_GET_BLOCK_BODIES_66 && bodies.Add(1) > 1 && silent.Load() {
				return -1
			}
			return 0
		}
		n := newHeadsNode(t, c.L1, headsOptions{peers: peers, config: request(200 * time.Millisecond)})

		n.step(t, signedHead(tip, 100, 1, false))
		n.requireHead(t, settled.Blocks[0])
		require.True(t, n.stored(t, unsettled.Blocks[seqsig.FetchBatch-1]), "the first batch is inserted")
		require.False(t, n.stored(t, unsettled.Blocks[seqsig.FetchBatch]))

		silent.Store(false)
		n.step(t)
		n.requireHead(t, tip)
		advanced := n.events("eez.node.follower.head.advanced")
		require.Len(t, advanced, 1)
		require.Contains(t, advanced[0], fmt.Sprintf("imported=%d", len(unsettled.Blocks)-seqsig.FetchBatch))
	})
}

// gatedReader holds boot at its L1 chain id until open is closed.
type gatedReader struct {
	l1.ChainReader
	open chan struct{}
}

func (r *gatedReader) ChainID(ctx context.Context) (uint64, error) {
	select {
	case <-r.open:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return r.ChainReader.ChainID(ctx)
}

// TestSignedHeadLoop: on the follower loop, a head is followed when it
// arrives and retried every tick, and heads that arrive during boot are
// tracked.
func TestSignedHeadLoop(t *testing.T) {
	t.Run("a head is followed on arrival", func(t *testing.T) {
		f := newForks(t)
		_, peers := mapPeers(f.blocks()...)
		n := newHeadsNode(t, f.c.L1, headsOptions{peers: peers, config: func(cfg *follower.Config) { cfg.HeadRetry = time.Hour }})
		n.run(t)
		n.heads <- signedHead(f.a2.Blocks[0], 200, 1, true)
		n.awaitHead(t, f.a2.Blocks[0])
	})

	t.Run("the tick releases a hold without a new head", func(t *testing.T) {
		f := newForks(t)
		b4 := f.c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte("b4"), Txs: [][]byte{}})
		_, peers := mapPeers(append(f.blocks(), b4.Blocks[0])...)
		var chain *failingExtend
		n := newHeadsNode(t, f.c.L1, headsOptions{
			peers:  peers,
			config: func(cfg *follower.Config) { cfg.HeadRetry = 20 * time.Millisecond },
			wrap: func(c deriver.Chain) deriver.Chain {
				chain = &failingExtend{Chain: c, number: 3}
				return chain
			},
		})
		n.run(t)
		// Run boots again; only the loop after boot decides on a head. A
		// probe too far ahead to follow shows the loop runs, and b4's head
		// from the same source replaces it.
		probe := types.NewBlock(&types.Header{Number: *uint256.NewInt(2 + seqsig.MaxAncestryWalk), ParentHash: common.Hash{1}}, nil, nil, nil, nil, nil)
		n.heads <- signedHead(probe, 100, 1, false)
		require.Eventually(t, func() bool { return len(n.events("eez.node.follower.head.ahead")) > 0 }, 30*time.Second, 5*time.Millisecond, "the loop runs")
		chain.fail.Store(true)
		f.c.Post(derivertest.Posting{Batch: f.b2}, derivertest.Posting{Batch: f.b3})
		require.Eventually(t, func() bool {
			s := n.follower.Status()
			return s.Cursor == 2 && s.Safe == 1
		}, 30*time.Second, 5*time.Millisecond, "b2 indexed above safe")

		n.heads <- signedHead(b4.Blocks[0], 100, 3, true)
		require.Eventually(t, func() bool { return len(n.events("eez.node.follower.head.held")) > 0 }, 30*time.Second, 5*time.Millisecond)
		chain.fail.Store(false)
		n.awaitHead(t, b4.Blocks[0])
	})

	t.Run("heads that arrive during boot are tracked", func(t *testing.T) {
		f := newForks(t)
		_, peers := mapPeers(f.blocks()...)
		reader := &gatedReader{ChainReader: f.c.L1, open: make(chan struct{})}
		// The overlay hands heads over without waiting; one slot shows
		// whether they are taken while boot runs.
		heads := make(chan *seqsig.VerifiedHead, 1)
		n := newHeadsNode(t, reader, headsOptions{peers: peers, heads: heads, noBoot: true})
		n.run(t)
		for _, h := range []*seqsig.VerifiedHead{signedHead(f.a2.Blocks[0], 200, 1, true), signedHead(f.a3.Blocks[0], 200, 2, true)} {
			select {
			case heads <- h:
			default:
				t.Fatal("head dropped")
			}
			require.Eventually(t, func() bool { return len(heads) == 0 }, 10*time.Second, 5*time.Millisecond, "head taken during boot")
		}
		close(reader.open)
		n.awaitHead(t, f.a3.Blocks[0])
	})
}
