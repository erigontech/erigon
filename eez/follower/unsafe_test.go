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
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/announce"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// sequencer serves m's chain the way a sequencer node does.
func sequencer(t *testing.T, m *execmoduletester.ExecModuleTester) *follower.RPCSequencer {
	t.Helper()
	logger := log.New()
	base := jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	eth := jsonrpc.NewEthAPI(base, m.DB, nil, nil, nil, &rpccfg.EthApiConfig{
		GasCap:                  5_000_000,
		FeeCap:                  ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:         100_000,
		RpcTxSyncDefaultTimeout: time.Second,
		RpcTxSyncMaxTimeout:     time.Second,
	}, logger)
	server := rpc.NewServer(1, false, false, true, logger, 0)
	require.NoError(t, server.RegisterName("eth", eth))
	require.NoError(t, server.RegisterName("debug", jsonrpc.NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})))
	t.Cleanup(server.Stop)
	hs := httptest.NewServer(server)
	t.Cleanup(hs.Close)
	s, err := follower.DialSequencer(t.Context(), hs.URL, logger)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

// TestUnsafeHead is T-NET-8 in process: the head follows the sequencer's
// unsettled blocks, which settlement then confirms without rebuilding, and
// a sequencer chain off the safe block is refused.
func TestUnsafeHead(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	c.Post(derivertest.Posting{Batch: settled})
	unsettled := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)

	m, chain := derivertest.NewL2(t)
	cfg := testConfig
	cfg.Dir = t.TempDir()
	cfg.Sequencer = sequencer(t, c.L2)
	n := &node{m: m, dir: cfg.Dir}
	n.chain = &countingChain{Chain: chain}
	f, err := follower.New(c.L1, n.chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	n.follower = f
	require.NoError(t, f.Boot(t.Context()))
	require.Equal(t, 1, n.chain.Built())

	require.NoError(t, f.StepOverlay(t.Context()))
	n.requireBlocks(t, append(settled.Blocks, unsettled.Blocks...)...)
	fc, err := m.ExecModule.GetForkChoice(t.Context())
	require.NoError(t, err)
	require.Equal(t, unsettled.Blocks[1].Hash(), fc.HeadHash)
	require.Equal(t, settled.Blocks[0].Hash(), fc.SafeHash, "safe stays derivation's")

	// Settlement confirms the blocks the head already holds.
	c.Post(derivertest.Posting{Batch: unsettled})
	head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.NoError(t, f.Handle(t.Context(), l1.NewHead{Head: *head}))
	require.Equal(t, 1, n.chain.Built())
	require.Equal(t, uint64(3), f.Deriver().Safe())

	// The sequencer reorgs its unsettled blocks: the head follows.
	c.Rewind(unsettled.Blocks[1].Header())
	next := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte("a"), Txs: [][]byte{}})
	c.Rewind(unsettled.Blocks[1].Header())
	replaced := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte("b"), Txs: [][]byte{}})
	require.NotEqual(t, next.Blocks[0].Hash(), replaced.Blocks[0].Hash())
	require.NoError(t, f.StepOverlay(t.Context()))
	n.requireBlocks(t, replaced.Blocks...)

	// A sequencer whose chain leaves the safe block is not followed.
	rogue := derivertest.NewComposer(t)
	rogue.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	cfg.Sequencer = sequencer(t, rogue.L2)
	again, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, again.Boot(t.Context()))
	require.NoError(t, again.StepOverlay(t.Context()))
	n.requireBlocks(t, replaced.Blocks...)
}

// badHead serves a sequencer chain whose head has a wrong state root, and
// counts the blocks it serves.
type badHead struct {
	follower.Sequencer
	head   *types.Block
	served atomic.Int32
}

func (s *badHead) Head(context.Context) (uint64, common.Hash, error) {
	return s.head.NumberU64(), s.head.Hash(), nil
}

func (s *badHead) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	s.served.Add(1)
	if number == s.head.NumberU64() {
		return s.head, nil
	}
	return s.Sequencer.BlockByNumber(ctx, number)
}

// TestUnsafeHeadRejected: a sequencer head that fails execution is fetched
// once, not again on every step while the sequencer stays on it.
func TestUnsafeHeadRejected(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	c.Post(derivertest.Posting{Batch: settled})
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}})

	header := unsettled.Blocks[0].Header()
	header.Root = common.Hash{1}
	seq := &badHead{Sequencer: sequencer(t, c.L2), head: unsettled.Blocks[0].WithSeal(header)}

	_, chain := derivertest.NewL2(t)
	cfg := testConfig
	cfg.Dir, cfg.Sequencer = t.TempDir(), seq
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))

	require.NoError(t, f.StepOverlay(t.Context()))
	served := seq.served.Load()
	require.Positive(t, served)
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, served, seq.served.Load(), "the rejected head is not fetched again")
	fc, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, settled.Blocks[0].Hash(), fc.Hash())
}

// announcing serves the announcements of envs, keyed by block number, for a
// sequencer chain.
type announcing struct {
	follower.Sequencer
	envs map[uint64][]byte
	// err, when set, fails every read.
	err   error
	reads atomic.Int32
}

func (s *announcing) Announcement(_ context.Context, number uint64) ([]byte, error) {
	s.reads.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	return s.envs[number], nil
}

// signer signs the announcements of a test chain with a fresh key each.
func signer(t *testing.T) func(chainID *uint256.Int, hash common.Hash) []byte {
	return func(chainID *uint256.Int, hash common.Hash) []byte {
		t.Helper()
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		env, err := announce.Sign(key, chainID, hash)
		require.NoError(t, err)
		return env
	}
}

// openAnnouncements returns an announcement store closed when t ends.
func openAnnouncements(t *testing.T) *announce.Store {
	t.Helper()
	st, err := announce.Open(t.Context(), t.TempDir(), log.New())
	require.NoError(t, err)
	t.Cleanup(st.Close)
	return st
}

// TestUnsafeHeadAnnounced is T-SEQ-8 on the follower: sequencer blocks are
// taken only when the announcement served for each is valid, whichever key
// signed it, and refused when one is missing, malformed or names another
// chain or block (S4.4). The announcements of the blocks taken are kept.
func TestUnsafeHeadAnnounced(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)})
	c.Post(derivertest.Posting{Batch: settled})
	unsettled := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	two, three := unsettled.Blocks[0].Hash(), unsettled.Blocks[1].Hash()
	chainID := derivertest.Config().ChainID
	sign := signer(t)
	for _, tc := range []struct {
		name  string
		envs  map[uint64][]byte
		taken bool
	}{
		{"signed", map[uint64][]byte{2: sign(chainID, two), 3: sign(chainID, three)}, true},
		{"unsigned", nil, false},
		{"first unsigned", map[uint64][]byte{3: sign(chainID, three)}, false},
		{"last unsigned", map[uint64][]byte{2: sign(chainID, two)}, false},
		{"another chain", map[uint64][]byte{2: sign(uint256.NewInt(1), two), 3: sign(chainID, three)}, false},
		{"another block", map[uint64][]byte{2: sign(chainID, two), 3: sign(chainID, two)}, false},
		{"malformed", map[uint64][]byte{2: {0xc0}, 3: sign(chainID, three)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, chain := derivertest.NewL2(t)
			st := openAnnouncements(t)
			cfg := testConfig
			cfg.Dir, cfg.Announcements = t.TempDir(), st
			cfg.Sequencer = &announcing{Sequencer: sequencer(t, c.L2), envs: tc.envs}
			f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
			require.NoError(t, err)
			require.NoError(t, f.Boot(t.Context()))
			require.NoError(t, f.StepOverlay(t.Context()))
			head, err := chain.Head(t.Context())
			require.NoError(t, err)
			want := settled.Blocks[0].Hash()
			if tc.taken {
				want = three
			}
			require.Equal(t, want, head.Hash())
			for n, hash := range map[uint64]common.Hash{2: two, 3: three} {
				kept, err := st.Get(t.Context(), n, hash)
				require.NoError(t, err)
				if !tc.taken {
					require.Nil(t, kept, "block %d", n)
					continue
				}
				require.Equal(t, tc.envs[n], kept, "block %d", n)
			}
		})
	}
}

// TestUnsafeHeadUnannounced: without Announcements, which only chains whose
// rule set has App. D.6 announcements configure, the overlay reads none, so a
// malformed one served refuses nothing.
func TestUnsafeHeadUnannounced(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	_, chain := derivertest.NewL2(t)
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: {0xc0}}}
	cfg := testConfig
	cfg.Dir, cfg.Sequencer = t.TempDir(), seq
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	require.NoError(t, f.StepOverlay(t.Context()))
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, unsettled.Blocks[0].Hash(), head.Hash())
	require.Zero(t, seq.reads.Load(), "announcements read")
}

// TestUnsafeHeadAnnouncedLater: a block served without an announcement is
// fetched again at the next poll and taken once announced; one served with an
// invalid announcement is refused until the sequencer moves on.
func TestUnsafeHeadAnnouncedLater(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	hash := unsettled.Blocks[0].Hash()
	sign := signer(t)
	for _, tc := range []struct {
		name  string
		first []byte
		taken bool
	}{
		{"missing", nil, true},
		{"invalid", sign(uint256.NewInt(1), hash), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, chain := derivertest.NewL2(t)
			seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: tc.first}}
			cfg := testConfig
			cfg.Dir, cfg.Sequencer, cfg.Announcements = t.TempDir(), seq, openAnnouncements(t)
			f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
			require.NoError(t, err)
			require.NoError(t, f.Boot(t.Context()))
			require.NoError(t, f.StepOverlay(t.Context()))
			head, err := chain.Head(t.Context())
			require.NoError(t, err)
			require.Zero(t, head.Number.Uint64())

			seq.envs[1] = sign(derivertest.Config().ChainID, hash)
			require.NoError(t, f.StepOverlay(t.Context()))
			head, err = chain.Head(t.Context())
			require.NoError(t, err)
			require.Equal(t, tc.taken, head.Hash() == hash)
		})
	}
}

// TestUnsafeHeadAnnouncementUnread: a failed read of an announcement fails
// the step, the head unchanged, and the next step takes the head; it is not
// refused.
func TestUnsafeHeadAnnouncementUnread(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	env := signer(t)(derivertest.Config().ChainID, unsettled.Blocks[0].Hash())
	_, chain := derivertest.NewL2(t)
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: env}, err: errors.New("read failed")}
	cfg := testConfig
	cfg.Dir, cfg.Sequencer, cfg.Announcements = t.TempDir(), seq, openAnnouncements(t)
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	require.ErrorIs(t, f.StepOverlay(t.Context()), seq.err)
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Zero(t, head.Number.Uint64())

	seq.err = nil
	require.NoError(t, f.StepOverlay(t.Context()))
	head, err = chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, unsettled.Blocks[0].Hash(), head.Hash())
}

// TestUnsafeHeadAnnouncementsPruned: the follower drops the announcements it
// keeps once L1 finality passes their blocks.
func TestUnsafeHeadAnnouncementsPruned(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}},
	)
	require.NoError(t, c.L1.Finalize(c.Post(derivertest.Posting{Batch: settled}).Header.Number))
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	hash := unsettled.Blocks[0].Hash()
	env := signer(t)(derivertest.Config().ChainID, hash)

	st := openAnnouncements(t)
	// Kept while block 1 was unsafe.
	require.NoError(t, st.Put(t.Context(), 1, settled.Blocks[0].Hash(), []byte{1}))
	_, chain := derivertest.NewL2(t)
	cfg := testConfig
	cfg.Dir, cfg.Announcements = t.TempDir(), st
	cfg.Sequencer = &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{3: env}}
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	require.Equal(t, uint64(2), f.Status().Finalized)
	require.NoError(t, f.StepOverlay(t.Context()))
	kept, err := st.Get(t.Context(), 3, hash)
	require.NoError(t, err)
	require.Equal(t, env, kept)
	kept, err = st.Get(t.Context(), 1, settled.Blocks[0].Hash())
	require.NoError(t, err)
	require.Nil(t, kept, "finalized")
}

// TestUnsafeHeadAnnouncementsPrunedHeadUnchanged: the follower drops the
// announcements L1 finality covers once it advances, the sequencer's head
// unchanged; a failed prune is retried at the next step.
func TestUnsafeHeadAnnouncementsPrunedHeadUnchanged(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}},
	)
	posted := c.Post(derivertest.Posting{Batch: settled}).Header.Number
	st := openAnnouncements(t)
	for i, b := range settled.Blocks {
		require.NoError(t, st.Put(t.Context(), uint64(i+1), b.Hash(), []byte{byte(i + 1)}))
	}
	_, chain := derivertest.NewL2(t)
	cfg := testConfig
	cfg.Dir, cfg.Announcements = t.TempDir(), st
	cfg.Sequencer = &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{}}
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	require.Equal(t, uint64(0), f.Status().Finalized)
	require.NoError(t, f.StepOverlay(t.Context()))

	require.NoError(t, f.Handle(t.Context(), l1.Finalized{Head: l1.Header{Number: posted}}))
	require.Equal(t, uint64(2), f.Status().Finalized)
	kept := func(i int) []byte {
		t.Helper()
		env, err := st.Get(t.Context(), uint64(i+1), settled.Blocks[i].Hash())
		require.NoError(t, err)
		return env
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, f.StepOverlay(ctx))
	require.NotNil(t, kept(0), "prune failed")
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Nil(t, kept(0), "finalized")
	require.Equal(t, []byte{2}, kept(1), "the finalized block's own")
}

// TestUnsafeHeadAnnouncementsPrunedKeepFailed: the follower drops the
// announcements L1 finality covers when those of the blocks a step takes are
// not kept.
func TestUnsafeHeadAnnouncementsPrunedKeepFailed(t *testing.T) {
	c := derivertest.NewComposer(t)
	settled := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 1)},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}},
	)
	require.NoError(t, c.L1.Finalize(c.Post(derivertest.Posting{Batch: settled}).Header.Number))
	head := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}}).Blocks[0].Hash()
	st := openAnnouncements(t)
	require.NoError(t, st.Put(t.Context(), 1, settled.Blocks[0].Hash(), []byte{1}))
	_, exec := derivertest.NewL2(t)
	chain := &forkChoiceCancels{ExecChain: exec}
	cfg := testConfig
	cfg.Dir, cfg.Announcements = t.TempDir(), st
	cfg.Sequencer = &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{3: signer(t)(derivertest.Config().ChainID, head)}}
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	require.Equal(t, uint64(2), f.Status().Finalized)
	stepKeepFailed(t, f, chain)
	local, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, head, local.Hash())
	kept, err := st.Get(t.Context(), 3, head)
	require.NoError(t, err)
	require.Nil(t, kept, "not kept")
	kept, err = st.Get(t.Context(), 1, settled.Blocks[0].Hash())
	require.NoError(t, err)
	require.Nil(t, kept, "finalized")
}

// forkChoiceCancels cancels the step's context once a fork choice succeeds,
// so that keeping the announcements fails.
type forkChoiceCancels struct {
	*deriver.ExecChain
	cancel context.CancelFunc
}

func (c *forkChoiceCancels) ForkChoice(ctx context.Context, head, safe, finalized common.Hash) error {
	err := c.ExecChain.ForkChoice(ctx, head, safe, finalized)
	if err == nil && c.cancel != nil {
		c.cancel()
	}
	return err
}

// stepKeepFailed steps f's overlay so that the blocks it takes have their
// announcements not kept.
func stepKeepFailed(t *testing.T, f *follower.Follower, chain *forkChoiceCancels) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	chain.cancel = cancel
	require.NoError(t, f.StepOverlay(ctx))
	chain.cancel = nil
}

// restarted returns a follower of chain, as after a restart, whose store st
// keeps none of the announcements of blocks, which a previous run took from
// c's sequencer.
func restarted(t *testing.T, c *derivertest.Composer, chain deriver.Chain, blocks []*types.Block, seq follower.Sequencer, st *announce.Store) *follower.Follower {
	t.Helper()
	sign := signer(t)
	envs := make(map[uint64][]byte, len(blocks))
	for _, b := range blocks {
		envs[b.NumberU64()] = sign(derivertest.Config().ChainID, b.Hash())
	}
	cfg := testConfig
	cfg.Dir, cfg.Announcements = t.TempDir(), openAnnouncements(t)
	cfg.Sequencer = &announcing{Sequencer: sequencer(t, c.L2), envs: envs}
	previous, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, previous.Boot(t.Context()))
	require.NoError(t, previous.StepOverlay(t.Context()))
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, blocks[len(blocks)-1].Hash(), head.Hash())

	cfg.Sequencer, cfg.Announcements = seq, st
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	return f
}

// TestUnsafeHeadAnnouncementReadAgain: a block taken without its
// announcement kept has it read again at each step, the head unchanged,
// until a valid one is served and kept; then no more.
func TestUnsafeHeadAnnouncementReadAgain(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	hash := unsettled.Blocks[0].Hash()
	chainID := derivertest.Config().ChainID
	sign := signer(t)
	env := sign(chainID, hash)
	_, exec := derivertest.NewL2(t)
	chain := &forkChoiceCancels{ExecChain: exec}
	st := openAnnouncements(t)
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: env}}
	cfg := testConfig
	cfg.Dir, cfg.Sequencer, cfg.Announcements = t.TempDir(), seq, st
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	stepKeepFailed(t, f, chain)
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, hash, head.Hash())
	kept, err := st.Get(t.Context(), 1, hash)
	require.NoError(t, err)
	require.Nil(t, kept)

	for _, served := range [][]byte{nil, sign(chainID, common.Hash{1}), env} {
		seq.envs[1] = served
		reads := seq.reads.Load()
		require.NoError(t, f.StepOverlay(t.Context()))
		require.Equal(t, reads+1, seq.reads.Load(), "read again")
		head, err := chain.Head(t.Context())
		require.NoError(t, err)
		require.Equal(t, hash, head.Hash())
		kept, err := st.Get(t.Context(), 1, hash)
		require.NoError(t, err)
		if served != nil && !bytes.Equal(served, env) {
			require.Nil(t, kept, "another block's kept")
		}
	}
	kept, err = st.Get(t.Context(), 1, hash)
	require.NoError(t, err)
	require.Equal(t, env, kept)
	reads := seq.reads.Load()
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, reads, seq.reads.Load(), "read once kept")
}

// TestUnsafeHeadAnnouncementLowest: of two blocks taken in turn without their
// announcements kept, reading again starts at the lower.
func TestUnsafeHeadAnnouncementLowest(t *testing.T) {
	c := derivertest.NewComposer(t)
	one := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}}).Blocks[0]
	chainID := derivertest.Config().ChainID
	sign := signer(t)
	_, exec := derivertest.NewL2(t)
	chain := &forkChoiceCancels{ExecChain: exec}
	st := openAnnouncements(t)
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: sign(chainID, one.Hash())}}
	cfg := testConfig
	cfg.Dir, cfg.Sequencer, cfg.Announcements = t.TempDir(), seq, st
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	stepKeepFailed(t, f, chain)

	two := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}}).Blocks[0]
	envs := map[uint64][]byte{1: sign(chainID, one.Hash()), 2: sign(chainID, two.Hash())}
	seq.envs = map[uint64][]byte{2: envs[2]}
	stepKeepFailed(t, f, chain)
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, two.Hash(), head.Hash())

	seq.envs = envs
	require.NoError(t, f.StepOverlay(t.Context()))
	for n, hash := range map[uint64]common.Hash{1: one.Hash(), 2: two.Hash()} {
		kept, err := st.Get(t.Context(), n, hash)
		require.NoError(t, err)
		require.Equal(t, envs[n], kept, "block %d", n)
	}
}

// TestUnsafeHeadAnnouncementSettled: a block taken without its announcement
// kept is not read again once L1 settles it.
func TestUnsafeHeadAnnouncementSettled(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	_, chain := derivertest.NewL2(t)
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{}}
	f := restarted(t, c, chain, unsettled.Blocks, seq, openAnnouncements(t))
	reads := seq.reads.Load()
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, reads+1, seq.reads.Load(), "read again while unsettled")

	c.Post(derivertest.Posting{Batch: unsettled})
	l1Head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.NoError(t, f.Handle(t.Context(), l1.NewHead{Head: *l1Head}))
	require.Equal(t, uint64(1), f.Deriver().Safe())
	reads = seq.reads.Load()
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, reads, seq.reads.Load(), "read again once settled")
}

// TestUnsafeHeadAnnouncementRestart: after a restart, the announcements of
// the unsettled blocks are read again from the lowest, though the head stays
// on the last; reading stops at the first still not served and skips those
// kept.
func TestUnsafeHeadAnnouncementRestart(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	chainID := derivertest.Config().ChainID
	sign := signer(t)
	envs := make([][]byte, len(unsettled.Blocks))
	for i, b := range unsettled.Blocks {
		envs[i] = sign(chainID, b.Hash())
	}
	_, chain := derivertest.NewL2(t)
	st := openAnnouncements(t)
	require.NoError(t, st.Put(t.Context(), 2, unsettled.Blocks[1].Hash(), envs[1]))
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{}}
	f := restarted(t, c, chain, unsettled.Blocks, seq, st)

	reads := seq.reads.Load()
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, reads+1, seq.reads.Load(), "block 1 alone")
	seq.envs[1], seq.envs[3] = envs[0], envs[2]
	reads = seq.reads.Load()
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, reads+2, seq.reads.Load(), "blocks 1 and 3")
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, unsettled.Blocks[2].Hash(), head.Hash())
	for i, b := range unsettled.Blocks {
		kept, err := st.Get(t.Context(), uint64(i+1), b.Hash())
		require.NoError(t, err)
		require.Equal(t, envs[i], kept, "block %d", i+1)
	}
}

// TestUnsafeHeadAnnouncementWindow: after a restart, the announcements of the
// unsettled blocks are read again from the store's floor of the head up
// only, so that the head's kept stays.
func TestUnsafeHeadAnnouncementWindow(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	chainID := derivertest.Config().ChainID
	sign := signer(t)
	envs := make([][]byte, len(unsettled.Blocks))
	for i, b := range unsettled.Blocks {
		envs[i] = sign(chainID, b.Hash())
	}
	_, chain := derivertest.NewL2(t)
	st, err := announce.OpenWindow(t.Context(), t.TempDir(), 1, log.New())
	require.NoError(t, err)
	t.Cleanup(st.Close)
	require.NoError(t, st.Put(t.Context(), 4, unsettled.Blocks[3].Hash(), envs[3]))
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: envs[0], 2: envs[1], 3: envs[2]}}
	f := restarted(t, c, chain, unsettled.Blocks, seq, st)

	reads := seq.reads.Load()
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, reads+1, seq.reads.Load(), "block 3 alone")
	for i, b := range unsettled.Blocks {
		kept, err := st.Get(t.Context(), uint64(i+1), b.Hash())
		require.NoError(t, err)
		if i < 2 {
			require.Nil(t, kept, "block %d", i+1)
			continue
		}
		require.Equal(t, envs[i], kept, "block %d", i+1)
	}
}

// TestUnsafeHeadAnnouncementBounded: after a restart, each step reads again
// the announcements of the overlay's limit of unsettled blocks, those kept
// included, and the next step resumes past them; once past the head no more
// are read.
func TestUnsafeHeadAnnouncementBounded(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	chainID := derivertest.Config().ChainID
	sign := signer(t)
	envs := make([][]byte, len(unsettled.Blocks))
	for i, b := range unsettled.Blocks {
		envs[i] = sign(chainID, b.Hash())
	}
	_, chain := derivertest.NewL2(t)
	st := openAnnouncements(t)
	require.NoError(t, st.Put(t.Context(), 2, unsettled.Blocks[1].Hash(), envs[1]))
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: envs[0], 3: envs[2], 4: envs[3]}}
	f := restarted(t, c, chain, unsettled.Blocks, seq, st)
	f.SetOverlayLimit(2)

	for i, step := range []struct {
		reads int32
		kept  int
		gap   uint64
	}{{1, 2, 3}, {2, 4, 5}, {0, 4, 0}, {0, 4, 0}} {
		reads := seq.reads.Load()
		require.NoError(t, f.StepOverlay(t.Context()))
		require.Equal(t, reads+step.reads, seq.reads.Load(), "step %d", i+1)
		require.Equal(t, step.gap, f.OverlayGap(), "step %d", i+1)
		for j, b := range unsettled.Blocks {
			kept, err := st.Get(t.Context(), uint64(j+1), b.Hash())
			require.NoError(t, err)
			if j < step.kept {
				require.Equal(t, envs[j], kept, "step %d, block %d", i+1, j+1)
				continue
			}
			require.Nil(t, kept, "step %d, block %d", i+1, j+1)
		}
	}
}

// TestUnsafeHeadAnnouncementBoundedUnserved: a step that resumes past the
// blocks a previous one checked stops at the first block not served and
// reads it again at the next step, until it is served.
func TestUnsafeHeadAnnouncementBoundedUnserved(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	chainID := derivertest.Config().ChainID
	sign := signer(t)
	envs := make([][]byte, len(unsettled.Blocks))
	for i, b := range unsettled.Blocks {
		envs[i] = sign(chainID, b.Hash())
	}
	_, chain := derivertest.NewL2(t)
	st := openAnnouncements(t)
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: envs[0], 2: envs[1], 3: envs[2]}}
	f := restarted(t, c, chain, unsettled.Blocks, seq, st)
	f.SetOverlayLimit(2)

	for i, step := range []struct {
		served bool
		reads  int32
		gap    uint64
	}{{false, 2, 3}, {false, 2, 4}, {false, 1, 4}, {true, 1, 0}} {
		if step.served {
			seq.envs[4] = envs[3]
		}
		reads := seq.reads.Load()
		require.NoError(t, f.StepOverlay(t.Context()))
		require.Equal(t, reads+step.reads, seq.reads.Load(), "step %d", i+1)
		require.Equal(t, step.gap, f.OverlayGap(), "step %d", i+1)
	}
	for i, b := range unsettled.Blocks {
		kept, err := st.Get(t.Context(), uint64(i+1), b.Hash())
		require.NoError(t, err)
		require.Equal(t, envs[i], kept, "block %d", i+1)
	}
}

var errHead = errors.New("head unread")

// headFails is a chain whose head cannot be read while fail is set.
type headFails struct {
	*deriver.ExecChain
	fail atomic.Bool
}

func (c *headFails) Head(ctx context.Context) (*types.Header, error) {
	if c.fail.Load() {
		return nil, errHead
	}
	return c.ExecChain.Head(ctx)
}

// TestUnsafeHeadAnnouncementHeadUnread: announcements are not read again
// while the local head cannot be read, and are once it can.
func TestUnsafeHeadAnnouncementHeadUnread(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}})
	hash := unsettled.Blocks[0].Hash()
	env := signer(t)(derivertest.Config().ChainID, hash)
	_, exec := derivertest.NewL2(t)
	chain := &headFails{ExecChain: exec}
	st := openAnnouncements(t)
	seq := &announcing{Sequencer: sequencer(t, c.L2), envs: map[uint64][]byte{1: env}}
	f := restarted(t, c, chain, unsettled.Blocks, seq, st)

	chain.fail.Store(true)
	reads := seq.reads.Load()
	require.ErrorIs(t, f.StepOverlay(t.Context()), errHead, "the step's own read")
	require.Equal(t, reads, seq.reads.Load(), "not read")
	chain.fail.Store(false)
	require.NoError(t, f.StepOverlay(t.Context()))
	require.Equal(t, reads+1, seq.reads.Load(), "read once the head is")
	kept, err := st.Get(t.Context(), 1, hash)
	require.NoError(t, err)
	require.Equal(t, env, kept)
}

// TestUnsafeHeadAnnouncedRejected: the valid announcement of a block that
// fails execution is not kept.
func TestUnsafeHeadAnnouncedRejected(t *testing.T) {
	c := derivertest.NewComposer(t)
	unsettled := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}})
	header := unsettled.Blocks[0].Header()
	header.Root = common.Hash{1}
	bad := unsettled.Blocks[0].WithSeal(header)
	_, chain := derivertest.NewL2(t)
	st := openAnnouncements(t)
	cfg := testConfig
	cfg.Dir, cfg.Announcements = t.TempDir(), st
	cfg.Sequencer = &announcing{
		Sequencer: &badHead{Sequencer: sequencer(t, c.L2), head: bad},
		envs:      map[uint64][]byte{1: signer(t)(derivertest.Config().ChainID, bad.Hash())},
	}
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	require.NoError(t, f.Boot(t.Context()))
	require.NoError(t, f.StepOverlay(t.Context()))
	head, err := chain.Head(t.Context())
	require.NoError(t, err)
	require.Zero(t, head.Number.Uint64(), "taken")
	kept, err := st.Get(t.Context(), 1, bad.Hash())
	require.NoError(t, err)
	require.Nil(t, kept)
}

// announcements is an eez namespace serving the announcements it holds; it
// fails for block 3.
type announcements map[uint64][]byte

func (a announcements) GetUnsafeBlockAnnouncement(_ context.Context, number hexutil.Uint64) (*hexutil.Bytes, error) {
	if number == 3 {
		return nil, errors.New("store closed")
	}
	env, ok := a[uint64(number)]
	if !ok {
		return nil, nil
	}
	return (*hexutil.Bytes)(&env), nil
}

// TestSequencerAnnouncement reads announcements over JSON-RPC: the one
// served, none for null, and an error for a failed call or a sequencer
// without the method, whose blocks are never announced.
func TestSequencerAnnouncement(t *testing.T) {
	server := rpc.NewServer(1, false, false, true, log.New(), 0)
	require.NoError(t, server.RegisterName("eez", announcements{1: {1, 2, 3}}))
	t.Cleanup(server.Stop)
	hs := httptest.NewServer(server)
	t.Cleanup(hs.Close)
	s, err := follower.DialSequencer(t.Context(), hs.URL, log.New())
	require.NoError(t, err)
	t.Cleanup(s.Close)

	env, err := s.Announcement(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, env)
	env, err = s.Announcement(t.Context(), 2)
	require.NoError(t, err)
	require.Nil(t, env)
	_, err = s.Announcement(t.Context(), 3)
	require.ErrorContains(t, err, "store closed")

	_, err = sequencer(t, derivertest.NewComposer(t).L2).Announcement(t.Context(), 1)
	require.ErrorContains(t, err, "eez_getUnsafeBlockAnnouncement", "no eez namespace")
}

// TestSequencerURLError checks that the sequencer client's failures, which
// the follower logs on every tick, print its endpoint without the path and
// query of its URL, where providers put API keys.
func TestSequencerURLError(t *testing.T) {
	srv := httptest.NewTLSServer(nil)
	defer srv.Close()
	s, err := follower.DialSequencer(t.Context(), srv.URL+"/v2/secret-key?apikey=secret", log.New())
	require.NoError(t, err)
	defer s.Close()
	_, _, err = s.Head(t.Context())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
	require.ErrorContains(t, err, "x509")
	_, err = s.BlockByNumber(t.Context(), 1)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
	_, err = s.Announcement(t.Context(), 1)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
	_, err = follower.DialSequencer(t.Context(), srv.URL+"/v2/secret-key\n", log.New())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
}
