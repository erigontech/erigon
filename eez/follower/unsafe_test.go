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
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
