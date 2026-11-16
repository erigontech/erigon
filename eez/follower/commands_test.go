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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// once provides its transactions to the first request only.
type once struct {
	mu  sync.Mutex
	txs []types.Transaction
}

func (o *once) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	txs := o.txs
	o.txs = nil
	return txs, nil
}

// cancelling ends its caller's context at its first request, while Run
// builds.
type cancelling struct {
	cancel context.CancelFunc
}

func (c cancelling) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	c.cancel()
	return nil, nil
}

// provide returns a provider of the transactions encoded, their senders
// recovered as a pool's are.
func provide(t *testing.T, encoded ...[]byte) *once {
	t.Helper()
	txs, err := deriver.DecodeTxs(encoded)
	require.NoError(t, err)
	signer := types.LatestSignerForChainID(derivertest.Config().ChainID)
	for _, tx := range txs {
		sender, err := signer.Sender(tx)
		require.NoError(t, err)
		tx.SetSender(sender)
	}
	return &once{txs: txs}
}

// inputs are the inputs of the block after parent with beneficiary alice.
func inputs(parent *types.Header) deriver.HeaderInputs {
	l2 := derivertest.Config().L2.(*config.L2Config)
	return deriver.DeriveInputs(l2, parent, &da.Span{Beneficiaries: []common.Address{alice}, ExtraData: [][]byte{{}}}, 0)
}

// TestCommands: Run performs the commands of the chain's other writers. A
// produced block survives its settlement, the head moves back no lower than
// safe, and a command on a moved head fails (T-SEQ-5, head rollback). The
// status reports the sequencer as the source of the heads they set.
func TestCommands(t *testing.T) {
	c := derivertest.NewComposer(t)
	n := newNode(t, c.L1)
	n.run(t)
	ctx := t.Context()
	head := func() *types.Header {
		h, err := n.chain.Head(ctx)
		require.NoError(t, err)
		return h
	}

	tx := c.Transfer(bob, 1)
	genesis := head()
	// The hook runs on the parent, and its function takes the block.
	var hookHead, committed common.Hash
	b1, err := n.follower.Produce(ctx, inputs(genesis), provide(t, tx), func() func(*types.Header) {
		hookHead = head().Hash()
		return func(h *types.Header) { committed = h.Hash() }
	})
	require.NoError(t, err)
	require.Equal(t, genesis.Hash(), hookHead)
	require.Equal(t, b1.Block.Hash(), committed)
	require.Len(t, b1.Block.Transactions(), 1)
	require.Equal(t, alice, b1.Block.Coinbase())
	require.Equal(t, b1.Block.Hash(), head().Hash())
	require.Equal(t, follower.HeadSequencer, n.follower.Status().HeadSource)

	_, err = n.follower.Produce(ctx, inputs(genesis), provide(t), func() func(*types.Header) {
		t.Error("the hook ran on a moved head")
		return nil
	})
	require.ErrorIs(t, err, follower.ErrStaleParent)

	// The settlement of the produced block keeps it.
	settled := c.Compose(nil, derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{tx}})
	require.Equal(t, b1.Block.Hash(), settled.Blocks[0].Hash())
	c.Post(derivertest.Posting{Batch: settled})
	require.Eventually(t, func() bool { return n.follower.Status().Safe == 1 }, 10*time.Second, 5*time.Millisecond)
	require.Zero(t, n.chain.Built(), "derivation rebuilt a produced block")
	require.Equal(t, follower.HeadSequencer, n.follower.Status().HeadSource, "the settlement moved no head")

	b2, err := n.follower.Produce(ctx, inputs(b1.Block.Header()), provide(t), nil)
	require.NoError(t, err)
	b3, err := n.follower.Produce(ctx, inputs(b2.Block.Header()), provide(t), nil)
	require.NoError(t, err)

	require.NoError(t, n.follower.HeadTo(ctx, b2.Block.Hash()))
	require.Equal(t, b2.Block.Hash(), head().Hash())
	require.Equal(t, follower.HeadSequencer, n.follower.Status().HeadSource)
	_, ok, err := n.chain.CanonicalHash(ctx, 3)
	require.NoError(t, err)
	require.False(t, ok, "block 3 left the canonical chain")

	require.ErrorIs(t, n.follower.HeadTo(ctx, genesis.Hash()), follower.ErrBelowSafe)
	require.Equal(t, b2.Block.Hash(), head().Hash())

	require.ErrorIs(t, n.follower.Insert(ctx, []*types.Block{b2.Block}), follower.ErrStaleParent)
	require.NoError(t, n.follower.Insert(ctx, []*types.Block{b3.Block}))
	require.Equal(t, b3.Block.Hash(), head().Hash())
	require.Equal(t, follower.HeadSequencer, n.follower.Status().HeadSource)
	fc, err := n.m.ExecModule.GetForkChoice(ctx)
	require.NoError(t, err)
	require.Equal(t, b1.Block.Hash(), fc.SafeHash)
}

// TestProduceOutlivesItsCaller: a caller whose context ends after Run took
// its block gets the block, which is the head; leaving earlier it would miss
// the head move.
func TestProduceOutlivesItsCaller(t *testing.T) {
	c := derivertest.NewComposer(t)
	n := newNode(t, c.L1)
	n.run(t)
	genesis, err := n.chain.Head(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	built, err := n.follower.Produce(ctx, inputs(genesis), cancelling{cancel}, nil)
	require.NoError(t, err)
	head, err := n.chain.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, head.Hash(), built.Block.Hash())
	require.Equal(t, uint64(1), head.Number.Uint64())
}
