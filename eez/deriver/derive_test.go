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

package deriver_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
)

var (
	alice = common.HexToAddress("0xa11ce")
	bob   = common.HexToAddress("0xb0b")
)

type follower struct {
	chain   *deriver.ExecChain
	deriver *deriver.Deriver
	dir     string
}

func newFollower(t *testing.T) *follower {
	t.Helper()
	m, chain := derivertest.NewL2(t)
	dir := t.TempDir()
	d, err := deriver.New(chain, m.ChainConfig, dir, log.New())
	require.NoError(t, err)
	return &follower{chain: chain, deriver: d, dir: dir}
}

// TestDeriveAnchorBatches is T-DRV-1 end to end: batches of user
// transactions derive to the composer's blocks, in fresh execution.
func TestDeriveAnchorBatches(t *testing.T) {
	c := derivertest.NewComposer(t)
	first := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 1), c.Transfer(bob, 2)}},
		derivertest.Block{Coinbase: alice, Extra: []byte("eez"), Txs: [][]byte{}},
		derivertest.Block{Coinbase: bob, Extra: []byte("eez"), Txs: [][]byte{c.Transfer(alice, 3)}},
	)
	c.Post(derivertest.Posting{Batch: first})
	second := c.Compose(nil,
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(alice, 4)}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	c.Post(derivertest.Posting{Batch: second})

	f := newFollower(t)
	batches := c.Scan()
	require.Len(t, batches, 2)
	require.NoError(t, f.deriver.Reconcile(t.Context(), batches))
	derivertest.RequireChain(t, f.chain, append(first.Blocks, second.Blocks...)...)
	require.Equal(t, uint64(5), f.deriver.Cursor())

	cp, err := deriver.LoadCheckpoint(f.dir)
	require.NoError(t, err)
	require.Equal(t, deriver.Checkpoint{L1Block: 2, L1BlockHash: batches[1].L1BlockHash, TxHash: batches[1].TxHash, L2Cursor: 5, L2BlockHash: second.Blocks[1].Hash()}, *cp)

	// A batch derived already is not derived again.
	require.NoError(t, f.deriver.Reconcile(t.Context(), batches))
	require.Equal(t, uint64(5), f.deriver.Cursor())
}

// inbound delivers value wei from an L1 account to to.
func inbound(to common.Address, value uint64) da.Action {
	return da.Action{
		SourceRollupID: protocol.MainnetRollupID,
		TargetRollupID: 1,
		SourceAddress:  common.HexToAddress("0x11"),
		TargetAddress:  to,
		Value:          *uint256.NewInt(value),
		Data:           []byte{},
		Success:        true,
		ReturnData:     []byte{},
	}
}

// outbound calls to on L1 from from.
func outbound(from, to common.Address) da.Action {
	return da.Action{
		SourceRollupID: 1,
		TargetRollupID: protocol.MainnetRollupID,
		SourceAddress:  from,
		TargetAddress:  to,
		Data:           []byte{},
		Success:        true,
		ReturnData:     []byte{},
	}
}

// TestDeriveSyncBlock derives a Sync block with system transactions, in
// full and settled in part.
func TestDeriveSyncBlock(t *testing.T) {
	c := derivertest.NewComposer(t)
	b := c.Compose([]da.Action{inbound(alice, 7), inbound(bob, 8)},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 1)}},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}},
	)
	sync := b.Blocks[1]
	require.Equal(t, protocol.SystemTxType, sync.Transactions()[0].Type())
	require.Len(t, sync.Transactions(), 3)

	t.Run("full", func(t *testing.T) {
		c.Post(derivertest.Posting{Batch: b})
		f := newFollower(t)
		require.NoError(t, f.deriver.Reconcile(t.Context(), c.Scan()))
		derivertest.RequireChain(t, f.chain, b.Blocks...)
		require.Equal(t, uint64(2), f.deriver.Cursor())
		receipts, err := f.chain.Receipts(t.Context(), sync)
		require.NoError(t, err)
		for i, r := range receipts {
			require.Equal(t, types.ReceiptStatusSuccessful, r.Status, "transaction %d", i)
		}
		nonce, err := f.chain.Nonce(t.Context(), 2, protocol.SystemAddress)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nonce)
	})
	t.Run("partial", func(t *testing.T) {
		c := derivertest.NewComposer(t)
		b := c.Compose([]da.Action{inbound(alice, 7), inbound(bob, 8)},
			derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 1)}},
			derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 2)}},
		)
		c.Post(derivertest.Posting{Batch: b, Applied: []int{0, 1}})
		f := newFollower(t)
		require.NoError(t, f.deriver.Reconcile(t.Context(), c.Scan()))
		derivertest.RequireChain(t, f.chain, b.Blocks[0], b.Syncs[1])
		require.Len(t, b.Syncs[1].Transactions(), 2, "the second delivery did not run")
	})
}

// TestDeriveOutboundGate refuses an outbound entry no L2 call made.
func TestDeriveOutboundGate(t *testing.T) {
	c := derivertest.NewComposer(t)
	b := c.Compose([]da.Action{outbound(derivertest.UserAddress(), bob)},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 1)}},
	)
	c.Post(derivertest.Posting{Batch: b})
	f := newFollower(t)
	err := f.deriver.Reconcile(t.Context(), c.Scan())
	require.ErrorIs(t, err, deriver.ErrDiverged)
	require.ErrorContains(t, err, deriver.ErrOutboundUnauthorized.Error())
	var div *deriver.DivergenceError
	require.ErrorAs(t, err, &div)
	require.Equal(t, uint64(1), div.L2Block)
	require.Zero(t, f.deriver.Cursor())
}

// TestDeriveDivergence halts on a settled block the payload does not
// reproduce.
func TestDeriveDivergence(t *testing.T) {
	c := derivertest.NewComposer(t)
	b := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 1)}},
	)
	// The DA payload names another beneficiary than the settled block has.
	b.Payload.Span.Beneficiaries[0] = bob
	b.Batch.CallData = da.Encode((&da.Payload{RollupID: 1, Operations: b.Payload.Span.Encode()}).Messages()...)
	c.Post(derivertest.Posting{Batch: b})

	f := newFollower(t)
	err := f.deriver.Reconcile(context.Background(), c.Scan())
	var div *deriver.DivergenceError
	require.ErrorAs(t, err, &div)
	require.Equal(t, uint64(1), div.L2Block)
	require.Zero(t, f.deriver.Cursor())
}
