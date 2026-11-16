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

package sequencer_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/sequencer"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

// TestRetreatOnTheNodesPool: on the txpool a node runs, fed by the chain's
// state changes, a block built on a head moved back under the pool's view
// waits only for the pool to apply the retreat, and takes the transactions
// it returned to the pool.
func TestRetreatOnTheNodesPool(t *testing.T) {
	c := derivertest.NewComposer(t)
	m, chain := derivertest.NewL2Of(t, networks.Devnet, execmoduletester.WithTxPool())
	cfg := follower.DefaultConfig
	cfg.Poll, cfg.Retry, cfg.MaxRetry, cfg.BootAttempts = 5*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond, 3
	f, err := follower.New(c.L1, chain, derivertest.Config(), cfg, log.New())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = f.Run(ctx) }()
	genesis, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	require.Eventually(t, m.TxPool.Started, 10*time.Second, 10*time.Millisecond)

	reply, err := m.TxPoolGrpcServer.Add(t.Context(), &txpoolproto.AddRequest{RlpTxs: [][]byte{c.Transfer(common.HexToAddress("0xb0b"), 1)}})
	require.NoError(t, err)
	require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_SUCCESS}, reply.Imported, reply.Errors)

	s, err := sequencer.New(derivertest.Config(), sequencer.Config{Timing: devnet()}, f, m.TxPool, log.New())
	require.NoError(t, err)
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12}))
	first, err := chain.BlockByNumber(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, first.Transactions(), 1)
	// The pool has applied block 1: the transaction left it as mined.
	require.Eventually(t, func() bool {
		pending, baseFee, queued := m.TxPool.CountContent()
		return pending+baseFee+queued == 0
	}, 10*time.Second, 10*time.Millisecond)

	require.NoError(t, f.HeadTo(t.Context(), genesis.Hash()))
	start := time.Now()
	require.NoError(t, s.Handle(t.Context(), sequencer.LiveTick{SyncHeight: 12}))
	require.Less(t, time.Since(start), 5*time.Second, "the build waited out its bound")
	again, err := chain.BlockByNumber(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, again.Transactions(), 1, "the retreat's transaction left out")
	require.Equal(t, first.Transactions()[0].Hash(), again.Transactions()[0].Hash())
}
