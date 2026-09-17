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

package jsonrpc

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc"
	ethapi2 "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// pendingTestAPIs builds a short committed chain and an API set whose Filters
// optionally carries the in-memory pending block a payload-building node holds.
func pendingTestAPIs(t *testing.T, withPending bool) (*APIImpl, *execmoduletester.ExecModuleTester, uint64) {
	t.Helper()
	m := execmoduletester.New(t)
	c, err := m.GenerateChain(5, func(i int, gen *blockgen.BlockGen) { gen.SetCoinbase(common.Address{1}) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, shards.NewEvents())
	tip := c.TopBlock
	pendingNum := tip.NumberU64() + 1
	if withPending {
		header := &types.Header{
			ParentHash: tip.Hash(),
			Number:     *uint256.NewInt(pendingNum),
			Difficulty: *uint256.NewInt(0),
			Time:       tip.Time() + 12,
			GasLimit:   30_000_000,
			BaseFee:    uint256.NewInt(875000000),
		}
		enc, err := rlp.EncodeToBytes(types.NewBlock(header, nil, nil, nil, nil, nil))
		require.NoError(t, err)
		ff.HandlePendingBlock(&txpoolproto.OnPendingBlockReply{RplBlock: enc})
		require.NotNil(t, ff.LastPendingBlock())
	}

	base := NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	return newEthApiForTest(base, m.DB, nil, nil), m, pendingNum
}

// TestPendingTagOnStateEndpoints pins the contract that the state endpoints
// serve the newest state the node can produce for "pending". Erigon has no
// queryable pending state, and feeding the in-memory pending block number into
// a committed-state reader used to fail outright on a payload-building node.
func TestPendingTagOnStateEndpoints(t *testing.T) {
	addr := common.Address{1}
	pending := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	for _, withPending := range []bool{true, false} {
		name := "pending block absent"
		if withPending {
			name = "pending block present"
		}
		t.Run(name, func(t *testing.T) {
			api, m, _ := pendingTestAPIs(t, withPending)
			ctx := m.Ctx

			wantBalance, err := api.GetBalance(ctx, addr, &latest)
			require.NoError(t, err)

			gotBalance, err := api.GetBalance(ctx, addr, &pending)
			require.NoError(t, err, "eth_getBalance must not fail on the pending tag")
			require.Equal(t, wantBalance, gotBalance)

			_, err = api.GetCode(ctx, addr, &pending)
			require.NoError(t, err)

			_, err = api.GetStorageAt(ctx, addr, "0x0", &pending)
			require.NoError(t, err)

			_, err = api.GetStorageValues(ctx, map[common.Address][]common.Hash{addr: {{}}}, &pending)
			require.NoError(t, err)

			wantGas, err := api.EstimateGas(ctx, &ethapi2.CallArgs{From: &addr, To: &addr}, &latest, nil, nil)
			require.NoError(t, err)
			gotGas, err := api.EstimateGas(ctx, &ethapi2.CallArgs{From: &addr, To: &addr}, &pending, nil, nil)
			require.NoError(t, err, "eth_estimateGas must not fail on the pending tag")
			require.Equal(t, wantGas, gotGas)

			base := api.BaseAPI
			_, err = NewOtterscanAPI(base, m.DB, 25).HasCode(ctx, addr, pending)
			require.NoError(t, err)

			_, err = NewErigonAPI(base, m.DB, nil).GetBalanceChangesInBlock(ctx, pending)
			require.NoError(t, err)

			gql := NewGraphQLAPI(base, m.DB, nil, nil, &rpccfg.GraphQLApiConfig{GasCap: 5000000})
			_, _, _, err = gql.GetAccountInfo(ctx, addr, rpc.PendingBlockNumber)
			require.NoError(t, err)
			_, err = gql.GetAccountStorage(ctx, addr, "0x0", rpc.PendingBlockNumber)
			require.NoError(t, err)
		})
	}
}

// TestPendingTagKeepsExplicitEndpointsIntact guards the endpoints that serve the
// in-memory pending block through their own pre-check: they must keep answering
// from it rather than falling back to the resolver's latest-executed block.
func TestPendingTagKeepsExplicitEndpointsIntact(t *testing.T) {
	api, m, pendingNum := pendingTestAPIs(t, true)
	ctx := m.Ctx
	base := api.BaseAPI
	pending := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)

	block, err := api.GetBlockByNumber(ctx, rpc.PendingBlockNumber, false)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Equal(t, pendingNum, uint64(block.Number.Uint64()), "must serve the pending block, not latest executed")

	count, err := api.GetBlockTransactionCountByNumber(ctx, rpc.PendingBlockNumber)
	require.NoError(t, err)
	require.NotNil(t, count)
	require.Zero(t, uint64(*count))

	uncles, err := api.GetUncleCountByBlockNumber(ctx, rpc.PendingBlockNumber)
	require.NoError(t, err)
	require.NotNil(t, uncles)
	require.Zero(t, uint64(*uncles), "pending block carries no uncles")

	dbg := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
	for _, tc := range []struct {
		name string
		call func() (any, error)
	}{
		{"debug_getRawHeader", func() (any, error) { return dbg.GetRawHeader(ctx, pending) }},
		{"debug_getRawBlock", func() (any, error) { return dbg.GetRawBlock(ctx, pending) }},
		{"debug_getRawReceipts", func() (any, error) { return dbg.GetRawReceipts(ctx, pending) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.call()
			require.NoError(t, err)
			require.Empty(t, got, "the raw debug family returns null for a published pending block")
		})
	}
}

// TestPendingTagStillRejectedByReplayEndpoints guards the replay paths that
// cannot acquire state matching a pending block.
func TestPendingTagStillRejectedByReplayEndpoints(t *testing.T) {
	api, m, _ := pendingTestAPIs(t, true)
	addr := common.Address{1}
	pending := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	args := ethapi2.CallArgs{From: &addr, To: &addr}

	_, err := api.Call(m.Ctx, args, &pending, nil, nil)
	require.ErrorIs(t, err, errPendingStateNotSupported)

	_, err = api.CreateAccessList(m.Ctx, args, &pending, nil, nil)
	require.ErrorIs(t, err, errPendingStateNotSupported)
}

// TestResolverKeepsCallerView is the core contract: the resolver answers from
// the view the caller handed it and never swaps in a different one. A committed
// tx must not see the overlay head even while an overlay is published.
func TestResolverKeepsCallerView(t *testing.T) {
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlayNum := overlayHeader.Number.Uint64()
	require.NoError(t, stages.SaveStageProgress(events.LatestSD().BlockOverlay(), stages.Execution, overlayNum))

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	committed, _, _, err := rpchelper.GetCanonicalBlockNumber(m.Ctx, latest, tx, m.BlockReader)
	require.NoError(t, err)
	require.Equal(t, overlayNum-1, committed, "a plain tx resolves the committed head")

	overlaid, _, _, err := rpchelper.GetCanonicalBlockNumber(m.Ctx, latest, base.filters.WithOverlay(tx), m.BlockReader)
	require.NoError(t, err)
	require.Equal(t, overlayNum, overlaid, "an overlay-aware tx resolves the overlay head")
}

// TestStateEndpointsPinTheOverlayOnce drives the overlay/commit race directly:
// the overlay is unpublished while the selector is being resolved, which used
// to leave the block number on one generation and the state read on another.
// A request that pins once keeps answering from the generation it started on.
func TestStateEndpointsPinTheOverlayOnce(t *testing.T) {
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	addr := common.Address{1}

	t.Run("eth_getBalance", func(t *testing.T) {
		base, m, overlayHeader := newOverlayUnpublishTestAPI(t)
		require.NoError(t, stages.SaveStageProgress(base.filters.LatestSD().BlockOverlay(), stages.Execution, overlayHeader.Number.Uint64()))
		api := newEthApiForTest(base, m.DB, nil, nil)

		_, err := api.GetBalance(m.Ctx, addr, &latest)
		require.NoError(t, err, "the overlay unpublished mid-resolution must not split the view")
	})

	t.Run("eth_estimateGas", func(t *testing.T) {
		base, m, overlayHeader := newOverlayUnpublishTestAPI(t)
		require.NoError(t, stages.SaveStageProgress(base.filters.LatestSD().BlockOverlay(), stages.Execution, overlayHeader.Number.Uint64()))
		api := newEthApiForTest(base, m.DB, nil, nil)

		_, err := api.EstimateGas(m.Ctx, &ethapi2.CallArgs{From: &addr, To: &addr}, &latest, nil, nil)
		require.NoError(t, err, "the overlay unpublished mid-resolution must not split the view")
	})
}
