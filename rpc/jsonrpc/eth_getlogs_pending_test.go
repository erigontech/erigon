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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// A node whose pre-executed block is at the canonical head's height (its own build of a block the chain already
// holds) has nothing ahead of the head to add: the pending range is the canonical one.
func TestPendingLogsDoNotRepeatTheHeadWhenThePreExecutedBlockIsAtIt(t *testing.T) {
	t.Parallel()

	m := setupLogsChain(t)
	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(m.Ctx, roTx, m.Log)
	require.NoError(t, err)
	defer sd.Close()
	head, err := m.BlockReader.BlockByNumber(m.Ctx, roTx, missingHeaderChainLen)
	require.NoError(t, err)
	var receipts types.Receipts
	for i := range head.Transactions() {
		receipts = append(receipts, &types.Receipt{Status: types.ReceiptStatusSuccessful, Logs: []*types.Log{{TxIndex: hexutil.Uint(i), Topics: []common.Hash{{0x11}}}}})
	}

	ff := rpchelper.New(m.Ctx, rpchelper.FiltersConfig{}, nil, nil, nil, func() {}, log.New(), nil)
	ff.SetPreconfirmedProvider(
		func() (*execctx.SharedDomains, common.Hash, uint64, func(), bool) {
			return sd, common.Hash{0xbb}, missingHeaderChainLen, func() {}, true
		},
		func(*execctx.SharedDomains, uint64) (types.Transactions, types.Receipts, bool) {
			return head.Transactions(), receipts, true
		},
		nil)
	api := newEthApiForTest(NewBaseApi(ff, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, nil, nil)

	logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(int64(rpc.PendingBlockNumber))})
	require.NoError(t, err)
	require.Len(t, logs, missingHeaderChainLen*missingHeaderLogsPerBlock)
}

// A block this node has sealed but the chain has not yet made canonical lies between the executed head and the block
// being built, so `pending` serves it from pre-exec alongside the block being built.
func TestPendingLogsServeTheSealedBlocksBetweenTheHeadAndTheBlockBeingBuilt(t *testing.T) {
	t.Parallel()

	m := setupLogsChain(t)
	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(m.Ctx, roTx, m.Log)
	require.NoError(t, err)
	defer sd.Close()
	head, err := m.BlockReader.BlockByNumber(m.Ctx, roTx, missingHeaderChainLen)
	require.NoError(t, err)
	receiptsWithTopic := func(topic byte) types.Receipts {
		var receipts types.Receipts
		for i := range head.Transactions() {
			receipts = append(receipts, &types.Receipt{Status: types.ReceiptStatusSuccessful, Logs: []*types.Log{{TxIndex: hexutil.Uint(i), Topics: []common.Hash{{topic}}}}})
		}
		return receipts
	}
	sealed := uint64(missingHeaderChainLen + 1)
	building := sealed + 1

	ff := rpchelper.New(m.Ctx, rpchelper.FiltersConfig{}, nil, nil, nil, func() {}, log.New(), nil)
	ff.SetPreconfirmedProvider(
		func() (*execctx.SharedDomains, common.Hash, uint64, func(), bool) {
			return sd, common.Hash{0xbb}, building, func() {}, true
		},
		func(*execctx.SharedDomains, uint64) (types.Transactions, types.Receipts, bool) {
			return head.Transactions(), receiptsWithTopic(0x22), true
		},
		func(above uint64) []rpchelper.PreconfirmedBlock {
			if above >= sealed {
				return nil
			}
			return []rpchelper.PreconfirmedBlock{{Number: sealed, Hash: common.Hash{0xaa}, Txs: head.Transactions(), Receipts: receiptsWithTopic(0x11)}}
		})
	api := newEthApiForTest(NewBaseApi(ff, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, nil, nil)

	logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(int64(rpc.PendingBlockNumber))})
	require.NoError(t, err)
	canonical := missingHeaderChainLen * missingHeaderLogsPerBlock
	require.Len(t, logs, canonical+2*len(head.Transactions()))
	for i, l := range logs[canonical:] {
		want, wantHash, wantTopic := sealed, common.Hash{0xaa}, common.Hash{0x11}
		if i >= len(head.Transactions()) {
			want, wantHash, wantTopic = building, common.Hash{0xbb}, common.Hash{0x22}
		}
		require.Equal(t, want, uint64(l.BlockNumber))
		require.Equal(t, wantHash, l.BlockHash)
		require.Equal(t, wantTopic, l.Topics[0])
	}
}

// A block the fork choice has published but not yet committed is already the head every other read reports, so
// `pending` serves its logs.
func TestPendingLogsServeABlockPublishedAheadOfTheCommit(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false

	h := newOverlayAheadHarness(t, true)
	num := h.overlayHeader.Number.Uint64()
	overlay := h.doms.BlockOverlay()
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, num))

	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	minTxNum, err := h.m.BlockReader.TxnumReader().Min(h.m.Ctx, tx, num)
	require.NoError(t, err)
	maxTxNum := minTxNum + 1 + uint64(len(h.overlayBody.Transactions))
	require.NoError(t, rawdbv3.TxNums.Append(overlay, num, maxTxNum))
	putDel := h.doms.AsPutDel(tx)
	require.NoError(t, rawdb.WriteReceiptCacheV2(putDel, nil, minTxNum))
	for i, txn := range h.overlayBody.Transactions {
		receipt := &types.Receipt{
			Type:              txn.Type(),
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: uint64(i+1) * params.TxGas,
			GasUsed:           params.TxGas,
			TransactionIndex:  uint(i),
			Logs:              []*types.Log{{Address: common.Address{0x77}, Topics: []common.Hash{{0x11}}, TxIndex: hexutil.Uint(i)}},
		}
		require.NoError(t, rawdb.WriteReceiptCacheV2(putDel, receipt, minTxNum+1+uint64(i)))
	}
	require.NoError(t, rawdb.WriteReceiptCacheV2(putDel, nil, maxTxNum))

	api := newEthApiForTest(h.base, h.m.DB, nil, nil)
	head, err := api.BlockNumber(h.m.Ctx)
	require.NoError(t, err)
	require.Equal(t, num, uint64(head), "fixture: the published block is the head")

	logs, err := api.GetLogs(h.m.Ctx, filters.FilterCriteria{FromBlock: new(big.Int).SetUint64(num), ToBlock: big.NewInt(int64(rpc.PendingBlockNumber))})
	require.NoError(t, err)
	require.Len(t, logs, len(h.overlayBody.Transactions))
	for i, l := range logs {
		require.Equal(t, num, uint64(l.BlockNumber))
		require.Equal(t, h.overlayHeader.Hash(), l.BlockHash)
		require.Equal(t, h.overlayBody.Transactions[i].Hash(), l.TxHash)
	}
}
