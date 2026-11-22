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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/execctx"
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
		})
	api := newEthApiForTest(NewBaseApi(ff, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, nil, nil)

	logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(int64(rpc.PendingBlockNumber))})
	require.NoError(t, err)
	require.Len(t, logs, missingHeaderChainLen*missingHeaderLogsPerBlock)
}
