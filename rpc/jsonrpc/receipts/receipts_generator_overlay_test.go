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

package receipts_test

import (
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// TestGetReceiptLogIndexThroughOverlay pins the wiring that lets GetReceipt see a
// block whose commit is in flight: the log index must be resolved through
// Filters.WithTemporalOverlay, not read from the committed tx. The overlay is
// seeded with a value the committed tx does not hold, so only a routed read can
// produce it.
func TestGetReceiptLogIndexThroughOverlay(t *testing.T) {
	signer := types.LatestSignerForChainID(nil)
	m := mockWithGenerator(t, 2, func(i int, block *blockgen.BlockGen) {
		txn, err := types.SignTx(
			types.NewTransaction(block.TxNonce(testAddr), testAddr, uint256.NewInt(1), params.TxGas, nil, nil),
			*signer, testKey)
		require.NoError(t, err)
		block.AddTx(txn)
	})

	tx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const blockNum = uint64(2)
	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 1)

	minTxNum, err := m.BlockReader.TxnumReader().Min(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	txNum := minTxNum + 1 // txIndex 0, past the block's system tx

	const overlayLogIdx = uint32(41)

	sd, err := execctx.NewSharedDomains(m.Ctx, tx, m.Log)
	require.NoError(t, err)
	defer sd.Close()
	require.NoError(t, sd.InitBlockOverlay(tx, t.TempDir()))
	require.NoError(t, rawtemporaldb.AppendReceiptMetadata(sd.AsPutDel(tx), overlayLogIdx, 0, 0, txNum))

	events := shards.NewEvents()
	events.PublishOverlay(sd)
	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, events)

	gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute, ff)
	receipt, err := gen.GetReceipt(m.Ctx, m.ChainConfig, tx, block.HeaderNoCopy(), block.Transactions()[0], 0, txNum, nil)
	require.NoError(t, err)
	require.Equal(t, overlayLogIdx, receipt.FirstLogIndexWithinBlock,
		"GetReceipt must resolve the log index through the block overlay")
}
