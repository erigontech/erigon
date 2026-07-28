// Copyright 2024 The Erigon Authors
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

package exec

import (
	"context"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// TestAwaitDrainExitsOnContextCancel reproduces the infinite-loop described in
// https://github.com/erigontech/erigon/issues/18252.
//
// Scenario: a map worker crashes after some results have already been moved from
// the resultCh into the heap (e.g. txnums 5..N), but the result the reduce is
// waiting for (txnum 0) was never produced.  The map goroutine calls
// out.Close(), which nils resultCh (because the channel is empty at that
// moment), and the errgroup cancels the shared context.
//
// Before the fix, AwaitDrain took the "resultCh == nil" fast-path and returned
// (false, nil) without ever inspecting ctx, so the reduce loop never terminated.
func TestAwaitDrainExitsOnContextCancel(t *testing.T) {
	q := NewResultsQueue(10, 10)

	// Simulate a worker that produced txnum=5 but the worker responsible for
	// txnum=0 already panicked without producing a result.
	bgCtx := context.Background()
	err := q.Add(bgCtx, &TxResult{Task: &TxTask{TxNum: 5}})
	require.NoError(t, err)

	// Let AwaitDrain move the item from the channel into the heap.
	_, err = q.AwaitDrain(bgCtx, 50*time.Millisecond)
	require.NoError(t, err)
	// resultCh is now empty (all items are in the heap).

	// Simulate the map goroutine calling out.Close() after crashing.
	// Because resultCh is empty, Close() closes and nils the channel immediately.
	q.Close()

	// Simulate the errgroup cancelling the context because the map goroutine
	// returned an error.
	cancelCtx, cancel := context.WithCancel(bgCtx)
	cancel()

	// The reduce loop must exit promptly; without the fix it spins forever.
	done := make(chan error, 1)
	go func() {
		for {
			closed, err := q.AwaitDrain(cancelCtx, 10*time.Millisecond)
			if err != nil {
				done <- err
				return
			}
			if closed {
				done <- nil
				return
			}
			// Simulate processResults making no progress (txnum 0 missing).
		}
	}()

	select {
	case err := <-done:
		require.Error(t, err, "reduce loop must exit with an error when ctx is cancelled")
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("AwaitDrain did not respect context cancellation – infinite loop detected (issue #18252)")
	}
}

// TestCreateReceiptTxIndex verifies the invariant that CreateReceipt assigns the local
// block transaction index parameter directly to the receipt's TransactionIndex, preventing
// regressions where the global TxNum is leaked into the receipt during partial block recovery.
func TestCreateReceiptTxIndex(t *testing.T) {
	t.Parallel()

	const (
		txIndex       = 196
		firstLogIndex = 7
	)
	const (
		txNum           uint64 = 3_548_828_125
		priorCumGasUsed uint64 = 47_198_456
		receiptGasUsed  uint64 = 21_000
		blockNumber     uint64 = 25_200_946
	)

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	to := crypto.PubkeyToAddress(key.PublicKey)
	config := chain.TestChainBerlinConfig
	signer := types.MakeSigner(config, blockNumber, 0)
	signedTx, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    0,
			To:       &to,
			Value:    *uint256.NewInt(0),
			GasLimit: receiptGasUsed,
		},
		GasPrice: *uint256.NewInt(1),
	}, *signer, key)
	require.NoError(t, err)

	txs := make(types.Transactions, txIndex+1)
	txs[txIndex] = signedTx
	txTask := &TxTask{
		TxNum:   txNum,
		TxIndex: txIndex,
		Header:  &types.Header{Number: *uint256.NewInt(blockNumber)},
		Txs:     txs,
		Config:  config,
	}
	result := &TxResult{
		Task: txTask,
		ExecutionResult: evmtypes.ExecutionResult{
			ReceiptGasUsed: receiptGasUsed,
		},
		Logs: []*types.Log{{}},
	}

	receipt, err := result.CreateReceipt(txTask.TxIndex, priorCumGasUsed+result.ExecutionResult.ReceiptGasUsed, firstLogIndex)
	require.NoError(t, err)

	require.Equal(t, uint(txIndex), receipt.TransactionIndex)
	require.NotEqual(t, uint(txNum), receipt.TransactionIndex)
	require.Equal(t, signedTx.Hash(), receipt.TxHash)
	require.Equal(t, txTask.BlockHash(), receipt.BlockHash)
	require.Equal(t, priorCumGasUsed+receiptGasUsed, receipt.CumulativeGasUsed)
	require.Equal(t, receiptGasUsed, receipt.GasUsed)
	require.Equal(t, uint32(firstLogIndex), receipt.FirstLogIndexWithinBlock)
	require.Len(t, receipt.Logs, 1)
	require.Equal(t, hexutil.Uint(firstLogIndex), receipt.Logs[0].Index)
}
