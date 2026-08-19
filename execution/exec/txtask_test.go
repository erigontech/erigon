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
	"fmt"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
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
		Logs: types.Logs{{}},
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

// logEmittingFinalizeEngine drives a fixed number of block-end system calls at
// one contract, each of which emits a log. failAt (1-based) makes Finalize fail
// right after that call.
type logEmittingFinalizeEngine struct {
	rules.Engine
	contract accounts.Address
	calls    int
	failAt   int
}

func (e *logEmittingFinalizeEngine) Finalize(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chainReader rules.ChainReader,
	syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	for i := 1; i <= e.calls; i++ {
		if _, err := syscall(e.contract, nil); err != nil {
			return nil, err
		}
		if i == e.failAt {
			return nil, fmt.Errorf("finalize syscall %d failed", i)
		}
	}
	return nil, nil
}

// TestHistoricalBlockEndLogs pins the block-end log run of the historical
// tracer: the finalize system calls share one IntraBlockState and one txIndex,
// so the state holds their cumulative logs, and collecting per call counted the
// earlier ones again — k(k+1)/2 logs for k calls. A failed finalize attaches no
// logs at all, matching the serial and parallel paths.
func TestHistoricalBlockEndLogs(t *testing.T) {
	const syscalls = 3

	code := []byte{byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.LOG0), byte(vm.STOP)}
	contract := common.HexToAddress("0x00000000000000000000000000000000000c0de0")

	for _, tc := range []struct {
		name     string
		failAt   int
		wantLogs int
	}{
		{name: "each syscall counted once", wantLogs: syscalls},
		{name: "failed syscall attaches nothing", failAt: 2, wantLogs: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := log.New()
			db := temporaltest.NewTestDBWithStepSize(t, datadir.New(t.TempDir()), 16)
			require.NoError(t, db.UpdateTemporal(t.Context(), func(rwTx kv.TemporalRwTx) error {
				domains, err := execctx.NewSharedDomains(t.Context(), rwTx, logger)
				if err != nil {
					return err
				}
				defer domains.Close()
				acc := accounts.NewAccount()
				acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
				putter := domains.AsPutDel(rwTx)
				if err := putter.DomainPut(kv.CodeDomain, contract[:], code, 0, nil); err != nil {
					return err
				}
				if err := putter.DomainPut(kv.AccountsDomain, contract[:], accounts.SerialiseV3(&acc), 0, nil); err != nil {
					return err
				}
				return domains.Flush(t.Context(), rwTx)
			}))

			roTx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer roTx.Rollback()

			engine := &logEmittingFinalizeEngine{
				Engine:   ethash.NewFaker(),
				contract: accounts.InternAddress(contract),
				calls:    syscalls,
				failAt:   tc.failAt,
			}
			txTask := &TxTask{
				TxNum:   1,
				TxIndex: 0,
				Header: &types.Header{
					Number:   *uint256.NewInt(1),
					GasLimit: 10_000_000,
				},
				Config: chain.TestChainBerlinConfig,
				Engine: engine,
			}
			result := &TxResult{Task: txTask}

			rws := NewResultsQueue(1, 1)
			_, err = rws.Drain(t.Context(), result)
			require.NoError(t, err)

			p := &historicalResultProcessor{}
			cfg := &ExecArgs{ChainConfig: chain.TestChainBerlinConfig, Engine: engine}
			consumer := TraceConsumerFunc(func(*BlockResult, *TxResult, kv.TemporalTx) error { return nil })

			_, _, err = p.processResults(consumer, cfg, rws, txTask.TxNum, roTx, false, logger)
			require.NoError(t, err)

			if tc.failAt > 0 {
				require.Error(t, result.Err)
			} else {
				require.NoError(t, result.Err)
			}
			require.Len(t, result.Logs, tc.wantLogs)
		})
	}
}
