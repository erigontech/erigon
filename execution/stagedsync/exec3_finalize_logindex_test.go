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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// logIndexContract is the address the block-end system call emits its log from.
var logIndexContract = common.HexToAddress("0x00000000000000000000000000000000000c0de0")

func indexedTxNums(t *testing.T, tx kv.TemporalTx, idx kv.InvertedIdx, key []byte) []uint64 {
	t.Helper()
	it, err := tx.IndexRange(idx, key, 0, -1, order.Asc, kv.Unlim)
	require.NoError(t, err)
	txNums, err := stream.ToArrayU64(it)
	require.NoError(t, err)
	return txNums
}

// A block-end system call emits its logs outside any transaction receipt, at the
// block's final txNum. Both executors must index them the same way: the log index
// files they build are compared against the published snapshots, and a chain whose
// consensus emits logs at block end, such as Gnosis, has one in nearly every block.
func TestSerialBlockEndLogsReachLogIndex(t *testing.T) {
	engine := &logEmittingSyscallEngine{
		Engine:   ethash.NewFaker(),
		contract: accounts.InternAddress(logIndexContract),
		calls:    1,
	}
	se, task := newSerialFinalizeTestExec(t, engine)
	task.Header.ReceiptHash = empty.RootHash
	rwTx := se.applyTx.(kv.TemporalRwTx)
	require.NoError(t, putLogEmittingContract(se.doms.AsPutDel(rwTx), logIndexContract))

	block := types.NewBlockFromStorage(common.Hash{}, task.Header, nil, nil, nil, nil)
	_, err := se.executeBlock(t.Context(), block, []exec.Task{task}, true, false)
	require.NoError(t, err)
	require.NoError(t, se.doms.Flush(t.Context(), rwTx))

	require.Equal(t, []uint64{task.TxNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
}

func TestParallelBlockEndLogsReachLogIndex(t *testing.T) {
	db := newResumeTestDB(t)
	config := chain.TestChainBerlinConfig
	seedLogEmittingContract(t, db, logIndexContract)

	logger := log.New()
	rwTx, domains := temporaltest.NewTestTxSD(t, db)
	pe := &parallelExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: config,
				db:          db,
				engine: &logEmittingSyscallEngine{
					Engine:   ethash.NewFaker(),
					contract: accounts.InternAddress(logIndexContract),
					calls:    1,
				},
				vmConfig: &vm.Config{},
			},
			doms:   domains,
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, false, logger)),
			logger: logger,
		},
	}

	txTask := &exec.TxTask{
		Header:  &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000},
		TxNum:   1,
		TxIndex: 0,
		Config:  config,
	}
	be := newBlockExec(newParallelTestBlock(1), new(protocol.GasPool).AddGas(10_000_000), nil, make(chan applyResult, 4), nil, false, nil)
	eTask := &execTask{Task: txTask, index: 0}
	be.tasks = []*execTask{eTask}
	be.results = []*execResult{nil}
	be.execTasks.setInProgress(0)

	txResult := &exec.TxResult{
		Task: &taskVersion{
			execTask: eTask,
			version:  state.Version{BlockNum: 1, TxIndex: 0, Incarnation: 1, TxNum: txTask.TxNum},
		},
		ExecutionResult: evmtypes.ExecutionResult{ReceiptGasUsed: 21000},
	}

	res, err := be.nextResult(t.Context(), pe, txResult, rwTx)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NoError(t, res.Err)
	require.Len(t, txResult.Logs, 1)
	require.NoError(t, domains.Flush(t.Context(), rwTx))

	require.Equal(t, []uint64{txTask.TxNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
}
