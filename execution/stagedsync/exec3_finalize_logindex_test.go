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
	"github.com/erigontech/erigon/db/state/execctx"
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

const logIndexFinalTxNum = uint64(101)

var (
	logIndexContract = common.HexToAddress("0x00000000000000000000000000000000000c0de0")
	logIndexTopic    = common.Hash{31: 0x42}
)

// An empty block still has a start and an end virtual task. Their txNums must
// differ from each other and from the block number so the index assertions
// detect using the wrong identifier.
func newLogIndexTestTasks(config *chain.Config) []exec.Task {
	header := &types.Header{Number: *uint256.NewInt(7), GasLimit: 10_000_000, ReceiptHash: empty.RootHash}
	tasks := make([]exec.Task, 2)
	for i := range tasks {
		tasks[i] = &exec.TxTask{
			Header:          header,
			TxNum:           logIndexFinalTxNum - 1 + uint64(i),
			TxIndex:         i - 1,
			Config:          config,
			EvmBlockContext: evmtypes.BlockContext{BlockNumber: header.Number.Uint64()},
		}
	}
	return tasks
}

func newParallelLogIndexTestExec(db kv.TemporalRwDB, domains *execctx.SharedDomains, calls int) *parallelExecutor {
	logger := log.New()
	return &parallelExecutor{
		txExecutor: txExecutor{
			cfg: ExecuteBlockCfg{
				chainConfig: chain.TestChainBerlinConfig,
				db:          db,
				engine: &logEmittingSyscallEngine{
					Engine:   ethash.NewFaker(),
					contract: accounts.InternAddress(logIndexContract),
					calls:    calls,
				},
				vmConfig: &vm.Config{},
			},
			doms:   domains,
			rs:     state.NewStateV3Buffered(state.NewStateV3(domains, false, logger)),
			logger: logger,
		},
	}
}

func runParallelLogIndexTestBlock(t *testing.T, pe *parallelExecutor, tx kv.TemporalTx) {
	t.Helper()
	tasks := newLogIndexTestTasks(pe.cfg.chainConfig)
	be := newBlockExec(newParallelTestBlockFromTasks(tasks), new(protocol.GasPool).AddGas(tasks[0].BlockGasLimit()),
		nil, make(chan applyResult, 4), nil, false, nil)
	be.tasks = make([]*execTask, len(tasks))
	be.results = make([]*execResult, len(tasks))
	for i, task := range tasks {
		be.tasks[i] = &execTask{Task: task, index: i}
	}
	for i, task := range be.tasks {
		be.execTasks.setInProgress(i)
		version := task.Version()
		version.Incarnation = 1
		res, err := be.nextResult(t.Context(), pe, &exec.TxResult{
			Task: &taskVersion{execTask: task, version: version},
		}, tx)
		require.NoError(t, err)
		if i < len(tasks)-1 {
			require.Nil(t, res)
		} else {
			require.NotNil(t, res)
			require.NoError(t, res.Err)
		}
	}
}

func indexedTxNums(t *testing.T, tx kv.TemporalTx, idx kv.InvertedIdx, key []byte) []uint64 {
	t.Helper()
	it, err := tx.IndexRange(idx, key, 0, -1, order.Asc, kv.Unlim)
	require.NoError(t, err)
	txNums, err := stream.ToArrayU64(it)
	require.NoError(t, err)
	return txNums
}

// Consensus system calls can emit logs after all regular transactions have
// finished. Those logs have no receipt, but snapshot indexes must still place
// them at the block's final virtual txNum.
func TestSerialBlockEndLogsReachLogIndex(t *testing.T) {
	engine := &logEmittingSyscallEngine{
		Engine:   ethash.NewFaker(),
		contract: accounts.InternAddress(logIndexContract),
		calls:    1,
	}
	se, _ := newSerialFinalizeTestExec(t, engine)
	tasks := newLogIndexTestTasks(se.cfg.chainConfig)
	rwTx := se.applyTx.(kv.TemporalRwTx)
	require.NoError(t, putLogEmittingContract(se.doms.AsPutDel(rwTx), logIndexContract))

	block := newParallelTestBlockFromTasks(tasks)
	_, err := se.executeBlock(t.Context(), block, tasks, false, false)
	require.NoError(t, err)
	require.NoError(t, se.doms.Flush(t.Context(), rwTx))

	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogTopicIdx, logIndexTopic[:]))
}

func TestParallelBlockEndLogsReachLogIndex(t *testing.T) {
	db := newResumeTestDB(t)
	seedLogEmittingContract(t, db, logIndexContract)

	rwTx, domains := temporaltest.NewTestTxSD(t, db)
	pe := newParallelLogIndexTestExec(db, domains, 1)
	runParallelLogIndexTestBlock(t, pe, rwTx)
	require.NoError(t, domains.Flush(t.Context(), rwTx))

	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogTopicIdx, logIndexTopic[:]))
}
