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
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/holiman/uint256"
)

// TestParallelBlockEndLogsReachLogIndex is the parallel analog of
// TestSerialBlockEndLogsReachLogIndex: a block-end system call emits a log at the
// block's final txNum, which must reach the log indexes. It drives nextResult to
// produce the finalize result, confirms that result carries the block-end syscall
// log, then indexes it exactly as the apply loop does and asserts reachability via
// IndexRange.
//
// This is a regression guard that the block-end logs flow through the finalize
// result to the index — i.e. moving indexing off the exec loop did not drop them.
// It is NOT a proof that the exec-loop/apply-loop data race is gone: both the old
// (exec-loop) and new (apply-loop) code index these logs, so only the race
// detector under real concurrency distinguishes them.
func TestParallelBlockEndLogsReachLogIndex(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mdbx InMem test databases are not supported on windows")
	}
	ctx := context.Background()
	db := newResumeTestDB(t)
	config := chain.TestChainBerlinConfig

	// Deploy the LOG1-emitting contract the block-end syscall targets.
	seedResumeTestDB(t, db, func(putter kv.TemporalPutDel) error {
		return putLogEmittingContract(putter, logIndexContract)
	})

	engine := &logEmittingSyscallEngine{
		Engine:   ethash.NewFaker(),
		contract: accounts.InternAddress(logIndexContract),
		calls:    1,
	}

	pe, roTx := newResumeTestExec(t, db, config)
	pe.cfg.engine = engine
	pe.cfg.vmConfig = &vm.Config{}

	// Capture the finalize result the exec loop sends to the apply loop.
	applyResults := make(chan applyResult, 16)
	consumers := newResultStream()
	consumers.register("applyResults", applyResults)

	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000}
	gasPool := new(protocol.GasPool).AddGas(10_000_000)
	be := newBlockExec(1, common.Hash{}, gasPool, nil, consumers, false, nil)

	// One block-end anchor task (TxIndex == len(Txs)); finalize anchors on it so
	// the coinbase/finalize sweep lands on an unsealed versionMap cell.
	endTask := &exec.TxTask{Header: header, TxNum: 1, TxIndex: 0, Config: config}
	eTaskEnd := &execTask{Task: endTask, index: 0}
	endVersion := &taskVersion{
		execTask: eTaskEnd,
		version:  state.Version{BlockNum: 1, TxIndex: 0, Incarnation: 1, TxNum: 1},
	}
	be.tasks = []*execTask{eTaskEnd}
	be.results = []*execResult{{TxResult: &exec.TxResult{Task: endVersion}}}
	be.finalizedResults[0] = &execResult{TxResult: &exec.TxResult{Task: endVersion}}
	be.execTasks.setComplete(0)
	be.validateTasks.setComplete(0)
	be.publishTasks.setComplete(0)

	driveRes := &exec.TxResult{Task: endVersion}
	_, err := be.nextResult(ctx, pe, driveRes, roTx)
	require.NoError(t, err)

	// Find the finalize result and confirm it carries the block-end syscall log.
	var fin *txResult
	for len(applyResults) > 0 {
		if tr, ok := (<-applyResults).(*txResult); ok && tr.isFinalize {
			fin = tr
		}
	}
	require.NotNil(t, fin, "a finalize result must be sent")
	require.NotEmpty(t, fin.logs, "the finalize result must carry the block-end syscall log")

	// Index it exactly as the apply loop does (skipReceiptCache for finalize), then
	// flush and assert the block-end log is reachable in the log indexes.
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	rs := state.NewStateV3Buffered(state.NewStateV3(pe.doms, false, pe.logger))
	require.NoError(t, rs.ApplyTxIndexes(rwTx, fin.txNum, fin.receipt, fin.cumulativeBlobGasUsed, fin.logs, fin.traceFroms, fin.traceTos, fin.isFinalize))
	require.NoError(t, pe.doms.Flush(ctx, rwTx))

	require.Equal(t, []uint64{fin.txNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
	topic := common.Hash{31: 0x42}
	require.Equal(t, []uint64{fin.txNum}, indexedTxNums(t, rwTx, kv.LogTopicIdx, topic[:]))
}
