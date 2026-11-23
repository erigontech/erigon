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
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

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
		prevBlocks: state.NewPrevBlockList(),
	}
}

func newLogIndexTestBlock(tasks []exec.Task) (*blockExecutor, chan applyResult) {
	applyResults := make(chan applyResult, len(tasks)+1)
	consumers := newResultStream()
	consumers.register("applyResults", applyResults)
	be := newBlockExec(tasks[0].Version().BlockNum, tasks[0].BlockHash(),
		new(protocol.GasPool).AddGas(tasks[0].BlockGasLimit()), nil, consumers, false, nil)
	be.tasks = make([]*execTask, len(tasks))
	be.results = make([]*execResult, len(tasks))
	for i, task := range tasks {
		be.tasks[i] = &execTask{Task: task, index: i}
	}
	return be, applyResults
}

// Drive the production apply path: nextResult only publishes the writes and
// indexes, and foldTx owns the SharedDomains mutations.
func foldLogIndexTestResults(t *testing.T, pe *parallelExecutor, tx kv.TemporalTx, results chan applyResult) *txResult {
	t.Helper()
	var finalize *txResult
	for len(results) > 0 {
		result := (<-results).(*txResult)
		if result.isFinalize {
			require.Nil(t, finalize, "finalize must be published only once")
			finalize = result
		}
		require.NoError(t, pe.foldTx(t.Context(), tx, result))
	}
	return finalize
}

// With receipt history enabled, block-end logs must reach the persisted indexes
// at the final virtual transaction's txNum. That transaction clears the latest
// cached receipt, while history retains the last regular receipt.
func TestParallelBlockEndLogsWithReceiptHistory(t *testing.T) {
	savedRCache := statecfg.Schema.RCacheDomain
	statecfg.EnableHistoricalRCache()
	t.Cleanup(func() { statecfg.Schema.RCacheDomain = savedRCache })

	db := newResumeTestDB(t)
	seedLogEmittingContract(t, db, logIndexContract)
	rwTx, domains := temporaltest.NewTestTxSD(t, db)
	pe := newParallelLogIndexTestExec(db, domains, 1)
	pe.rs = state.NewStateV3Buffered(state.NewStateV3(domains, true, pe.logger))

	tasks := newLogIndexTestTasks(pe.cfg.chainConfig)
	txs := types.Transactions{signSelfSendTx(t, 0, 0, 1, 21_000, pe.cfg.chainConfig, 0)}
	for i, task := range tasks {
		txTask := task.(*exec.TxTask)
		txTask.TxIndex = i
		txTask.Txs = txs
	}
	be, applyResults := newLogIndexTestBlock(tasks)
	regularTask, finalTask := be.tasks[0], be.tasks[1]
	regularVersion := regularTask.Version()
	regularVersion.Incarnation = 1
	be.execTasks.setInProgress(0)
	res, err := be.nextResult(t.Context(), pe, &exec.TxResult{
		Task: &taskVersion{execTask: regularTask, version: regularVersion},
		ExecutionResult: evmtypes.ExecutionResult{
			ReceiptGasUsed: 21_000,
			TxnGasUsage: mdgas.TxnGasUsage{
				BlockExecutionGasUsed: 21_000,
			},
		},
	}, rwTx)
	require.NoError(t, err)
	require.Nil(t, res)
	require.Nil(t, foldLogIndexTestResults(t, pe, rwTx, applyResults))
	cachedReceipt, _, err := domains.GetLatest(kv.RCacheDomain, rwTx, rawtemporaldb.ReceiptCacheKey)
	require.NoError(t, err)
	require.NotEmpty(t, cachedReceipt)

	finalVersion := finalTask.Version()
	finalVersion.Incarnation = 1
	be.execTasks.setInProgress(1)
	res, err = be.nextResult(t.Context(), pe, &exec.TxResult{
		Task: &taskVersion{execTask: finalTask, version: finalVersion},
	}, rwTx)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NoError(t, res.Err)
	require.NotNil(t, foldLogIndexTestResults(t, pe, rwTx, applyResults))
	require.NoError(t, domains.Flush(t.Context(), rwTx))

	priorReceipt, ok, err := rwTx.HistorySeek(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, regularVersion.TxNum+1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, cachedReceipt, priorReceipt)
	latestReceipt, _, err := rwTx.GetLatest(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Empty(t, latestReceipt)
	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogTopicIdx, logIndexTopic[:]))
}

type indexCountingMemBatch struct {
	// SharedDomains also calls changeset methods outside kv.TemporalMemBatch.
	// Embedding the concrete batch keeps those methods available.
	*dbstate.TemporalMemBatch
	adds map[indexAddition]int
}

type indexAddition struct {
	table kv.InvertedIdx
	txNum uint64
}

func (m *indexCountingMemBatch) IndexAdd(table kv.InvertedIdx, key []byte, txNum uint64) error {
	m.adds[indexAddition{table: table, txNum: txNum}]++
	return m.TemporalMemBatch.IndexAdd(table, key, txNum)
}

// Finalize system calls share an IntraBlockState and txIndex, so GetRawLogs
// returns all logs collected so far. Indexing that list after each call would
// index earlier logs again. Count index additions before storage deduplicates
// equal (key, txNum) pairs, which would hide that duplication in index queries.
func TestParallelBlockEndLogsCountEachSyscallOnce(t *testing.T) {
	const syscalls = 3

	db := newResumeTestDB(t)
	seedLogEmittingContract(t, db, logIndexContract)

	logger := log.New()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)
	indexes := &indexCountingMemBatch{
		TemporalMemBatch: dbstate.NewTemporalMemBatch(rwTx, db, kvmetrics.NewDomainMetrics()),
		adds:             make(map[indexAddition]int),
	}
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, logger, execctx.WithMemBatch(indexes))
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	pe := newParallelLogIndexTestExec(db, domains, syscalls)
	tasks := newLogIndexTestTasks(pe.cfg.chainConfig)
	be, applyResults := newLogIndexTestBlock(tasks)
	for i, task := range be.tasks {
		be.execTasks.setInProgress(i)
		version := task.Version()
		version.Incarnation = 1
		res, err := be.nextResult(t.Context(), pe, &exec.TxResult{
			Task: &taskVersion{execTask: task, version: version},
		}, rwTx)
		require.NoError(t, err)
		if i < len(tasks)-1 {
			require.Nil(t, res)
		} else {
			require.NotNil(t, res)
			require.NoError(t, res.Err)
		}
	}
	finalize := foldLogIndexTestResults(t, pe, rwTx, applyResults)
	require.NotNil(t, finalize, "block-end logs must reach the apply loop")
	require.Equal(t, logIndexFinalTxNum, finalize.txNum)
	require.Len(t, finalize.logs, syscalls)

	require.Equal(t, map[indexAddition]int{
		{table: kv.LogAddrIdx, txNum: logIndexFinalTxNum}:  syscalls,
		{table: kv.LogTopicIdx, txNum: logIndexFinalTxNum}: syscalls,
	}, indexes.adds)
	require.NoError(t, domains.Flush(t.Context(), rwTx))

	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
	require.Equal(t, []uint64{logIndexFinalTxNum}, indexedTxNums(t, rwTx, kv.LogTopicIdx, logIndexTopic[:]))
}

// A tx charged at validation records one copy of its writes, kept by TxOut as well, and
// the worker's own write-set stays free of the sender charge and the tip.
func TestChargedTxKeepsOneCopyOfItsWrites(t *testing.T) {
	db := newResumeTestDB(t)
	sender := senderIsCoinbaseKey.rawAddress
	seedResumeTestDB(t, db, func(putter kv.TemporalPutDel) error {
		acc := accounts.NewAccount()
		acc.Balance = *uint256.NewInt(1_000_000_000)
		return putter.DomainPut(kv.AccountsDomain, sender[:], accounts.SerialiseV3(&acc), 0, nil)
	})
	rwTx, domains := temporaltest.NewTestTxSD(t, db)
	pe := newParallelLogIndexTestExec(db, domains, 0)

	coinbase := common.HexToAddress("0xc01b")
	recipient := common.HexToAddress("0xa11ce")
	tasks := newLogIndexTestTasks(pe.cfg.chainConfig)
	signed, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{To: &recipient, GasLimit: 21_000},
		GasPrice: *uint256.NewInt(1),
	}, *types.MakeSigner(pe.cfg.chainConfig, 1, 0), senderIsCoinbaseKey.key)
	require.NoError(t, err)
	for i, task := range tasks {
		txTask := task.(*exec.TxTask)
		txTask.TxIndex = i
		txTask.Txs = types.Transactions{signed}
		txTask.Header.Coinbase = coinbase
	}
	be, _ := newLogIndexTestBlock(tasks)
	regular := be.tasks[0]

	created := accounts.InternAddress(recipient)
	workerWrites := &state.WriteSet{}
	workerWrites.SetNonce(created, &state.VersionedWrite[uint64]{
		WriteHeader: state.WriteHeader{Address: created, Path: state.NoncePath, Version: regular.Version()},
		Val:         1,
	})
	be.versionMap.FlushVersionedWrites(workerWrites, false)

	be.execTasks.setInProgress(0)
	res, err := be.nextResult(t.Context(), pe, &exec.TxResult{
		Task:     &taskVersion{execTask: regular, version: regular.Version()},
		TxOut:    workerWrites,
		Coinbase: accounts.InternAddress(coinbase),
		ExecutionResult: evmtypes.ExecutionResult{
			ReceiptGasUsed: 21_000,
			FeeTipped:      *uint256.NewInt(21_000),
			TxnGasUsage:    mdgas.TxnGasUsage{BlockExecutionGasUsed: 21_000},
		},
	}, rwTx)
	require.NoError(t, err)
	require.Nil(t, res)

	finalized := be.finalizedResults[0]
	require.NotNil(t, finalized, "the tx must reach the finalize tail")
	recorded := be.blockIO.WriteSet(regular.Version().TxIndex)
	require.Same(t, recorded, finalized.TxOut)

	_, charged := recorded.GetBalance(accounts.InternAddress(sender))
	require.True(t, charged, "the sender charge must be recorded")
	_, tipped := recorded.GetBalance(accounts.InternAddress(coinbase))
	require.True(t, tipped, "the tip must be recorded")
	_, kept := recorded.GetNonce(created)
	require.True(t, kept, "the worker's writes must be recorded")
	require.Equal(t, 1, workerWrites.Count(), "the worker's write-set must stay as the worker left it")
}
