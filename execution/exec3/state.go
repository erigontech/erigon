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

package exec3

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3/calltracer"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

var noop = state.NewNoopWriter()

type WorkerMetrics struct {
	Active              activeCount
	GasUsed             activeCount
	Duration            activeDuration
	StorageReadCount    atomic.Int64
	StorageReadDuration activeDuration
	WriteDuration       activeDuration
}

func NewWorkerMetrics() *WorkerMetrics {
	return &WorkerMetrics{
		Active:              activeCount{Ema: metrics.NewEmaWithBeta[int64](0, 1, 0.2)},
		GasUsed:             activeCount{Ema: metrics.NewEma[int64](0, 0.3)},
		Duration:            activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		StorageReadDuration: activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		WriteDuration:       activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
	}
}

type activeDuration struct {
	atomic.Int64
	Ema *metrics.EMA[time.Duration]
}

func (d *activeDuration) Add(i time.Duration) {
	d.Int64.Add(int64(i))
	d.Ema.Update(i)
}

type activeCount struct {
	atomic.Int64
	Total atomic.Int64
	Ema   *metrics.EMA[int64]
}

func (c *activeCount) Add(i int64) {
	c.Int64.Add(i)
	if i > 0 {
		c.Total.Add(i)
	}
	c.Ema.Update(c.Load())
}

type Worker struct {
	lock        *sync.RWMutex
	notifier    *sync.Cond
	runnable    atomic.Bool
	logger      log.Logger
	chainDb     kv.RoDB
	chainTx     kv.Tx
	background  bool // if true - worker does manage RoTx (begin/rollback) in .ResetTx()
	blockReader services.FullBlockReader
	in          *exec.QueueWithRetry
	rs          *state.StateV3Buffered
	stateWriter state.StateWriter
	stateReader state.StateReader
	historyMode bool // if true - stateReader is HistoryReaderV3, otherwise it's state reader
	chainConfig *chain.Config

	ctx     context.Context
	engine  consensus.Engine
	genesis *types.Genesis
	results *exec.ResultsQueue
	chain   consensus.ChainReader

	evm *vm.EVM
	ibs *state.IntraBlockState

	dirs datadir.Dirs

	metrics *WorkerMetrics
}

func NewWorker(ctx context.Context, background bool, metrics *WorkerMetrics, chainDb kv.RoDB, in *exec.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, results *exec.ResultsQueue, engine consensus.Engine, dirs datadir.Dirs, logger log.Logger) *Worker {
	lock := &sync.RWMutex{}

	w := &Worker{
		lock:    lock,
		chainDb: chainDb,
		in:      in,

		logger: logger,
		ctx:    ctx,

		background:  background,
		blockReader: blockReader,

		chainConfig: chainConfig,
		genesis:     genesis,
		results:     results,
		engine:      engine,

		evm: vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),

		dirs:    dirs,
		metrics: metrics,
	}
	w.runnable.Store(true)
	w.ibs = state.New(w.stateReader)
	return w
}

func (rw *Worker) Pause() {
	rw.runnable.Store(false)
}

func (rw *Worker) Paused() (waiter chan any, paused bool) {
	if rw.runnable.Load() {
		return nil, false
	}

	rw.results.Lock()
	defer rw.results.Unlock()

	canlock := rw.lock.TryLock()

	if canlock {
		rw.lock.Unlock()
	} else {
		waiter = rw.results.AddWaiter(false)
	}

	return waiter, canlock
}

func (rw *Worker) Resume() {
	rw.runnable.Store(true)
	rw.notifier.Signal()
}

func (rw *Worker) LogLRUStats() { rw.evm.Config().JumpDestCache.LogStats() }

func (rw *Worker) ResetState(rs *state.StateV3Buffered, chainTx kv.Tx, stateReader state.ResettableStateReader, stateWriter state.StateWriter, accumulator *shards.Accumulator) {
	rw.lock.Lock()
	defer rw.lock.Unlock()

	rw.rs = rs
	rw.resetTx(chainTx)

	if stateReader != nil {
		rw.SetReader(stateReader)
	} else {
		rw.SetReader(state.NewBufferedReader(rs, state.NewReaderV3(rs.Domains().AsGetter(rw.chainTx))))
	}

	if stateWriter != nil {
		rw.stateWriter = stateWriter
	} else {
		rw.stateWriter = state.NewWriter(rs.Domains().AsPutDel(rw.chainTx), accumulator, 0)
	}
}

func (rw *Worker) Tx() kv.Tx { return rw.chainTx }
func (rw *Worker) ResetTx(chainTx kv.Tx) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.resetTx(chainTx)
}

func (rw *Worker) resetTx(chainTx kv.Tx) {
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
	}

	rw.chainTx = chainTx

	if rw.chainTx != nil {
		type resettable interface {
			SetGetter(kv.TemporalGetter)
		}

		if resettable, ok := rw.stateReader.(resettable); ok {
			resettable.SetGetter(rw.rs.Domains().AsGetter(rw.chainTx))
		}

		if resettable, ok := rw.stateWriter.(resettable); ok {
			resettable.SetGetter(rw.rs.Domains().AsGetter(rw.chainTx))
		}

		rw.chain = consensuschain.NewReader(rw.chainConfig, rw.chainTx, rw.blockReader, rw.logger)
	} else {
		rw.chain = nil
		rw.stateReader = nil
		rw.stateWriter = nil
	}
}

func (rw *Worker) Run() (err error) {
	defer func() { // convert panic to err - because it's background workers
		if rec := recover(); rec != nil {
			err = fmt.Errorf("exec3.Worker panic: %s, %s", rec, dbg.Stack())
		}
	}()

	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		result := rw.RunTxTask(txTask)
		if err := rw.results.Add(rw.ctx, result); err != nil {
			return err
		}
	}
	return nil
}

func (rw *Worker) RunTxTask(txTask exec.Task) (result *exec.TxResult) {
	//fmt.Println("RTX", txTask.Version().BlockNum, txTask.Version().TxIndex, txTask.Version().TxNum, txTask.IsBlockEnd())
	//defer fmt.Println("RTX DONE", txTask.Version().BlockNum, txTask.Version().TxIndex)
	rw.lock.Lock()
	defer rw.lock.Unlock()

	for !rw.runnable.Load() {
		rw.notifier.Wait()
	}

	if rw.metrics != nil {
		rw.metrics.Active.Add(1)
		start := time.Now()
		defer func() {
			rw.metrics.Duration.Add(time.Since(start))
			if readDuration := rw.ibs.StorageReadDuration(); readDuration > 0 {
				rw.metrics.StorageReadDuration.Add(rw.ibs.StorageReadDuration())
				rw.metrics.StorageReadCount.Add(rw.ibs.StorageReadCount())
			}
			if result != nil && result.ExecutionResult != nil {
				rw.metrics.GasUsed.Add(int64(result.ExecutionResult.GasUsed))
			}
			rw.metrics.Active.Add(-1)
		}()
	}

	result = rw.RunTxTaskNoLock(txTask)
	return result
}

// Needed to set history reader when need to offset few txs from block beginning and does not break processing,
// like compute gas used for block and then to set state reader to continue processing on latest data.
func (rw *Worker) SetReader(reader state.StateReader) {
	rw.stateReader = reader
	rw.stateReader.SetTx(rw.Tx())
	rw.ibs = state.New(rw.stateReader)

	switch reader.(type) {
	case *state.HistoryReaderV3:
		rw.historyMode = true
	case *state.ReaderV3:
		rw.historyMode = false
	default:
		rw.historyMode = false
	}
}

func (rw *Worker) RunTxTaskNoLock(txTask exec.Task) *exec.TxResult {
	if txTask.IsHistoric() && !rw.historyMode {
		// in case if we cancelled execution and commitment happened in the middle of the block, we have to process block
		// from the beginning until committed txNum and only then disable history mode.
		// Needed to correctly evaluate spent gas and other things.
		rw.SetReader(state.NewHistoryReaderV3())
	} else if !txTask.IsHistoric() && rw.historyMode {
		rw.SetReader(state.NewBufferedReader(rw.rs, state.NewReaderV3(rw.rs.Domains().AsGetter(rw.chainTx))))
	}

	if rw.background && rw.chainTx == nil {
		chainTx, err := rw.chainDb.(kv.TemporalRoDB).BeginTemporalRo(rw.ctx)

		if err != nil {
			return &exec.TxResult{
				Task: txTask,
				Err:  err,
			}
		}

		rw.resetTx(chainTx)
	}

	txIndex := txTask.Version().TxIndex

	var callTracer *calltracer.CallTracer

	if txIndex != -1 && !txTask.IsBlockEnd() {
		callTracer = calltracer.NewCallTracer(txTask.TracingHooks())
	}

	if err := txTask.Reset(rw.evm, rw.ibs, callTracer); err != nil {
		return &exec.TxResult{
			Task: txTask,
			Err:  err,
		}
	}

	result := txTask.Execute(rw.evm, rw.engine, rw.genesis, rw.ibs, rw.stateWriter, rw.chainConfig, rw.chain, rw.dirs, true)

	if result.Task == nil {
		result.Task = txTask
	}

	if callTracer != nil {
		result.TraceFroms = callTracer.Froms()
		result.TraceTos = callTracer.Tos()
	}

	return result
}

func NewWorkersPool(ctx context.Context, accumulator *shards.Accumulator, background bool, chainDb kv.RoDB,
	rs *state.StateV3Buffered, stateReader state.ResettableStateReader, stateWriter state.StateWriter, in *exec.QueueWithRetry, blockReader services.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis,
	engine consensus.Engine, workerCount int, metrics *WorkerMetrics, dirs datadir.Dirs, isMining bool, logger log.Logger) (reconWorkers []*Worker, applyWorker *Worker, rws *exec.ResultsQueue, clear func(), wait func()) {
	reconWorkers = make([]*Worker, workerCount)

	resultsSize := workerCount * 8
	rws = exec.NewResultsQueue(resultsSize, workerCount)

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < workerCount; i++ {
		reconWorkers[i] = NewWorker(gctx, background, metrics, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, logger)

		if rs != nil {
			reader := stateReader

			if reader == nil {
				reader = state.NewBufferedReader(rs, state.NewReaderV3(rs.Domains().AsGetter(nil), nil))
			}

			reconWorkers[i].ResetState(rs, nil, reader, stateWriter, accumulator)
		}
	}
	if background {
		for i := 0; i < workerCount; i++ {
			i := i
			g.Go(func() error {
				return reconWorkers[i].Run()
			})
		}
		wait = func() { g.Wait() }
	}

	var clearDone bool
	clear = func() {
		if clearDone {
			return
		}
		clearDone = true
		g.Wait()
		for _, w := range reconWorkers {
			w.ResetTx(nil)
		}
		//applyWorker.ResetTx(nil)
	}
	applyWorker = NewWorker(ctx, false, nil, chainDb, in, blockReader, chainConfig, genesis, rws, engine, dirs, logger)

	return reconWorkers, applyWorker, rws, clear, wait
}
