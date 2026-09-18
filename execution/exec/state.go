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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/node/shards"
)

var noop = state.NewNoopWriter()

type WorkerMetrics struct {
	Active              activeCount
	GasUsed             activeCount
	Duration            activeDuration
	ReadCount           atomic.Int64
	ReadDuration        activeDuration
	AccountReadCount    atomic.Int64
	AccountReadDuration activeDuration
	StorageReadCount    atomic.Int64
	StorageReadDuration activeDuration
	CodeReadCount       atomic.Int64
	CodeReadDuration    activeDuration
	WriteDuration       activeDuration
}

func NewWorkerMetrics() *WorkerMetrics {
	return &WorkerMetrics{
		Active:              activeCount{Ema: metrics.NewEmaWithBeta[int64](0, 1, 0.2)},
		GasUsed:             activeCount{Ema: metrics.NewEma[int64](0, 0.3)},
		Duration:            activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		ReadDuration:        activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		AccountReadDuration: activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		StorageReadDuration: activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
		CodeReadDuration:    activeDuration{Ema: metrics.NewEma[time.Duration](0, 0.3)},
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

type WorkerContext struct {
	lock    *sync.RWMutex
	logger  log.Logger
	chainDb kv.TemporalRoDB
	// chainTx is the overlay-aware read view (the dispatch goroutine's roTx
	// wrapped with the SharedDomains BlockOverlay if one is active). The context
	// only borrows the tx; the dispatch goroutine owns its lifetime and rolls it
	// back on exit.
	chainTx     kv.TemporalTx
	background  bool
	blockReader dbservices.FullBlockReader
	rs          *state.StateV3Buffered
	stateWriter state.StateWriter
	stateReader state.StateReader
	historyMode bool // if true - stateReader is HistoryReaderV3, otherwise it's state reader
	// prevBlockReg, when non-nil, keeps the IBS reading its committed base through a
	// per-task PrevBlockReader; SetReader re-wraps so switching reader (e.g. into
	// history mode and back) does not drop the prior-blocks overlay.
	prevBlockReg *state.PrevBlockList
	chainConfig  *chain.Config

	ctx     context.Context
	engine  rules.Engine
	genesis *types.Genesis
	chain   rules.ChainReader

	evm *vm.EVM
	ibs *state.IntraBlockState

	dirs datadir.Dirs

	metrics *WorkerMetrics
	// per-worker, single-owner domain read-metrics accumulator.
	readMetrics *kvmetrics.DomainMetrics
}

// installWorkerGetHash points the EVM's BLOCKHASH lookup at the worker's own
// chainTx, avoiding sharing the executeBlocks goroutine's roTx across workers.
// chainTx is overlay-aware, so headers staged in the BlockOverlay are visible.
func (rw *WorkerContext) installWorkerGetHash(txTask Task) {
	header := txTask.BlockHeader()
	if header == nil {
		return
	}
	workerTx := rw.chainTx
	br := rw.blockReader
	ctx := rw.ctx
	rw.evm.Context.GetHash = protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
		h, err := br.Header(ctx, workerTx, hash, number)
		if h == nil && err == nil {
			h = &types.Header{}
		}
		return h, err
	})
}

func NewWorkerContext(ctx context.Context, background bool, metrics *WorkerMetrics, chainDb kv.TemporalRoDB, blockReader dbservices.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis, engine rules.Engine, dirs datadir.Dirs, logger log.Logger) *WorkerContext {
	w := &WorkerContext{
		lock:    &sync.RWMutex{},
		chainDb: chainDb,

		logger: logger,
		ctx:    ctx,

		background:  background,
		blockReader: blockReader,

		chainConfig: chainConfig,
		genesis:     genesis,
		engine:      engine,

		evm: vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),

		dirs:        dirs,
		metrics:     metrics,
		readMetrics: kvmetrics.NewDomainMetrics(),
	}
	w.ibs = state.New(w.stateReader)
	return w
}

func (rw *WorkerContext) ResetState(rs *state.StateV3Buffered, chainTx kv.TemporalTx, stateReader state.StateReader, stateWriter state.StateWriter, accumulator *shards.Accumulator) error {
	rw.lock.Lock()
	defer rw.lock.Unlock()

	rw.rs = rs

	if stateReader != nil {
		rw.SetReader(stateReader)
	} else {
		// bindTx points the reader's getter at chainTx (nil until a dispatch
		// goroutine binds its roTx).
		rw.SetReader(state.NewReaderV3(nil))
	}

	if stateWriter != nil {
		rw.stateWriter = stateWriter
	} else {
		rw.stateWriter = state.NewWriter(nil, accumulator, 0)
	}

	return rw.bindTx(chainTx)
}

func (rw *WorkerContext) ResetTx(chainTx kv.TemporalTx) error {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	return rw.bindTx(chainTx)
}

// BindTxHeld rebinds the worker to chainTx assuming the caller already holds
// rw.lock — used for a mid-execution rebind from inside RunTxTask (which holds
// the lock for the whole run), where ResetTx would re-lock and self-deadlock.
func (rw *WorkerContext) BindTxHeld(chainTx kv.TemporalTx) error {
	return rw.bindTx(chainTx)
}

func (rw *WorkerContext) resetTxNum(txNum uint64) {
	type resettable interface {
		SetTxNum(txNum uint64)
	}

	if resettable, ok := rw.stateReader.(resettable); ok {
		resettable.SetTxNum(txNum)
	}

	if resettable, ok := rw.stateWriter.(resettable); ok {
		resettable.SetTxNum(txNum)
	}
}

// bindTx points this worker's reader, writer, and chain reader at chainTx,
// overlay-wrapped so block metadata staged in the SharedDomains BlockOverlay is
// visible. A nil tx detaches (teardown); readers/writer keep their type and are
// re-pointed on the next bind.
func (rw *WorkerContext) bindTx(chainTx kv.TemporalTx) error {
	if chainTx == nil {
		rw.chainTx = nil
		return nil
	}

	if rw.rs != nil {
		if sd := rw.rs.Domains(); sd != nil {
			if overlay := sd.BlockOverlay(); overlay != nil {
				chainTx = overlay.NewReadView(chainTx)
			}
		}
	}
	rw.chainTx = chainTx

	type latest interface{ SetGetter(execctxapi.StateGetter) }
	type historic interface{ SetTx(kv.TemporalTx) }
	switch typedReader := rw.stateReader.(type) {
	case latest:
		typedReader.SetGetter(rw.rs.Domains().AsGetterMetered(chainTx, rw.readMetrics))
	case historic:
		typedReader.SetTx(chainTx)
	default:
		if rw.stateReader != nil {
			return fmt.Errorf("can't set tx for reader: %T", rw.stateReader)
		}
	}

	// Writers that implement neither (NoopWriter, LightCollector) accumulate in
	// memory and need no tx — not an error.
	type withPutter interface{ SetPutDel(kv.TemporalPutDel) }
	type withTx interface{ SetTx(kv.TemporalTx) }
	switch typedWriter := rw.stateWriter.(type) {
	case withPutter:
		typedWriter.SetPutDel(rw.rs.Domains().AsPutDel(chainTx))
	case withTx:
		typedWriter.SetTx(chainTx)
	}

	rw.chain = consensuschain.NewReader(rw.chainConfig, chainTx, rw.blockReader, rw.logger)
	return nil
}

func (rw *WorkerContext) RunTxTask(txTask Task) (result *TxResult) {
	rw.lock.Lock()
	defer rw.lock.Unlock()

	if rw.metrics != nil && dbg.KVReadLevelledMetrics {
		rw.metrics.Active.Add(1)
		start := time.Now()
		defer func() {
			rw.metrics.Duration.Add(time.Since(start))
			if readDuration := rw.ibs.ReadDuration(); readDuration > 0 {
				rw.metrics.ReadDuration.Add(rw.ibs.ReadDuration())
				rw.metrics.ReadCount.Add(rw.ibs.ReadCount())
				rw.metrics.AccountReadDuration.Add(rw.ibs.AccountReadDuration())
				rw.metrics.AccountReadCount.Add(rw.ibs.AccountReadCount())
				rw.metrics.StorageReadDuration.Add(rw.ibs.StorageReadDuration())
				rw.metrics.StorageReadCount.Add(rw.ibs.StorageReadCount())
				rw.metrics.CodeReadDuration.Add(rw.ibs.CodeReadDuration())
				rw.metrics.CodeReadCount.Add(rw.ibs.CodeReadCount())
			}
			if result != nil {
				// EIP-8037: per-tx max(regular, state) overestimates vs the true block gas
				// (max of sums, not sum of maxes), but is a safe upper bound for metrics.
				rw.metrics.GasUsed.Add(int64(max(result.ExecutionResult.BlockExecutionGasUsed, result.ExecutionResult.BlockStateGasUsed)))
			}
			rw.metrics.Active.Add(-1)
		}()
	}

	result = rw.RunTxTaskNoLock(txTask)
	return result
}

// PublishReadMetrics folds this worker's accumulated reads into the per-batch
// sd.metrics that the slow-block emitter samples, then resets. The per-read
// increments stay lock-free (single-owner readMetrics); only this boundary
// merge takes sd.metrics' lock, and it runs off the critical path (after a run,
// on worker release). No-op unless levelled read metrics are enabled.
func (rw *WorkerContext) PublishReadMetrics() {
	if !dbg.KVReadLevelledMetrics || rw.rs == nil {
		return
	}
	rw.rs.Domains().LogMergeMetrics(rw.readMetrics)
	rw.readMetrics.Reset()
}

// Needed to set history reader when need to offset few txs from block beginning and does not break processing,
// like compute gas used for block and then to set state reader to continue processing on latest data.
func (rw *WorkerContext) SetReader(reader state.StateReader) {
	rw.stateReader = reader
	type latest interface {
		SetGetter(execctxapi.StateGetter)
	}

	type historic interface {
		SetTx(kv.TemporalTx)
	}

	switch typedReader := rw.stateReader.(type) {
	case latest:
		typedReader.SetGetter(rw.rs.Domains().AsGetterMetered(rw.chainTx, rw.readMetrics))
	case historic:
		typedReader.SetTx(rw.chainTx)
	}
	if rw.prevBlockReg != nil {
		rw.ibs = state.New(state.NewPrevBlockReader(rw.stateReader, rw.prevBlockReg))
	} else {
		rw.ibs = state.New(rw.stateReader)
	}

	switch reader.(type) {
	case *state.HistoryReaderV3:
		rw.historyMode = true
	default:
		rw.historyMode = false
	}
}

// EnablePrevBlockReads makes the worker's IBS read its committed base through a
// per-task reader over the finished-but-not-yet-committed prior blocks. The raw
// reader stays as rw.stateReader (the getter plumbing keeps targeting it) and the
// per-task reader wraps it by reference. Call once after ResetState; per task the
// block is set via ibs.StateReader().(*PrevBlockReader).SetBlock.
func (rw *WorkerContext) EnablePrevBlockReads(reg *state.PrevBlockList) {
	rw.prevBlockReg = reg
	rw.ibs = state.New(state.NewPrevBlockReader(rw.stateReader, reg))
}

func (rw *WorkerContext) RunTxTaskNoLock(txTask Task) *TxResult {
	if txTask.IsHistoric() && !rw.historyMode {
		// in case if we cancelled execution and commitment happened in the middle of the block, we have to process block
		// from the beginning until committed txNum and only then disable history mode.
		// Needed to correctly evaluate spent gas and other things.
		// Chain sd.mem → chainTx so historic-mode reads see prior-tx writes
		// from the current batch.
		rw.SetReader(state.NewHistoryReaderV3WithSharedDomains(rw.chainTx, rw.rs.Domains(), txTask.Version().TxNum))
	} else if !txTask.IsHistoric() && (rw.stateReader == nil || rw.historyMode) {
		rw.SetReader(state.NewReaderV3(rw.rs.Domains().AsGetterMetered(rw.chainTx, rw.readMetrics)))
	}

	if rw.background && rw.chainTx == nil {
		return &TxResult{
			Task: txTask,
			Err:  fmt.Errorf("worker run without a bound roTx: the dispatch goroutine must bind a roTx before RunTxTask"),
		}
	}

	txIndex := txTask.Version().TxIndex

	var callTracer *calltracer.CallTracer

	if txIndex != -1 && !txTask.IsBlockEnd() {
		callTracer = calltracer.NewCallTracer(txTask.TracingHooks())
	}

	rw.resetTxNum(txTask.Version().TxNum)

	if err := txTask.Reset(rw.evm, rw.ibs, callTracer); err != nil {
		return &TxResult{
			Task: txTask,
			Err:  err,
		}
	}

	if rw.background && rw.chainTx != nil && rw.blockReader != nil {
		rw.installWorkerGetHash(txTask)
	}

	result := txTask.Execute(rw.evm, rw.engine, rw.genesis, rw.ibs, rw.stateWriter, rw.chainConfig, rw.chain, rw.dirs, true)

	if result.Task == nil {
		result.Task = txTask
	}

	if callTracer != nil {
		result.TraceFroms = callTracer.Froms()
		result.TraceTos = callTracer.Tos()
	}

	// Extract LightCollector's accumulated writes so finalize can use them
	// directly without IBS reconstruction.
	if lc, ok := rw.stateWriter.(*state.LightCollector); ok {
		result.CollectorWrites = lc.TakeWrites()
	}

	return result
}

func NewWorkersPool(ctx context.Context, accumulator *shards.Accumulator, background bool, chainDb kv.TemporalRoDB,
	rs *state.StateV3Buffered, stateReader state.StateReader, stateWriter state.StateWriter, blockReader dbservices.FullBlockReader, chainConfig *chain.Config, genesis *types.Genesis,
	engine rules.Engine, workerCount int, metrics *WorkerMetrics, dirs datadir.Dirs, logger log.Logger) (reconWorkers []*WorkerContext, applyWorker *WorkerContext, clear func(), wait func(), err error) {
	reconWorkers = make([]*WorkerContext, workerCount)

	g, gctx := errgroup.WithContext(ctx)

	// Assigned before the fallible ResetState loop so every early return hands
	// back a callable closure — the teardown path invokes it unconditionally.
	var clearDone bool
	clear = func() {
		if clearDone {
			return
		}
		clearDone = true
		_ = g.Wait()
		for _, w := range reconWorkers {
			if w != nil {
				_ = w.ResetTx(nil)
			}
		}
	}

	for i := range workerCount {
		reconWorkers[i] = NewWorkerContext(gctx, background, metrics, chainDb, blockReader, chainConfig, genesis, engine, dirs, logger)

		if rs != nil {
			reader := stateReader

			if reader == nil {
				reader = state.NewReaderV3(rs.Domains().AsGetterMetered(nil, reconWorkers[i].readMetrics))
			}

			if err = reconWorkers[i].ResetState(rs, nil, reader, stateWriter, accumulator); err != nil {
				return
			}
		}
	}
	if background {
		// Worker contexts are created (each with its own roTx via ResetState) but
		// driven directly by the dispatcher (goroutine-per-task), not via a pull loop.
		wait = func() { _ = g.Wait() }
	}

	applyWorker = NewWorkerContext(ctx, false, nil, chainDb, blockReader, chainConfig, genesis, engine, dirs, logger)

	return reconWorkers, applyWorker, clear, wait, err
}
