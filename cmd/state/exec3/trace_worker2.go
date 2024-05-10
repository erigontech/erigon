package exec3

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/consensuschain"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

type TraceWorker2 struct {
	consumer TraceConsumer
	in       *state.QueueWithRetry
	resultCh *state.ResultsQueue

	stateReader *state.HistoryReaderV3
	ibs         *state.IntraBlockState
	evm         *vm.EVM

	chainTx     kv.Tx
	background  bool
	ctx         context.Context
	stateWriter state.StateWriter
	chain       consensus.ChainReader
	logger      log.Logger

	execArgs *ExecArgs

	taskGasPool *core.GasPool

	// calculated by .changeBlock()
	blockHash common.Hash
	blockNum  uint64
	header    *types.Header
	blockCtx  *evmtypes.BlockContext
	rules     *chain.Rules
	signer    *types.Signer
	vmConfig  *vm.Config
}

type TraceConsumer struct {
	NewTracer func() GenericTracer
	//Reduce receiving results of execution. They are sorted and have no gaps.
	Reduce func(task *state.TxTask, tx kv.Tx) error
}

func NewTraceWorker2(
	consumer TraceConsumer,
	in *state.QueueWithRetry,
	resultCh *state.ResultsQueue,

	ctx context.Context,
	execArgs *ExecArgs,
	logger log.Logger,
) *TraceWorker2 {
	stateReader := state.NewHistoryReaderV3()
	ie := &TraceWorker2{
		consumer: consumer,
		in:       in,
		resultCh: resultCh,

		execArgs: execArgs,

		stateReader: stateReader,
		evm:         vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, execArgs.ChainConfig, vm.Config{}),
		vmConfig:    &vm.Config{},
		ibs:         state.New(stateReader),
		background:  true,
		ctx:         ctx,
		logger:      logger,
		taskGasPool: new(core.GasPool),
	}
	ie.taskGasPool.AddBlobGas(execArgs.ChainConfig.GetMaxBlobGasPerBlock())
	ie.ibs = state.New(ie.stateReader)

	return ie
}

func (rw *TraceWorker2) Run() error {
	for txTask, ok := rw.in.Next(rw.ctx); ok; txTask, ok = rw.in.Next(rw.ctx) {
		rw.RunTxTask(txTask)
		if err := rw.resultCh.Add(rw.ctx, txTask); err != nil {
			return err
		}
	}
	return nil
}

func (rw *TraceWorker2) RunTxTask(txTask *state.TxTask) {
	if rw.background && rw.chainTx == nil {
		var err error
		if rw.chainTx, err = rw.execArgs.ChainDB.BeginRo(rw.ctx); err != nil {
			panic(fmt.Errorf("BeginRo: %w", err))
		}
		rw.stateReader.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.execArgs.ChainConfig, rw.chainTx, rw.execArgs.BlockReader, rw.logger)
	}

	rw.stateReader.SetTxNum(txTask.TxNum)
	//rw.stateWriter.SetTxNum(rw.ctx, txTask.TxNum)
	rw.stateReader.ResetReadSet()
	//rw.stateWriter.ResetWriteSet()
	rw.stateWriter = state.NewNoopWriter()

	rw.ibs.Reset()
	ibs := rw.ibs

	rules := txTask.Rules
	var err error
	header := txTask.Header

	switch {
	case txTask.TxIndex == -1:
		if txTask.BlockNum == 0 {
			// Genesis block
			_, ibs, err = core.GenesisToBlock(rw.execArgs.Genesis, rw.execArgs.Dirs.Tmp, rw.logger)
			if err != nil {
				panic(fmt.Errorf("GenesisToBlock: %w", err))
			}
			// For Genesis, rules should be empty, so that empty accounts can be included
			rules = &chain.Rules{} //nolint
			break
		}

		// Block initialisation
		syscall := func(contract common.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
			return core.SysCallContract(contract, data, rw.execArgs.ChainConfig, ibs, header, rw.execArgs.Engine, constCall /* constCall */)
		}
		rw.execArgs.Engine.Initialize(rw.execArgs.ChainConfig, rw.chain, header, ibs, syscall, rw.logger)
		txTask.Error = ibs.FinalizeTx(rules, noop)
	case txTask.Final:
		if txTask.BlockNum == 0 {
			break
		}

		// End of block transaction in a block
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, rw.execArgs.ChainConfig, ibs, header, rw.execArgs.Engine, false /* constCall */)
		}

		_, _, err := rw.execArgs.Engine.Finalize(rw.execArgs.ChainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, txTask.BlockReceipts, txTask.Withdrawals, rw.chain, syscall, rw.logger)
		if err != nil {
			txTask.Error = err
		}
	default:
		txHash := txTask.Tx.Hash()
		rw.taskGasPool.Reset(txTask.Tx.GetGas(), rw.execArgs.ChainConfig.GetMaxBlobGasPerBlock())
		if tracer := rw.consumer.NewTracer(); tracer != nil {
			rw.vmConfig.Debug = true
			rw.vmConfig.Tracer = tracer
		}
		rw.vmConfig.SkipAnalysis = txTask.SkipAnalysis
		ibs.SetTxContext(txHash, txTask.BlockHash, txTask.TxIndex)
		msg := txTask.TxAsMessage

		rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, *rw.vmConfig, rules)

		if msg.FeeCap().IsZero() {
			// Only zero-gas transactions may be service ones
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, rw.execArgs.ChainConfig, ibs, header, rw.execArgs.Engine, true /* constCall */)
			}
			msg.SetIsFree(rw.execArgs.Engine.IsServiceTransaction(msg.From(), syscall))
		}

		// MA applytx
		applyRes, err := core.ApplyMessage(rw.evm, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */)
		if err != nil {
			log.Warn("[dbg] applyerr: ", "err", err)
			txTask.Error = err
		} else {
			txTask.Failed = applyRes.Failed()
			txTask.UsedGas = applyRes.UsedGas
			// Update the state with pending changes
			ibs.SoftFinalise()
			txTask.Logs = ibs.GetLogs(txHash)
		}
		//txTask.Tracer = tracer
	}
}
func (rw *TraceWorker2) ResetTx(chainTx kv.Tx) {
	//log.Warn("[dbg] ResetTx", "bg", rw.background, "stack", dbg.Stack())
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
		rw.chainTx = nil
	}
	if chainTx != nil {
		rw.chainTx = chainTx
		rw.stateReader.SetTx(rw.chainTx)
		//rw.stateWriter.SetTx(rw.chainTx)
		rw.chain = consensuschain.NewReader(rw.execArgs.ChainConfig, rw.chainTx, rw.execArgs.BlockReader, rw.logger)
	}
}

// immutable (aka. global) params required for block execution. can instantiate once at app-start
type ExecArgs struct {
	ChainDB     kv.RoDB
	Genesis     *types.Genesis
	BlockReader services.FullBlockReader
	Prune       prune.Mode
	Engine      consensus.Engine
	Dirs        datadir.Dirs
	ChainConfig *chain.Config
	Workers     int
}

func NewTraceWorkers2Pool(consumer TraceConsumer, cfg *ExecArgs, ctx context.Context, toTxNum uint64, in *state.QueueWithRetry, workerCount int, outputTxNum *atomic.Uint64, logger log.Logger) (g *errgroup.Group, clearFunc func()) {
	workers := make([]*TraceWorker2, workerCount)

	resultChSize := workerCount * 8
	rws := state.NewResultsQueue(resultChSize, workerCount) // workerCount * 4
	// we all errors in background workers (except ctx.Cancel), because applyLoop will detect this error anyway.
	// and in applyLoop all errors are critical
	ctx, cancel := context.WithCancel(ctx)
	g, ctx = errgroup.WithContext(ctx)
	for i := 0; i < workerCount; i++ {
		workers[i] = NewTraceWorker2(consumer, in, rws, ctx, cfg, logger)
	}
	for i := 0; i < workerCount; i++ {
		i := i
		g.Go(func() (err error) {
			defer func() {
				if rec := recover(); rec != nil {
					err = fmt.Errorf("%s, %s", rec, dbg.Stack())
				}
			}()

			return workers[i].Run()
		})
	}

	//Reducer
	g.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("%s, %s", rec, dbg.Stack())
			}
		}()

		tx, err := cfg.ChainDB.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		defer logger.Warn("[dbg] reduce goroutine exit", "toTxNum", toTxNum, "txptr", fmt.Sprintf("%p", tx))
		applyWorker := NewTraceWorker2(consumer, in, rws, ctx, cfg, logger)
		applyWorker.background = false
		applyWorker.ResetTx(tx)
		for outputTxNum.Load() <= toTxNum {
			if err := rws.Drain(ctx); err != nil {
				return fmt.Errorf("rws.Drain: %w", err)
			}

			processedTxNum, _, err := processResultQueue2(consumer, rws, outputTxNum.Load(), applyWorker, true)
			if err != nil {
				return fmt.Errorf("processResultQueue2: %w", err)
			}
			if processedTxNum > 0 {
				outputTxNum.Store(processedTxNum)
			}
		}
		return nil
	})

	var clearDone bool
	clearFunc = func() {
		if clearDone {
			return
		}
		clearDone = true
		cancel()
		g.Wait()
		for _, w := range workers {
			w.ResetTx(nil)
		}
	}

	return g, clearFunc
}

func processResultQueue2(consumer TraceConsumer, rws *state.ResultsQueue, outputTxNumIn uint64, applyWorker *TraceWorker2, forceStopAtBlockEnd bool) (outputTxNum uint64, stopedAtBlockEnd bool, err error) {
	rwsIt := rws.Iter()
	defer rwsIt.Close()

	var receipts types.Receipts
	var usedGas, blobGasUsed uint64

	var i int
	outputTxNum = outputTxNumIn
	log.Warn("[dbg] applyWorker.chainTx0", "txptr", fmt.Sprintf("%p", applyWorker.chainTx))
	for rwsIt.HasNext(outputTxNum) {
		txTask := rwsIt.PopNext()
		if txTask.Final {
			txTask.Reset()
			//re-exec right here, because gnosis expecting TxTask.BlockReceipts field - receipts of all
			txTask.BlockReceipts = receipts
			log.Warn("[dbg] applyWorker.chainTx1", "txptr", fmt.Sprintf("%p", applyWorker.chainTx))
			applyWorker.RunTxTask(txTask)
			log.Warn("[dbg] applyWorker.chainTx2", "txptr", fmt.Sprintf("%p", applyWorker.chainTx))
		}
		if txTask.Error != nil {
			err := fmt.Errorf("%w: %v, blockNum=%d, TxNum=%d, TxIndex=%d, Final=%t", consensus.ErrInvalidBlock, txTask.Error, txTask.BlockNum, txTask.TxNum, txTask.TxIndex, txTask.Final)
			return outputTxNum, false, err
		}
		log.Warn("[dbg] applyWorker.chainTx3", "txptr", fmt.Sprintf("%p", applyWorker.chainTx))
		if err := consumer.Reduce(txTask, applyWorker.chainTx); err != nil {
			return outputTxNum, false, err
		}

		if !txTask.Final && txTask.TxIndex >= 0 {
			// by the tx.
			receipt := &types.Receipt{
				BlockNumber:       txTask.Header.Number,
				TransactionIndex:  uint(txTask.TxIndex),
				Type:              txTask.Tx.Type(),
				CumulativeGasUsed: usedGas,
				TxHash:            txTask.Tx.Hash(),
				Logs:              txTask.Logs,
			}
			if txTask.Failed {
				receipt.Status = types.ReceiptStatusFailed
			} else {
				receipt.Status = types.ReceiptStatusSuccessful
			}
			// if the transaction created a contract, store the creation address in the receipt.
			//if msg.To() == nil {
			//	receipt.ContractAddress = crypto.CreateAddress(evm.Origin, tx.GetNonce())
			//}
			// Set the receipt logs and create a bloom for filtering
			//receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
			receipts = append(receipts, receipt)
		}

		usedGas += txTask.UsedGas
		if txTask.Tx != nil {
			blobGasUsed += txTask.Tx.GetBlobGas()
		}

		i++
		outputTxNum++
		stopedAtBlockEnd = txTask.Final
		if forceStopAtBlockEnd && txTask.Final {
			break
		}
	}
	return
}

func CustomTraceMapReduce(fromBlock, toBlock uint64, consumer TraceConsumer, ctx context.Context, tx kv.TemporalTx, cfg *ExecArgs, logger log.Logger) (err error) {
	log.Info("[CustomTraceMapReduce] start", "fromBlock", fromBlock, "toBlock", toBlock, "workers", cfg.Workers)
	br := cfg.BlockReader
	chainConfig := cfg.ChainConfig
	getHeaderFunc := func(hash common.Hash, number uint64) (h *types.Header) {
		var err error
		if err = cfg.ChainDB.View(ctx, func(tx kv.Tx) error {
			h, err = cfg.BlockReader.Header(ctx, tx, hash, number)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			panic(err)
		}
		return h
	}

	fromTxNum, err := rawdbv3.TxNums.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	toTxNum, err := rawdbv3.TxNums.Max(tx, toBlock)
	if err != nil {
		return err
	}

	// "Map-Reduce on history" is conflict-free - means we don't need "Retry" feature.
	// But still can use this data-type as simple queue.
	in := state.NewQueueWithRetry(100_000)
	defer in.Close()

	var WorkerCount = estimate.AlmostAllCPUs() * 2
	if cfg.Workers > 0 {
		WorkerCount = cfg.Workers
	}
	outTxNum := &atomic.Uint64{}
	outTxNum.Store(fromTxNum)
	workers, cleanup := NewTraceWorkers2Pool(consumer, cfg, ctx, toTxNum, in, WorkerCount, outTxNum, logger)
	defer workers.Wait()
	defer cleanup()

	workersExited := &atomic.Bool{}
	go func() {
		workers.Wait()
		workersExited.Store(true)
	}()

	inputTxNum, err := rawdbv3.TxNums.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		var b *types.Block
		b, err = blockWithSenders(nil, tx, br, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			// TODO: panic here and see that overall process deadlock
			return fmt.Errorf("nil block %d", blockNum)
		}
		txs := b.Transactions()
		header := b.HeaderNoCopy()
		skipAnalysis := core.SkipAnalysis(chainConfig, blockNum)
		signer := *types.MakeSigner(chainConfig, blockNum, header.Time)

		f := core.GetHashFn(header, getHeaderFunc)
		getHashFnMute := &sync.Mutex{}
		getHashFn := func(n uint64) common.Hash {
			getHashFnMute.Lock()
			defer getHashFnMute.Unlock()
			return f(n)
		}
		blockContext := core.NewEVMBlockContext(header, getHashFn, cfg.Engine, nil /* author */)

		rules := chainConfig.Rules(blockNum, b.Time())
		for txIndex := -1; txIndex <= len(txs); txIndex++ {
			// Do not oversend, wait for the result heap to go under certain size
			txTask := &state.TxTask{
				BlockNum:        blockNum,
				Header:          header,
				Coinbase:        b.Coinbase(),
				Uncles:          b.Uncles(),
				Rules:           rules,
				Txs:             txs,
				TxNum:           inputTxNum,
				TxIndex:         txIndex,
				BlockHash:       b.Hash(),
				SkipAnalysis:    skipAnalysis,
				Final:           txIndex == len(txs),
				GetHashFn:       getHashFn,
				EvmBlockContext: blockContext,
				Withdrawals:     b.Withdrawals(),

				// use history reader instead of state reader to catch up to the tx where we left off
				HistoryExecution: true,
			}
			if txIndex >= 0 && txIndex < len(txs) {
				txTask.Tx = txs[txIndex]
				txTask.TxAsMessage, err = txTask.Tx.AsMessage(signer, header.BaseFee, txTask.Rules)
				if err != nil {
					return err
				}

				if sender, ok := txs[txIndex].GetSender(); ok {
					txTask.Sender = &sender
				} else {
					sender, err := signer.Sender(txTask.Tx)
					if err != nil {
						return err
					}
					txTask.Sender = &sender
					logger.Warn("[Execution] expensive lazy sender recovery", "blockNum", txTask.BlockNum, "txIdx", txTask.TxIndex)
				}
			}
			if workersExited.Load() {
				return workers.Wait()
			}
			log.Info("[dbg] alex", "txTask.BlockNum", txTask.BlockNum)
			logger.Warn("[Execution] expensive lazy sender recovery", "blockNum", txTask.BlockNum, "txIdx", txTask.TxIndex)
			in.Add(ctx, txTask)
			inputTxNum++
		}
	}

	if err := workers.Wait(); err != nil {
		return fmt.Errorf("WorkersPool: %w", err)
	}

	return nil
}

func blockWithSenders(db kv.RoDB, tx kv.Tx, blockReader services.BlockReader, blockNum uint64) (b *types.Block, err error) {
	if tx == nil {
		tx, err = db.BeginRo(context.Background())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}
	b, err = blockReader.BlockByNumber(context.Background(), tx, blockNum)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	for _, txn := range b.Transactions() {
		_ = txn.Hash()
	}
	return b, err
}
