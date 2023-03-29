package exec3

import (
	"context"
	"math/big"
	"sync"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type Worker struct {
	lock        sync.Locker
	chainDb     kv.RoDB
	chainTx     kv.Tx
	background  bool // if true - worker does manage RoTx (begin/rollback) in .ResetTx()
	blockReader services.FullBlockReader
	rs          *state.StateV3
	stateWriter *state.StateWriterV3
	stateReader *state.StateReaderV3
	chainConfig *chain.Config
	getHeader   func(hash libcommon.Hash, number uint64) *types.Header

	ctx      context.Context
	engine   consensus.Engine
	logger   log.Logger
	genesis  *core.Genesis
	resultCh chan *exec22.TxTask
	epoch    EpochReader
	chain    ChainReader
	isPoSA   bool
	posa     consensus.PoSA

	callTracer  *CallTracer
	taskGasPool *core.GasPool

	evm *vm.EVM
	ibs *state.IntraBlockState
}

func NewWorker(lock sync.Locker, ctx context.Context, background bool, chainDb kv.RoDB, rs *state.StateV3, blockReader services.FullBlockReader, chainConfig *chain.Config, logger log.Logger, genesis *core.Genesis, resultCh chan *exec22.TxTask, engine consensus.Engine) *Worker {
	w := &Worker{
		lock:        lock,
		chainDb:     chainDb,
		rs:          rs,
		background:  background,
		blockReader: blockReader,
		stateWriter: state.NewStateWriterV3(rs),
		stateReader: state.NewStateReaderV3(rs),
		chainConfig: chainConfig,

		ctx:      ctx,
		logger:   logger,
		genesis:  genesis,
		resultCh: resultCh,
		engine:   engine,

		evm:         vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),
		callTracer:  NewCallTracer(),
		taskGasPool: new(core.GasPool),
	}
	w.getHeader = func(hash libcommon.Hash, number uint64) *types.Header {
		h, err := blockReader.Header(ctx, w.chainTx, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}

	w.ibs = state.New(w.stateReader)

	w.posa, w.isPoSA = engine.(consensus.PoSA)
	return w
}

func (rw *Worker) Tx() kv.Tx        { return rw.chainTx }
func (rw *Worker) DiscardReadList() { rw.stateReader.DiscardReadList() }
func (rw *Worker) ResetTx(chainTx kv.Tx) {
	if rw.background && rw.chainTx != nil {
		rw.chainTx.Rollback()
		rw.chainTx = nil
	}
	if chainTx != nil {
		rw.chainTx = chainTx
		rw.stateReader.SetTx(rw.chainTx)
		rw.epoch = EpochReader{tx: rw.chainTx}
		rw.chain = ChainReader{config: rw.chainConfig, tx: rw.chainTx, blockReader: rw.blockReader}
	}
}

func (rw *Worker) Run() error {
	for txTask, ok := rw.rs.Schedule(); ok; txTask, ok = rw.rs.Schedule() {
		rw.RunTxTask(txTask)
		select {
		case rw.resultCh <- txTask: // Needs to have outside of the lock
		case <-rw.ctx.Done():
			return rw.ctx.Err()
		}
	}
	return nil
}

func (rw *Worker) RunTxTask(txTask *exec22.TxTask) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.RunTxTaskNoLock(txTask)
}

func (rw *Worker) RunTxTaskNoLock(txTask *exec22.TxTask) {
	if rw.background && rw.chainTx == nil {
		var err error
		if rw.chainTx, err = rw.chainDb.BeginRo(rw.ctx); err != nil {
			panic(err)
		}
		rw.stateReader.SetTx(rw.chainTx)
		rw.epoch = EpochReader{tx: rw.chainTx}
		rw.chain = ChainReader{config: rw.chainConfig, tx: rw.chainTx, blockReader: rw.blockReader}
	}
	txTask.Error = nil
	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateWriter.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetReadSet()
	rw.stateWriter.ResetWriteSet()
	rw.ibs.Reset()
	ibs := rw.ibs

	rules := txTask.Rules
	daoForkTx := rw.chainConfig.DAOForkSupport && rw.chainConfig.DAOForkBlock != nil && rw.chainConfig.DAOForkBlock.Uint64() == txTask.BlockNum && txTask.TxIndex == -1
	var err error
	header := txTask.Header
	if txTask.BlockNum == 0 && txTask.TxIndex == -1 {
		//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
		// Genesis block
		_, ibs, err = rw.genesis.ToBlock("")
		if err != nil {
			panic(err)
		}
		// For Genesis, rules should be empty, so that empty accounts can be included
		rules = &chain.Rules{}
	} else if daoForkTx {
		//fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txTask.TxNum, txTask.BlockNum)
		misc.ApplyDAOHardFork(ibs)
		ibs.SoftFinalise()
	} else if txTask.TxIndex == -1 {
		// Block initialisation
		//fmt.Printf("txNum=%d, blockNum=%d, initialisation of the block\n", txTask.TxNum, txTask.BlockNum)
		if rw.isPoSA {
			systemcontracts.UpgradeBuildInSystemContract(rw.chainConfig, header.Number, ibs)
		}
		syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, *rw.chainConfig, ibs, header, rw.engine, false /* constCall */)
		}
		rw.engine.Initialize(rw.chainConfig, rw.chain, rw.epoch, header, ibs, txTask.Txs, txTask.Uncles, syscall)
	} else if txTask.Final {
		if txTask.BlockNum > 0 {
			//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
			// End of block transaction in a block
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *rw.chainConfig, ibs, header, rw.engine, false /* constCall */)
			}

			if _, _, err := rw.engine.Finalize(rw.chainConfig, types.CopyHeader(header), ibs, txTask.Txs, txTask.Uncles, nil /* receipts */, txTask.Withdrawals, rw.epoch, rw.chain, syscall); err != nil {
				//fmt.Printf("error=%v\n", err)
				txTask.Error = err
			} else {
				txTask.TraceTos = map[libcommon.Address]struct{}{}
				txTask.TraceTos[txTask.Coinbase] = struct{}{}
				for _, uncle := range txTask.Uncles {
					txTask.TraceTos[uncle.Coinbase] = struct{}{}
				}
			}
		}
	} else {
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		if rw.isPoSA {
			if isSystemTx, err := rw.posa.IsSystemTransaction(txTask.Tx, header); err != nil {
				panic(err)
			} else if isSystemTx {
				//fmt.Printf("System tx\n")
				return
			}
		}
		txHash := txTask.Tx.Hash()
		rw.taskGasPool.Reset(txTask.Tx.GetGas())
		rw.callTracer.Reset()
		vmConfig := vm.Config{Debug: true, Tracer: rw.callTracer, SkipAnalysis: txTask.SkipAnalysis}
		ibs.Prepare(txHash, txTask.BlockHash, txTask.TxIndex)
		msg := txTask.TxAsMessage

		blockContext := txTask.EvmBlockContext
		if !rw.background {
			getHashFn := core.GetHashFn(header, rw.getHeader)
			blockContext = core.NewEVMBlockContext(header, getHashFn, rw.engine, nil /* author */)
		}
		rw.evm.ResetBetweenBlocks(blockContext, core.NewEVMTxContext(msg), ibs, vmConfig, rules)
		vmenv := rw.evm

		applyRes, err := core.ApplyMessage(vmenv, msg, rw.taskGasPool, true /* refunds */, false /* gasBailout */)
		if err != nil {
			txTask.Error = err
			//fmt.Printf("error=%v\n", err)
		} else {
			txTask.UsedGas = applyRes.UsedGas
			// Update the state with pending changes
			ibs.SoftFinalise()
			txTask.Logs = ibs.GetLogs(txHash)
			txTask.TraceFroms = rw.callTracer.Froms()
			txTask.TraceTos = rw.callTracer.Tos()
		}
	}
	// Prepare read set, write set and balanceIncrease set and send for serialisation
	if txTask.Error == nil {
		txTask.BalanceIncreaseSet = ibs.BalanceIncreaseSet()
		//for addr, bal := range txTask.BalanceIncreaseSet {
		//	fmt.Printf("BalanceIncreaseSet [%x]=>[%d]\n", addr, &bal)
		//}
		if err = ibs.MakeWriteSet(rules, rw.stateWriter); err != nil {
			panic(err)
		}
		txTask.ReadLists = rw.stateReader.ReadSet()
		txTask.WriteLists = rw.stateWriter.WriteSet()
		txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = rw.stateWriter.PrevAndDels()
		size := (20 + 32) * len(txTask.BalanceIncreaseSet)
		for _, list := range txTask.ReadLists {
			for _, b := range list.Keys {
				size += len(b)
			}
			for _, b := range list.Vals {
				size += len(b)
			}
		}
		for _, list := range txTask.WriteLists {
			for _, b := range list.Keys {
				size += len(b)
			}
			for _, b := range list.Vals {
				size += len(b)
			}
		}
		txTask.ResultsSize = int64(size)
	}
}

type ChainReader struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader services.FullBlockReader
}

func NewChainReader(config *chain.Config, tx kv.Tx, blockReader services.FullBlockReader) ChainReader {
	return ChainReader{config: config, tx: tx, blockReader: blockReader}
}

func (cr ChainReader) Config() *chain.Config        { return cr.config }
func (cr ChainReader) CurrentHeader() *types.Header { panic("") }
func (cr ChainReader) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr ChainReader) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr ChainReader) GetHeaderByHash(hash libcommon.Hash) *types.Header {
	if cr.blockReader != nil {
		number := rawdb.ReadHeaderNumber(cr.tx, hash)
		if number == nil {
			return nil
		}
		return cr.GetHeader(hash, *number)
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr ChainReader) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		log.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}

type EpochReader struct {
	tx kv.Tx
}

func NewEpochReader(tx kv.Tx) EpochReader { return EpochReader{tx: tx} }

func (cr EpochReader) GetEpoch(hash libcommon.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadEpoch(cr.tx, number, hash)
}
func (cr EpochReader) PutEpoch(hash libcommon.Hash, number uint64, proof []byte) error {
	panic("")
}
func (cr EpochReader) GetPendingEpoch(hash libcommon.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadPendingEpoch(cr.tx, number, hash)
}
func (cr EpochReader) PutPendingEpoch(hash libcommon.Hash, number uint64, proof []byte) error {
	panic("")
}
func (cr EpochReader) FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash libcommon.Hash, transitionProof []byte, err error) {
	return rawdb.FindEpochBeforeOrEqualNumber(cr.tx, number)
}

func NewWorkersPool(lock sync.Locker, ctx context.Context, background bool, chainDb kv.RoDB, rs *state.StateV3, blockReader services.FullBlockReader, chainConfig *chain.Config, logger log.Logger, genesis *core.Genesis, engine consensus.Engine, workerCount int) (reconWorkers []*Worker, applyWorker *Worker, resultCh chan *exec22.TxTask, clear func(), wait func()) {
	queueSize := workerCount * 256
	reconWorkers = make([]*Worker, workerCount)
	resultCh = make(chan *exec22.TxTask, queueSize)
	{
		// we all errors in background workers (except ctx.Cancele), because applyLoop will detect this error anyway.
		// and in applyLoop all errors are critical
		ctx, cancel := context.WithCancel(ctx)
		g, ctx := errgroup.WithContext(ctx)
		for i := 0; i < workerCount; i++ {
			reconWorkers[i] = NewWorker(lock, ctx, background, chainDb, rs, blockReader, chainConfig, logger, genesis, resultCh, engine)
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
			cancel()
			g.Wait()
			for _, w := range reconWorkers {
				w.ResetTx(nil)
			}
			//applyWorker.ResetTx(nil)
			close(resultCh)
		}
	}
	applyWorker = NewWorker(lock, ctx, false, chainDb, rs, blockReader, chainConfig, logger, genesis, resultCh, engine)

	return reconWorkers, applyWorker, resultCh, clear, wait
}
