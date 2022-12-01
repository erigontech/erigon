package exec3

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	state "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
)

type ScanWorker struct {
	txNum          uint64
	as             *libstate.AggregatorStep
	fromKey, toKey []byte
	currentKey     []byte
	bitmap         roaring64.Bitmap
}

func NewScanWorker(txNum uint64, as *libstate.AggregatorStep) *ScanWorker {
	sw := &ScanWorker{
		txNum: txNum,
		as:    as,
	}
	return sw
}

type FillWorker struct {
	txNum          uint64
	doneCount      *atomic.Uint64
	ac             *libstate.Aggregator22Context
	fromKey, toKey []byte
	currentKey     []byte
	bitmap         roaring64.Bitmap

	total, progress *atomic.Uint64
}

func NewFillWorker(txNum uint64, doneCount *atomic.Uint64, a *libstate.Aggregator22, fromKey, toKey []byte) *FillWorker {
	fw := &FillWorker{
		txNum:     txNum,
		doneCount: doneCount,
		ac:        a.MakeContext(),
		fromKey:   fromKey,
		toKey:     toKey,
		total:     atomic.NewUint64(0),
		progress:  atomic.NewUint64(0),
	}
	return fw
}

func (sw *ScanWorker) Bitmap() *roaring64.Bitmap { return &sw.bitmap }

func (fw *FillWorker) Total() uint64 {
	return fw.total.Load()
}

func (fw *FillWorker) Progress() uint64 {
	return fw.progress.Load()
}

func (fw *FillWorker) FillAccounts(plainStateCollector *etl.Collector) {
	defer fw.doneCount.Inc()
	it := fw.ac.IterateAccountsHistory(fw.fromKey, fw.toKey, fw.txNum)
	fw.total.Store(it.Total())
	value := make([]byte, 1024)
	for it.HasNext() {
		key, val, progress := it.Next()
		fw.progress.Store(progress)
		fw.currentKey = key
		if len(val) > 0 {
			var a accounts.Account
			a.Reset()
			pos := 0
			nonceBytes := int(val[pos])
			pos++
			if nonceBytes > 0 {
				a.Nonce = bytesToUint64(val[pos : pos+nonceBytes])
				pos += nonceBytes
			}
			balanceBytes := int(val[pos])
			pos++
			if balanceBytes > 0 {
				a.Balance.SetBytes(val[pos : pos+balanceBytes])
				pos += balanceBytes
			}
			codeHashBytes := int(val[pos])
			pos++
			if codeHashBytes > 0 {
				copy(a.CodeHash[:], val[pos:pos+codeHashBytes])
				pos += codeHashBytes
			}
			incBytes := int(val[pos])
			pos++
			if incBytes > 0 {
				a.Incarnation = bytesToUint64(val[pos : pos+incBytes])
			}
			if a.Incarnation > 0 {
				a.Incarnation = state.FirstContractIncarnation
			}
			value = value[:a.EncodingLengthForStorage()]
			a.EncodeForStorage(value)
			if err := plainStateCollector.Collect(key, value); err != nil {
				panic(err)
			}
			//fmt.Printf("Account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", key, &a.Balance, a.Nonce, a.Root, a.CodeHash)
		} else {
			if err := plainStateCollector.Collect(key, nil); err != nil {
				panic(err)
			}
		}
	}
}

func (fw *FillWorker) FillStorage(plainStateCollector *etl.Collector) {
	defer fw.doneCount.Inc()
	it := fw.ac.IterateStorageHistory(fw.fromKey, fw.toKey, fw.txNum)
	fw.total.Store(it.Total())
	var compositeKey = make([]byte, length.Addr+length.Incarnation+length.Hash)
	binary.BigEndian.PutUint64(compositeKey[20:], state.FirstContractIncarnation)
	for it.HasNext() {
		key, val, progress := it.Next()
		fw.progress.Store(progress)
		fw.currentKey = key
		copy(compositeKey[:20], key[:20])
		copy(compositeKey[20+8:], key[20:])
		if len(val) > 0 {
			if err := plainStateCollector.Collect(compositeKey, val); err != nil {
				panic(err)
			}
			//fmt.Printf("Storage [%x] => [%x]\n", compositeKey, val)
		} else {
			if err := plainStateCollector.Collect(compositeKey, nil); err != nil {
				panic(err)
			}
		}
	}
}

func (fw *FillWorker) FillCode(codeCollector, plainContractCollector *etl.Collector) {
	defer fw.doneCount.Inc()
	it := fw.ac.IterateCodeHistory(fw.fromKey, fw.toKey, fw.txNum)
	fw.total.Store(it.Total())
	var compositeKey = make([]byte, length.Addr+length.Incarnation)
	binary.BigEndian.PutUint64(compositeKey[length.Addr:], state.FirstContractIncarnation)

	for it.HasNext() {
		key, val, progress := it.Next()
		fw.progress.Store(progress)
		fw.currentKey = key
		copy(compositeKey, key)
		if len(val) > 0 {

			codeHash, err := common.HashData(val)
			if err != nil {
				panic(err)
			}
			if err = codeCollector.Collect(codeHash[:], val); err != nil {
				panic(err)
			}
			if err = plainContractCollector.Collect(compositeKey, codeHash[:]); err != nil {
				panic(err)
			}
			//fmt.Printf("Code [%x] => %d\n", compositeKey, len(val))
		} else {
			if err := plainContractCollector.Collect(compositeKey, nil); err != nil {
				panic(err)
			}
		}
	}
}

func (fw *FillWorker) ResetProgress() {
	fw.total.Store(0)
	fw.progress.Store(0)
}

var addr1 common.Address = common.HexToAddress("0xfffffffff047852f159827f782a42141f39857ed")

func (sw *ScanWorker) BitmapAccounts(accountCollectorX *etl.Collector) {
	it := sw.as.IterateAccountsTxs(sw.txNum)
	var txKey [8]byte
	for it.HasNext() {
		key, txNum, del := it.Next()
		if bytes.Equal(key, addr1[:]) {
			fmt.Printf("BitmapAccounts %x %d %t\n", key, txNum, del)
		}
		if del {
			if err := accountCollectorX.Collect(key, nil); err != nil {
				panic(err)
			}
		} else {
			binary.BigEndian.PutUint64(txKey[:], txNum)
			if err := accountCollectorX.Collect(key, txKey[:]); err != nil {
				panic(err)
			}
			sw.bitmap.Add(txNum)
		}
	}
}

func (sw *ScanWorker) BitmapStorage(storageCollectorX *etl.Collector) {
	it := sw.as.IterateStorageTxs(sw.txNum)
	var txKey [8]byte
	for it.HasNext() {
		key, txNum, del := it.Next()
		if del {
			if err := storageCollectorX.Collect(key, nil); err != nil {
				panic(err)
			}
		} else {
			binary.BigEndian.PutUint64(txKey[:], txNum)
			if err := storageCollectorX.Collect(key, txKey[:]); err != nil {
				panic(err)
			}
			sw.bitmap.Add(txNum)
		}
	}
}

func (sw *ScanWorker) BitmapCode(codeCollectorX *etl.Collector) {
	it := sw.as.IterateCodeTxs(sw.txNum)
	var txKey [8]byte
	for it.HasNext() {
		key, txNum, del := it.Next()
		if del {
			if err := codeCollectorX.Collect(key, nil); err != nil {
				panic(err)
			}
		} else {
			binary.BigEndian.PutUint64(txKey[:], txNum)
			if err := codeCollectorX.Collect(key, txKey[:]); err != nil {
				panic(err)
			}
			sw.bitmap.Add(txNum)
		}
	}
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

type ReconWorker struct {
	lock        sync.Locker
	wg          *sync.WaitGroup
	rs          *state.ReconState
	blockReader services.FullBlockReader
	stateWriter *state.StateReconWriterInc
	stateReader *state.HistoryReaderInc
	getHeader   func(hash common.Hash, number uint64) *types.Header
	ctx         context.Context
	engine      consensus.Engine
	chainConfig *params.ChainConfig
	logger      log.Logger
	genesis     *core.Genesis
	epoch       EpochReader
	chain       ChainReader
	isPoSA      bool
	posa        consensus.PoSA
}

func NewReconWorker(lock sync.Locker, wg *sync.WaitGroup, rs *state.ReconState,
	a *libstate.Aggregator22, as *libstate.AggregatorStep, blockReader services.FullBlockReader,
	chainConfig *params.ChainConfig, logger log.Logger, genesis *core.Genesis, engine consensus.Engine,
	chainTx kv.Tx,
) *ReconWorker {
	ac := a.MakeContext()
	rw := &ReconWorker{
		lock:        lock,
		wg:          wg,
		rs:          rs,
		blockReader: blockReader,
		ctx:         context.Background(),
		stateWriter: state.NewStateReconWriterInc(ac, rs),
		stateReader: state.NewHistoryReaderInc(as, rs),
		chainConfig: chainConfig,
		logger:      logger,
		genesis:     genesis,
		engine:      engine,
	}
	rw.epoch = NewEpochReader(chainTx)
	rw.chain = NewChainReader(chainConfig, chainTx, blockReader)
	rw.posa, rw.isPoSA = engine.(consensus.PoSA)
	return rw
}

func (rw *ReconWorker) SetTx(tx kv.Tx) {
	rw.stateReader.SetTx(tx)
	rw.stateWriter.SetTx(tx)
}

func (rw *ReconWorker) Run() {
	defer rw.wg.Done()
	rw.getHeader = func(hash common.Hash, number uint64) *types.Header {
		h, err := rw.blockReader.Header(rw.ctx, nil, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}
	for txTask, ok := rw.rs.Schedule(); ok; txTask, ok = rw.rs.Schedule() {
		rw.runTxTask(txTask)
	}
}

var noop = state.NewNoopWriter()

func (rw *ReconWorker) runTxTask(txTask *state.TxTask) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetError()
	rw.stateWriter.SetTxNum(txTask.TxNum)
	rw.stateReader.SetTrace(txTask.TxNum == 102137677)
	rules := txTask.Rules
	ibs := state.New(rw.stateReader)
	daoForkTx := rw.chainConfig.DAOForkSupport && rw.chainConfig.DAOForkBlock != nil && rw.chainConfig.DAOForkBlock.Uint64() == txTask.BlockNum && txTask.TxIndex == -1
	var err error
	if txTask.BlockNum == 0 && txTask.TxIndex == -1 {
		//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
		// Genesis block
		_, ibs, err = rw.genesis.ToBlock()
		if err != nil {
			panic(err)
		}
		// For Genesis, rules should be empty, so that empty accounts can be included
		rules = &params.Rules{}
	} else if daoForkTx {
		//fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txNum, blockNum)
		misc.ApplyDAOHardFork(ibs)
		ibs.SoftFinalise()
	} else if txTask.Final {
		if txTask.BlockNum > 0 {
			//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
			// End of block transaction in a block
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine, false /* constCall */)
			}
			if _, _, err = rw.engine.Finalize(rw.chainConfig, txTask.Header, ibs, txTask.Txs, txTask.Uncles, nil /* receipts */, rw.epoch, rw.chain, syscall); err != nil {
				if _, readError := rw.stateReader.ReadError(); !readError {
					panic(fmt.Errorf("finalize of block %d failed: %w", txTask.BlockNum, err))
				}
			}
		}
	} else if txTask.TxIndex == -1 {
		// Block initialisation
		if rw.isPoSA {
			systemcontracts.UpgradeBuildInSystemContract(rw.chainConfig, txTask.Header.Number, ibs)
		}
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine, false /* constCall */)
		}

		rw.engine.Initialize(rw.chainConfig, rw.chain, rw.epoch, txTask.Header, ibs, txTask.Txs, txTask.Uncles, syscall)
	} else {
		if rw.isPoSA {
			if isSystemTx, err := rw.posa.IsSystemTransaction(txTask.Tx, txTask.Header); err != nil {
				if _, readError := rw.stateReader.ReadError(); !readError {
					panic(err)
				}
			} else if isSystemTx {
				return
			}
		}
		gp := new(core.GasPool).AddGas(txTask.Tx.GetGas())
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: txTask.SkipAnalysis}
		getHashFn := core.GetHashFn(txTask.Header, rw.getHeader)
		ibs.Prepare(txTask.Tx.Hash(), txTask.BlockHash, txTask.TxIndex)
		msg := txTask.TxAsMessage

		var vmenv vm.VMInterface
		if txTask.Tx.IsStarkNet() {
			vmenv = &vm.CVMAdapter{Cvm: vm.NewCVM(ibs)}
		} else {
			blockContext := core.NewEVMBlockContext(txTask.Header, getHashFn, rw.engine, nil /* author */)
			txContext := core.NewEVMTxContext(msg)
			vmenv = vm.NewEVM(blockContext, txContext, ibs, rw.chainConfig, vmConfig)
		}
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, evm=%p\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex, vmenv)
		_, err = core.ApplyMessage(vmenv, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			if _, readError := rw.stateReader.ReadError(); !readError {
				panic(fmt.Errorf("could not apply blockNum=%d, txIdx=%d [%x] failed: %w", txTask.BlockNum, txTask.TxIndex, txTask.Tx.Hash(), err))
			}
		}
		if err = ibs.FinalizeTx(rules, noop); err != nil {
			if _, readError := rw.stateReader.ReadError(); !readError {
				panic(err)
			}
		}
	}
	if dependency, ok := rw.stateReader.ReadError(); ok || err != nil {
		//fmt.Printf("rollback %d\n", txNum)
		rw.rs.RollbackTx(txTask, dependency)
	} else {
		if err = ibs.CommitBlock(rules, rw.stateWriter); err != nil {
			panic(err)
		}
		//fmt.Printf("commit %d\n", txNum)
		rw.rs.CommitTxNum(txTask.TxNum)
	}
}
