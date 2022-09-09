package exec22

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	state2 "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

type FillWorker struct {
	txNum          uint64
	doneCount      *uint64
	ac             *state.Aggregator22Context
	fromKey, toKey []byte
	currentKey     []byte
	bitmap         roaring64.Bitmap
	total          uint64
	progress       uint64
}

func NewFillWorker(txNum uint64, doneCount *uint64, a *state.Aggregator22, fromKey, toKey []byte) *FillWorker {
	fw := &FillWorker{
		txNum:     txNum,
		doneCount: doneCount,
		ac:        a.MakeContext(),
		fromKey:   fromKey,
		toKey:     toKey,
	}
	return fw
}

func (fw *FillWorker) Total() uint64 {
	return atomic.LoadUint64(&fw.total)
}
func (fw *FillWorker) Bitmap() *roaring64.Bitmap { return &fw.bitmap }

func (fw *FillWorker) Progress() uint64 {
	return atomic.LoadUint64(&fw.progress)
}

func (fw *FillWorker) FillAccounts(plainStateCollector *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateAccountsHistory(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		key, val, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
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
				a.Incarnation = state2.FirstContractIncarnation
			}
			value := make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(value)
			if err := plainStateCollector.Collect(key, value); err != nil {
				panic(err)
			}
			//fmt.Printf("Account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", key, &a.Balance, a.Nonce, a.Root, a.CodeHash)
		}
	}
}

func (fw *FillWorker) FillStorage(plainStateCollector *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateStorageHistory(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		key, val, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.currentKey = key
		compositeKey := dbutils.PlainGenerateCompositeStorageKey(key[:20], state2.FirstContractIncarnation, key[20:])
		if len(val) > 0 {
			if err := plainStateCollector.Collect(compositeKey, val); err != nil {
				panic(err)
			}
			//fmt.Printf("Storage [%x] => [%x]\n", compositeKey, val)
		}
	}
}

func (fw *FillWorker) FillCode(codeCollector, plainContractCollector *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateCodeHistory(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	for it.HasNext() {
		key, val, progress := it.Next()
		atomic.StoreUint64(&fw.progress, progress)
		fw.currentKey = key
		compositeKey := dbutils.PlainGenerateStoragePrefix(key, state2.FirstContractIncarnation)
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
		}
	}
}

func (fw *FillWorker) ResetProgress() {
	fw.total = 0
	fw.progress = 0
}

func (fw *FillWorker) BitmapAccounts(accountCollectorX *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateAccountsReconTxs(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	var txKey [8]byte
	for it.HasNext() {
		key, txNum, progress := it.Next()
		binary.BigEndian.PutUint64(txKey[:], txNum)
		if err := accountCollectorX.Collect(key, txKey[:]); err != nil {
			panic(err)
		}
		atomic.StoreUint64(&fw.progress, progress)
		fw.bitmap.Add(txNum)
	}
}

func (fw *FillWorker) BitmapStorage(storageCollectorX *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateStorageReconTxs(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	var txKey [8]byte
	for it.HasNext() {
		key, txNum, progress := it.Next()
		binary.BigEndian.PutUint64(txKey[:], txNum)
		if err := storageCollectorX.Collect(key, txKey[:]); err != nil {
			panic(err)
		}
		atomic.StoreUint64(&fw.progress, progress)
		fw.bitmap.Add(txNum)
	}
}

func (fw *FillWorker) BitmapCode(codeCollectorX *etl.Collector) {
	defer func() {
		atomic.AddUint64(fw.doneCount, 1)
	}()
	it := fw.ac.IterateCodeReconTxs(fw.fromKey, fw.toKey, fw.txNum)
	atomic.StoreUint64(&fw.total, it.Total())
	var txKey [8]byte
	for it.HasNext() {
		key, txNum, progress := it.Next()
		binary.BigEndian.PutUint64(txKey[:], txNum)
		if err := codeCollectorX.Collect(key, txKey[:]); err != nil {
			panic(err)
		}
		atomic.StoreUint64(&fw.progress, progress)
		fw.bitmap.Add(txNum)
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
	lock         sync.Locker
	wg           *sync.WaitGroup
	rs           *state2.ReconState
	blockReader  services.FullBlockReader
	allSnapshots *snapshotsync.RoSnapshots
	stateWriter  *state2.StateReconWriter
	stateReader  *state2.HistoryReaderNoState
	getHeader    func(hash common.Hash, number uint64) *types.Header
	ctx          context.Context
	engine       consensus.Engine
	chainConfig  *params.ChainConfig
	logger       log.Logger
	genesis      *core.Genesis
	epoch        EpochReader
	chain        ChainReader
	isPoSA       bool
	posa         consensus.PoSA
}

func NewReconWorker(lock sync.Locker, wg *sync.WaitGroup, rs *state2.ReconState,
	a *state.Aggregator22, blockReader services.FullBlockReader, allSnapshots *snapshotsync.RoSnapshots,
	chainConfig *params.ChainConfig, logger log.Logger, genesis *core.Genesis, engine consensus.Engine,
	chainTx kv.Tx,
) *ReconWorker {
	ac := a.MakeContext()
	rw := &ReconWorker{
		lock:         lock,
		wg:           wg,
		rs:           rs,
		blockReader:  blockReader,
		allSnapshots: allSnapshots,
		ctx:          context.Background(),
		stateWriter:  state2.NewStateReconWriter(ac, rs),
		stateReader:  state2.NewHistoryReaderNoState(ac, rs),
		chainConfig:  chainConfig,
		logger:       logger,
		genesis:      genesis,
		engine:       engine,
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

func (rw *ReconWorker) runTxTask(txTask *state2.TxTask) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetError()
	rw.stateWriter.SetTxNum(txTask.TxNum)
	noop := state2.NewNoopWriter()
	rules := rw.chainConfig.Rules(txTask.BlockNum)
	ibs := state2.New(rw.stateReader)
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
			//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txNum, blockNum)
			// End of block transaction in a block
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine)
			}
			if _, _, err := rw.engine.Finalize(rw.chainConfig, txTask.Header, ibs, txTask.Block.Transactions(), txTask.Block.Uncles(), nil /* receipts */, rw.epoch, rw.chain, syscall); err != nil {
				panic(fmt.Errorf("finalize of block %d failed: %w", txTask.BlockNum, err))
			}
		}
	} else if txTask.TxIndex == -1 {
		// Block initialisation
		if rw.isPoSA {
			systemcontracts.UpgradeBuildInSystemContract(rw.chainConfig, txTask.Header.Number, ibs)
		}
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine)
		}
		rw.engine.Initialize(rw.chainConfig, rw.chain, rw.epoch, txTask.Header, txTask.Block.Transactions(), txTask.Block.Uncles(), syscall)
	} else {
		if rw.isPoSA {
			if isSystemTx, err := rw.posa.IsSystemTransaction(txTask.Tx, txTask.Header); err != nil {
				panic(err)
			} else if isSystemTx {
				return
			}
		}
		txHash := txTask.Tx.Hash()
		gp := new(core.GasPool).AddGas(txTask.Tx.GetGas())
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(rw.chainConfig, txTask.BlockNum)}
		getHashFn := core.GetHashFn(txTask.Header, rw.getHeader)
		blockContext := core.NewEVMBlockContext(txTask.Header, getHashFn, rw.engine, nil /* author */)
		ibs.Prepare(txHash, txTask.BlockHash, txTask.TxIndex)
		msg, err := txTask.Tx.AsMessage(*types.MakeSigner(rw.chainConfig, txTask.BlockNum), txTask.Header.BaseFee, rules)
		if err != nil {
			panic(err)
		}
		txContext := core.NewEVMTxContext(msg)
		vmenv := vm.NewEVM(blockContext, txContext, ibs, rw.chainConfig, vmConfig)
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d, evm=%p\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex, vmenv)
		_, err = core.ApplyMessage(vmenv, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			panic(fmt.Errorf("could not apply tx %d [%x] failed: %w", txTask.TxIndex, txHash, err))
		}
		if err = ibs.FinalizeTx(rules, noop); err != nil {
			panic(err)
		}
	}
	if dependency, ok := rw.stateReader.ReadError(); ok {
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
