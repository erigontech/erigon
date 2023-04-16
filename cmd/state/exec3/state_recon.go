package exec3

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type ScanWorker struct {
	txNum  uint64
	as     *libstate.AggregatorStep
	toKey  []byte
	bitmap roaring64.Bitmap
}

func NewScanWorker(txNum uint64, as *libstate.AggregatorStep) *ScanWorker {
	sw := &ScanWorker{
		txNum: txNum,
		as:    as,
	}
	return sw
}

type FillWorker struct {
	txNum uint64
	as    *libstate.AggregatorStep
}

func NewFillWorker(txNum uint64, as *libstate.AggregatorStep) *FillWorker {
	fw := &FillWorker{
		txNum: txNum,
		as:    as,
	}
	return fw
}

func (fw *FillWorker) FillAccounts(plainStateCollector *etl.Collector) error {
	it := fw.as.IterateAccountsHistory(fw.txNum)
	value := make([]byte, 1024)
	for it.HasNext() {
		key, val, err := it.Next()
		if err != nil {
			return err
		}
		if len(val) > 0 {
			var a accounts.Account
			//if err:=accounts.DeserialiseV3(&a, val);err!=nil {
			//	panic(err)
			//}
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
				return err
			}
			//fmt.Printf("Account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x}\n", key, &a.Balance, a.Nonce, a.Root, a.CodeHash)
		} else {
			if err := plainStateCollector.Collect(key, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (fw *FillWorker) FillStorage(plainStateCollector *etl.Collector) error {
	it := fw.as.IterateStorageHistory(fw.txNum)
	var compositeKey = make([]byte, length.Addr+length.Incarnation+length.Hash)
	binary.BigEndian.PutUint64(compositeKey[20:], state.FirstContractIncarnation)
	for it.HasNext() {
		key, val, err := it.Next()
		if err != nil {
			return err
		}
		copy(compositeKey[:20], key[:20])
		copy(compositeKey[20+8:], key[20:])
		if len(val) > 0 {
			if err := plainStateCollector.Collect(compositeKey, val); err != nil {
				return err
			}
			//fmt.Printf("Storage [%x] => [%x]\n", compositeKey, val)
		} else {
			if err := plainStateCollector.Collect(compositeKey, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (fw *FillWorker) FillCode(codeCollector, plainContractCollector *etl.Collector) error {
	it := fw.as.IterateCodeHistory(fw.txNum)
	var compositeKey = make([]byte, length.Addr+length.Incarnation)
	binary.BigEndian.PutUint64(compositeKey[length.Addr:], state.FirstContractIncarnation)

	for it.HasNext() {
		key, val, err := it.Next()
		if err != nil {
			return err
		}
		copy(compositeKey, key)
		if len(val) > 0 {

			codeHash, err := common.HashData(val)
			if err != nil {
				return err
			}
			if err = codeCollector.Collect(codeHash[:], val); err != nil {
				return err
			}
			if err = plainContractCollector.Collect(compositeKey, codeHash[:]); err != nil {
				return err
			}
			//fmt.Printf("Code [%x] => %d\n", compositeKey, len(val))
		} else {
			if err := plainContractCollector.Collect(compositeKey, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sw *ScanWorker) BitmapAccounts() error {
	it := sw.as.IterateAccountsTxs()
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return err
		}
		sw.bitmap.Add(v)
	}
	return nil
}

func (sw *ScanWorker) BitmapStorage() error {
	it := sw.as.IterateStorageTxs()
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return err
		}
		sw.bitmap.Add(v)
	}
	return nil
}

func (sw *ScanWorker) BitmapCode() error {
	it := sw.as.IterateCodeTxs()
	for it.HasNext() {
		v, err := it.Next()
		if err != nil {
			return err
		}
		sw.bitmap.Add(v)
	}
	return nil
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

func (sw *ScanWorker) Bitmap() *roaring64.Bitmap { return &sw.bitmap }

type ReconWorker struct {
	lock        sync.Locker
	rs          *state.ReconState
	blockReader services.FullBlockReader
	stateWriter *state.StateReconWriterInc
	stateReader *state.HistoryReaderInc
	ctx         context.Context
	engine      consensus.Engine
	chainConfig *chain.Config
	logger      log.Logger
	genesis     *types.Genesis
	chain       ChainReader

	evm *vm.EVM
	ibs *state.IntraBlockState
}

func NewReconWorker(lock sync.Locker, ctx context.Context, rs *state.ReconState,
	as *libstate.AggregatorStep, blockReader services.FullBlockReader,
	chainConfig *chain.Config, logger log.Logger, genesis *types.Genesis, engine consensus.Engine,
	chainTx kv.Tx,
) *ReconWorker {
	rw := &ReconWorker{
		lock:        lock,
		ctx:         ctx,
		rs:          rs,
		blockReader: blockReader,
		stateWriter: state.NewStateReconWriterInc(as, rs),
		stateReader: state.NewHistoryReaderInc(as, rs),
		chainConfig: chainConfig,
		logger:      logger,
		genesis:     genesis,
		engine:      engine,
		evm:         vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, chainConfig, vm.Config{}),
	}
	rw.chain = NewChainReader(chainConfig, chainTx, blockReader)
	rw.ibs = state.New(rw.stateReader)
	return rw
}

func (rw *ReconWorker) SetTx(tx kv.Tx) {
	rw.stateReader.SetTx(tx)
	rw.stateWriter.SetTx(tx)
}

func (rw *ReconWorker) SetChainTx(chainTx kv.Tx) {
	rw.stateReader.SetChainTx(chainTx)
	rw.stateWriter.SetChainTx(chainTx)
}

func (rw *ReconWorker) Run() error {
	for txTask, ok, err := rw.rs.Schedule(rw.ctx); ok || err != nil; txTask, ok, err = rw.rs.Schedule(rw.ctx) {
		if err != nil {
			return err
		}
		if err := rw.runTxTask(txTask); err != nil {
			return err
		}
	}
	return nil
}

var noop = state.NewNoopWriter()

func (rw *ReconWorker) runTxTask(txTask *exec22.TxTask) error {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.stateReader.SetTxNum(txTask.TxNum)
	rw.stateReader.ResetError()
	rw.stateWriter.SetTxNum(txTask.TxNum)
	rw.ibs.Reset()
	ibs := rw.ibs
	rules := txTask.Rules
	daoForkTx := rw.chainConfig.DAOForkBlock != nil && rw.chainConfig.DAOForkBlock.Uint64() == txTask.BlockNum && txTask.TxIndex == -1
	var err error
	if txTask.BlockNum == 0 && txTask.TxIndex == -1 {
		//fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txTask.TxNum, txTask.BlockNum)
		// Genesis block
		_, ibs, err = core.GenesisToBlock(rw.genesis, "")
		if err != nil {
			return err
		}
		// For Genesis, rules should be empty, so that empty accounts can be included
		rules = &chain.Rules{}
	} else if daoForkTx {
		//fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txNum, blockNum)
		misc.ApplyDAOHardFork(ibs)
		ibs.SoftFinalise()
	} else if txTask.Final {
		if txTask.BlockNum > 0 {
			//fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txTask.TxNum, txTask.BlockNum)
			// End of block transaction in a block
			syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine, false /* constCall */, nil /*excessDataGas*/)
			}
			if _, _, err := rw.engine.Finalize(rw.chainConfig, types.CopyHeader(txTask.Header), ibs, txTask.Txs, txTask.Uncles, nil, txTask.Withdrawals, rw.chain, syscall); err != nil {
				if _, readError := rw.stateReader.ReadError(); !readError {
					return fmt.Errorf("finalize of block %d failed: %w", txTask.BlockNum, err)
				}
			}
		}
	} else if txTask.TxIndex == -1 {
		// Block initialisation
		syscall := func(contract libcommon.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, *rw.chainConfig, ibs, txTask.Header, rw.engine, false /* constCall */, nil /*excessDataGas*/)
		}

		rw.engine.Initialize(rw.chainConfig, rw.chain, txTask.Header, ibs, txTask.Txs, txTask.Uncles, syscall)
	} else {
		gp := new(core.GasPool).AddGas(txTask.Tx.GetGas())
		vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: txTask.SkipAnalysis}
		ibs.Prepare(txTask.Tx.Hash(), txTask.BlockHash, txTask.TxIndex)
		msg := txTask.TxAsMessage

		rw.evm.ResetBetweenBlocks(txTask.EvmBlockContext, core.NewEVMTxContext(msg), ibs, vmConfig, txTask.Rules)
		vmenv := rw.evm
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txTask.TxNum, txTask.BlockNum, txTask.TxIndex)
		_, err = core.ApplyMessage(vmenv, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			if _, readError := rw.stateReader.ReadError(); !readError {
				return fmt.Errorf("could not apply blockNum=%d, txIdx=%d txNum=%d [%x] failed: %w", txTask.BlockNum, txTask.TxIndex, txTask.TxNum, txTask.Tx.Hash(), err)
			}
		}
		if err = ibs.FinalizeTx(rules, noop); err != nil {
			if _, readError := rw.stateReader.ReadError(); !readError {
				return err
			}
		}
	}
	if dependency, ok := rw.stateReader.ReadError(); ok || err != nil {
		//fmt.Printf("rollback %d\n", txNum)
		rw.rs.RollbackTx(txTask, dependency)
	} else {
		if err = ibs.CommitBlock(rules, rw.stateWriter); err != nil {
			return err
		}
		//fmt.Printf("commit %d\n", txNum)
		rw.rs.CommitTxNum(txTask.TxNum)
	}
	return nil
}
