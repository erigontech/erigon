package stateless

import (
	"bufio"
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

type TxTracer struct {
	gasForSSTORE         uint64
	gasForCREATE         uint64
	gasForEthSendingCALL uint64
	currentBlock         uint64
	sinceAccounts        map[common.Address]uint64
	sinceStorage         map[common.Address]map[common.Hash]uint64
	created              map[common.Address]struct{}
	lastAccessedAccounts map[common.Address]uint64
	lastAccessedStorage  map[common.Address]map[common.Hash]uint64
	measureCreate        bool
	measureDepth         int
	measureCurrentGas    uint64
	trace                bool
}

func NewTxTracer() *TxTracer {
	return &TxTracer{
		lastAccessedAccounts: make(map[common.Address]uint64),
		lastAccessedStorage:  make(map[common.Address]map[common.Hash]uint64),
	}
}

func (tt *TxTracer) ResetCounters() {
	tt.gasForSSTORE = 0
	tt.gasForCREATE = 0
	tt.gasForEthSendingCALL = 0
	tt.sinceAccounts = make(map[common.Address]uint64)
	tt.sinceStorage = make(map[common.Address]map[common.Hash]uint64)
	tt.created = make(map[common.Address]struct{})
}

func (tt *TxTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	tt.queryAccountAccess(to)
	return nil
}

func (tt *TxTracer) markAccountAccess(account common.Address) {
	tt.lastAccessedAccounts[account] = tt.currentBlock
}

func (tt *TxTracer) markStorageAccess(account common.Address, storageKey common.Hash) {
	if m, ok := tt.lastAccessedStorage[account]; ok {
		m[storageKey] = tt.currentBlock
	} else {
		m = make(map[common.Hash]uint64)
		tt.lastAccessedStorage[account] = m
		m[storageKey] = tt.currentBlock
	}
}

func (tt *TxTracer) queryAccountAccess(account common.Address) {
	var q uint64 = tt.currentBlock
	if blockNum, ok := tt.lastAccessedAccounts[account]; ok {
		q = tt.currentBlock - blockNum
	}
	tt.sinceAccounts[account] = q
}

func (tt *TxTracer) queryStorageAccess(account common.Address, storageKey common.Hash) {
	var q uint64 = tt.currentBlock
	if m, ok1 := tt.lastAccessedStorage[account]; ok1 {
		if blockNum, ok2 := m[storageKey]; ok2 {
			q = tt.currentBlock - blockNum
		}
	}
	if m, ok := tt.sinceStorage[account]; ok {
		m[storageKey] = q
	} else {
		m = make(map[common.Hash]uint64)
		tt.sinceStorage[account] = m
		m[storageKey] = q
	}
}

func (tt *TxTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, rData []byte, contract *vm.Contract, depth int, err error) error {
	if tt.measureCreate && tt.measureDepth+1 == depth {
		tt.gasForCREATE += (tt.measureCurrentGas - gas)
	}
	tt.measureCreate = false
	switch op {
	case vm.SSTORE:
		if cost <= gas {
			tt.gasForSSTORE += cost
		}
		if stack.Len() > 0 {
			tt.queryStorageAccess(contract.Address(), common.Hash(stack.Back(0).Bytes32()))
			tt.markStorageAccess(contract.Address(), common.Hash(stack.Back(0).Bytes32()))
		}
	case vm.SLOAD:
		if stack.Len() > 0 {
			tt.markStorageAccess(contract.Address(), common.Hash(stack.Back(0).Bytes32()))
		}
	case vm.CREATE, vm.CREATE2:
		tt.measureCurrentGas = gas
		tt.measureDepth = depth
		tt.measureCreate = true
	case vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL:
		var callGas uint64
		if stack.Len() > 0 && cost <= gas {
			callGas = stack.Back(0).Uint64()
			if callGas > gas {
				callGas = gas
			}
			tt.gasForEthSendingCALL += (cost - callGas)
		}
		if stack.Len() > 1 {
			tt.queryAccountAccess(common.Address(stack.Back(1).Bytes20()))
		}
	case vm.SELFDESTRUCT:
		if stack.Len() > 0 {
			tt.queryAccountAccess(common.Address(stack.Back(0).Bytes20()))
		}
	}
	return nil
}
func (tt *TxTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	return nil
}
func (tt *TxTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}
func (tt *TxTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	tt.markAccountAccess(creation)
	tt.created[creation] = struct{}{}
	return nil
}
func (tt *TxTracer) CaptureAccountRead(account common.Address) error {
	tt.markAccountAccess(account)
	return nil
}
func (tt *TxTracer) CaptureAccountWrite(account common.Address) error {
	tt.markAccountAccess(account)
	return nil
}

//nolint:deadcode,unused
func transactionStats(blockNum uint64) {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ethDb := ethdb.MustOpen("/home/akhounov/.ethereum/geth/chaindata")
	//ethDb := ethdb.MustOpen("/Volumes/tb41/turbo-geth/geth/chaindata")
	//ethDb := ethdb.MustOpen("/Users/alexeyakhunov/Library/Ethereum/geth/chaindata")
	defer ethDb.Close()
	ethTx, err1 := ethDb.KV().Begin(context.Background(), nil, ethdb.RO)
	check(err1)
	defer ethTx.Rollback()
	f, err := os.OpenFile("txs.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fa, err := os.OpenFile("txs_accounts.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer fa.Close()
	wa := bufio.NewWriter(fa)
	defer wa.Flush()
	fs, err := os.OpenFile("txs_storage.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer fs.Close()
	ws := bufio.NewWriter(fs)
	defer ws.Flush()
	fc, err := os.OpenFile("txs_created.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer fc.Close()
	wc := bufio.NewWriter(fc)
	defer wc.Flush()
	tt := NewTxTracer()
	chainConfig := params.MainnetChainConfig
	vmConfig := vm.Config{Tracer: tt, Debug: true}
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	bc, err := core.NewBlockChain(ethDb, nil, chainConfig, ethash.NewFaker(), vmConfig, nil, txCacher)
	check(err)
	defer bc.Stop()
	interrupt := false
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		dbstate := state.NewPlainDBState(ethTx, block.NumberU64()-1)
		statedb := state.New(dbstate)
		signer := types.MakeSigner(chainConfig, block.Number())
		for txIdx, tx := range block.Transactions() {
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			tt.ResetCounters()
			tt.currentBlock = blockNum
			tt.measureCreate = tx.To() == nil
			tt.measureDepth = 0
			tt.measureCurrentGas = tx.Gas()
			tt.trace = (blockNum == 1279578) && (txIdx == 3)
			if result, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			} else {
				usedGas := result.UsedGas
				var neededGas uint64
				usedGas += statedb.GetRefund()
				if usedGas > tt.gasForSSTORE+tt.gasForCREATE+tt.gasForEthSendingCALL {
					neededGas = usedGas - (tt.gasForSSTORE + tt.gasForCREATE + tt.gasForEthSendingCALL)
				}
				fmt.Fprintf(w, "%d,%d,%d,%d\n", blockNum, txIdx, len(tx.Data()), neededGas)
				//
				for account, since := range tt.sinceAccounts {
					precompile := true
					for i := 0; i < 19; i++ {
						if account[i] != 0 {
							precompile = false
							break
						}
					}
					if !precompile && since != 0 {
						fmt.Fprintf(wa, "%d,%d,%x,%d\n", blockNum, txIdx, account, since)
					}
				}
				//
				for account, m := range tt.sinceStorage {
					for storageKey, since := range m {
						if since != 0 {
							fmt.Fprintf(ws, "%d,%d,%x,%x,%d\n", blockNum, txIdx, account, storageKey, since)
						}
					}
				}
				//
				for account := range tt.created {
					size := len(statedb.GetCode(account))
					if size > 0 {
						fmt.Fprintf(wc, "%d,%d,%x,%d\n", blockNum, txIdx, account, size)
					}
				}
			}
		}
		blockNum++
		if blockNum%1000 == 0 {
			fmt.Printf("Processed %d blocks\n", blockNum)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}
	}
	fmt.Printf("Next time specify -block %d\n", blockNum)
}
