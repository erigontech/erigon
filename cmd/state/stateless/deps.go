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

type DepTracer struct {
	accountsReadSet       map[common.Address]struct{}
	accountsWriteSet      map[common.Address]struct{}
	accountsWriteSetFrame map[common.Address]struct{}
	storageReadSet        map[common.Address]map[common.Hash]struct{}
	storageWriteSet       map[common.Address]map[common.Hash]struct{}
	storageWriteSetFrame  map[common.Address]map[common.Hash]struct{}
}

func NewDepTracer() *DepTracer {
	return &DepTracer{
		accountsReadSet:       make(map[common.Address]struct{}),
		accountsWriteSet:      make(map[common.Address]struct{}),
		accountsWriteSetFrame: make(map[common.Address]struct{}),
		storageReadSet:        make(map[common.Address]map[common.Hash]struct{}),
		storageWriteSet:       make(map[common.Address]map[common.Hash]struct{}),
		storageWriteSetFrame:  make(map[common.Address]map[common.Hash]struct{}),
	}
}

func (dt *DepTracer) CaptureStart(depth int, from common.Address, to common.Address, call bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}
func (dt *DepTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, _ []byte, contract *vm.Contract, depth int, err error) error {
	if op == vm.SSTORE {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.Hash(stack.Back(0).Bytes32())
		if smap, ok := dt.storageWriteSetFrame[addr]; ok {
			smap[loc] = struct{}{}
		} else {
			smapDest := make(map[common.Hash]struct{})
			smapDest[loc] = struct{}{}
			dt.storageWriteSetFrame[addr] = smapDest
		}
	} else if op == vm.SLOAD {
		addr := contract.Address()
		if stack.Len() == 0 {
			return nil
		}
		loc := common.Hash(stack.Back(0).Bytes32())
		if smap, ok := dt.storageReadSet[addr]; ok {
			smap[loc] = struct{}{}
		} else {
			smapDest := make(map[common.Hash]struct{})
			smapDest[loc] = struct{}{}
			dt.storageReadSet[addr] = smapDest
		}
	}
	return nil
}
func (dt *DepTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	dt.accountsWriteSetFrame = make(map[common.Address]struct{})
	dt.storageWriteSetFrame = make(map[common.Address]map[common.Hash]struct{})
	return nil
}
func (dt *DepTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	if err == nil {
		// Merge frame writes with the tx writes
		for addr := range dt.accountsWriteSetFrame {
			dt.accountsWriteSet[addr] = struct{}{}
		}
		for addr, smap := range dt.storageWriteSetFrame {
			if smapDest, ok := dt.storageWriteSet[addr]; ok {
				for loc := range smap {
					smapDest[loc] = struct{}{}
				}
			} else {
				dt.storageWriteSet[addr] = smap
			}
		}
	}
	dt.accountsWriteSetFrame = make(map[common.Address]struct{})
	dt.storageWriteSetFrame = make(map[common.Address]map[common.Hash]struct{})
	return nil
}
func (dt *DepTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (dt *DepTracer) CaptureAccountRead(account common.Address) error {
	dt.accountsReadSet[account] = struct{}{}
	return nil
}
func (dt *DepTracer) CaptureAccountWrite(account common.Address) error {
	dt.accountsWriteSetFrame[account] = struct{}{}
	return nil
}

//nolint:deadcode,unused
func dataDependencies(blockNum uint64) {
	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ethDb := ethdb.MustOpen("/Volumes/tb4/turbo-geth-10/geth/chaindata")
	defer ethDb.Close()
	ethTx, err1 := ethDb.KV().Begin(context.Background(), nil, false)
	check(err1)
	defer ethTx.Rollback()

	chainConfig := params.MainnetChainConfig
	depFile, err := os.OpenFile("/Volumes/tb4/turbo-geth/data_dependencies.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	check(err)
	defer depFile.Close()
	w := bufio.NewWriter(depFile)
	defer w.Flush()
	dt := NewDepTracer()
	vmConfig := vm.Config{Tracer: dt, Debug: true}
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
		statedb.SetTracer(dt)
		signer := types.MakeSigner(chainConfig, block.Number())
		var accountsReadSets []map[common.Address]struct{}
		var accountsWriteSets []map[common.Address]struct{}
		var storageReadSets []map[common.Address]map[common.Hash]struct{}
		var storageWriteSets []map[common.Address]map[common.Hash]struct{}
		for _, tx := range block.Transactions() {
			dt.accountsReadSet = make(map[common.Address]struct{})
			dt.accountsWriteSet = make(map[common.Address]struct{})
			dt.accountsWriteSetFrame = make(map[common.Address]struct{})
			dt.storageReadSet = make(map[common.Address]map[common.Hash]struct{})
			dt.storageWriteSet = make(map[common.Address]map[common.Hash]struct{})
			dt.storageWriteSetFrame = make(map[common.Address]map[common.Hash]struct{})
			// Assemble the transaction call message and return if the requested offset
			msg, _ := tx.AsMessage(signer)
			context := core.NewEVMContext(msg, block.Header(), bc, nil)
			// Not yet the searched for transaction, execute on top of the current state
			vmenv := vm.NewEVM(context, statedb, chainConfig, vmConfig)
			if result, err := core.ApplyOnlyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.Gas())); err != nil {
				panic(fmt.Errorf("tx %x failed: %v", tx.Hash(), err))
			} else {
				if result.Failed() {
					// Only consider reads
					accountsReadSets = append(accountsReadSets, dt.accountsReadSet)
					accountsWriteSets = append(accountsWriteSets, make(map[common.Address]struct{}))
					storageReadSets = append(storageReadSets, dt.storageReadSet)
					storageWriteSets = append(storageWriteSets, make(map[common.Address]map[common.Hash]struct{}))
				} else {
					accountsReadSets = append(accountsReadSets, dt.accountsReadSet)
					accountsWriteSets = append(accountsWriteSets, dt.accountsWriteSet)
					storageReadSets = append(storageReadSets, dt.storageReadSet)
					storageWriteSets = append(storageWriteSets, dt.storageWriteSet)
				}
			}
		}
		// Analyse data dependencies

		//fmt.Fprintf(w, "%d,%d,%d,%d,%d\n", blockNum, st.totalSstores, st.nakedSstores, st.totalSloads, st.nakedSloads)
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
	fmt.Printf("Processed %d blocks\n", blockNum)
	fmt.Printf("Next time specify -block %d\n", blockNum)
	fmt.Printf("Storage read/write analysis took %s\n", time.Since(startTime))
}
