package commands

import (
	"bufio"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/holiman/uint256"
	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
)

var (
	numBlocks   uint64
	saveOpcodes bool
	saveBBlocks bool
)

func init() {
	withBlock(opcodeTracerCmd)
	withDataDir(opcodeTracerCmd)
	opcodeTracerCmd.Flags().Uint64Var(&numBlocks, "numBlocks", 1, "number of blocks to run the operation on")
	opcodeTracerCmd.Flags().BoolVar(&saveOpcodes, "saveOpcodes", false, "set to save the opcodes")
	opcodeTracerCmd.Flags().BoolVar(&saveBBlocks, "saveBBlocks", false, "set to save the basic blocks")

	rootCmd.AddCommand(opcodeTracerCmd)
}

var opcodeTracerCmd = &cobra.Command{
	Use:   "opcodeTracer",
	Short: "Re-executes historical transactions in read-only mode and traces them at the opcode level",
	RunE: func(cmd *cobra.Command, args []string) error {
		return OpcodeTracer(genesis, block, chaindata, numBlocks, saveOpcodes, saveBBlocks)
	},
}

// const MaxUint = ^uint(0)
// const MaxUint64 = ^uint64(0)
const MaxUint16 = ^uint16(0)

type opcode struct {
	Pc uint16
	Op vm.OpCode
	//StackTop 		*stack.Stack
	//StackTop 		[]uint256.Int
	//RetStackTop		RetStackTop
	//MaxStack 		int
	//MaxRStack 		int
	Fault string
}

type RetStackTop []uint32

type tx struct {
	From        libcommon.Address
	To          libcommon.Address
	TxHash      *libcommon.Hash
	CodeHash    *libcommon.Hash
	Opcodes     sliceOpcodes
	Input       sliceBytes //ByteSliceAsHex
	Bblocks     sliceBblocks
	Fault       string //a fault set by CaptureEnd
	OpcodeFault string //a fault set by CaptureState
	TxAddr      string
	CodeSize    int
	Depth       int
	lastPc16    uint16
	lastOp      vm.OpCode
	Create      bool
}

// types for slices are necessary for easyjson's generated un/marshalers
type sliceBytes []byte
type sliceOpcodes []opcode
type sliceBblocks []bblock

//easyjson:json
type slicePtrTx []*tx

type opcodeTracer struct {
	Txs        slicePtrTx
	fsumWriter *bufio.Writer
	stack      slicePtrTx
	txsInDepth []int16

	saveOpcodes bool
	saveBblocks bool
	blockNumber uint64
	depth       int
	env         vm.VMInterface
}

func NewOpcodeTracer(blockNum uint64, saveOpcodes bool, saveBblocks bool) *opcodeTracer {
	res := new(opcodeTracer)
	res.txsInDepth = make([]int16, 1, 4)
	res.stack = make([]*tx, 0, 8)
	res.Txs = make([]*tx, 0, 64)
	res.saveOpcodes = saveOpcodes
	res.saveBblocks = saveBblocks
	res.blockNumber = blockNum
	return res
}

// prepare to trace a new block
func resetOpcodeTracer(ot *opcodeTracer) {
	//sanity check
	// at the end of a block, when no transactions are running, depth == 0. Our tracking should reflect that.
	if len(ot.txsInDepth) != 1 || len(ot.stack) != 0 {
		panic(fmt.Sprintf("At end of block, tracer should be almost reset but isn't: lstack=%d, lTID=%d, TID[0]=%d",
			len(ot.stack), len(ot.txsInDepth), ot.txsInDepth[0]))
	}
	// allocate new storage, allow past storage to be GCed
	ot.Txs = make([]*tx, 0, 64)
	// reset the counter of Txs at depth 0
	ot.txsInDepth[0] = 0
}

type bblock struct {
	Start uint16
	End   uint16
}

type bblockDump struct {
	Tx          *libcommon.Hash
	TxAddr      *string
	CodeHash    *libcommon.Hash
	Bblocks     *sliceBblocks
	OpcodeFault *string
	Fault       *string
	Create      bool
	CodeSize    int
}

type blockTxs struct {
	BlockNum uint64
	Txs      slicePtrTx
}

func (ot *opcodeTracer) CaptureTxStart(gasLimit uint64) {}

func (ot *opcodeTracer) CaptureTxEnd(restGas uint64) {}

func (ot *opcodeTracer) captureStartOrEnter(from, to libcommon.Address, create bool, input []byte) {
	//fmt.Fprint(ot.summary, ot.lastLine)

	// When a CaptureStart is called, a Tx is starting. Create its entry in our list and initialize it with the partial data available
	//calculate the "address" of the Tx in its tree
	ltid := len(ot.txsInDepth)
	if ltid-1 != ot.depth {
		panic(fmt.Sprintf("Wrong addr slice depth: d=%d, slice len=%d", ot.depth, ltid))
	}

	ot.txsInDepth[ot.depth]++
	ot.txsInDepth = append(ot.txsInDepth, 0)

	ls := len(ot.stack)
	txAddr := ""
	if ls > 0 {
		txAddr = ot.stack[ls-1].TxAddr + "-" + strconv.Itoa(int(ot.txsInDepth[ot.depth])) // fmt.Sprintf("%s-%d", ot.stack[ls-1].TxAddr, ot.txsInDepth[depth])
	} else {
		txAddr = strconv.Itoa(int(ot.txsInDepth[ot.depth]))
	}

	newTx := tx{From: from, To: to, Create: create, Input: input, Depth: ot.depth, TxAddr: txAddr, lastOp: 0xfe, lastPc16: MaxUint16}
	ot.Txs = append(ot.Txs, &newTx)

	// take note in our own stack that the tx stack has grown
	ot.stack = append(ot.stack, &newTx)
}

func (ot *opcodeTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ot.env = env
	ot.depth = 0
	ot.captureStartOrEnter(from, to, create, input)
}

func (ot *opcodeTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	ot.depth++
	ot.captureStartOrEnter(from, to, create, input)
}

func (ot *opcodeTracer) captureEndOrExit(err error) {
	// When a CaptureEnd is called, a Tx has finished. Pop our stack
	ls := len(ot.stack)
	currentEntry := ot.stack[ls-1]
	ot.stack = ot.stack[:ls-1]
	ot.txsInDepth = ot.txsInDepth[:ot.depth+1]

	// sanity check: depth of stack == depth reported by system
	if ls-1 != ot.depth || ot.depth != currentEntry.Depth {
		panic(fmt.Sprintf("End of Tx at d=%d but stack has d=%d and entry has d=%d", ot.depth, ls, currentEntry.Depth))
	}

	// Close the last bblock
	if ot.saveBblocks {
		lseg := len(currentEntry.Bblocks)
		if lseg > 0 {
			cee := currentEntry.Bblocks[lseg-1].End
			if cee != 0 && cee != currentEntry.lastPc16 {
				panic(fmt.Sprintf("CaptureEnd wanted to close last bblock with %d but already contains %d", currentEntry.lastPc16, cee))
			}
			currentEntry.Bblocks[lseg-1].End = currentEntry.lastPc16
			//fmt.Fprintf(ot.fsumWriter,"Bblock %d ends\n", lseg)
		}
	}

	errstr := ""
	if err != nil {
		errstr = err.Error()
		currentEntry.Fault = errstr
	}
}

func (ot *opcodeTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	ot.captureEndOrExit(err)
}

func (ot *opcodeTracer) CaptureExit(output []byte, usedGas uint64, err error) {
	ot.captureEndOrExit(err)
	ot.depth--
}

func (ot *opcodeTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, opDepth int, err error) {
	//CaptureState sees the system as it is before the opcode is run. It seems to never get an error.
	contract := scope.Contract

	//sanity check
	if pc > uint64(MaxUint16) {
		panic(fmt.Sprintf("PC is bigger than uint16! pc=%d=0x%x", pc, pc))
	}

	pc16 := uint16(pc)
	currentTxHash := ot.env.TxContext().TxHash
	currentTxDepth := opDepth - 1

	ls := len(ot.stack)
	currentEntry := ot.stack[ls-1]

	//sanity check
	if currentEntry.Depth != currentTxDepth {
		panic(fmt.Sprintf("Depth should be the same but isn't: current tx's %d, current entry's %d", currentTxDepth, currentEntry.Depth))
	}

	// is the Tx entry still not fully initialized?
	if currentEntry.TxHash == nil {
		// CaptureStart creates the entry for a new Tx, but doesn't have access to EVM data, like the Tx Hash
		// here we ASSUME that the tx entry was recently created by CaptureStart
		// AND that this is the first CaptureState that has happened since then
		// AND that both Captures are for the same transaction
		// AND that we can't go into another depth without executing at least 1 opcode
		// Note that the only connection between CaptureStart and CaptureState that we can notice is that the current op's depth should be lastTxEntry.Depth+1

		// fill in the missing data in the entry
		currentEntry.TxHash = new(libcommon.Hash)
		currentEntry.TxHash.SetBytes(currentTxHash.Bytes())
		currentEntry.CodeHash = new(libcommon.Hash)
		currentEntry.CodeHash.SetBytes(contract.CodeHash.Bytes())
		currentEntry.CodeSize = len(contract.Code)
		if ot.saveOpcodes {
			currentEntry.Opcodes = make([]opcode, 0, 200)
		}
		//fmt.Fprintf(ot.w, "%sFilled in TxHash\n", strings.Repeat("\t",depth))

		if ot.saveBblocks {
			currentEntry.Bblocks = make(sliceBblocks, 0, 10)
		}
	}

	// prepare the opcode's stack for saving
	//stackTop := &stack.Stack{Data: make([]uint256.Int, 0, 7)}//stack.New()
	// the most stack positions consumed by any opcode is 7
	//for i:= min(7, st.Len()-1); i>=0; i-- {
	//	stackTop.Push(st.Back(i))
	//}
	//THIS VERSION SHOULD BE FASTER BUT IS UNTESTED
	//stackTop := make([]uint256.Int, 7, 7)
	//sl := st.Len()
	//minl := min(7, sl)
	//startcopy := sl-minl
	//stackTop := &stack.Stack{Data: make([]uint256.Int, minl, minl)}//stack.New()
	//copy(stackTop.Data, st.Data[startcopy:sl])

	//sanity check
	if currentEntry.OpcodeFault != "" {
		panic(fmt.Sprintf("Running opcodes but fault is already set. txFault=%s, opFault=%v, op=%s",
			currentEntry.OpcodeFault, err, op.String()))
	}

	// if it is a Fault, check whether we already have a record of the opcode. If so, just add the flag to it
	errstr := ""
	if err != nil {
		errstr = err.Error()
		currentEntry.OpcodeFault = errstr
	}

	faultAndRepeated := false

	if pc16 == currentEntry.lastPc16 && op == currentEntry.lastOp {
		//it's a repeated opcode. We assume this only happens when it's a Fault.
		if err == nil {
			panic(fmt.Sprintf("Duplicate opcode with no fault. bn=%d txaddr=%s pc=%x op=%s",
				ot.blockNumber, currentEntry.TxAddr, pc, op.String()))
		}
		faultAndRepeated = true
		//ot.fsumWriter.WriteString("Fault for EXISTING opcode\n")
		//ot.fsumWriter.Flush()
		if ot.saveOpcodes {
			lo := len(currentEntry.Opcodes)
			currentEntry.Opcodes[lo-1].Fault = errstr
		}
	} else {
		// it's a new opcode
		if ot.saveOpcodes {
			newOpcode := opcode{pc16, op, errstr}
			currentEntry.Opcodes = append(currentEntry.Opcodes, newOpcode)
		}
	}

	// detect and store bblocks
	if ot.saveBblocks {
		// PC discontinuities can only happen because of a PUSH (which is followed by the data to be pushed) or a JUMP (which lands into a JUMPDEST)
		// Therefore, after a PC discontinuity we either have op==JUMPDEST or lastOp==PUSH
		// Only the JUMPDEST case is a real control flow discontinuity and therefore starts a new bblock

		lseg := len(currentEntry.Bblocks)
		isFirstBblock := lseg == 0
		isContinuous := pc16 == currentEntry.lastPc16+1 || currentEntry.lastOp.IsPush()
		if isFirstBblock || !isContinuous {
			// Record the end of the past bblock, if there is one
			if !isFirstBblock {
				//fmt.Fprintf(ot.fsumWriter,"Bblock %d ends\n", lseg)
				currentEntry.Bblocks[lseg-1].End = currentEntry.lastPc16
				//fmt.Printf("End\t%x\t%s\n", lastPc, lastOp.String())
			}
			// Start a new bblock
			// Note that it can happen that a new bblock starts with an opcode that triggers an Out Of Gas fault, so it'd be a bblock with only 1 opcode (JUMPDEST)
			// The only case where we want to avoid creating a new bblock is if the opcode is repeated, because then it was already in the previous bblock
			if !faultAndRepeated {
				//fmt.Fprintf(ot.fsumWriter,"Bblock %d begins\n", lseg+1)
				currentEntry.Bblocks = append(currentEntry.Bblocks, bblock{Start: pc16})
				//fmt.Printf("Start\t%x\t%s\n", o.Pc.uint64, o.Op.String())

				//sanity check
				// we're starting a bblock, so either we're in PC=0 or we have OP=JUMPDEST
				if pc16 != 0 && op.String() != "JUMPDEST" {
					panic(fmt.Sprintf("Bad bblock? lastpc=%x, lastOp=%s; pc=%x, op=%s; bn=%d txaddr=%s tx=%d-%s",
						currentEntry.lastPc16, currentEntry.lastOp.String(), pc, op.String(), ot.blockNumber, currentEntry.TxAddr, currentEntry.Depth, currentEntry.TxHash.String()))
				}
			}
		}
	}

	currentEntry.lastPc16 = pc16
	currentEntry.lastOp = op
}

func (ot *opcodeTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, opDepth int, err error) {
	// CaptureFault sees the system as it is after the fault happens

	// CaptureState might have already recorded the opcode before it failed. Let's centralize the processing there.
	ot.CaptureState(pc, op, gas, cost, scope, nil, opDepth, err)

}

type segPrefix struct {
	BlockNum uint64
	NumTxs   uint
}

// OpcodeTracer re-executes historical transactions in read-only mode
// and traces them at the opcode level
func OpcodeTracer(genesis *core.Genesis, blockNum uint64, chaindata string, numBlocks uint64,
	saveOpcodes bool, saveBblocks bool) error {
	blockNumOrig := blockNum

	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ot := NewOpcodeTracer(blockNum, saveOpcodes, saveBblocks)

	chainDb := mdbx.MustOpen(chaindata)
	defer chainDb.Close()
	historyDb := chainDb
	historyTx, err1 := historyDb.BeginRo(context.Background())
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	chainConfig := genesis.Config
	vmConfig := vm.Config{Tracer: ot, Debug: true}

	noOpWriter := state.NewNoopWriter()

	interrupt := false

	var fsum *os.File

	var chanOpcodes chan blockTxs
	if saveOpcodes {
		chanOpcodes = make(chan blockTxs, 10)
		defer close(chanOpcodes)

		go func() {
			defer debug.LogPanic()
			var fops *os.File
			var fopsWriter *bufio.Writer
			var fopsEnc *gob.Encoder
			var e error

			for blockTxs := range chanOpcodes {
				bn := blockTxs.BlockNum
				bnStr := strconv.Itoa(int(bn))

				if fops != nil && bn%1000 == 0 {
					fopsWriter.Flush()
					fops.Close()
					fops = nil
				}

				if fops == nil {
					if fops, e = os.Create("./opcodes-" + bnStr); e != nil {
						panic(e)
					}
					fopsWriter = bufio.NewWriter(fops)
					fopsEnc = gob.NewEncoder(fopsWriter)
				}

				if e = fopsEnc.Encode(blockTxs); e != nil {
					panic(e)
				}

			}

			if fopsWriter != nil {
				fopsWriter.Flush()
				fops.Close()
				fops = nil
			}

			lo := len(chanOpcodes)
			if lo > 0 {
				panic(fmt.Sprintf("Opcode channel not empty at the end: lo=%d", lo))
			}

		}()
	}

	var chanSegDump chan bblockDump
	var chanSegPrefix chan segPrefix
	if saveBblocks {
		chanSegDump = make(chan bblockDump, 1024)
		defer close(chanSegDump)
		chanSegPrefix = make(chan segPrefix, 1024)
		defer close(chanSegPrefix)

		go func() {
			var f *os.File
			var fWriter *bufio.Writer
			var fwEnc *json.Encoder
			var e error

			for sp := range chanSegPrefix {
				bn := sp.BlockNum
				bnStr := strconv.Itoa(int(bn))

				if f != nil && bn%1000 == 0 {
					if _, e = fWriter.WriteString("\n}"); e != nil {
						panic(e)
					}
					fWriter.Flush()
					f.Close()
					f = nil
				}

				if f == nil {
					if f, e = os.Create("./bblocks-" + bnStr + ".json"); e != nil {
						panic(e)
					}
					fWriter = bufio.NewWriter(f)
					fwEnc = json.NewEncoder(fWriter)
				}

				if _, e = fWriter.WriteString(",\n\"" + bnStr + "\":[\n"); e != nil {
					panic(e)
				}
				for i := uint(0); i < sp.NumTxs; i++ {
					if i != 0 {
						if _, e = fWriter.WriteString(","); e != nil {
							panic(e)
						}
					}
					sd := <-chanSegDump
					if e = fwEnc.Encode(sd); e != nil {
						panic(e)
					}
				}
				if _, e = fWriter.WriteString("]"); e != nil {
					panic(e)
				}
			}

			if fWriter != nil {
				if _, e = fWriter.WriteString("\n}"); e != nil {
					panic(e)
				}
				fWriter.Flush()
				f.Close()
				f = nil
			}

			lsp := len(chanSegPrefix)
			lsd := len(chanSegDump)
			if lsp > 0 || lsd > 0 {
				panic(fmt.Sprintf("Bblock channels not empty at the end: sp=%d sd=%d", lsp, lsd))
			}
		}()
	}

	timeLastBlock := startTime
	blockNumLastReport := blockNum

	var historyV3 bool
	chainDb.View(context.Background(), func(tx kv.Tx) (err error) {
		historyV3, err = kvcfg.HistoryV3.Enabled(tx)
		if err != nil {
			return err
		}
		return nil
	})

	for !interrupt {
		var block *types.Block
		if err := chainDb.View(context.Background(), func(tx kv.Tx) (err error) {
			block, err = rawdb.ReadBlockByNumber(tx, blockNum)
			return err
		}); err != nil {
			panic(err)
		}
		if block == nil {
			break
		}
		bnStr := strconv.Itoa(int(blockNum))

		if fsum == nil {
			var err error
			if fsum, err = os.Create("./summary-" + bnStr); err != nil {
				return err
			}

			ot.fsumWriter = bufio.NewWriter(fsum)
		}

		dbstate, err := rpchelper.CreateHistoryStateReader(historyTx, block.NumberU64(), 0, historyV3, chainConfig.ChainName)
		if err != nil {
			return err
		}
		intraBlockState := state.New(dbstate)

		getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
			return rawdb.ReadHeader(historyTx, hash, number)
		}
		receipts, err1 := runBlock(ethash.NewFullFaker(), intraBlockState, noOpWriter, noOpWriter, chainConfig, getHeader, block, vmConfig, false)
		if err1 != nil {
			return err1
		}
		if chainConfig.IsByzantium(block.NumberU64()) {
			receiptSha := types.DeriveSha(receipts)
			if receiptSha != block.ReceiptHash() {
				return fmt.Errorf("mismatched receipt headers for block %d", block.NumberU64())
			}
		}

		// go through the traces and act on them
		if saveBblocks {
			sp := segPrefix{blockNum, uint(len(ot.Txs))}
			chanSegPrefix <- sp
		}
		chanBblocksIsBlocking := false
		for i := range ot.Txs {
			t := ot.Txs[i]

			if saveBblocks {
				sd := bblockDump{t.TxHash, &t.TxAddr, t.CodeHash, &t.Bblocks, &t.OpcodeFault, &t.Fault, t.Create, t.CodeSize}
				//fsegEnc.Encode(sd)
				chanBblocksIsBlocking = len(chanSegDump) == cap(chanSegDump)-1
				chanSegDump <- sd
			}

			for j := range t.Opcodes {
				o := &t.Opcodes[j]
				//only print to the summary the opcodes that are interesting
				isOpFault := o.Fault != ""
				if isOpFault {
					fmt.Fprintf(ot.fsumWriter, "Opcode FAULT\tb=%d taddr=%s TxF=%s opF=%s tx=%s\n", blockNum, t.TxAddr, t.Fault, t.OpcodeFault, t.TxHash.String())
					fmt.Fprint(ot.fsumWriter, "\n")
				}
			}
			isTxFault := t.Fault != ""
			if !isTxFault {
				continue
			}
			if t.OpcodeFault == t.Fault {
				continue
			}
			if t.Fault == "out of gas" {
				// frequent and uninteresting
				continue
			}
			ths := ""
			if t.TxHash != nil {
				ths = t.TxHash.String()
			}
			fmt.Fprintf(ot.fsumWriter, "Tx FAULT\tb=%d opF=%s\tTxF=%s\ttaddr=%s\ttx=%s\n", blockNum, t.OpcodeFault, t.Fault, t.TxAddr, ths)
		}
		if chanBblocksIsBlocking {
			log.Debug("Channel for bblocks got full and caused some blocking", "block", blockNum)
		}

		if saveOpcodes {
			// just save everything
			bt := blockTxs{blockNum, ot.Txs}
			chanOpcodesIsBlocking := len(chanOpcodes) == cap(chanOpcodes)-1
			chanOpcodes <- bt
			if chanOpcodesIsBlocking {
				log.Debug("Channel for opcodes got full and caused some blocking", "block", blockNum)
			}
		}

		blockNum++
		resetOpcodeTracer(ot)
		ot.blockNumber = blockNum

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Println("interrupted, please wait for cleanup...")
		default:
		}

		if blockNum >= blockNumOrig+numBlocks {
			interrupt = true
		}

		if interrupt || blockNum%1000 == 0 {
			bps := float64(blockNum-blockNumLastReport) / time.Since(timeLastBlock).Seconds()
			timeLastBlock = time.Now()
			blockNumLastReport = blockNum
			bpss := fmt.Sprintf("%.2f", bps)
			log.Info("Checked", "blocks", blockNum, "blocks/s", bpss)

			ot.fsumWriter.Flush()
			fi, err := fsum.Stat()
			if err != nil {
				return err
			}
			// if the summary file for the just-finished range of blocks is empty, delete it
			if fi.Size() == 0 {
				os.Remove(fi.Name())
			}
			fsum.Close()
			fsum = nil
		}
	}

	bps := float64(blockNum-blockNumOrig) / time.Since(startTime).Seconds()
	bpss := fmt.Sprintf("%.2f", bps)
	log.Info("Checked", "blocks", blockNum, "next time specify --block", blockNum, "duration", time.Since(startTime), "blocks/s", bpss)

	return nil
}

func runBlock(engine consensus.Engine, ibs *state.IntraBlockState, txnWriter state.StateWriter, blockWriter state.StateWriter,
	chainConfig *chain2.Config, getHeader func(hash libcommon.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config, trace bool) (types.Receipts, error) {
	header := block.Header()
	vmConfig.TraceJumpDest = true
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	if chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	systemcontracts.UpgradeBuildInSystemContract(chainConfig, header.Number, ibs)
	rules := chainConfig.Rules(block.NumberU64(), block.Time())
	for i, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, txnWriter, header, tx, usedGas, vmConfig)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		if trace {
			fmt.Printf("tx idx %d, gas used %d\n", i, receipt.GasUsed)
		}
		receipts = append(receipts, receipt)
	}

	if !vmConfig.ReadOnly {
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		tx := block.Transactions()
		if _, _, _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, tx, block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil); err != nil {
			return nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
		}

		if err := ibs.CommitBlock(rules, blockWriter); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
		}
	}

	return receipts, nil
}
