package stateless

import (
	"bufio"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

//const MaxUint = ^uint(0)
//const MaxUint64 = ^uint64(0)
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
	TxHash      *common.Hash
	Depth       int
	TxAddr      string
	CodeHash    *common.Hash
	From        common.Address
	To          common.Address
	Input       sliceBytes //ByteSliceAsHex
	Segments    sliceSegment
	Create      bool
	Fault       string //a fault set by CaptureEnd
	OpcodeFault string //a fault set by CaptureState
	Opcodes     sliceOpcodes
	CodeSize    int
	lastPc16    uint16
	lastOp      vm.OpCode
}

// types for slices are necessary for easyjson's generated un/marshalers
type sliceBytes []byte
type sliceOpcodes []opcode
type sliceSegment []segment
type sliceSegmentDump []segmentDump

//easyjson:json
type slicePtrTx []*tx

type opcodeTracer struct {
	Txs        slicePtrTx
	fsumWriter *bufio.Writer
	stack      slicePtrTx
	txsInDepth []int16

	saveOpcodes  bool
	saveSegments bool
	blockNumber  uint64
}

func NewOpcodeTracer(blockNum uint64, saveOpcodes bool, saveSegments bool) *opcodeTracer {
	res := new(opcodeTracer)
	res.txsInDepth = make([]int16, 1, 4)
	res.stack = make([]*tx, 0, 8)
	res.Txs = make([]*tx, 0, 64)
	res.saveOpcodes = saveOpcodes
	res.saveSegments = saveSegments
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

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

type segment struct {
	Start uint16
	End   uint16
}

type segmentDump struct {
	Tx          *common.Hash
	TxAddr      *string
	CodeHash    *common.Hash
	Segments    *sliceSegment
	OpcodeFault *string
	Fault       *string
	Create      bool
	CodeSize    int
}

type blockTxs struct {
	BlockNum uint64
	Txs      slicePtrTx
}

func (ot *opcodeTracer) CaptureStart(depth int, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) error {
	//fmt.Fprint(ot.summary, ot.lastLine)

	// When a CaptureStart is called, a Tx is starting. Create its entry in our list and initialize it with the partial data available
	//calculate the "address" of the Tx in its tree
	ltid := len(ot.txsInDepth)
	if ltid-1 != depth {
		panic(fmt.Sprintf("Wrong addr slice depth: d=%d, slice len=%d", depth, ltid))
	}

	ot.txsInDepth[depth]++
	ot.txsInDepth = append(ot.txsInDepth, 0)

	ls := len(ot.stack)
	txAddr := ""
	if ls > 0 {
		txAddr = ot.stack[ls-1].TxAddr + "-" + strconv.Itoa(int(ot.txsInDepth[depth])) // fmt.Sprintf("%s-%d", ot.stack[ls-1].TxAddr, ot.txsInDepth[depth])
	} else {
		txAddr = strconv.Itoa(int(ot.txsInDepth[depth]))
	}

	newTx := tx{From: from, To: to, Create: create, Input: input, Depth: depth, TxAddr: txAddr, lastOp: 0xfe, lastPc16: MaxUint16}
	ot.Txs = append(ot.Txs, &newTx)

	// take note in our own stack that the tx stack has grown
	ot.stack = append(ot.stack, &newTx)

	return nil
}

func (ot *opcodeTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	// When a CaptureEnd is called, a Tx has finished. Pop our stack
	ls := len(ot.stack)
	currentEntry := ot.stack[ls-1]
	ot.stack = ot.stack[:ls-1]
	ot.txsInDepth = ot.txsInDepth[:depth+1]

	// sanity check: depth of stack == depth reported by system
	if ls-1 != depth || depth != currentEntry.Depth {
		panic(fmt.Sprintf("End of Tx at d=%d but stack has d=%d and entry has d=%", depth, ls, currentEntry.Depth))
	}

	// Close the last segment
	if ot.saveSegments {
		lseg := len(currentEntry.Segments)
		if lseg > 0 {
			cee := currentEntry.Segments[lseg-1].End
			if cee != 0 && cee != currentEntry.lastPc16 {
				panic(fmt.Sprintf("CaptureEnd wanted to close last segment with %d but already contains %d", currentEntry.lastPc16, cee))
			}
			currentEntry.Segments[lseg-1].End = currentEntry.lastPc16
			//fmt.Fprintf(ot.fsumWriter,"Segment %d ends\n", lseg)
		}
	}

	errstr := ""
	if err != nil {
		errstr = err.Error()
		currentEntry.Fault = errstr
	}

	return nil
}

func (ot *opcodeTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, st *stack.Stack, retst *stack.ReturnStack, rData []byte, contract *vm.Contract, opDepth int, err error) error {
	//CaptureState sees the system as it is before the opcode is run. It seems to never get an error.

	//sanity check
	if pc > uint64(MaxUint16) {
		panic(fmt.Sprintf("PC is bigger than uint16! pc=%d=0x%x", pc, pc))
	}

	pc16 := uint16(pc)
	currentTxHash := env.TxHash
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
		currentEntry.TxHash = new(common.Hash)
		currentEntry.TxHash.SetBytes(currentTxHash.Bytes())
		currentEntry.CodeHash = new(common.Hash)
		currentEntry.CodeHash.SetBytes(contract.CodeHash.Bytes())
		currentEntry.CodeSize = len(contract.Code)
		if ot.saveOpcodes {
			currentEntry.Opcodes = make([]opcode, 0, 200)
		}
		//fmt.Fprintf(ot.w, "%sFilled in TxHash\n", strings.Repeat("\t",depth))

		if ot.saveSegments {
			currentEntry.Segments = make(sliceSegment, 0, 10)
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

	// deal with the RStack - is it used at all??
	lrs := len(retst.Data())
	var retStackTop []uint32
	if lrs > 0 {
		fmt.Fprintf(ot.fsumWriter, "RStack used in b=%d, tx=%s, txaddr=%s", ot.blockNumber, currentEntry.TxHash, currentEntry.TxAddr)
		//fmt.Printf("RStack used in b=%d, tx=%s, txaddr=%s", ot.blockNumber, currentEntry.TxHash, currentEntry.TxAddr)
		retStackTop = make([]uint32, lrs, lrs)
		copy(retStackTop, retst.Data())
	}

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

	// detect and store segments
	if ot.saveSegments {
		// PC discontinuities can only happen because of a PUSH (which is followed by the data to be pushed) or a JUMP (which lands into a JUMPDEST)
		// Therefore, after a PC discontinuity we either have op==JUMPDEST or lastOp==PUSH
		// Only the JUMPDEST case is a real control flow discontinuity and therefore starts a new segment

		lseg := len(currentEntry.Segments)
		isFirstSegment := lseg == 0
		isContinuous := pc16 == currentEntry.lastPc16+1 || currentEntry.lastOp.IsPush()
		if isFirstSegment || !isContinuous {
			// Record the end of the past segment, if there is one
			if !isFirstSegment {
				//fmt.Fprintf(ot.fsumWriter,"Segment %d ends\n", lseg)
				currentEntry.Segments[lseg-1].End = currentEntry.lastPc16
				//fmt.Printf("End\t%x\t%s\n", lastPc, lastOp.String())
			}
			// Start a new segment
			// Note that it can happen that a new segment starts with an opcode that triggers an Out Of Gas fault, so it'd be a segment with only 1 opcode (JUMPDEST)
			// The only case where we want to avoid creating a new segment is if the opcode is repeated, because then it was already in the previous segment
			if !faultAndRepeated {
				//fmt.Fprintf(ot.fsumWriter,"Segment %d begins\n", lseg+1)
				currentEntry.Segments = append(currentEntry.Segments, segment{Start: pc16})
				//fmt.Printf("Start\t%x\t%s\n", o.Pc.uint64, o.Op.String())

				//sanity check
				// we're starting a segment, so either we're in PC=0 or we have OP=JUMPDEST
				if pc16 != 0 && op.String() != "JUMPDEST" {
					panic(fmt.Sprintf("Bad segment? lastpc=%x, lastOp=%s; pc=%x, op=%s; bn=%d txaddr=%s tx=%d-%s",
						currentEntry.lastPc16, currentEntry.lastOp.String(), pc, op.String(), ot.blockNumber, currentEntry.TxAddr, currentEntry.Depth, currentEntry.TxHash.String()))
				}
			}
		}
	}

	currentEntry.lastPc16 = pc16
	currentEntry.lastOp = op
	return nil
}

func (ot *opcodeTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rst *stack.ReturnStack, contract *vm.Contract, opDepth int, err error) error {
	// CaptureFault sees the system as it is after the fault happens

	// CaptureState might have already recorded the opcode before it failed. Let's centralize the processing there.
	e := ot.CaptureState(env, pc, op, gas, cost, memory, stack, rst, nil, contract, opDepth, err)

	return e
}

func (ot *opcodeTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}
func (ot *opcodeTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ot *opcodeTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

func stackAsString(st *stack.Stack) (str string) {
	if l := st.Len(); l > 0 {
		str = fmt.Sprintf("%d:", l)
		for i := 0; i < l; i++ {
			str += fmt.Sprintf("%x ", st.Back(i))
		}
	}
	return str
}

type segPrefix struct {
	BlockNum uint64
	NumTxs   uint
}

// OpcodeTracer re-executes historical transactions in read-only mode
// and traces them at the opcode level
func OpcodeTracer(genesis *core.Genesis, blockNum uint64, chaindata string, numBlocks uint64,
	saveOpcodes bool, saveSegments bool) error {
	blockNumOrig := blockNum

	startTime := time.Now()
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	ot := NewOpcodeTracer(blockNum, saveOpcodes, saveSegments)

	chainDb := ethdb.MustOpen(chaindata)
	defer chainDb.Close()
	historyDb := chainDb
	historyTx, err1 := historyDb.KV().Begin(context.Background(), nil, ethdb.RO)
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	chainConfig := genesis.Config
	engine := ethash.NewFaker()
	vmConfig := vm.Config{Tracer: ot, Debug: true}
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	bc, err := core.NewBlockChain(chainDb, nil, chainConfig, engine, vmConfig, nil, txCacher)
	if err != nil {
		return err
	}
	defer bc.Stop()

	noOpWriter := state.NewNoopWriter()

	interrupt := false

	var fsum *os.File

	var chanOpcodes chan blockTxs
	if saveOpcodes {
		chanOpcodes = make(chan blockTxs, 10)
		defer close(chanOpcodes)

		go func() {
			var fops *os.File
			var fopsWriter *bufio.Writer
			var fopsEnc *gob.Encoder
			var err error

			for blockTxs := range chanOpcodes {
				bn := blockTxs.BlockNum
				bnStr := strconv.Itoa(int(bn))

				if fops != nil && bn%1000 == 0 {
					fopsWriter.Flush()
					fops.Close()
					fops = nil
				}

				if fops == nil {
					fops, err = os.Create("./opcodes-" + bnStr)
					check(err)
					fopsWriter = bufio.NewWriter(fops)
					fopsEnc = gob.NewEncoder(fopsWriter)
				}

				err = fopsEnc.Encode(blockTxs)
				check(err)

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

	var chanSegDump chan segmentDump
	var chanSegPrefix chan segPrefix
	if saveSegments {
		chanSegDump = make(chan segmentDump, 1024)
		defer close(chanSegDump)
		chanSegPrefix = make(chan segPrefix, 1024)
		defer close(chanSegPrefix)

		go func() {
			var f *os.File
			var fWriter *bufio.Writer
			var fwEnc *json.Encoder
			var err error

			for sp := range chanSegPrefix {
				bn := sp.BlockNum
				bnStr := strconv.Itoa(int(bn))

				if f != nil && bn%1000 == 0 {
					_, err = fWriter.WriteString("\n}")
					check(err)
					fWriter.Flush()
					f.Close()
					f = nil
				}

				if f == nil {
					f, err = os.Create("./segments-" + bnStr + ".json")
					check(err)
					fWriter = bufio.NewWriter(f)
					fwEnc = json.NewEncoder(fWriter)
				}

				_, err = fWriter.WriteString(",\n\"" + bnStr + "\":[\n")
				check(err)
				for i := uint(0); i < sp.NumTxs; i++ {
					if i != 0 {
						_, err = fWriter.WriteString(",")
						check(err)
					}
					sd := <-chanSegDump
					err = fwEnc.Encode(sd)
					check(err)
				}
				_, err = fWriter.WriteString("]")
				check(err)
			}

			if fWriter != nil {
				_, err = fWriter.WriteString("\n}")
				check(err)
				fWriter.Flush()
				f.Close()
				f = nil
			}

			lsp := len(chanSegPrefix)
			lsd := len(chanSegDump)
			if lsp > 0 || lsd > 0 {
				panic(fmt.Sprintf("Segment channels not empty at the end: sp=%d sd=%d", lsp, lsd))
			}
		}()
	}

	timeLastBlock := startTime
	blockNumLastReport := blockNum
	for !interrupt {
		block := bc.GetBlockByNumber(blockNum)
		if block == nil {
			break
		}
		bnStr := strconv.Itoa(int(blockNum))

		if fsum == nil {
			fsum, err = os.Create("./summary-" + bnStr)
			check(err)
			ot.fsumWriter = bufio.NewWriter(fsum)
		}

		dbstate := state.NewPlainDBState(historyTx, block.NumberU64()-1)
		intraBlockState := state.New(dbstate)
		intraBlockState.SetTracer(ot)

		receipts, err1 := runBlock(intraBlockState, noOpWriter, noOpWriter, chainConfig, bc, block, vmConfig)
		if err1 != nil {
			return err1
		}
		if chainConfig.IsByzantium(block.Number()) {
			receiptSha := types.DeriveSha(receipts)
			if receiptSha != block.Header().ReceiptHash {
				return fmt.Errorf("mismatched receipt headers for block %d", block.NumberU64())
			}
		}

		// go through the traces and act on them
		if saveSegments {
			sp := segPrefix{blockNum, uint(len(ot.Txs))}
			chanSegPrefix <- sp
		}
		chanSegmentsIsBlocking := false
		for i := range ot.Txs {
			t := ot.Txs[i]

			if saveSegments {
				sd := segmentDump{t.TxHash, &t.TxAddr, t.CodeHash, &t.Segments, &t.OpcodeFault, &t.Fault, t.Create, t.CodeSize}
				//fsegEnc.Encode(sd)
				chanSegmentsIsBlocking = len(chanSegDump) == cap(chanSegDump)-1
				chanSegDump <- sd
			}

			for j := range t.Opcodes {
				o := &t.Opcodes[j]
				//only print to the summary the opcodes that are interesting
				//isRStackUsed := o.MaxRStack != 0
				isOpFault := o.Fault != ""
				if isOpFault { // && !isRStackUsed {
					fmt.Fprintf(ot.fsumWriter, "Opcode FAULT\tb=%d taddr=%s TxF=%s opF=%s tx=%s\n", blockNum, t.TxAddr, t.Fault, t.OpcodeFault, t.TxHash.String())
					fmt.Fprint(ot.fsumWriter, "\n")

					//print the stack
					//if l := o.StackTop.Len(); l>0 {
					//	fmt.Fprintf(ot.summary, "\t%d:", o.MaxStack)
					//	for i := 0; i < l; i++ {
					//		fmt.Fprintf(ot.summary, "%x ", o.StackTop.Back(i))
					//	}
					//}

					//print the Rstack
					//if o.MaxRStack > 0 {
					//	fmt.Fprintf(ot.fsumWriter, "\trs:%d:", o.MaxRStack)
					//	//fmt.Printf("return stack used in block %d, tx %s", BlockNum)
					//	for i := 0; i < o.MaxRStack; i++ {
					//		fmt.Fprintf(ot.fsumWriter, "%x ", o.RetStackTop[i])
					//	}
					//}
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
		if chanSegmentsIsBlocking {
			log.Debug("Channel for segments got full and caused some blocking", "block", blockNum)
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
			check(err)
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
