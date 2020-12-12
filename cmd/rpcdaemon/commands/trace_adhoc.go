package commands

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

const (
	CALL   = "call"
	CREATE = "create"
)

// TraceCallParam (see SendTxArgs -- this allows optional prams plus don't use MixedcaseAddress
type TraceCallParam struct {
	From     *common.Address `json:"from"`
	To       *common.Address `json:"to"`
	Gas      *hexutil.Uint64 `json:"gas"`
	GasPrice *hexutil.Big    `json:"gasPrice"`
	Value    *hexutil.Big    `json:"value"`
	Data     hexutil.Bytes   `json:"data"`
}

// TraceCallParams array of callMany structs
type TraceCallParams []TraceCallParam

// TraceCallResult is the response to `trace_call` method
type TraceCallResult struct {
	Output    hexutil.Bytes       `json:"output"`
	StateDiff *TraceCallStateDiff `json:"stateDiff"`
	Trace     []*ParityTrace      `json:"trace"`
	VmTrace   *TraceCallVmTrace   `json:"vmTrace"`
}

// TraceCallStateDiff is the part of `trace_call` response that is under "stateDiff" tag
type TraceCallStateDiff struct {
}

// TraceCallVmTrace is the part of `trace_call` response that is under "vmTrace" tag
type TraceCallVmTrace struct {
}

// ToMessage converts CallArgs to the Message type used by the core evm
func (args *TraceCallParam) ToMessage(globalGasCap uint64) types.Message {
	// Set sender address or use zero address if none specified.
	var addr common.Address
	if args.From != nil {
		addr = *args.From
	}

	// Set default gas & gas price if none were set
	gas := globalGasCap
	if gas == 0 {
		gas = uint64(math.MaxUint64 / 2)
	}
	if args.Gas != nil {
		gas = uint64(*args.Gas)
	}
	if globalGasCap != 0 && globalGasCap < gas {
		log.Warn("Caller gas above allowance, capping", "requested", gas, "cap", globalGasCap)
		gas = globalGasCap
	}
	gasPrice := new(uint256.Int)
	if args.GasPrice != nil {
		gasPrice.SetFromBig(args.GasPrice.ToInt())
	}

	value := new(uint256.Int)
	if args.Value != nil {
		value.SetFromBig(args.Value.ToInt())
	}

	var input []byte
	if args.Data != nil {
		input = args.Data
	}

	msg := types.NewMessage(addr, args.To, 0, value, gas, gasPrice, input, false)
	return msg
}

// OpenEthereum-style tracer
type OeTracer struct {
	r          *TraceCallResult
	traceAddr  []int
	traceStack []*ParityTrace
}

func (ot *OeTracer) CaptureStart(depth int, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) error {
	if gas > 500000000 {
		gas = 500000001 - (0x8000000000000000 - gas)
	}
	fmt.Printf("CaptureStart depth %d, from %x, to %x, create %t, input %x, gas %d, value %d\n", depth, from, to, create, input, gas, value)
	trace := &ParityTrace{}
	if create {
		trace.Type = CREATE
	} else {
		trace.Type = CALL
	}
	if depth > 0 {
		topTrace := ot.traceStack[len(ot.traceStack)-1]
		traceIdx := topTrace.Subtraces
		ot.traceAddr = append(ot.traceAddr, traceIdx)
		topTrace.Subtraces++
	}
	trace.TraceAddress = make([]int, len(ot.traceAddr))
	copy(trace.TraceAddress, ot.traceAddr)
	if create {
		action := CreateTraceAction{}
		action.From = from
		action.Gas.ToInt().SetUint64(gas)
		action.Init = common.CopyBytes(input)
		action.Value.ToInt().Set(value)
		trace.Action = &action
	} else {
		action := CallTraceAction{}
		action.CallType = CALL
		action.Gas.ToInt().SetUint64(gas)
		action.Input = common.CopyBytes(input)
		action.Value.ToInt().Set(value)
		trace.Action = &action
	}
	ot.r.Trace = append(ot.r.Trace, trace)
	ot.traceStack = append(ot.traceStack, trace)
	return nil
}

func (ot *OeTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	fmt.Printf("CaptureEnd depth %d, output %x, gasUsed %d\n", depth, output, gasUsed)
	if depth == 0 {
		ot.r.Output = common.CopyBytes(output)
	}
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	topTrace.Result.Output = common.CopyBytes(output)
	topTrace.Result.GasUsed = new(hexutil.Big)
	topTrace.Result.GasUsed.ToInt().SetUint64(gasUsed)
	ot.traceStack = ot.traceStack[:len(ot.traceStack)-1]
	if depth > 0 {
		ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	}
	return nil
}

func (ot *OeTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, st *stack.Stack, retst *stack.ReturnStack, rData []byte, contract *vm.Contract, opDepth int, err error) error {
	return nil
}

func (ot *OeTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rst *stack.ReturnStack, contract *vm.Contract, opDepth int, err error) error {
	return nil
}

func (ot *OeTracer) CaptureCreate(creator common.Address, creation common.Address) error {
	//fmt.Printf("CaptureCreate creator %x, creation %x\n", creator, creation)
	return nil
}
func (ot *OeTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ot *OeTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

const callTimeout = 5 * time.Minute

// Call implements trace_call.
func (api *TraceAPIImpl) Call(ctx context.Context, args TraceCallParam, traceTypes []string, blockNrOrHash *rpc.BlockNumberOrHash) (*TraceCallResult, error) {
	dbtx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := getChainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	if blockNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		blockNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, err := rpchelper.GetBlockNumber(*blockNrOrHash, dbtx)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(dbtx)
	} else {
		stateReader = state.NewPlainDBState(dbtx, blockNumber)
	}
	state := state.New(stateReader)

	header := rawdb.ReadHeader(dbtx, hash, blockNumber)
	if header == nil {
		return nil, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}

	// Setup context so it may be cancelled the call has completed
	// or, in case of unmetered gas, setup a context with a timeout.
	var cancel context.CancelFunc
	if callTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, callTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Make sure the context is cancelled when the call has completed
	// this makes sure resources are cleaned up.
	defer cancel()

	var ot OeTracer
	ot.r = &TraceCallResult{}
	ot.traceAddr = []int{}

	// Get a new instance of the EVM.
	msg := args.ToMessage(api.gasCap)

	evmCtx := transactions.GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, dbtx)

	evm := vm.NewEVM(evmCtx, state, chainConfig, vm.Config{Debug: true, Tracer: &ot})

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas())
	_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */)
	if err != nil {
		return nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", callTimeout)
	}

	return ot.r, nil
}

// TODO(tjayrush) - try to use a concrete type here
// TraceCallManyParam array of callMany structs
// type TraceCallManyParam struct {
// 	obj        TraceCallParam
// 	traceTypes []string
// }

// TraceCallManyParams array of callMany structs
// type TraceCallManyParams struct {
// 	things []TraceCallManyParam
// }

// CallMany implements trace_callMany.
func (api *TraceAPIImpl) CallMany(ctx context.Context, calls []interface{}, blockNr *rpc.BlockNumberOrHash) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_callMany")
}

// RawTransaction implements trace_rawTransaction.
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_rawTransaction")
}

// ReplayBlockTransactions implements trace_replayBlockTransactions.
func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, blockNr rpc.BlockNumber, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_replayBlockTransactions")
}

// ReplayTransaction implements trace_replayTransaction.
func (api *TraceAPIImpl) ReplayTransaction(ctx context.Context, txHash common.Hash, traceTypes []string) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "trace_replayTransaction")
}
