package commands

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

const (
	CALL               = "call"
	CALLCODE           = "callcode"
	DELEGATECALL       = "delegatecall"
	STATICCALL         = "staticcall"
	CREATE             = "create"
	SUICIDE            = "suicide"
	TraceTypeTrace     = "trace"
	TraceTypeStateDiff = "stateDiff"
	TraceTypeVmTrace   = "vmTrace"
)

// TraceCallParam (see SendTxArgs -- this allows optional prams plus don't use MixedcaseAddress
type TraceCallParam struct {
	From       *common.Address   `json:"from"`
	To         *common.Address   `json:"to"`
	Gas        *hexutil.Uint64   `json:"gas"`
	GasPrice   *hexutil.Big      `json:"gasPrice"`
	Tip        *hexutil.Big      `json:"tip"`
	FeeCap     *hexutil.Big      `json:"feeCap"`
	Value      *hexutil.Big      `json:"value"`
	Data       hexutil.Bytes     `json:"data"`
	AccessList *types.AccessList `json:"accessList"`
}

// TraceCallResult is the response to `trace_call` method
type TraceCallResult struct {
	Output    hexutil.Bytes                        `json:"output"`
	StateDiff map[common.Address]*StateDiffAccount `json:"stateDiff"`
	Trace     []*ParityTrace                       `json:"trace"`
	VmTrace   *TraceCallVmTrace                    `json:"vmTrace"`
}

// StateDiffAccount is the part of `trace_call` response that is under "stateDiff" tag
type StateDiffAccount struct {
	Balance interface{}                            `json:"balance"` // Can be either string "=" or mapping "*" => {"from": "hex", "to": "hex"}
	Code    interface{}                            `json:"code"`
	Nonce   interface{}                            `json:"nonce"`
	Storage map[common.Hash]map[string]interface{} `json:"storage"`
}

type StateDiffBalance struct {
	From *hexutil.Big `json:"from"`
	To   *hexutil.Big `json:"to"`
}

type StateDiffCode struct {
	From hexutil.Bytes `json:"from"`
	To   hexutil.Bytes `json:"to"`
}

type StateDiffNonce struct {
	From hexutil.Uint64 `json:"from"`
	To   hexutil.Uint64 `json:"to"`
}

type StateDiffStorage struct {
	From common.Hash `json:"from"`
	To   common.Hash `json:"to"`
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
	var tip *uint256.Int
	if args.Tip != nil {
		tip.SetFromBig(args.Tip.ToInt())
	}
	var feeCap *uint256.Int
	if args.FeeCap != nil {
		feeCap.SetFromBig(args.FeeCap.ToInt())
	}
	value := new(uint256.Int)
	if args.Value != nil {
		value.SetFromBig(args.Value.ToInt())
	}
	var data []byte
	if args.Data != nil {
		data = args.Data
	}
	var accessList types.AccessList
	if args.AccessList != nil {
		accessList = *args.AccessList
	}

	msg := types.NewMessage(addr, args.To, 0, value, gas, gasPrice, feeCap, tip, data, accessList, false /* checkNonce */)
	return msg
}

// OpenEthereum-style tracer
type OeTracer struct {
	r          *TraceCallResult
	traceAddr  []int
	traceStack []*ParityTrace
	precompile bool // Whether the last CaptureStart was called with `precompile = true`
}

func (ot *OeTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int) error {
	if precompile {
		ot.precompile = true
		return nil
	}
	if gas > 500000000 {
		gas = 500000001 - (0x8000000000000000 - gas)
	}
	//fmt.Printf("CaptureStart depth %d, from %x, to %x, create %t, input %x, gas %d, value %d\n", depth, from, to, create, input, gas, value)
	trace := &ParityTrace{}
	if create {
		trResult := &CreateTraceResult{}
		trace.Type = CREATE
		trResult.Address = new(common.Address)
		copy(trResult.Address[:], to.Bytes())
		trace.Result = trResult
	} else {
		trace.Result = &TraceResult{}
		trace.Type = CALL
	}
	if depth > 0 {
		topTrace := ot.traceStack[len(ot.traceStack)-1]
		traceIdx := topTrace.Subtraces
		ot.traceAddr = append(ot.traceAddr, traceIdx)
		topTrace.Subtraces++
		if calltype == vm.DELEGATECALLT {
			switch action := topTrace.Action.(type) {
			case *CreateTraceAction:
				value = action.Value.ToInt()
			case *CallTraceAction:
				value = action.Value.ToInt()
			}
		}
		if calltype == vm.STATICCALLT {
			value = big.NewInt(0)
		}
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
		switch calltype {
		case vm.CALLT:
			action.CallType = CALL
		case vm.CALLCODET:
			action.CallType = CALLCODE
		case vm.DELEGATECALLT:
			action.CallType = DELEGATECALL
		case vm.STATICCALLT:
			action.CallType = STATICCALL
		}
		action.From = from
		action.To = to
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
	if ot.precompile {
		ot.precompile = false
		return nil
	}
	//fmt.Printf("CaptureEnd depth %d, output %x, gasUsed %d, err %v\n", depth, output, gasUsed, err)
	if depth == 0 {
		ot.r.Output = common.CopyBytes(output)
	}
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	if err != nil {
		switch err {
		case vm.ErrInvalidJump:
			topTrace.Error = "Bad jump destination"
		case vm.ErrOutOfGas:
			topTrace.Error = "Out of gas"
		case vm.ErrExecutionReverted:
			topTrace.Error = "Reverted"
		default:
			switch err.(type) {
			case *vm.ErrStackUnderflow:
				topTrace.Error = "Stack underflow"
			case *vm.ErrInvalidOpCode:
				topTrace.Error = "Bad instruction"
			default:
				topTrace.Error = err.Error()
			}
		}
		topTrace.Result = nil
	} else {
		if len(output) > 0 {
			switch topTrace.Type {
			case CALL:
				topTrace.Result.(*TraceResult).Output = common.CopyBytes(output)
			case CREATE:
				topTrace.Result.(*CreateTraceResult).Code = common.CopyBytes(output)
			}
		}
		switch topTrace.Type {
		case CALL:
			topTrace.Result.(*TraceResult).GasUsed = new(hexutil.Big)
			topTrace.Result.(*TraceResult).GasUsed.ToInt().SetUint64(gasUsed)
		case CREATE:
			topTrace.Result.(*CreateTraceResult).GasUsed = new(hexutil.Big)
			topTrace.Result.(*CreateTraceResult).GasUsed.ToInt().SetUint64(gasUsed)
		}
	}
	ot.traceStack = ot.traceStack[:len(ot.traceStack)-1]
	if depth > 0 {
		ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	}
	return nil
}

func (ot *OeTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, st *stack.Stack, rData []byte, contract *vm.Contract, opDepth int, err error) error {
	return nil
}

func (ot *OeTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, opDepth int, err error) error {
	//fmt.Printf("CaptureFault depth %d\n", opDepth)
	return nil
}

func (ot *OeTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	trace := &ParityTrace{}
	trace.Type = SUICIDE
	action := &SuicideTraceAction{}
	action.Address = from
	action.RefundAddress = to
	action.Balance.ToInt().Set(value)
	trace.Action = action
	topTrace := ot.traceStack[len(ot.traceStack)-1]
	traceIdx := topTrace.Subtraces
	ot.traceAddr = append(ot.traceAddr, traceIdx)
	topTrace.Subtraces++
	trace.TraceAddress = make([]int, len(ot.traceAddr))
	copy(trace.TraceAddress, ot.traceAddr)
	ot.traceAddr = ot.traceAddr[:len(ot.traceAddr)-1]
	ot.r.Trace = append(ot.r.Trace, trace)
}

func (ot *OeTracer) CaptureAccountRead(account common.Address) error {
	return nil
}
func (ot *OeTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}

// Implements core/state/StateWriter to provide state diffs
type StateDiff struct {
	sdMap map[common.Address]*StateDiffAccount
}

func (sd *StateDiff) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

func (sd *StateDiff) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if *original == *value {
		return nil
	}
	accountDiff := sd.sdMap[address]
	if accountDiff == nil {
		accountDiff = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
		sd.sdMap[address] = accountDiff
	}
	m := make(map[string]interface{})
	m["*"] = &StateDiffStorage{From: common.BytesToHash(original.Bytes()), To: common.BytesToHash(value.Bytes())}
	accountDiff.Storage[*key] = m
	return nil
}

func (sd *StateDiff) CreateContract(address common.Address) error {
	if _, ok := sd.sdMap[address]; !ok {
		sd.sdMap[address] = &StateDiffAccount{Storage: make(map[common.Hash]map[string]interface{})}
	}
	return nil
}

// CompareStates uses the addresses accumulated in the sdMap and compares balances, nonces, and codes of the accounts, and fills the rest of the sdMap
func (sd *StateDiff) CompareStates(initialIbs, ibs *state.IntraBlockState) {
	var toRemove []common.Address
	for addr, accountDiff := range sd.sdMap {
		initialExist := initialIbs.Exist(addr)
		exist := ibs.Exist(addr)
		if initialExist {
			if exist {
				var allEqual = len(accountDiff.Storage) == 0
				fromBalance := initialIbs.GetBalance(addr).ToBig()
				toBalance := ibs.GetBalance(addr).ToBig()
				if fromBalance.Cmp(toBalance) == 0 {
					accountDiff.Balance = "="
				} else {
					m := make(map[string]*StateDiffBalance)
					m["*"] = &StateDiffBalance{From: (*hexutil.Big)(fromBalance), To: (*hexutil.Big)(toBalance)}
					accountDiff.Balance = m
					allEqual = false
				}
				fromCode := initialIbs.GetCode(addr)
				toCode := ibs.GetCode(addr)
				if bytes.Equal(fromCode, toCode) {
					accountDiff.Code = "="
				} else {
					m := make(map[string]*StateDiffCode)
					m["*"] = &StateDiffCode{From: fromCode, To: toCode}
					accountDiff.Code = m
					allEqual = false
				}
				fromNonce := initialIbs.GetNonce(addr)
				toNonce := ibs.GetNonce(addr)
				if fromNonce == toNonce {
					accountDiff.Nonce = "="
				} else {
					m := make(map[string]*StateDiffNonce)
					m["*"] = &StateDiffNonce{From: hexutil.Uint64(fromNonce), To: hexutil.Uint64(toNonce)}
					accountDiff.Nonce = m
					allEqual = false
				}
				if allEqual {
					toRemove = append(toRemove, addr)
				}
			} else {
				{
					m := make(map[string]*hexutil.Big)
					m["-"] = (*hexutil.Big)(initialIbs.GetBalance(addr).ToBig())
					accountDiff.Balance = m
				}
				{
					m := make(map[string]hexutil.Bytes)
					m["-"] = initialIbs.GetCode(addr)
					accountDiff.Code = m
				}
				{
					m := make(map[string]hexutil.Uint64)
					m["-"] = hexutil.Uint64(initialIbs.GetNonce(addr))
					accountDiff.Nonce = m
				}
			}
		} else if exist {
			{
				m := make(map[string]*hexutil.Big)
				m["+"] = (*hexutil.Big)(ibs.GetBalance(addr).ToBig())
				accountDiff.Balance = m
			}
			{
				m := make(map[string]hexutil.Bytes)
				m["+"] = ibs.GetCode(addr)
				accountDiff.Code = m
			}
			{
				m := make(map[string]hexutil.Uint64)
				m["+"] = hexutil.Uint64(ibs.GetNonce(addr))
				accountDiff.Nonce = m
			}
			// Transform storage
			for _, sm := range accountDiff.Storage {
				str := sm["*"].(*StateDiffStorage)
				delete(sm, "*")
				sm["+"] = &str.To
			}
		} else {
			toRemove = append(toRemove, addr)
		}
	}
	for _, addr := range toRemove {
		delete(sd.sdMap, addr)
	}
}

const callTimeout = 5 * time.Minute

// Call implements trace_call.
func (api *TraceAPIImpl) Call(ctx context.Context, args TraceCallParam, traceTypes []string, blockNrOrHash *rpc.BlockNumberOrHash) (*TraceCallResult, error) {
	dbtx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	if blockNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		blockNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, err := rpchelper.GetBlockNumber(*blockNrOrHash, dbtx, api.pending)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(dbtx)
	} else {
		stateReader = state.NewPlainKvState(dbtx, blockNumber)
	}
	ibs := state.New(stateReader)

	header := rawdb.ReadHeader(ethdb.NewRoTxDb(dbtx), hash, blockNumber)
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

	traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
	var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
	for _, traceType := range traceTypes {
		switch traceType {
		case TraceTypeTrace:
			traceTypeTrace = true
		case TraceTypeStateDiff:
			traceTypeStateDiff = true
		case TraceTypeVmTrace:
			traceTypeVmTrace = true
		default:
			return nil, fmt.Errorf("unrecognized trace type: %s", traceType)
		}
	}
	var ot OeTracer
	if traceTypeTrace {
		ot.r = traceResult
		ot.traceAddr = []int{}
	}

	// Get a new instance of the EVM.
	msg := args.ToMessage(api.gasCap)

	blockCtx, txCtx := transactions.GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, dbtx)

	evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: traceTypeTrace, Tracer: &ot})

	// Wait for the context to be done and cancel the evm. Even if the
	// EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas())
	var execResult *core.ExecutionResult
	execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, true /* gasBailout */)
	if err != nil {
		return nil, err
	}
	traceResult.Output = execResult.ReturnData
	if traceTypeStateDiff {
		sdMap := make(map[common.Address]*StateDiffAccount)
		traceResult.StateDiff = sdMap
		sd := &StateDiff{sdMap: sdMap}
		if err = ibs.FinalizeTx(ctx, sd); err != nil {
			return nil, err
		}
		// Create initial IntraBlockState, we will compare it with ibs (IntraBlockState after the transaction)
		initialIbs := state.New(stateReader)
		sd.CompareStates(initialIbs, ibs)
	}
	if traceTypeVmTrace {
		return nil, fmt.Errorf("vmTrace not implemented yet")
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = %v)", callTimeout)
	}

	return traceResult, nil
}

// CallMany implements trace_callMany.
func (api *TraceAPIImpl) CallMany(ctx context.Context, calls json.RawMessage, blockNrOrHash *rpc.BlockNumberOrHash) ([]*TraceCallResult, error) {
	dbtx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	return api.doCallMany(ctx, dbtx, calls, blockNrOrHash)
}

func (api *TraceAPIImpl) doCallMany(ctx context.Context, dbtx ethdb.Tx, calls json.RawMessage, blockNrOrHash *rpc.BlockNumberOrHash) ([]*TraceCallResult, error) {
	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	if blockNrOrHash == nil {
		var num = rpc.LatestBlockNumber
		blockNrOrHash = &rpc.BlockNumberOrHash{BlockNumber: &num}
	}
	blockNumber, hash, err := rpchelper.GetBlockNumber(*blockNrOrHash, dbtx, api.pending)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		stateReader = state.NewPlainStateReader(dbtx)
	} else {
		stateReader = state.NewPlainKvState(dbtx, blockNumber)
	}
	stateCache := shards.NewStateCache(32, 0 /* no limit */)
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, stateCache)

	header := rawdb.ReadHeader(ethdb.NewRoTxDb(dbtx), hash, blockNumber)
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
	results := []*TraceCallResult{}

	dec := json.NewDecoder(bytes.NewReader(calls))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if tok != json.Delim('[') {
		return nil, fmt.Errorf("expected array of [callparam, tracetypes]")
	}
	txIndex := 0
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return nil, err
		}
		if tok != json.Delim('[') {
			return nil, fmt.Errorf("expected [callparam, tracetypes]")
		}
		var args TraceCallParam
		if err = dec.Decode(&args); err != nil {
			return nil, err
		}
		var traceTypes []string
		if err = dec.Decode(&traceTypes); err != nil {
			return nil, err
		}
		tok, err = dec.Token()
		if err != nil {
			return nil, err
		}
		if tok != json.Delim(']') {
			return nil, fmt.Errorf("expected end of [callparam, tracetypes]")
		}
		traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
		var traceTypeTrace, traceTypeStateDiff, traceTypeVmTrace bool
		for _, traceType := range traceTypes {
			switch traceType {
			case TraceTypeTrace:
				traceTypeTrace = true
			case TraceTypeStateDiff:
				traceTypeStateDiff = true
			case TraceTypeVmTrace:
				traceTypeVmTrace = true
			default:
				return nil, fmt.Errorf("unrecognized trace type: %s", traceType)
			}
		}
		var ot OeTracer
		if traceTypeTrace {
			ot.r = traceResult
			ot.traceAddr = []int{}
		}

		// Get a new instance of the EVM.
		msg := args.ToMessage(api.gasCap)

		blockCtx, txCtx := transactions.GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, dbtx)
		ibs := state.New(cachedReader)
		// Create initial IntraBlockState, we will compare it with ibs (IntraBlockState after the transaction)

		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: traceTypeTrace, Tracer: &ot})

		gp := new(core.GasPool).AddGas(msg.Gas())
		var execResult *core.ExecutionResult
		// Clone the state cache before applying the changes, clone is discarded
		var cloneReader state.StateReader
		if traceTypeStateDiff {
			cloneCache := stateCache.Clone()
			cloneReader = state.NewCachedReader(stateReader, cloneCache)
		}
		execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, true /* gasBailout */)
		if err != nil {
			return nil, fmt.Errorf("first run for txIndex %d error: %w", txIndex, err)
		}
		traceResult.Output = execResult.ReturnData
		if traceTypeStateDiff {
			initialIbs := state.New(cloneReader)
			sdMap := make(map[common.Address]*StateDiffAccount)
			traceResult.StateDiff = sdMap
			sd := &StateDiff{sdMap: sdMap}
			if err = ibs.FinalizeTx(ctx, sd); err != nil {
				return nil, err
			}
			sd.CompareStates(initialIbs, ibs)
			if err = ibs.CommitBlock(ctx, cachedWriter); err != nil {
				return nil, err
			}
		} else {
			if err = ibs.FinalizeTx(ctx, noop); err != nil {
				return nil, err
			}
			if err = ibs.CommitBlock(ctx, cachedWriter); err != nil {
				return nil, err
			}
		}

		if traceTypeVmTrace {
			return nil, fmt.Errorf("vmTrace not implemented yet")
		}
		results = append(results, traceResult)
		txIndex++
	}
	tok, err = dec.Token()
	if err != nil {
		return nil, err
	}
	if tok != json.Delim(']') {
		return nil, fmt.Errorf("expected end of array of [callparam, tracetypes]")
	}
	return results, nil
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
