package sentio

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	tracers.RegisterLookup(false, newSentioTracer)
}

type Trace struct {
	Type    vm.OpCode           `json:"type"`
	Pc      uint64              `json:"pc"`
	GasIn   math.HexOrDecimal64 `json:"gasIn"` // TODO this should be hex
	Gas     math.HexOrDecimal64 `json:"gas"`
	GasCost math.HexOrDecimal64 `json:"gasCost"`
	GasUsed math.HexOrDecimal64 `json:"gasUsed"`
	Output  hexutil.Bytes       `json:"output,omitempty"`
	From    *libcommon.Address  `json:"from,omitempty"`

	// Used by call
	To          *libcommon.Address `json:"to,omitempty"`
	Input       string             `json:"input,omitempty"` // TODO better struct it and make it bytes
	Value       hexutil.Bytes      `json:"value,omitempty"`
	ErrorString string             `json:"error,omitempty"`

	// Used by jump
	InputStack   []uint256.Int `json:"inputStack,omitempty"`
	InputMemory  *[]string     `json:"inputMemory,omitempty"`
	OutputStack  []uint256.Int `json:"outputStack,omitempty"`
	OutputMemory *[]string     `json:"outputMemory,omitempty"`

	// Used by log
	Address *libcommon.Address `json:"address,omitempty"`
	Data    hexutil.Bytes      `json:"data,omitempty"`
	Topics  []hexutil.Bytes    `json:"topics,omitempty"`

	// Only used by root
	Traces []Trace `json:"traces,omitempty"`

	// Use for internal call stack organization
	// The jump to go into the function
	//enterPc uint64
	exitPc uint64

	// the function get called
	function *functionInfo
}

type sentioTracer struct {
	config sentioTracerConfig
	env    vm.VMInterface
	//activePrecompiles []libcommon.Address
	functionMap map[string]map[uint64]functionInfo

	previousJump *Trace
	//internalCallStack []internalCallStack

	//traces    []Trace
	descended bool
	//index     int // TODO no need
	//currentDepth int
	currentGas  math.HexOrDecimal64
	callsNumber int
	//rootTrace   Trace
	callstack []Trace
	gasLimit  uint64
}

func (t *sentioTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

func (t *sentioTracer) CaptureTxEnd(restGas uint64) {
	t.callstack[0].GasUsed = math.HexOrDecimal64(t.gasLimit - restGas)
}

func (t *sentioTracer) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.env = env
	// Update list of precompiles based on current block
	//rules := env.ChainConfig().Rules(env.Context().BlockNumber, env.Context().Time)
	//t.activePrecompiles = vm.ActivePrecompiles(rules)

	t.callstack = append(t.callstack, Trace{
		//Index: 0,
		Type:  vm.CALL,
		From:  &from,
		To:    &to,
		Gas:   math.HexOrDecimal64(gas),
		Input: hexutil.Bytes(input).String(),
		//Value: (*hexutil.Big)(value.ToBig()),
	})

	if value != nil {
		t.callstack[0].Value = value.Bytes()
	}
	if create {
		t.callstack[0].Type = vm.CREATE
	}
}
func (t *sentioTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	t.callstack[0].GasUsed = math.HexOrDecimal64(usedGas)
	t.callstack[0].Output = output
	//t.callStack[0].Traces = t.traces
}

func (t *sentioTracer) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	//if typ == vm.CREATE || typ == vm.CREATE2 {
	//	t.traces[len(t.traces)-1].To = &to
	//}

	call := Trace{
		Type:  typ,
		From:  &from,
		To:    &to,
		Input: hexutil.Bytes(input).String(),
		Gas:   math.HexOrDecimal64(gas),
	}
	if value != nil {
		call.Value = value.Bytes()
	}
	//t.callstack = append(t.callstack, call)
	t.callstack = append(t.callstack, call)
	t.callsNumber++
	t.descended = true
}

func (t *sentioTracer) CaptureExit(output []byte, usedGas uint64, err error) {
	t.callsNumber--

	size := len(t.callstack)
	if size <= 1 {
		return
	}

	stackSize := len(t.callstack)
	for i := stackSize - 1; i >= 0; i-- {
		if t.callstack[i].function != nil {
			continue
		}

		if stackSize-i > 1 {
			log.Info(fmt.Sprintf("tail call optimization [external] size %d", stackSize-i))
		}

		for j := stackSize - 1; j >= i; j-- {
			t.callstack[j].Output = output
			t.callstack[j-1].Traces = append(t.callstack[j-1].Traces, t.callstack[j])
		}
		break
	}

	call := t.callstack[size-1]
	t.callstack = t.callstack[:size-1]
	size -= 1

	call.GasUsed = math.HexOrDecimal64(usedGas)

	t.callstack[size-1].Traces = append(t.callstack[size-1].Traces, call)

	call.Output = output
}

func (t *sentioTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Capture any errors immediately
	if err != nil {
		log.Error("error in tracer", "err", err)
		//	t.traces = append(t.traces, Trace{
		//		Type:        "ERROR",
		//		ErrorString: err.Error(), //TODO ask pengcheng
		//	})
		//	return
	}

	//t.index++

	var mergeBase = func(trace Trace) Trace {
		//trace.op = op
		trace.Pc = pc
		trace.Type = op
		//trace.Index = t.index - 1
		trace.GasIn = math.HexOrDecimal64(gas)
		trace.GasCost = math.HexOrDecimal64(cost)
		return trace
	}

	var copyMemory = func(offset *uint256.Int, size *uint256.Int) hexutil.Bytes {
		// it's important to get copy
		return scope.Memory.GetCopy(int64(offset.Uint64()), int64(size.Uint64()))
	}

	var formatMemory = func() *[]string {
		memory := make([]string, 0, (scope.Memory.Len()+31)/32)
		for i := 0; i+32 <= scope.Memory.Len(); i += 32 {
			memory = append(memory, fmt.Sprintf("%x", scope.Memory.GetPtr(int64(i), 32)))
		}
		return &memory
	}

	switch op {
	case vm.CREATE, vm.CREATE2, vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL, vm.SELFDESTRUCT:
		return
	case vm.JUMP:
		from := scope.Contract.CodeAddr
		jump := mergeBase(Trace{
			From:       from,
			InputStack: append([]uint256.Int(nil), scope.Stack.Data...), // TODO only need partial
		})
		if t.previousJump != nil {
			log.Error("Unexpected previous jump", t.previousJump)
		}
		t.previousJump = &jump
		return

	case vm.JUMPDEST:
		from := scope.Contract.CodeAddr
		fromStr := from.String()

		if t.previousJump != nil { // vm.JumpDest and match with a previous jump (otherwise it's a jumpi)
			// Check if this is return
			// TODO pontentially maintain a map for fast filtering
			//log.Info("fromStr" + fromStr + ", callstack size" + fmt.Sprint(len(t.callStack)))
			stackSize := len(t.callstack)
			for i := stackSize - 1; i >= 0; i-- {
				//log.Info("callstack" + fmt.Sprint(t.callStack[i]))
				functionInfo := t.callstack[i].function
				if functionInfo == nil {
					break
				}

				if functionInfo.address != fromStr {
					break
				}

				if t.callstack[i].exitPc == pc {
					// find a match, pop the stack, copy memory if needed

					if stackSize-i > 1 {
						log.Info(fmt.Sprintf("tail call optimization size %d", stackSize-i))
					}

					// TODO maybe don't need return all
					for j := stackSize - 1; j >= i; j-- {
						t.callstack[j].OutputStack = append([]uint256.Int(nil), scope.Stack.Data...)
						if functionInfo.OutputMemory {
							t.callstack[j].OutputMemory = formatMemory()
						}
						t.callstack[j-1].Traces = append(t.callstack[j-1].Traces, t.callstack[j])
					}
					t.callstack = t.callstack[:i]
					t.previousJump = nil
					return
				}
			}

			funcInfo := t.getFunctionInfo(fromStr, pc)
			//log.Info("function info" + fmt.Sprint(funcInfo))

			if funcInfo != nil {
				if funcInfo.InputSize >= scope.Stack.Len() {
					// TODO check if this misses data
					log.Debug("Unexpected stack size" + "function:" + fmt.Sprint(funcInfo) + ", stack" + fmt.Sprint(scope.Stack.Data))
					log.Debug("previous jump" + fmt.Sprint(*t.previousJump))
					t.previousJump = nil
					return
				}

				// confirmed that we are in an internal call
				//t.internalCallStack = append(t.internalCallStack, internalCallStack{
				//	enterPc:  t.previousJump.Pc,
				//	exitPc:   scope.Stack.Back(funcInfo.InputSize).Uint64(),
				//	function: funcInfo,
				//})
				//jump.enterPc = t.previousJump.Pc
				t.previousJump.exitPc = scope.Stack.Back(funcInfo.InputSize).Uint64()
				t.previousJump.function = funcInfo
				if funcInfo.InputMemory {
					t.previousJump.InputMemory = formatMemory()
				}
				t.callstack = append(t.callstack, *t.previousJump)
				//t.callstack = append(t.callstack, callStack{
			}

			// reset previous jump regardless
			t.previousJump = nil
		}

		return
	case vm.LOG0, vm.LOG1, vm.LOG2, vm.LOG3, vm.LOG4:
		topicCount := int(op - vm.LOG0)
		logOffset := scope.Stack.Peek()
		logSize := scope.Stack.Back(1)
		data := copyMemory(logOffset, logSize)
		var topics []hexutil.Bytes
		//stackLen := scope.Stack.Len()
		for i := 0; i < int(topicCount); i++ {
			topics = append(topics, scope.Stack.Back(2+i).Bytes())
		}
		addr := scope.Contract.Address()
		l := mergeBase(Trace{
			Address: &addr,
			Data:    data,
			Topics:  topics,
		})
		t.callstack[len(t.callstack)-1].Traces = append(t.callstack[len(t.callstack)-1].Traces, l)
		return
	}

	// If we've just descended into an inner call, retrieve it's true allowance. We
	// need to extract if from within the call as there may be funky gas dynamics
	// with regard to requested and actually given gas (2300 stipend, 63/64 rule).
	if t.descended {
		if depth >= t.callsNumber { // how about currentDepth?
			t.currentGas = math.HexOrDecimal64(gas)
			//t.traces[topIdx].Gas = gas
		} else {
			// TODO(karalabe): The call was made to a plain account. We currently don't
			// have access to the true gas amount inside the call and so any amount will
			// mostly be wrong since it depends on a lot of input args. Skip gas for now.
		}
		t.descended = false
	}
	if op == vm.REVERT {
		trace := mergeBase(Trace{
			ErrorString: "execution reverted",
		})
		log.Error("execution reverted" + err.Error()) // TODO check error handling here
		t.callstack[len(t.callstack)-1].Traces = append(t.callstack[len(t.callstack)-1].Traces, trace)
		return
	}

}

func (t *sentioTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *sentioTracer) CapturePreimage(pc uint64, hash libcommon.Hash, preimage []byte) {}

func (t *sentioTracer) GetResult() (json.RawMessage, error) {
	return json.Marshal(t.callstack[0])
	//return json.RawMessage(`{}`), nil
}

func (t *sentioTracer) Stop(err error) {

}

func newSentioTracer(name string, ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	if name != "sentioTracer" {
		return nil, errors.New("no tracer found")
	}

	var config sentioTracerConfig
	functionMap := map[string]map[uint64]functionInfo{}

	if cfg != nil {
		if err := json.Unmarshal(cfg, &config); err != nil {
			return nil, err
		}

		for address, functions := range config.Functions {
			checkSumAddress := libcommon.HexToAddress(address).String()
			functionMap[checkSumAddress] = make(map[uint64]functionInfo)

			for _, function := range functions {
				function.address = checkSumAddress
				functionMap[checkSumAddress][function.Pc] = function
			}
		}

		log.Info("create sentioTracer config with " + fmt.Sprint(len(functionMap)) + " functions")
	}

	return &sentioTracer{
		config:      config,
		functionMap: functionMap,
		callsNumber: 1,
	}, nil
}

//func (t *sentioTracer) isPrecompiled(addr libcommon.Address) bool {
//	for _, p := range t.activePrecompiles {
//		if p == addr {
//			return true
//		}
//	}
//	return false
//}

func (t *sentioTracer) getFunctionInfo(address string, pc uint64) *functionInfo {
	m, ok := t.functionMap[address]
	if !ok || m == nil {
		return nil
	}
	info, ok := m[pc]
	if ok {
		return &info
	}

	return nil
}
