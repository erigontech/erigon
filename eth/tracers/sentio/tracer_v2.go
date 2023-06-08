package sentio

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	tracers.RegisterLookup(false, newSentioTracerV2)
}

type functionInfo struct {
	address      string
	Pc           uint64 `json:"pc"`
	InputSize    int    `json:"inputSize"`
	InputMemory  bool   `json:"inputMemory"`
	OutputMemory bool   `json:"outputMemory"`
}

type sentioTracerConfig struct {
	Functions map[string][]functionInfo `json:"functions"`
}

type internalCallStack struct {
	// The jump to go into the function
	enterPc uint64
	exitPc  uint64

	// the function get called
	function *functionInfo
}

type sentioTracerV2 struct {
	config            sentioTracerConfig
	env               vm.VMInterface
	activePrecompiles []libcommon.Address
	functionMap       map[string]map[uint64]functionInfo

	previousJump *Trace
	callStack    []internalCallStack

	traces    []Trace
	descended bool
	index     int
	//currentDepth int
	currentGas  math.HexOrDecimal64
	callsNumber int
	rootTrace   Trace
	gasLimit    uint64
}

func (t *sentioTracerV2) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

func (t *sentioTracerV2) CaptureTxEnd(restGas uint64) {
	t.rootTrace.GasUsed = math.HexOrDecimal64(t.gasLimit - restGas)
}

func (t *sentioTracerV2) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	t.env = env
	// Update list of precompiles based on current block
	rules := env.ChainConfig().Rules(env.Context().BlockNumber, env.Context().Time)
	t.activePrecompiles = vm.ActivePrecompiles(rules)

	t.rootTrace = Trace{
		Index: 0,
		//Type:  typ.String(),
		From:  &from,
		To:    &to,
		Gas:   math.HexOrDecimal64(gas),
		Input: hexutil.Bytes(input).String(),
		Value: value.Bytes(),
	}
}
func (t *sentioTracerV2) CaptureEnd(output []byte, usedGas uint64, err error) {
	t.rootTrace.GasUsed = math.HexOrDecimal64(usedGas)
	t.rootTrace.Output = output
	t.rootTrace.Traces = t.traces
}
func (t *sentioTracerV2) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	if t.rootTrace.Type == "" {
		t.rootTrace.Type = typ.String()
	}
	if typ == vm.CREATE || typ == vm.CREATE2 {
		t.traces[len(t.traces)-1].To = &to
	}
}

func (t *sentioTracerV2) CaptureExit(output []byte, usedGas uint64, err error) {
	//if depth == t.callsNumber-1 {
	output = common.CopyBytes(output)
	t.callsNumber--
	trace := Trace{
		Type: "CALLEND",
		//GasIn: math.HexOrDecimal64(gas),
		GasUsed: math.HexOrDecimal64(usedGas),
		Value:   output,
	}
	//if unpacked, err := abi.UnpackRevert(output); err == nil {
	//	f.Revertal = unpacked
	//}

	if t.currentGas != 0 {
		trace.Gas = t.currentGas
		t.currentGas = 0
	}
	t.traces = append(t.traces, trace)
	//}
}

func (t *sentioTracerV2) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// Capture any errors immediately
	if err != nil {
		t.traces = append(t.traces, Trace{
			Type:        "ERROR",
			ErrorString: err.Error(), //TODO ask pengcheng
		})
		return
	}

	t.index++
	var mergeBase = func(trace Trace) Trace {
		//trace.op = op
		trace.Pc = pc
		trace.Type = op.String()
		trace.Index = t.index - 1
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
	case vm.RETURN:
		outputOffset := scope.Stack.Peek()
		outputSize := scope.Stack.Back(1)
		trace := mergeBase(Trace{
			Value: copyMemory(outputOffset, outputSize),
		})
		t.traces = append(t.traces, trace)
		return
	case vm.CREATE, vm.CREATE2:
		// If a new contract is being created, add to the call stack
		inputOffset := scope.Stack.Back(1)
		inputSize := scope.Stack.Back(2)
		from := scope.Contract.Address()
		// to will be captured later in CaptureEnter
		trace := mergeBase(Trace{
			From:  &from,
			Input: copyMemory(inputOffset, inputSize).String(),
			Value: scope.Stack.Peek().Bytes(),
		})
		t.traces = append(t.traces, trace)
		t.callsNumber++
		t.descended = true
		return
	case vm.SELFDESTRUCT:
		// If a contract is being self destructed, gather that as a subcall too
		from := scope.Contract.Address()
		to := libcommon.BytesToAddress(scope.Stack.Peek().Bytes())
		trace := mergeBase(Trace{
			From:  &from,
			To:    &to,
			Value: t.env.IntraBlockState().GetBalance(from).Bytes(),
		})
		t.traces = append(t.traces, trace)
		return
	case vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL:
		// If a new method invocation is being done, add to the call stack
		to := libcommon.BytesToAddress(scope.Stack.Back(1).Bytes())
		if t.isPrecompiled(to) {
			//log.Warn("precompiled", "index: ", t.index)
			return
		}
		offset := 1
		if op == vm.DELEGATECALL || op == vm.STATICCALL {
			offset = 0
		}
		inputOffset := scope.Stack.Back(offset + 2)
		inputSize := scope.Stack.Back(offset + 3)
		from := scope.Contract.Address()
		trace := mergeBase(Trace{
			From:  &from,
			To:    &to,
			Input: copyMemory(inputOffset, inputSize).String(),
		})
		if op == vm.CALL || op == vm.CALLCODE {
			trace.Value = scope.Stack.Back(2).Bytes()
		}
		t.traces = append(t.traces, trace)
		t.callsNumber++
		t.descended = true
		return
	case vm.JUMP, vm.JUMPDEST:
		from := scope.Contract.CodeAddr
		jump := mergeBase(Trace{
			From:  from,
			Stack: append([]uint256.Int(nil), scope.Stack.Data...), // TODO only need partial
		})

		if op == vm.JUMP {
			if t.previousJump != nil {
				log.Error("Unexpected previous jump", t.previousJump)
			}
			t.previousJump = &jump
		} else if t.previousJump != nil { // vm.JumpDest and match with a previous jump (otherwise it's a jumpi)

			// Check if this is return
			// TODO pontentially maintain a map for fast filtering
			addressStr := from.String()
			//log.Info("addressStr" + addressStr + ", callstack size" + fmt.Sprint(len(t.callStack)))
			for i := len(t.callStack) - 1; i >= 0; i-- {
				//log.Info("callstack" + fmt.Sprint(t.callStack[i]))

				if t.callStack[i].function.address != addressStr {
					break
				}

				if t.callStack[i].exitPc == pc {
					// find a match, pop the stack, copy memory if needed
					if t.callStack[i].function.OutputMemory {
						jump.Memory = formatMemory()
					}
					t.traces = append(t.traces, *t.previousJump, jump)
					t.callStack = t.callStack[0:i]
					t.previousJump = nil
					return
				}
			}

			funcInfo := t.getFunctionInfo(addressStr, pc)
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
				t.callStack = append(t.callStack, internalCallStack{
					enterPc:  t.previousJump.Pc,
					exitPc:   scope.Stack.Back(funcInfo.InputSize).Uint64(),
					function: funcInfo,
				})
				if funcInfo.InputMemory {
					jump.Memory = formatMemory()
				}
				t.traces = append(t.traces, *t.previousJump, jump)
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
		t.traces = append(t.traces, l)
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
		t.traces = append(t.traces, trace)
		return
	}

}

func (t *sentioTracerV2) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CapturePreimage records a SHA3 preimage discovered during execution.
func (t *sentioTracerV2) CapturePreimage(pc uint64, hash libcommon.Hash, preimage []byte) {}

func (t *sentioTracerV2) GetResult() (json.RawMessage, error) {
	return json.Marshal(t.rootTrace)
	//return json.RawMessage(`{}`), nil
}

func (t *sentioTracerV2) Stop(err error) {

}

func newSentioTracerV2(name string, ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	if name != "sentioTracerV2" {
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

	return &sentioTracerV2{
		config:      config,
		functionMap: functionMap,
		callsNumber: 1,
	}, nil
}

func (t *sentioTracerV2) isPrecompiled(addr libcommon.Address) bool {
	for _, p := range t.activePrecompiles {
		if p == addr {
			return true
		}
	}
	return false
}

func (t *sentioTracerV2) getFunctionInfo(address string, pc uint64) *functionInfo {
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
