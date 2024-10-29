package logger

import (
	"context"
	"encoding/hex"
	"sort"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
)

// JsonStreamLogger is an EVM state logger and implements Tracer.
//
// JsonStreamLogger can capture state based on the given Log configuration and also keeps
// a track record of modified storage which is used in reporting snapshots of the
// contract their storage.
type JsonStreamLogger_ZkEvm struct {
	ctx          context.Context
	cfg          LogConfig
	stream       *jsoniter.Stream
	hexEncodeBuf [128]byte
	firstCapture bool

	locations common.Hashes // For sorting
	storage   map[libcommon.Address]Storage
	logs      []StructLog
	env       *vm.EVM

	counterCollector *vm.CounterCollector
	stateClosed      bool
	memSize          int
}

// NewStructLogger returns a new logger
func NewJsonStreamLogger_ZkEvm(cfg *LogConfig, ctx context.Context, stream *jsoniter.Stream, counterCollector *vm.CounterCollector) *JsonStreamLogger_ZkEvm {
	logger := &JsonStreamLogger_ZkEvm{
		ctx:              ctx,
		stream:           stream,
		storage:          make(map[libcommon.Address]Storage),
		firstCapture:     true,
		counterCollector: counterCollector,
		stateClosed:      true,
	}
	if cfg != nil {
		logger.cfg = *cfg
	}
	return logger
}

// not needed for this tracer
func (l *JsonStreamLogger_ZkEvm) CaptureTxStart(gasLimit uint64) {
	// not needed for this tracer
}

// not needed for this tracer
func (l *JsonStreamLogger_ZkEvm) CaptureTxEnd(restGas uint64) {
	// not needed for this tracer
}

// CaptureStart implements the Tracer interface to initialize the tracing operation.
func (l *JsonStreamLogger_ZkEvm) CaptureStart(env *vm.EVM, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	l.env = env
}

// not needed for this tracer
func (l *JsonStreamLogger_ZkEvm) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	// not needed for this tracer
}

// CaptureState logs a new structured log message and pushes it out to the environment
//
// CaptureState also tracks SLOAD/SSTORE ops to track storage change.
func (l *JsonStreamLogger_ZkEvm) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	select {
	case <-l.ctx.Done():
		return
	default:
	}
	// check if already accumulated the specified number of logs
	if l.cfg.Limit != 0 && l.cfg.Limit <= len(l.logs) {
		return
	}

	if !l.stateClosed {
		l.writeStateClose()
	}
	if !l.firstCapture {
		l.stream.WriteMore()
	} else {
		l.firstCapture = false
	}

	outputStorage := l.prepareStorage(scope, scope.Contract, op)

	l.stream.WriteObjectStart()
	l.stateClosed = false
	l.writeOpSnapshot(pc, op, gas, cost, depth, err)

	l.writeError(err)

	l.writeStack(scope.Stack)

	l.writeReturnData(rData)

	l.writeMemory(scope.Memory)

	if outputStorage {
		l.writeStorage(scope.Contract)
	}

	l.writeCounters()
}

func (l *JsonStreamLogger_ZkEvm) prepareStorage(scope *vm.ScopeContext, contract *vm.Contract, op vm.OpCode) bool {
	stack := scope.Stack
	var outputStorage bool
	if !l.cfg.DisableStorage {
		// initialise new changed values storage container for this contract
		// if not present.
		if l.storage[contract.Address()] == nil {
			l.storage[contract.Address()] = make(Storage)
		}
		// capture SLOAD opcodes and record the read entry in the local storage
		if op == vm.SLOAD && stack.Len() >= 1 {
			var (
				address = libcommon.Hash(stack.Data[stack.Len()-1].Bytes32())
				value   uint256.Int
			)
			l.env.IntraBlockState().GetState(contract.Address(), &address, &value)
			l.storage[contract.Address()][address] = value.Bytes32()
			outputStorage = true
		}
		// capture SSTORE opcodes and record the written entry in the local storage.
		if op == vm.SSTORE && stack.Len() >= 2 {
			var (
				value   = libcommon.Hash(stack.Data[stack.Len()-2].Bytes32())
				address = libcommon.Hash(stack.Data[stack.Len()-1].Bytes32())
			)
			l.storage[contract.Address()][address] = value
			outputStorage = true
		}
	}

	return outputStorage
}

func (l *JsonStreamLogger_ZkEvm) writeOpSnapshot(pc uint64, op vm.OpCode, gas, cost uint64, depth int, err error) {
	// create a new snapshot of the EVM.
	l.stream.WriteObjectField("pc")
	l.stream.WriteUint64(pc)
	l.stream.WriteMore()
	l.stream.WriteObjectField("op")
	l.stream.WriteString(op.String())
	l.stream.WriteMore()
	l.stream.WriteObjectField("gas")
	l.stream.WriteUint64(gas)
	l.stream.WriteMore()
	l.stream.WriteObjectField("gasCost")
	l.stream.WriteUint64(cost)
	l.stream.WriteMore()
	l.stream.WriteObjectField("depth")
	l.stream.WriteInt(depth)
}

func (l *JsonStreamLogger_ZkEvm) writeError(err error) {
	if err != nil {
		l.stream.WriteMore()
		l.stream.WriteObjectField("error")
		l.stream.WriteString(err.Error())
	}
}

func (l *JsonStreamLogger_ZkEvm) writeStorage(contract *vm.Contract) {
	l.stream.WriteMore()
	l.stream.WriteObjectField("storage")
	l.stream.WriteObjectStart()
	first := true
	// Sort storage by locations for easier comparison with geth
	if l.locations != nil {
		l.locations = l.locations[:0]
	}
	s := l.storage[contract.Address()]
	for loc := range s {
		l.locations = append(l.locations, loc)
	}
	sort.Sort(l.locations)
	for _, loc := range l.locations {
		value := s[loc]
		if first {
			first = false
		} else {
			l.stream.WriteMore()
		}
		l.stream.WriteObjectField(string(l.hexEncodeBuf[0:hex.Encode(l.hexEncodeBuf[:], loc[:])]))
		l.stream.WriteString(string(l.hexEncodeBuf[0:hex.Encode(l.hexEncodeBuf[:], value[:])]))
	}
	l.stream.WriteObjectEnd()
}

func (l *JsonStreamLogger_ZkEvm) writeMemory(memory *vm.Memory) {
	if !l.cfg.DisableMemory {
		memData := memory.Data()

		// on first occurance don't expand memory
		// this is because in interpreter we expand the memory before we execute the opcode
		// and the state for traced opcode should be before the execution of the opcode
		if l.memSize < len(memData) {
			size := len(memData)
			memData = memData[:l.memSize]
			l.memSize = size
		}

		l.stream.WriteMore()
		l.stream.WriteObjectField("memory")
		l.stream.WriteArrayStart()
		for i := 0; i+32 <= len(memData); i += 32 {
			if i != 0 { // add a comma for all but the first 32 bytes
				l.stream.WriteMore()
			}
			l.stream.WriteString(string(l.hexEncodeBuf[0:hex.Encode(l.hexEncodeBuf[:], memData[i:i+32])]))
		}

		l.stream.WriteArrayEnd()
	}
}

func (l *JsonStreamLogger_ZkEvm) writeReturnData(rData []byte) {
	if !l.cfg.DisableReturnData && len(rData) > 0 {
		l.stream.WriteMore()
		l.stream.WriteObjectField("returnData")
		l.stream.WriteString("0x" + hex.EncodeToString(rData))
	}
}

func (l *JsonStreamLogger_ZkEvm) writeStack(stack *stack.Stack) {
	if !l.cfg.DisableStack {
		l.stream.WriteMore()
		l.stream.WriteObjectField("stack")
		l.stream.WriteArrayStart()
		for i, stackValue := range stack.Data {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteString(stackValue.Hex())
		}
		l.stream.WriteArrayEnd()
	}
}

func (l *JsonStreamLogger_ZkEvm) writeCounters() {
	if l.counterCollector != nil {
		differences := l.counterCollector.Counters().UsedAsMap()

		l.stream.WriteMore()
		l.stream.WriteObjectField("counters")
		l.stream.WriteObjectStart()
		first := true
		for key, value := range differences {
			if first {
				first = false
			} else {
				l.stream.WriteMore()
			}
			l.stream.WriteObjectField(key)
			l.stream.WriteInt(value)
		}
		l.stream.WriteObjectEnd()
	}
}

func (l *JsonStreamLogger_ZkEvm) writeStateClose() {
	l.stream.WriteObjectEnd()
	_ = l.stream.Flush()
	l.stateClosed = true
}

// CaptureFault implements the Tracer interface to trace an execution fault
// while running an opcode.
func (l *JsonStreamLogger_ZkEvm) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
	if !l.stateClosed {
		l.writeError(err)
		l.writeStateClose()
	}
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (l *JsonStreamLogger_ZkEvm) CaptureEnd(output []byte, usedGas uint64, err error) {
	if !l.stateClosed {
		l.writeStateClose()
	}
}

// not needed for this tracer
func (l *JsonStreamLogger_ZkEvm) CaptureExit(output []byte, usedGas uint64, err error) {
	// not needed for this tracer
}
