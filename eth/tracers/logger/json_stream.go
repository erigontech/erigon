package logger

import (
	"context"
	"encoding/hex"
	"sort"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
)

// JsonStreamLogger is an EVM state logger and implements Tracer.
//
// JsonStreamLogger can capture state based on the given Log configuration and also keeps
// a track record of modified storage which is used in reporting snapshots of the
// contract their storage.
type JsonStreamLogger struct {
	ctx          context.Context
	cfg          LogConfig
	stream       *jsoniter.Stream
	hexEncodeBuf [128]byte
	firstCapture bool

	locations common.Hashes // For sorting
	storage   map[common.Address]Storage
	logs      []StructLog
	output    []byte //nolint
	err       error  //nolint
	env       *vm.EVM
}

// NewStructLogger returns a new logger
func NewJsonStreamLogger(cfg *LogConfig, ctx context.Context, stream *jsoniter.Stream) *JsonStreamLogger {
	logger := &JsonStreamLogger{
		ctx:          ctx,
		stream:       stream,
		storage:      make(map[common.Address]Storage),
		firstCapture: true,
	}
	if cfg != nil {
		logger.cfg = *cfg
	}
	return logger
}

func (l *JsonStreamLogger) CaptureTxStart(gasLimit uint64) {}

func (l *JsonStreamLogger) CaptureTxEnd(restGas uint64) {}

// CaptureStart implements the Tracer interface to initialize the tracing operation.
func (l *JsonStreamLogger) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	l.env = env
}

func (l *JsonStreamLogger) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

// CaptureState logs a new structured log message and pushes it out to the environment
//
// CaptureState also tracks SLOAD/SSTORE ops to track storage change.
func (l *JsonStreamLogger) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	contract := scope.Contract
	memory := scope.Memory
	stack := scope.Stack

	select {
	case <-l.ctx.Done():
		return
	default:
	}
	// check if already accumulated the specified number of logs
	if l.cfg.Limit != 0 && l.cfg.Limit <= len(l.logs) {
		return
	}
	if !l.firstCapture {
		l.stream.WriteMore()
	} else {
		l.firstCapture = false
	}
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
				address = common.Hash(stack.Data[stack.Len()-1].Bytes32())
				value   uint256.Int
			)
			l.env.IntraBlockState().GetState(contract.Address(), &address, &value)
			l.storage[contract.Address()][address] = value.Bytes32()
			outputStorage = true
		}
		// capture SSTORE opcodes and record the written entry in the local storage.
		if op == vm.SSTORE && stack.Len() >= 2 {
			var (
				value   = common.Hash(stack.Data[stack.Len()-2].Bytes32())
				address = common.Hash(stack.Data[stack.Len()-1].Bytes32())
			)
			l.storage[contract.Address()][address] = value
			outputStorage = true
		}
	}
	// create a new snapshot of the EVM.
	l.stream.WriteObjectStart()
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
	if err != nil {
		l.stream.WriteMore()
		l.stream.WriteObjectField("error")
		l.stream.WriteObjectStart()
		l.stream.WriteObjectEnd()
		//l.stream.WriteString(err.Error())
	}
	if !l.cfg.DisableStack {
		l.stream.WriteMore()
		l.stream.WriteObjectField("stack")
		l.stream.WriteArrayStart()
		for i, stackValue := range stack.Data {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteString(stackValue.String())
		}
		l.stream.WriteArrayEnd()
	}
	if !l.cfg.DisableMemory {
		memData := memory.Data()
		l.stream.WriteMore()
		l.stream.WriteObjectField("memory")
		l.stream.WriteArrayStart()
		for i := 0; i+32 <= len(memData); i += 32 {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteString(string(l.hexEncodeBuf[0:hex.Encode(l.hexEncodeBuf[:], memData[i:i+32])]))
		}
		l.stream.WriteArrayEnd()
	}
	if outputStorage {
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
	l.stream.WriteObjectEnd()
	_ = l.stream.Flush()
}

// CaptureFault implements the Tracer interface to trace an execution fault
// while running an opcode.
func (l *JsonStreamLogger) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CaptureEnd is called after the call finishes to finalize the tracing.
func (l *JsonStreamLogger) CaptureEnd(output []byte, usedGas uint64, err error) {
}

func (l *JsonStreamLogger) CaptureExit(output []byte, usedGas uint64, err error) {
}
