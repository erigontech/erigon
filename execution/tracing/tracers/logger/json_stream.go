// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package logger

import (
	"context"
	"encoding/hex"
	"errors"
	"maps"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

// JsonStreamLogger is an EVM state logger and implements Tracer.
//
// JsonStreamLogger can capture state based on the given Log configuration and also keeps
// a track record of modified storage which is used in reporting snapshots of the
// contract their storage.
type JsonStreamLogger struct {
	ctx    context.Context
	cfg    LogConfig
	stream *jsonstream.Stream
	// Scratch for the hex helpers below. Every result aliases it, so only one is
	// live at a time: hand it to the stream, which copies, before encoding the next.
	hexEncodeBuf [128]byte
	stack        stackJSON
	firstCapture bool
	opcodeSteps  int // steps captured so far; executed-but-suppressed ones don't count

	locations common.Hashes // For sorting
	storage   map[accounts.Address]Storage
	env       *tracing.VMContext

	// The last captured step is written at the next event, so that a halt found while its
	// opcode runs (OnFaultV2) can still mark it. What the opcode may change before then is
	// kept from the capture: the stack encoding, and copies of memory and return data.
	pending    pendingStep
	memory     []byte
	returnData []byte
}

type pendingStep struct {
	ok       bool
	pc       uint64
	op       vm.OpCode
	gas      mdgas.MdGas
	cost     mdgas.MdGasCost
	depth    int
	refund   uint64
	err      error
	stack    []byte // the stack.encode result, valid until the next step is captured
	contract accounts.Address
	storage  bool
}

// NewStructLogger returns a new logger
func NewJsonStreamLogger(cfg *LogConfig, ctx context.Context, stream *jsonstream.Stream) *JsonStreamLogger {
	logger := &JsonStreamLogger{
		ctx:          ctx,
		stream:       stream,
		storage:      make(map[accounts.Address]Storage),
		firstCapture: true,
	}
	if cfg != nil {
		logger.cfg = *cfg
	}
	return logger
}

func (l *JsonStreamLogger) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart:           l.OnTxStart,
			OnSystemCallStartV2: l.OnSystemCallStartV2,
			OnExitV2:            l.OnExitV2,
			OnOpcodeV2:          l.OnOpcodeV2,
			OnFaultV2:           l.OnFaultV2,
		},
	}
}

func (l *JsonStreamLogger) OnTxStart(env *tracing.VMContext, tx types.Transaction, from accounts.Address) {
	l.env = env
}

func (l *JsonStreamLogger) OnSystemCallStartV2(env *tracing.VMContext) {
	l.env = env
}

// stackJSON is the JSON array of the stack last written. Between steps only the top
// of the stack changes, so the encoding of the unchanged bottom is kept.
type stackJSON struct {
	vals []uint256.Int
	ends []int // len(json) after each value
	json []byte
}

// encode returns stack as a JSON array, valid until the next call.
func (s *stackJSON) encode(stack []uint256.Int) []byte {
	k := 0
	for k < len(stack) && k < len(s.vals) && s.vals[k] == stack[k] {
		k++
	}
	s.vals = append(s.vals[:k], stack[k:]...)
	s.ends = s.ends[:k]
	if k == 0 {
		s.json = append(s.json[:0], '[')
	} else {
		s.json = s.json[:s.ends[k-1]]
	}
	for i := k; i < len(stack); i++ {
		if i > 0 {
			s.json = append(s.json, ',')
		}
		s.json = append(s.json, '"')
		s.json, _ = hexutil.U256(stack[i]).AppendText(s.json)
		s.json = append(s.json, '"')
		s.ends = append(s.ends, len(s.json))
	}
	s.json = append(s.json, ']')
	return s.json
}

// hexWithPrefix encodes h into hexEncodeBuf as 0x-prefixed hex. It takes a hash
// rather than a slice so the result is known to fit; the buffer is not resized.
func (l *JsonStreamLogger) hexWithPrefix(h *common.Hash) string {
	l.hexEncodeBuf[0] = '0'
	l.hexEncodeBuf[1] = 'x'
	n := hex.Encode(l.hexEncodeBuf[2:], h[:])
	return common.ToStringZeroCopy(l.hexEncodeBuf[:2+n])
}

// writeWord writes a word as a 0x-prefixed hex string padded to 32 bytes.
func (l *JsonStreamLogger) writeWord(word []byte) {
	padded := l.hexEncodeBuf[:32]
	clear(padded[copy(padded, word):])
	l.stream.WriteHex(padded)
}

func (l *JsonStreamLogger) OnExitV2(depth int, output []byte, gasUsed mdgas.MdGasUsage, err error, reverted bool) {
	l.writePending()
	l.writePrologueOnce()
}

// OnFaultV2 marks the pending step with the exceptional halt of its opcode. A REVERT that
// completes also ends with an error here, but it is not a halt.
func (l *JsonStreamLogger) OnFaultV2(pc uint64, op byte, gas mdgas.MdGas, cost mdgas.MdGasCost, scope tracing.OpContext, depth int, err error) {
	if p := &l.pending; p.pc == pc && p.op == vm.OpCode(op) && p.depth == depth && !errors.Is(err, vm.ErrExecutionReverted) {
		p.err = err
	}
}

// writePrologueOnce opens the response object and the structLogs array. Every
// frame exits through OnExitV2, and the caller closes one object and one array, so
// a second prologue would leave the response unbalanced.
func (l *JsonStreamLogger) writePrologueOnce() {
	if !l.firstCapture {
		return
	}
	l.firstCapture = false
	l.stream.WriteObjectStart()
	l.stream.Field("structLogs")
	l.stream.WriteArrayStart()
}

// OnOpcodeV2 also tracks SLOAD/SSTORE ops to track storage change.
func (l *JsonStreamLogger) OnOpcodeV2(pc uint64, typ byte, gas mdgas.MdGas, cost mdgas.MdGasCost, scope tracing.OpContext, rData []byte, depth int, err error) {
	l.writePending()
	contractAddr := scope.Address()
	memory := scope.MemoryData()
	stack := scope.StackData()

	op := vm.OpCode(typ)
	select {
	case <-l.ctx.Done():
		return
	default:
	}
	// check if already captured the specified number of opcode steps. Execution
	// keeps going, we just stop recording. Must happen before anything is written
	// to the stream, otherwise a dangling separator would be emitted.
	if l.cfg.Limit != 0 && l.cfg.Limit <= l.opcodeSteps {
		return
	}
	l.writePrologueOnce()
	l.opcodeSteps++
	var outputStorage bool
	if !l.cfg.DisableStorage && err == nil {
		// initialise new changed values storage container for this contract
		// if not present.
		if l.storage[contractAddr] == nil {
			l.storage[contractAddr] = make(Storage)
		}
		// capture SLOAD opcodes and record the read entry in the local storage
		if op == vm.SLOAD && len(stack) >= 1 {
			var (
				address = accounts.InternKey(stack[len(stack)-1].Bytes32())
				value   uint256.Int
			)
			value, _ = l.env.IntraBlockState.GetState(contractAddr, address)
			l.storage[contractAddr][address.Value()] = value.Bytes32()
			outputStorage = true
		}
		// capture SSTORE opcodes and record the written entry in the local storage.
		if op == vm.SSTORE && len(stack) >= 2 {
			var (
				value   = common.Hash(stack[len(stack)-2].Bytes32())
				address = common.Hash(stack[len(stack)-1].Bytes32())
			)
			l.storage[contractAddr][address] = value
			outputStorage = true
		}
	}
	l.pending = pendingStep{
		ok: true, pc: pc, op: op, gas: gas, cost: cost, depth: depth,
		refund: l.env.IntraBlockState.GetRefund(), err: err, contract: contractAddr, storage: outputStorage,
	}
	if !l.cfg.DisableStack {
		l.pending.stack = l.stack.encode(stack)
	}
	if l.cfg.EnableMemory {
		l.memory = append(l.memory[:0], memory...)
	}
	if l.cfg.EnableReturnData {
		l.returnData = append(l.returnData[:0], rData...)
	}
}

// writePending writes the entry of the pending step, if any. The storage of its contract
// changes only when a later step is captured, so it is read here.
func (l *JsonStreamLogger) writePending() {
	p := &l.pending
	if !p.ok {
		return
	}
	p.ok = false
	pc, op, gas, cost, depth, refund, err, stack := p.pc, p.op, p.gas, p.cost, p.depth, p.refund, p.err, p.stack
	memory, rData, contractAddr, outputStorage := l.memory, l.returnData, p.contract, p.storage
	if err != nil {
		// A step that halts exceptionally reports no cost.
		cost = mdgas.MdGasCost{}
	}
	// create a new snapshot of the EVM.
	l.stream.WriteObjectStart()
	l.stream.Field("pc")
	l.stream.Uint(pc)
	l.stream.Field("op")
	l.stream.WriteString(op.String())
	l.stream.Field("gas")
	l.stream.Uint(gas.Execution)
	l.stream.Field("gasCost")
	l.stream.Uint(cost.Execution)
	if cost.State != 0 {
		l.stream.Field("stateGasCost")
		l.stream.Int(cost.State)
	}
	if gas.State != 0 {
		l.stream.Field("stateGasReservoir")
		l.stream.Uint(gas.State)
	}
	l.stream.Field("depth")
	l.stream.Int(int64(depth))
	if refund != 0 {
		l.stream.Field("refund")
		l.stream.Uint(refund)
	}

	if err != nil {
		l.stream.Field("error")
		l.stream.WriteString(err.Error())
	}
	if !l.cfg.DisableStack {
		l.stream.Field("stack")
		l.stream.WriteRawBytes(stack)
	}
	if l.cfg.EnableMemory && len(memory) > 0 {
		l.stream.Field("memory")
		l.stream.WriteArrayStart()
		for i := 0; i < len(memory); i += 32 {
			end := min(i+32, len(memory))
			l.writeWord(memory[i:end])
		}
		l.stream.WriteArrayEnd()
	}
	if l.cfg.EnableReturnData && len(rData) > 0 {
		ethjson.Data(l.stream, "returnData", rData)
	}
	if outputStorage {
		l.stream.Field("storage")
		l.stream.WriteObjectStart()
		// Sorted by location for easier comparison with geth
		s := l.storage[contractAddr]
		l.locations = slices.AppendSeq(l.locations[:0], maps.Keys(s))
		l.locations.Sort()
		for i := range l.locations {
			loc := &l.locations[i]
			value := s[*loc]
			l.stream.Field(l.hexWithPrefix(loc))
			l.writeWord(value[:])
		}
		l.stream.WriteObjectEnd()
	}
	l.stream.WriteObjectEnd()
}
