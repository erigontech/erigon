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
	"maps"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// JsonStreamLogger is an EVM state logger and implements Tracer.
//
// JsonStreamLogger can capture state based on the given Log configuration and also keeps
// a track record of modified storage which is used in reporting snapshots of the
// contract their storage.
type JsonStreamLogger struct {
	ctx    context.Context
	cfg    LogConfig
	stream jsonstream.Stream
	// Scratch for the hex helpers below. Every result aliases it, so only one is
	// live at a time: hand it to the stream, which copies, before encoding the next.
	hexEncodeBuf [128]byte
	firstCapture bool
	opcodeSteps  int // steps captured so far; executed-but-suppressed ones don't count

	locations common.Hashes // For sorting
	storage   map[accounts.Address]Storage
	env       *tracing.VMContext
}

// NewStructLogger returns a new logger
func NewJsonStreamLogger(cfg *LogConfig, ctx context.Context, stream jsonstream.Stream) *JsonStreamLogger {
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
			OnExit:              l.OnExit,
			OnOpcode:            l.OnOpcode,
		},
	}
}

func (l *JsonStreamLogger) OnTxStart(env *tracing.VMContext, tx types.Transaction, from accounts.Address) {
	l.env = env
}

func (l *JsonStreamLogger) OnSystemCallStartV2(env *tracing.VMContext) {
	l.env = env
}

// hexWithPrefix encodes h into hexEncodeBuf as 0x-prefixed hex. It takes a hash
// rather than a slice so the result is known to fit; the buffer is not resized.
func (l *JsonStreamLogger) hexWithPrefix(h *common.Hash) string {
	l.hexEncodeBuf[0] = '0'
	l.hexEncodeBuf[1] = 'x'
	n := hex.Encode(l.hexEncodeBuf[2:], h[:])
	return common.ToStringZeroCopy(l.hexEncodeBuf[:2+n])
}

// hexQuoted encodes v as a complete JSON string, quotes included, for WriteRaw.
func (l *JsonStreamLogger) hexQuoted(v *uint256.Int) string {
	l.hexEncodeBuf[0] = '"'
	b, _ := hexutil.U256(*v).AppendText(l.hexEncodeBuf[:1])
	return common.ToStringZeroCopy(append(b, '"'))
}

// hexQuotedHash is hexWithPrefix plus the quotes, ready for WriteRaw.
func (l *JsonStreamLogger) hexQuotedHash(h *common.Hash) string {
	l.hexEncodeBuf[0], l.hexEncodeBuf[1], l.hexEncodeBuf[2] = '"', '0', 'x'
	n := hex.Encode(l.hexEncodeBuf[3:], h[:])
	l.hexEncodeBuf[3+n] = '"'
	return common.ToStringZeroCopy(l.hexEncodeBuf[:4+n])
}

// writeMemoryWordRaw writes a memory word as a JSON string "0x<hex>" directly
// to the stream without any heap allocations. Pads to 32 bytes if needed.
func (l *JsonStreamLogger) writeMemoryWordRaw(chunk []byte) {
	if len(chunk) < 32 {
		var word [32]byte
		copy(word[:], chunk)
		hex.Encode(l.hexEncodeBuf[:], word[:])
	} else {
		hex.Encode(l.hexEncodeBuf[:], chunk)
	}
	l.stream.WriteRaw(`"0x`)
	l.stream.WriteRawBytes(l.hexEncodeBuf[:64])
	l.stream.WriteRaw(`"`)
}

func (l *JsonStreamLogger) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	// no log entry are producer
	if l.firstCapture {
		l.stream.WriteObjectStart()
		l.stream.WriteObjectField("structLogs")
		l.stream.WriteArrayStart()
	}
}

// OnOpcode also tracks SLOAD/SSTORE ops to track storage change.
func (l *JsonStreamLogger) OnOpcode(pc uint64, typ byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
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
	l.opcodeSteps++
	if !l.firstCapture {
		l.stream.WriteMore()
	} else {
		l.stream.WriteObjectStart()
		l.stream.WriteObjectField("structLogs")
		l.stream.WriteArrayStart()

		l.firstCapture = false
	}
	var outputStorage bool
	if !l.cfg.DisableStorage {
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
	refund := l.env.IntraBlockState.GetRefund()
	if refund != 0 {
		l.stream.WriteMore()
		l.stream.WriteObjectField("refund")
		l.stream.WriteUint64(refund)
	}

	if err != nil {
		l.stream.WriteMore()
		l.stream.WriteObjectField("error")
		l.stream.WriteString(err.Error())
	}
	if !l.cfg.DisableStack {
		l.stream.WriteMore()
		l.stream.WriteObjectField("stack")
		l.stream.WriteArrayStart()
		for i := range stack {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteRaw(l.hexQuoted(&stack[i]))
		}
		l.stream.WriteArrayEnd()
	}
	if l.cfg.EnableMemory && len(memory) > 0 {
		l.stream.WriteMore()
		l.stream.WriteObjectField("memory")
		l.stream.WriteArrayStart()
		for i := 0; i < len(memory); i += 32 {
			end := min(i+32, len(memory))
			if i > 0 {
				l.stream.WriteMore()
			}
			l.writeMemoryWordRaw(memory[i:end])
		}
		l.stream.WriteArrayEnd()
	}
	if l.cfg.EnableReturnData && len(rData) > 0 {
		l.stream.WriteMore()
		l.stream.WriteObjectField("returnData")
		l.stream.WriteString(hexutil.Encode(rData))
	}
	if outputStorage {
		l.stream.WriteMore()
		l.stream.WriteObjectField("storage")
		l.stream.WriteObjectStart()
		// Sorted by location for easier comparison with geth
		s := l.storage[contractAddr]
		l.locations = slices.AppendSeq(l.locations[:0], maps.Keys(s))
		l.locations.Sort()
		for i := range l.locations {
			if i > 0 {
				l.stream.WriteMore()
			}
			loc := &l.locations[i]
			value := s[*loc]
			l.stream.WriteObjectField(l.hexWithPrefix(loc))
			l.stream.WriteRaw(l.hexQuotedHash(&value))
		}
		l.stream.WriteObjectEnd()
	}
	l.stream.WriteObjectEnd()
}
