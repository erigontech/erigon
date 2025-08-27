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
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// JsonStreamLogger is an EVM state logger and implements Tracer.
//
// JsonStreamLogger can capture state based on the given Log configuration and also keeps
// a track record of modified storage which is used in reporting snapshots of the
// contract their storage.
type JsonStreamLogger struct {
	ctx          context.Context
	cfg          LogConfig
	stream       jsonstream.Stream
	hexEncodeBuf [128]byte
	firstCapture bool

	locations common.Hashes // For sorting
	storage   map[common.Address]Storage
	logs      []StructLog
	output    []byte //nolint
	err       error  //nolint
	env       *tracing.VMContext
}

// NewStructLogger returns a new logger
func NewJsonStreamLogger(cfg *LogConfig, ctx context.Context, stream jsonstream.Stream) *JsonStreamLogger {
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

func (l *JsonStreamLogger) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: l.OnTxStart,
			OnExit:    l.OnExit,
			OnOpcode:  l.OnOpcode,
		},
	}
}

func (l *JsonStreamLogger) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	l.env = env
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
	// check if already accumulated the specified number of logs
	if l.cfg.Limit != 0 && l.cfg.Limit <= len(l.logs) {
		return
	}
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
				address = common.Hash(stack[len(stack)-1].Bytes32())
				value   uint256.Int
			)
			l.env.IntraBlockState.GetState(contractAddr, address, &value)
			l.storage[contractAddr][address] = value.Bytes32()
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
		l.stream.WriteUint64(l.env.IntraBlockState.GetRefund())
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
		for i, stackValue := range stack {
			if i > 0 {
				l.stream.WriteMore()
			}
			l.stream.WriteString(stackValue.Hex())
		}
		l.stream.WriteArrayEnd()
	}
	if !l.cfg.DisableMemory {
		memData := memory
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
		s := l.storage[contractAddr]
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
