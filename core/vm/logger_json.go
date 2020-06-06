// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"encoding/json"
	"io"
	"math/big"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
)

type JSONLogger struct {
	encoder *json.Encoder
	cfg     *LogConfig
}

// NewJSONLogger creates a new EVM tracer that prints execution steps as JSON objects
// into the provided stream.
func NewJSONLogger(cfg *LogConfig, writer io.Writer) *JSONLogger {
	l := &JSONLogger{json.NewEncoder(writer), cfg}
	if l.cfg == nil {
		l.cfg = &LogConfig{}
	}
	return l
}

func (l *JSONLogger) CaptureStart(depth int, from common.Address, to common.Address, create bool, input []byte, gas uint64, value *big.Int) error {
	return nil
}

// CaptureState outputs state information on the logger.
func (l *JSONLogger) CaptureState(env *EVM, pc uint64, op OpCode, gas, cost uint64, memory *Memory, stack *stack.Stack, contract *Contract, depth int, err error) error {
	log := StructLog{
		Pc:            pc,
		Op:            op,
		Gas:           gas,
		GasCost:       cost,
		MemorySize:    memory.Len(),
		Storage:       nil,
		Depth:         depth,
		RefundCounter: env.IntraBlockState.GetRefund(),
		Err:           err,
	}
	if !l.cfg.DisableMemory {
		log.Memory = memory.Data()
	}
	if !l.cfg.DisableStack {
		log.Stack = make([]*big.Int, len(stack.GetData()))
		for i, item := range stack.GetData() {
			log.Stack[i] = item.ToBig()
		}
	}
	return l.encoder.Encode(log)
}

// CaptureFault outputs state information on the logger.
func (l *JSONLogger) CaptureFault(env *EVM, pc uint64, op OpCode, gas, cost uint64, memory *Memory, stack *stack.Stack, contract *Contract, depth int, err error) error {
	return nil
}

// CaptureEnd is triggered at end of execution.
func (l *JSONLogger) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	type endLog struct {
		Output  string              `json:"output"`
		GasUsed math.HexOrDecimal64 `json:"gasUsed"`
		Time    time.Duration       `json:"time"`
		Err     string              `json:"error,omitempty"`
	}
	if depth != 0 {
		return nil
	}
	if err != nil {
		return l.encoder.Encode(endLog{common.Bytes2Hex(output), math.HexOrDecimal64(gasUsed), t, err.Error()})
	}
	return l.encoder.Encode(endLog{common.Bytes2Hex(output), math.HexOrDecimal64(gasUsed), t, ""})
}

func (l *JSONLogger) CaptureCreate(creator common.Address, creation common.Address) error {
	return nil
}

func (l *JSONLogger) CaptureAccountRead(account common.Address) error {
	return nil
}

func (l *JSONLogger) CaptureAccountWrite(account common.Address) error {
	return nil
}
