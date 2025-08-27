// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"encoding/json"
	"io"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/types"
)

type JSONLogger struct {
	encoder *json.Encoder
	cfg     *LogConfig
	env     *tracing.VMContext
}

// NewJSONLogger creates a new EVM tracer that prints execution steps as JSON objects
// into the provided stream.
func NewJSONLogger(cfg *LogConfig, writer io.Writer) *JSONLogger {
	l := &JSONLogger{json.NewEncoder(writer), cfg, nil}
	if l.cfg == nil {
		l.cfg = &LogConfig{}
	}
	return l
}

func (l *JSONLogger) Tracer() *tracers.Tracer {
	return &tracers.Tracer{
		Hooks: &tracing.Hooks{
			OnTxStart: l.OnTxStart,
			OnExit:    l.OnExit,
			OnOpcode:  l.OnOpcode,
			OnFault:   l.OnFault,
		},
	}
}

func (l *JSONLogger) OnTxStart(env *tracing.VMContext, tx types.Transaction, from common.Address) {
	l.env = env
}

// OnOpcode outputs state information on the logger.
func (l *JSONLogger) OnOpcode(pc uint64, typ byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	memory := scope.MemoryData()
	stack := scope.StackData()
	op := vm.OpCode(typ)

	log := StructLog{
		Pc:            pc,
		Op:            op,
		Gas:           gas,
		GasCost:       cost,
		MemorySize:    len(memory),
		Storage:       nil,
		Depth:         depth,
		RefundCounter: l.env.IntraBlockState.GetRefund(),
		Err:           err,
	}
	if !l.cfg.DisableMemory {
		log.Memory = memory
	}
	if !l.cfg.DisableStack {
		//TODO(@holiman) improve this
		logstack := make([]*big.Int, len(stack))
		for i, item := range stack {
			logstack[i] = item.ToBig()
		}
		log.Stack = logstack
	}
	_ = l.encoder.Encode(log)
}

func (l *JSONLogger) OnFault(pc uint64, op byte, gas uint64, cost uint64, scope tracing.OpContext, depth int, err error) {
}

func (l *JSONLogger) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if depth > 0 {
		return
	}

	type endLog struct {
		Output  string              `json:"output"`
		GasUsed math.HexOrDecimal64 `json:"gasUsed"`
		Err     string              `json:"error,omitempty"`
	}
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	_ = l.encoder.Encode(endLog{common.Bytes2Hex(output), math.HexOrDecimal64(gasUsed), errMsg})
}
