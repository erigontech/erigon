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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// jsonStructLog is the EIP-3155 wire form of a StructLog. The wire types live
// here rather than on StructLog so that encoding stays off the json.Marshaler
// path, which re-scans and copies every entry.
type jsonStructLog struct {
	Pc                uint64              `json:"pc"`
	Op                vm.OpCode           `json:"op"`
	Gas               math.HexOrDecimal64 `json:"gas"`
	GasCost           math.HexOrDecimal64 `json:"gasCost"`
	StateGasCost      uint64              `json:"stateGasCost,omitempty"`
	StateGasReservoir uint64              `json:"stateGasReservoir,omitempty"`
	Memory            hexutil.Bytes       `json:"memory"`
	MemorySize        int                 `json:"memSize"`
	Stack             []hexutil.U256      `json:"stack"`
	ReturnData        hexutil.Bytes       `json:"returnData"`
	Depth             int                 `json:"depth"`
	RefundCounter     uint64              `json:"refund"`
	OpName            string              `json:"opName"`
	ErrorString       string              `json:"error,omitempty"`
}

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
			OnTxStart:           l.OnTxStart,
			OnSystemCallStartV2: l.OnSystemCallStartV2,
			OnExitV2:            l.OnExitV2,
			OnOpcodeV2:          l.OnOpcodeV2,
			OnFaultV2:           l.OnFaultV2,
		},
	}
}

func (l *JSONLogger) OnTxStart(env *tracing.VMContext, tx types.Transaction, from accounts.Address) {
	l.env = env
}

func (l *JSONLogger) OnSystemCallStartV2(env *tracing.VMContext) {
	l.env = env
}

// OnOpcodeV2 outputs state information on the logger.
func (l *JSONLogger) OnOpcodeV2(pc uint64, typ byte, gas, cost mdgas.MdGas, scope tracing.OpContext, rData []byte, depth int, err error) {
	memory := scope.MemoryData()
	stack := scope.StackData()
	op := vm.OpCode(typ)

	log := jsonStructLog{
		Pc:                pc,
		Op:                op,
		Gas:               math.HexOrDecimal64(gas.Execution),
		GasCost:           math.HexOrDecimal64(cost.Execution),
		StateGasCost:      cost.State,
		StateGasReservoir: gas.State,
		MemorySize:        len(memory),
		Depth:             depth,
		RefundCounter:     l.env.IntraBlockState.GetRefund(),
		OpName:            op.String(),
	}
	if err != nil {
		log.ErrorString = err.Error()
	}
	if l.cfg.EnableMemory {
		log.Memory = memory
	}
	if !l.cfg.DisableStack {
		logstack := make([]hexutil.U256, len(stack))
		for i := range stack {
			logstack[i] = hexutil.U256(stack[i])
		}
		log.Stack = logstack
	}
	if l.cfg.EnableReturnData {
		log.ReturnData = rData
	}
	_ = l.encoder.Encode(log) //nolint:errchkjson
}

func (l *JSONLogger) OnFaultV2(pc uint64, op byte, gas, cost mdgas.MdGas, scope tracing.OpContext, depth int, err error) {
}

func (l *JSONLogger) OnExitV2(depth int, output []byte, gasUsed mdgas.MdGasUsage, err error, reverted bool) {
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
	_ = l.encoder.Encode(endLog{common.Bytes2Hex(output), math.HexOrDecimal64(gasUsed.Execution), errMsg}) //nolint:errchkjson
}
