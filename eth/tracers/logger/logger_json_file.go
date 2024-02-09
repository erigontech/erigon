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

package logger

import (
	"encoding/json"
	"os"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

type JSONFileLogger struct {
	cfg        *LogConfig
	env        vm.VMInterface
	txHash     string
	opContexts []OpContext
}

// NewJSONFileLogger creates a new EVM tracer that prints execution steps as JSON objects
// into the provided stream.
func NewJSONFileLogger(cfg *LogConfig, txHash string) *JSONFileLogger {
	l := &JSONFileLogger{cfg, nil, txHash, []OpContext{}}
	if l.cfg == nil {
		l.cfg = &LogConfig{}
	}

	return l
}

func (l *JSONFileLogger) CaptureTxStart(gasLimit uint64) {}

func (l *JSONFileLogger) CaptureTxEnd(restGas uint64) {}

func (l *JSONFileLogger) CaptureStart(env vm.VMInterface, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	l.env = env
}

func (l *JSONFileLogger) CaptureEnter(typ vm.OpCode, from libcommon.Address, to libcommon.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
}

// CaptureState outputs state information on the logger.
func (l *JSONFileLogger) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	// memory := scope.Memory
	stack := scope.Stack

	log := OpContext{
		Pc:      pc,
		Op:      op.String(),
		Gas:     gas,
		GasCost: cost,
		Depth:   depth,
		Refund:  l.env.IntraBlockState().GetRefund(),
		// Err:     err.Error(),
	}

	if !l.cfg.DisableStack {
		//TODO(@holiman) improve this
		logstack := make([]string, len(stack.Data))
		for i, item := range stack.Data {
			logstack[i] = item.Hex()
		}
		log.Stack = logstack
	}

	l.opContexts = append(l.opContexts, log)
}

// CaptureFault outputs state information on the logger.
func (l *JSONFileLogger) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// CaptureEnd is triggered at end of execution.
func (l *JSONFileLogger) CaptureEnd(output []byte, usedGas uint64, err error) {
	json, _ := json.Marshal(l.opContexts)

	writeErr := os.WriteFile("traces/"+l.txHash+".json", json, 0600)
	if err != nil {
		log.Error(writeErr)
	}
}

func (l *JSONFileLogger) CaptureExit(output []byte, usedGas uint64, err error) {
}

type OpContext struct {
	Pc      uint64   `json:"pc"`
	Op      string   `json:"op"`
	Gas     uint64   `json:"gas"`
	GasCost uint64   `json:"gasCost"`
	Depth   int      `json:"depth"`
	Stack   []string `json:"stack"`
	Memory  string   `json:"memory"`
	Refund  uint64   `json:"refund"`
	// Err     string   `json:"error"`
}

func (l *OpContext) Cmp(b OpContext) bool {
	if l.Pc != b.Pc ||
		l.Op != b.Op ||
		l.Gas != b.Gas ||
		// l.GasCost != b.GasCost ||
		l.Depth != b.Depth ||
		l.Refund != b.Refund ||
		len(l.Stack) != len(b.Stack) {
		return false
	}

	var length int
	if len(l.Stack) > len(b.Stack) {
		length = len(b.Stack)
	} else {
		length = len(l.Stack)
	}
	for i := 0; i < length; i++ {
		value := l.Stack[i]
		value2 := b.Stack[i]

		if value != value2 {
			return false
		}
	}

	return true
}
