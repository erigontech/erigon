// Copyright 2026 The Erigon Authors
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
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm"
)

func captureJSONLoggerStep(t *testing.T, cfg *LogConfig, scope *mockOpContext, rData []byte, err error) string {
	t.Helper()
	var buf bytes.Buffer
	l := NewJSONLogger(cfg, &buf)
	l.env = &tracing.VMContext{IntraBlockState: &mockIBS{}}
	l.OnOpcode(42, byte(vm.SSTORE), 1_000_000, 2100, scope, rData, 3, err)
	return strings.TrimSuffix(buf.String(), "\n")
}

// The cases pin the shapes an EIP-3155 consumer distinguishes: a fully populated
// step, an errored step whose stack is suppressed, and a step whose stack and
// buffers are empty rather than absent.
func TestJSONLoggerStepEncoding(t *testing.T) {
	t.Parallel()

	full := &mockOpContext{
		memory: []byte{0xde, 0xad, 0xbe, 0xef},
		stack:  []uint256.Int{*uint256.NewInt(0), *uint256.NewInt(0x1234), *uint256.NewInt(1 << 40)},
	}

	assert.Equal(t,
		`{"pc":42,"op":85,"gas":"0xf4240","gasCost":"0x834","memory":"0xdeadbeef","memSize":4,`+
			`"stack":["0x0","0x1234","0x10000000000"],"returnData":"0x0102","depth":3,"refund":0,"opName":"SSTORE"}`,
		captureJSONLoggerStep(t, &LogConfig{EnableMemory: true, EnableReturnData: true}, full, []byte{0x01, 0x02}, nil))

	assert.Equal(t,
		`{"pc":42,"op":85,"gas":"0xf4240","gasCost":"0x834","memory":"0x","memSize":4,"stack":null,`+
			`"returnData":"0x","depth":3,"refund":0,"opName":"SSTORE","error":"out of gas"}`,
		captureJSONLoggerStep(t, &LogConfig{DisableStack: true}, full, nil, errors.New("out of gas")))

	assert.Equal(t,
		`{"pc":42,"op":85,"gas":"0xf4240","gasCost":"0x834","memory":"0x","memSize":0,"stack":[],`+
			`"returnData":"0x","depth":3,"refund":0,"opName":"SSTORE"}`,
		captureJSONLoggerStep(t, &LogConfig{}, &mockOpContext{}, nil, nil))
}
