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

package native

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// TestCallTracerNotRevertedExit pins the ExitHook contract documented in
// tracing/hooks.go: a frame that ends with an error but without reverting is
// not a failure, so it keeps its output, its logs and its created address.
func TestCallTracerNotRevertedExit(t *testing.T) {
	t.Parallel()
	var (
		from       = accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa"))
		created    = accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000bb"))
		deployCode = []byte{0x60, 0x60, 0x60, 0x40}
	)

	tracer, err := tracers.New("callTracer", new(tracers.Context), json.RawMessage(`{"withLog":true}`))
	require.NoError(t, err)

	tracer.OnEnter(0, byte(vm.CREATE), from, created, false, nil, 100000, *uint256.NewInt(0), nil)
	tracer.OnLog(&types.Log{Address: created.Value()})
	tracer.OnExit(0, deployCode, 50000, vm.ErrCodeStoreOutOfGas, false /* reverted */)
	tracer.OnTxEnd(&types.Receipt{GasUsed: 50000}, nil)

	res, err := tracer.GetResult()
	require.NoError(t, err)
	var frame callFrame
	require.NoError(t, json.Unmarshal(res, &frame))

	require.Empty(t, frame.Error, "a non-reverted exit must not report an error")
	require.Equal(t, deployCode, []byte(frame.Output))
	require.NotNil(t, frame.To, "the created address survives a non-reverted exit")
	require.Len(t, frame.Logs, 1, "logs of a non-failed frame must not be cleared")
}
