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

package debug

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
)

func TestGasChangeV2Recording(t *testing.T) {
	old := mdgas.MdGas{Execution: 100}
	new := mdgas.MdGas{Execution: 200}
	var received [][2]mdgas.MdGas
	var reasons []tracing.GasChangeReason
	recorder := &Tracer{wrapped: &tracers.Tracer{Hooks: &tracing.Hooks{OnGasChangeV2: func(old, new mdgas.MdGas, reason tracing.GasChangeReason) {
		received = append(received, [2]mdgas.MdGas{old, new})
		reasons = append(reasons, reason)
	}}}}
	recorder.Hooks().EmitGasChange(old, new, tracing.GasChangeCallOpCode)
	require.Equal(t, [][2]mdgas.MdGas{{old, new}}, received)
	require.Equal(t, []tracing.GasChangeReason{tracing.GasChangeCallOpCode}, reasons)
	encoded, err := json.Marshal(recorder.traces)
	require.NoError(t, err)
	require.JSONEq(t, `{"traces":[{"onGasChangeV2":{"old":{"Execution":100,"State":0},"new":{"Execution":200,"State":0},"reason":"GasChangeCallOpCode"}}]}`, string(encoded))
}
