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

package rpchelper

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type logTracerFrame struct {
	logs     int
	inner    []logTracerFrame
	reverted bool
}

func (f logTracerFrame) run(t *LogTracer, depth int) {
	t.onEnterV2(depth, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil, mdgas.MdGas{}, uint256.Int{}, nil)
	for i := 0; i < f.logs; i++ {
		t.onLog(&types.Log{})
	}
	for _, inner := range f.inner {
		inner.run(t, depth+1)
	}
	t.onExitV2(depth, nil, mdgas.MdGasUsage{}, nil, f.reverted)
}

func TestLogTracerLogIndex(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		calls []logTracerFrame
		want  []hexutil.Uint
	}{
		{
			name:  "failed call before a successful one",
			calls: []logTracerFrame{{logs: 1, reverted: true}, {logs: 1}},
			want:  []hexutil.Uint{0},
		},
		{
			name:  "reverted inner frame",
			calls: []logTracerFrame{{logs: 1, inner: []logTracerFrame{{logs: 1, reverted: true}, {logs: 1}}}},
			want:  []hexutil.Uint{0, 1},
		},
		{
			name:  "failed call with a successful inner frame",
			calls: []logTracerFrame{{logs: 1, inner: []logTracerFrame{{logs: 2}}, reverted: true}, {logs: 1}},
			want:  []hexutil.Uint{0},
		},
		{
			name:  "indices continue across calls",
			calls: []logTracerFrame{{logs: 1}, {logs: 1, reverted: true}, {logs: 1}},
			want:  []hexutil.Uint{0, 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracer := NewLogTracer(false, 1, common.Hash{}, common.Hash{}, 0)
			var have []hexutil.Uint
			for i, call := range tc.calls {
				tracer.Reset(common.Hash{}, uint(i))
				call.run(tracer, 0)
				for _, log := range tracer.Logs() {
					have = append(have, log.Index)
				}
			}
			require.Equal(t, tc.want, have)
		})
	}
}
