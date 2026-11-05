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

package jsonrpc

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// withReplayCPUs makes the block replays of this test see n cores: one core keeps the
// sequential path, more cores take the parallel one.
func withReplayCPUs(t *testing.T, n int) {
	prev := replayCPUs
	replayCPUs = func() int { return n }
	t.Cleanup(func() { replayCPUs = prev })
}

// TestTraceBlockParallelMatchesSequential pins that tracing a block's transactions on
// parallel workers, each on the history state before it, writes the same bytes as replaying
// them one after another on one state.
func TestTraceBlockParallelMatchesSequential(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	api := newDebugApiForTest(m)

	prestate := "prestateTracer"
	diffMode := json.RawMessage(`{"diffMode":true}`)
	configs := map[string]func() *tracersConfig.TraceConfig{
		"callTracer withLog": withLogCallTracer,
		"prestateTracer diffMode": func() *tracersConfig.TraceConfig {
			return &tracersConfig.TraceConfig{Tracer: &prestate, TracerConfig: &diffMode}
		},
	}
	trace := func(t *testing.T, cpus int, config *tracersConfig.TraceConfig) []byte {
		withReplayCPUs(t, cpus)
		var buf bytes.Buffer
		stream := jsonstream.New(&buf)
		require.NoError(t, api.TraceBlockByNumber(m.Ctx, rpc.BlockNumber(1), config, stream))
		require.NoError(t, stream.Flush())
		return buf.Bytes()
	}
	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			sequential := trace(t, 1, config())
			parallel := trace(t, 16, config())
			require.Equal(t, string(sequential), string(parallel))
		})
	}
}
