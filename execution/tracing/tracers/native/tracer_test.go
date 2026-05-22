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

package native_test

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/stretchr/testify/require"
)

// TestTracerStopRace exercises the concurrent Stop / GetResult path that the
// trace RPC handler uses: a timeout watchdog goroutine calls Stop while the
// main goroutine is still running the trace and will eventually call
// GetResult. Under -race, writes to the interruption reason field must not
// race with reads, for every tracer that implements it.
//
// callTracer and flatCallTracer's GetResult short-circuits when the callstack
// is empty, before loading the reason. For those tracers the test pushes a
// single top-level call frame via OnEnter so GetResult reaches the reason.Load()
// path where the race can be observed under -race.
func TestTracerStopRace(t *testing.T) {
	type setup struct {
		name       string
		needsFrame bool // whether GetResult requires a top-level call frame
	}
	cases := []setup{
		{"callTracer", true},
		{"flatCallTracer", true},
		{"4byteTracer", false},
		{"prestateTracer", false},
	}
	for _, s := range cases {
		t.Run(s.name, func(t *testing.T) {
			const iterations = 1000
			stopErr := errors.New("execution timeout")

			for range iterations {
				tr, err := tracers.New(s.name, &tracers.Context{}, json.RawMessage("{}"))
				require.NoError(t, err)

				if s.needsFrame && tr.OnEnter != nil {
					// Push a single top-level call frame so GetResult doesn't
					// short-circuit before reading the interruption reason.
					tr.OnEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil, 0, uint256.Int{}, nil)
				}

				start := make(chan struct{})
				var ready sync.WaitGroup
				var wg sync.WaitGroup
				ready.Add(2)
				wg.Add(2)
				go func() {
					defer wg.Done()
					ready.Done()
					<-start
					tr.Stop(stopErr)
				}()
				go func() {
					defer wg.Done()
					ready.Done()
					<-start
					_, _ = tr.GetResult()
				}()
				ready.Wait()
				close(start)
				wg.Wait()
			}
		})
	}
}
