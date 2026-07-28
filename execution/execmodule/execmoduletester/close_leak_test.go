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

package execmoduletester_test

import (
	"bytes"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
)

// A goroutine surviving Close roots the tester's whole object graph (DB,
// aggregator, caches), so every goroutine the tester starts must stop.
func TestCloseStopsAllGoroutines(t *testing.T) {
	execmoduletester.New(nil).Close() // warm lazily-started package singletons
	baseline := stableGoroutines()

	execmoduletester.New(nil).Close()

	deadline := time.Now().Add(15 * time.Second)
	n := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		n = runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 1)
	t.Fatalf("Close leaked goroutines: baseline=%d now=%d\n%s", baseline, n, buf.String())
}

// stableGoroutines waits until the goroutine count stops decreasing and
// returns it.
func stableGoroutines() int {
	n := runtime.NumGoroutine()
	for range 100 {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		next := runtime.NumGoroutine()
		if next >= n {
			return n
		}
		n = next
	}
	return n
}
