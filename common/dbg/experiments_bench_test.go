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

package dbg

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

var benchSink atomic.Pointer[[]byte]

// Busy goroutines make the stop-the-world of runtime.ReadMemStats visible.
func BenchmarkMemUsage(b *testing.B) {
	for _, bc := range []struct {
		name string
		read func()
	}{
		{"ReadMemStats", func() {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
		}},
		{"MemUsage", func() { MemUsage() }},
	} {
		b.Run(bc.name, func(b *testing.B) {
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for range runtime.GOMAXPROCS(0) - 1 {
				wg.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
							buf := make([]byte, 256)
							benchSink.Store(&buf)
						}
					}
				})
			}
			for b.Loop() {
				bc.read()
			}
			close(stop)
			wg.Wait()
		})
	}
}
