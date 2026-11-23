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

package execmodule

import (
	"runtime"
	"runtime/metrics"

	"github.com/erigontech/erigon/diagnostics/mem"
)

var gcDueMetrics = [...]string{
	"/gc/cycles/total:gc-cycles",
	"/gc/heap/live:bytes",
	"/gc/heap/goal:bytes",
	"/memory/classes/heap/objects:bytes",
}

func (e *ExecModule) startIdleAfterCommit() {
	if !e.idleAfterCommitBusy.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer e.idleAfterCommitBusy.Store(false)
		mem.RefreshVirtualMemStats(mem.VirtualMemStatsAge)
		e.gcIfDue()
	}()
}

func (e *ExecModule) gcIfDue() {
	s := make([]metrics.Sample, len(gcDueMetrics))
	for i, name := range gcDueMetrics {
		s[i].Name = name
	}
	metrics.Read(s)
	cycles, live, goal, heap := s[0].Value.Uint64(), s[1].Value.Uint64(), s[2].Value.Uint64(), s[3].Value.Uint64()
	ranSinceLastCommit := cycles != e.gcCyclesAtLastCommit
	e.gcCyclesAtLastCommit = cycles
	if gcDue(heap, live, goal, ranSinceLastCommit) {
		runtime.GC()
	}
}

func gcDue(heap, live, goal uint64, ranSinceLastCommit bool) bool {
	if ranSinceLastCommit || goal <= live {
		return false
	}
	runway := goal - live
	return heap >= live+runway/2 && heap < live+runway/64*45
}
