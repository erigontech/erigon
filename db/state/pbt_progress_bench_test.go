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

package state

import (
	"testing"
	"time"
)

func BenchmarkPBTProgressBatch(b *testing.B) {
	progress := NewPBTProgress(PBTProgressConfig{Phase: "benchmark", Interval: time.Hour})
	var done, reported uint64
	b.ResetTimer()
	for range b.N {
		done++
		if done-reported >= 4096 {
			progress.Add(done - reported)
			reported = done
		}
	}
	b.StopTimer()
	if done != reported {
		progress.Add(done - reported)
	}
}
