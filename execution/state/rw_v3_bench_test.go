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

	"github.com/erigontech/erigon/execution/notifications"
)

func BenchmarkNotifyAccumulatorEmptyWriteSets(b *testing.B) {
	rs := &StateV3{}
	accumulator := notifications.NewAccumulator()
	// Empty views isolate per-transaction notification overhead without database reads.
	writes := make([]WriteSetView, 1000)
	for i := range writes {
		writes[i] = NewVersionMapWriteView(&WriteSet{}, nil, i)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := rs.NotifyAccumulator(accumulator, nil, writes); err != nil {
			b.Fatal(err)
		}
	}
}
