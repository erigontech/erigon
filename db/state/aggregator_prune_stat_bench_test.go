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
	"fmt"
	"testing"
)

func BenchmarkAggregatorPruneStatString(b *testing.B) {
	as := newAggregatorPruneStat()
	for i := range 7 {
		as.Domains[fmt.Sprintf("domain%d", i)] = &DomainPruneStat{Values: 10}
	}
	for i := range 5 {
		as.Indices[fmt.Sprintf("index%d", i)] = &InvertedIndexPruneStat{PruneCountValues: 10}
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = as.String()
	}
}
