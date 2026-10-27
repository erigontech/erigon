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

package base_encoding

import (
	"io"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const benchmarkQueueLength = 100_000

func newBenchmarkPendingDeposit(i int) *solid.PendingDeposit {
	return &solid.PendingDeposit{Amount: uint64(i), Slot: uint64(i)}
}

func benchmarkPendingDepositList(values []*solid.PendingDeposit) *solid.ListSSZ[*solid.PendingDeposit] {
	list := solid.NewStaticListSSZ[*solid.PendingDeposit](2*benchmarkQueueLength, solid.SizePendingDeposit)
	for _, value := range values {
		list.Append(value)
	}
	return list
}

func BenchmarkSSZQueueEncoderPendingDeposits(b *testing.B) {
	previousValues := make([]*solid.PendingDeposit, benchmarkQueueLength)
	for i := range previousValues {
		previousValues[i] = newBenchmarkPendingDeposit(i)
	}
	appended := func(count int) []*solid.PendingDeposit {
		values := make([]*solid.PendingDeposit, count)
		for i := range values {
			values[i] = newBenchmarkPendingDeposit(benchmarkQueueLength + i)
		}
		return values
	}
	changedFirst := *previousValues[0]
	changedFirst.Amount++

	cases := []struct {
		name    string
		updated []*solid.PendingDeposit
	}{
		{name: "unchanged", updated: previousValues},
		{name: "append16", updated: slices.Concat(previousValues, appended(16))},
		{name: "pop16append4", updated: slices.Concat(previousValues[16:], appended(4))},
		{name: "setFirstFallback", updated: append([]*solid.PendingDeposit{&changedFirst}, previousValues[1:]...)},
	}
	previous := benchmarkPendingDepositList(previousValues)
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			updated := benchmarkPendingDepositList(c.updated)
			encoder := NewSSZQueueEncoder(func(a, b *solid.PendingDeposit) bool { return *a == *b })
			b.ReportAllocs()
			for b.Loop() {
				encoder.Initialize(previous)
				if err := encoder.WriteDiff(io.Discard, updated); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
