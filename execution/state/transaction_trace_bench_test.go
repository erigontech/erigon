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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkTransactionTraceEvents(b *testing.B) {
	for _, count := range []int{0, 1, 100} {
		b.Run(fmt.Sprintf("logs=%d", count), func(b *testing.B) {
			ibs := New(NewNoopReader())
			b.Cleanup(ibs.Close)
			ctx := ibs.NewFrameContext(&types.FrameTransaction{Frames: []types.Frame{{Mode: types.FrameAssert}}}, uint256.Int{})
			log := &types.Log{Topics: make([]common.Hash, 4), Data: make([]byte, 1024)}
			for range count {
				ibs.AddLog(log)
			}
			b.ReportAllocs()
			for b.Loop() {
				ctx.trace = nil
				if _, err := ctx.TransactionTrace(ibs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
