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

package types

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
)

func receiptsOfBlock(receipts, logsPerReceipt int, stamped bool) []Logs {
	out := make([]Logs, receipts)
	for i := range out {
		for j := range logsPerReceipt {
			out[i] = append(out[i], &Log{Address: common.Address{byte(j % 2)}, Topics: make([]common.Hash, 3), Data: make([]byte, 32)})
		}
		if stamped {
			out[i].SetBlockTimestamp(7)
		}
	}
	return out
}

func BenchmarkAppendFilteredLogsOfBlock(b *testing.B) {
	f := NewLogFilter([]common.Address{{0}}, nil)
	for _, n := range []int{20, 150, 400} {
		for _, stamped := range []bool{false, true} {
			block := receiptsOfBlock(n, 2, stamped)
			b.Run(fmt.Sprintf("receipts=%d/stamped=%v", n, stamped), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var dst Logs
					for _, r := range block {
						dst = r.AppendFilteredLogs(dst, f, 7, 0)
					}
				}
			})
		}
	}
}
