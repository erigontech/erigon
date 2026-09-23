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

package network

import (
	"fmt"
	"testing"
)

func BenchmarkBlobHistoryDownloaderAddSparseRetrySlots(b *testing.B) {
	for _, failures := range []int{1024, 2048, 4096} {
		b.Run(fmt.Sprintf("failures_%d", failures), func(b *testing.B) {
			b.ReportMetric(float64(failures), "failures/op")
			for b.Loop() {
				downloader := &BlobHistoryDownloader{}
				for i := range failures {
					downloader.addRetrySlot(uint64(i)*1_000_000 + 1)
				}
			}
		})
	}
}
