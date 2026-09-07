// Copyright 2024 The Erigon Authors
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

package snapshotsync

import (
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func BenchmarkFindMergeRange(t *testing.B) {
	merger := NewMerger("x", 1, log.LvlInfo, nil, chainspec.Mainnet.Config, nil)
	merger.DisableFsync()
	t.Run("big", func(t *testing.B) {
		for j := 0; j < t.N; j++ {
			var RangesOld []Range
			for i := range 24 {
				RangesOld = append(RangesOld, NewRange(uint64(i*100_000), uint64((i+1)*100_000)))
			}
			merger.FindMergeRanges(RangesOld, uint64(24*100_000))

			var RangesNew []Range
			start := uint64(19_000_000)
			for i := range uint64(24) {
				RangesNew = append(RangesNew, NewRange(start+(i*100_000), start+((i+1)*100_000)))
			}
			merger.FindMergeRanges(RangesNew, uint64(24*100_000))
		}
	})

	t.Run("small", func(t *testing.B) {
		for j := 0; j < t.N; j++ {
			var RangesOld Ranges
			for i := range uint64(240) {
				RangesOld = append(RangesOld, NewRange(i*10_000, (i+1)*10_000))
			}
			merger.FindMergeRanges(RangesOld, uint64(240*10_000))

			var RangesNew Ranges
			start := uint64(19_000_000)
			for i := range uint64(240) {
				RangesNew = append(RangesNew, NewRange(start+i*10_000, start+(i+1)*10_000))
			}
			merger.FindMergeRanges(RangesNew, uint64(240*10_000))
		}
	})

}
