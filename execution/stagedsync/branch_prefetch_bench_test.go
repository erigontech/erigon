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

package stagedsync

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
)

func BenchmarkBranchCarryMerge(b *testing.B) {
	const perBlock = 5000
	data := make([]byte, 430)
	key := func(block, i int) []byte {
		k := make([]byte, 12)
		binary.BigEndian.PutUint32(k, uint32(block))
		binary.BigEndian.PutUint64(k[4:], uint64(i))
		return k
	}
	deltasFor := func(block int) [][]commitment.BranchDelta {
		part := make([]commitment.BranchDelta, perBlock)
		for i := range part {
			part[i] = commitment.BranchDelta{Key: key(block, i), Data: data}
		}
		return [][]commitment.BranchDelta{part}
	}
	p := newPrefetchRecords()
	merge := func(block int) {
		ov := overlayOf(deltasFor(block), uint64(block), 0)
		p.overlay.Store(&ov)
		p.overlayBlock = uint64(block)
		p.mergeOverlay()
	}
	block := 1
	for ; block <= branchCarryBlocks; block++ {
		merge(block)
	}
	for b.Loop() {
		merge(block)
		block++
	}
}
