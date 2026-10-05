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

package antiquary

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestNextBlobSegment(t *testing.T) {
	// Deneb at slot 132,608*32 = 4,243,456, so the lowest blob segment starts at 4,240,000.
	cfg := &clparams.BeaconChainConfig{DenebForkEpoch: 132_608, SlotsPerEpoch: 32}
	for _, tc := range []struct {
		name                      string
		frozenBlobs, frozenBlocks uint64
		from, to, backlogTo       uint64
		ok                        bool
	}{
		{name: "blobs caught up with blocks", frozenBlobs: 11_250_000, frozenBlocks: 11_249_999},
		// BlocksAvailable is the last frozen slot and FrozenBlobs the end of the last blob
		// segment, so one segment behind reads as 9,999 slots and is not yet retirable.
		{name: "one slot short of a segment", frozenBlobs: 11_240_000, frozenBlocks: 11_249_999},
		{
			name: "exactly one segment", frozenBlobs: 11_240_000, frozenBlocks: 11_250_000,
			from: 11_240_000, to: 11_250_000, backlogTo: 11_250_000, ok: true,
		},
		{
			name: "a backlog retires only its first segment", frozenBlobs: 10_470_000, frozenBlocks: 11_189_999,
			from: 10_470_000, to: 10_480_000, backlogTo: 11_189_999, ok: true,
		},
		{
			name: "no blob segments yet starts at the Deneb segment", frozenBlobs: 0, frozenBlocks: 4_279_999,
			from: 4_240_000, to: 4_250_000, backlogTo: 4_279_999, ok: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, to, backlogTo, ok := nextBlobSegment(tc.frozenBlobs, tc.frozenBlocks, cfg)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.from, from)
			require.Equal(t, tc.to, to)
			require.Equal(t, tc.backlogTo, backlogTo)
		})
	}
}

// Compression parallelism must follow the backlog, not the one segment dumped from it: a
// single segment on its own reads as tip retirement and would compress a catch-up on one worker.
func TestNextBlobSegmentKeepsTheBacklogForCompressionParallelism(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{DenebForkEpoch: 132_608, SlotsPerEpoch: 32}

	from, to, backlogTo, ok := nextBlobSegment(10_470_000, 11_189_999, cfg)

	require.True(t, ok)
	require.False(t, isBlobBacklog(from, to))
	require.True(t, isBlobBacklog(from, backlogTo))
}
