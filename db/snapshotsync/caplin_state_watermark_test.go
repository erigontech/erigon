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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// The availability watermarks are read from the published generation payload (I4): SegmentsMax
// is the max .seg height (to-1), IndicesMax the min visible .idx height (to), and
// BlocksAvailable their min. A single covered type pins concrete values.
func TestCaplinStateWatermarksFromPublishedGeneration(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 100_000, logger)
	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	require.Equal(t, uint64(99_999), s.SegmentsMax(), "SegmentsMax is the max .seg height (to-1)")
	require.Equal(t, uint64(100_000), s.IndicesMax(), "IndicesMax is the min visible .idx height (to)")
	require.Equal(t, uint64(99_999), s.BlocksAvailable(), "BlocksAvailable = min(SegmentsMax, IndicesMax)")
}

// A configured state type with no visible segment caps availability at 0, even when another
// configured type is fully covered: idxAvailabilityFrom returns 0 for the empty type, so both
// IndicesMax and BlocksAvailable collapse to 0. Guards the I4 min-across-configured-types edge.
func TestCaplinStateWatermarkCapsAtZeroForUncoveredType(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	present := kv.BlockRoot
	absent := kv.StateRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, present, 0, 100_000, logger)

	types := SnapshotTypes{
		KeyValueGetters: map[CaplinStateType]KeyValueGetter{
			mustCaplinStateType(t, present): nil,
			mustCaplinStateType(t, absent):  nil,
		},
		Compression: map[CaplinStateType]bool{},
	}
	s := NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, nil, dirs, types, logger)
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())

	require.Zero(t, s.IndicesMax(), "an uncovered configured type must cap IndicesMax at 0")
	require.Zero(t, s.BlocksAvailable(), "BlocksAvailable must be 0 when any configured type has no segment")
}

// idxAvailabilityFrom is the I4-critical watermark computed from the candidate bundle before
// publish. Exercise it directly: min .to across configured types, and 0 whenever a configured
// type is empty or the type list is empty.
func TestIdxAvailabilityFrom(t *testing.T) {
	a, b := CaplinStateType(0), CaplinStateType(1)
	segs := func(fill map[CaplinStateType]VisibleSegments) []VisibleSegments {
		out := make([]VisibleSegments, caplinStateTypeCount)
		for k, v := range fill {
			out[k] = v
		}
		return out
	}
	vs := func(to uint64) VisibleSegments {
		return VisibleSegments{{Range: Range{from: 0, to: to}}}
	}

	require.Equal(t, uint64(100), idxAvailabilityFrom(segs(map[CaplinStateType]VisibleSegments{a: vs(100), b: vs(150)}), []CaplinStateType{a, b}),
		"min .to across configured types")
	require.Zero(t, idxAvailabilityFrom(segs(map[CaplinStateType]VisibleSegments{a: vs(100)}), []CaplinStateType{a, b}),
		"a configured type with no visible segment caps at 0")
	require.Zero(t, idxAvailabilityFrom(segs(nil), nil),
		"no configured types yields 0")
}
