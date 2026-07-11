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
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestViewHotPathNoExtraAllocs gates the reader hot path against allocation regressions from
// the generic visibleGenerations core: acquire/currentPayload/release must monomorphize to
// concrete types and box nothing. The only heap allocations are the ones that predate the
// refactor — the *View / *RoTx values and the standalone-release closure. A jump means the
// payload started escaping through an interface.
func TestViewHotPathNoExtraAllocs(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	// View: the *View plus one *RoTx per type. Pinning the generation and reading its payload
	// add nothing.
	wantView := float64(1 + len(s.enums))
	gotView := testing.AllocsPerRun(1000, func() {
		s.View().Close()
	})
	require.Equal(t, wantView, gotView, "View must allocate only the *View and one *RoTx per type")

	// Standalone RoTx: one *RoTx plus its release closure.
	gotType := testing.AllocsPerRun(1000, func() {
		s.ViewType(snaptype2.Headers).Close()
	})
	require.Equal(t, float64(2), gotType, "ViewType must allocate only the *RoTx and its release closure")

	gotSingle := testing.AllocsPerRun(1000, func() {
		_, _, closeFn := s.ViewSingleFile(snaptype2.Headers, 0)
		closeFn()
	})
	require.Equal(t, float64(2), gotSingle, "ViewSingleFile must not allocate beyond ViewType")
}

func BenchmarkViewHotPath(b *testing.B) {
	logger := log.New()
	dir := b.TempDir()
	s := NewBaseRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dir, snaptype2.BlockSnapshotTypes, true, logger)
	defer s.Close()

	b.Run("View", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.View().Close()
		}
	})
	b.Run("ViewType", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s.ViewType(snaptype2.Headers).Close()
		}
	})
}
