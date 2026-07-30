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

package freezeblocks

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Every read path must reach the visible set through one pointer load, so readers can
// run lock-free against a concurrent recalc. Under -race this fails on any unguarded
// field read — FrozenBlobs took none before the generation was published atomically.
func TestCaplinSnapshotsConcurrentRecalcAndReaders(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	// DenebForkEpoch must be set, else FrozenBlobs returns before touching the set.
	beaconCfg := &clparams.BeaconChainConfig{DenebForkEpoch: 0}
	s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, beaconCfg, dirs, log.New())
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	const rounds = 200
	var wg sync.WaitGroup
	wg.Go(func() {
		for range rounds {
			s.recalcVisibleFiles()
		}
	})
	wg.Go(func() {
		for range rounds {
			require.Zero(t, s.FrozenBlobs())
			require.Zero(t, s.IndicesMax())
			s.BlocksAvailable()
		}
	})
	wg.Go(func() {
		for range rounds {
			view := s.View()
			require.Empty(t, view.BeaconBlocks())
			view.Close()
		}
	})
	wg.Wait()
}

// idxMax must travel with the generation it was computed from, so a reader that sees
// the new segment set never pairs it with the previous set's height.
func TestCaplinSnapshotsIdxMaxMatchesItsGeneration(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	s := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.BeaconChainConfig{}, dirs, log.New())
	defer s.Close()
	require.NoError(t, s.OpenFolder())

	v := s.visible.Load()
	require.Len(t, v.segments, int(snaptype.MaxEnum))
	require.Equal(t, caplinIdxAvailability(v.segments), v.idxMax)
	require.Equal(t, v.idxMax, s.IndicesMax())
}
