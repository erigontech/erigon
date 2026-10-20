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

package raw

import (
	"sync"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestBeaconStateCopyIntoDeepCopiesPtcWindow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	source := New(&cfg)
	source.SetVersion(clparams.GloasVersion)
	source.GetPtcWindow().Get(0).Set(0, 11)
	target := New(&cfg)
	targetInner := target.GetPtcWindow().Get(0)
	sourceRoot, err := source.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	_, err = target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)

	require.NoError(t, source.CopyInto(target))
	require.Same(t, targetInner, target.GetPtcWindow().Get(0))
	require.NotSame(t, source.GetPtcWindow().Get(0), target.GetPtcWindow().Get(0))
	targetRoot, err := target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, sourceRoot, targetRoot)
	target.GetPtcWindow().Get(0).Set(0, 22)
	target.GetPtcWindow().Set(0, target.GetPtcWindow().Get(0))

	require.Equal(t, uint64(11), source.GetPtcWindow().Get(0).Get(0))
	require.Equal(t, uint64(22), target.GetPtcWindow().Get(0).Get(0))
	unchangedSourceRoot, err := source.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, sourceRoot, unchangedSourceRoot)
	mutatedTargetRoot, err := target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	source.GetPtcWindow().Get(0).Set(0, 33)
	source.GetPtcWindow().Set(0, source.GetPtcWindow().Get(0))
	unchangedTargetRoot, err := target.GetPtcWindow().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, mutatedTargetRoot, unchangedTargetRoot)
}

func TestBeaconStateCopyIntoPtcWindowConcurrentFirstHash(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	source := New(&cfg)
	source.SetVersion(clparams.GloasVersion)
	target := New(&cfg)
	require.NoError(t, source.CopyInto(target))

	var waitGroup sync.WaitGroup
	errs := make(chan error, 2)
	for _, state := range []*BeaconState{source, target} {
		waitGroup.Go(func() {
			_, err := state.GetPtcWindow().HashSSZ()
			errs <- err
		})
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
