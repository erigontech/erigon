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
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestBeaconStateReadersWaitForStateLock(t *testing.T) {
	tests := map[string]func(*BeaconState) error{
		"copy": func(source *BeaconState) error {
			return source.CopyInto(New(source.BeaconConfig()))
		},
		"current sync committee branch": func(source *BeaconState) error {
			_, err := source.CurrentSyncCommitteeBranch()
			return err
		},
		"next sync committee branch": func(source *BeaconState) error {
			_, err := source.NextSyncCommitteeBranch()
			return err
		},
		"finality root branch": func(source *BeaconState) error {
			_, err := source.FinalityRootBranch()
			return err
		},
	}

	for name, read := range tests {
		t.Run(name, func(t *testing.T) {
			source := New(&clparams.MainnetBeaconConfig)
			source.mu.Lock()
			done := make(chan error, 1)
			go func() { done <- read(source) }()

			select {
			case err := <-done:
				t.Fatalf("reader completed while state lock was held: %v", err)
			case <-time.After(20 * time.Millisecond):
			}

			source.mu.Unlock()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("reader did not complete after state lock was released")
			}
		})
	}
}

func TestBeaconStateCopyIntoSharesReadLock(t *testing.T) {
	source := New(&clparams.MainnetBeaconConfig)
	source.mu.RLock()
	defer source.mu.RUnlock()
	done := make(chan error, 1)
	go func() { done <- source.CopyInto(New(source.BeaconConfig())) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("copy waited for a read lock held by another reader")
	}
}

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
