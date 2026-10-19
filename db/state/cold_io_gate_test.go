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

package state

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitReturns(g *coldIOGate, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- g.Wait(ctx) }()
	return done
}

func TestColdIOGateBlocksWhilePaused(t *testing.T) {
	t.Parallel()
	g := newColdIOGate()
	require.NoError(t, g.Wait(t.Context()))

	g.Pause()
	g.Pause()
	done := waitReturns(g, t.Context())
	g.Resume()
	select {
	case <-done:
		t.Fatal("Wait returned while one pause is still held")
	case <-time.After(50 * time.Millisecond):
	}
	g.Resume()
	require.NoError(t, <-done)

	g.Resume()
	require.NoError(t, g.Wait(t.Context()))
}

func TestColdIOGateWaitReturnsOnCancel(t *testing.T) {
	t.Parallel()
	g := newColdIOGate()
	g.Pause()
	ctx, cancel := context.WithCancel(t.Context())
	done := waitReturns(g, ctx)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestAggregatorDemotionCopyWaitsWhileColdIOPaused(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithRanges(t, []testFileRange{{0, 1}, {1, 2}})
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.0-1.kv")
	coldDir := t.TempDir()

	pausedBefore := mxDemotionPausedSeconds.GetValue()
	at := agg.BeginFilesRo()
	defer at.Close()
	resume := at.PauseColdIO()
	require.True(t, agg.DemoteInBackground(agg.StepSize(), coldDir))
	time.Sleep(100 * time.Millisecond)
	_, err := os.Stat(filepath.Join(coldDir, filepath.Base(ssdPath)))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.False(t, isSymlink(t, ssdPath))

	resume()
	waitDemotionIdle(t, agg)
	require.True(t, isSymlink(t, ssdPath))
	require.Positive(t, mxDemotionPausedSeconds.GetValue()-pausedBefore)
}
