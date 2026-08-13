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
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// tickHarness drives retirementTick without a ticker: each step counts its calls and
// returns whatever the test tells it to.
type tickHarness struct {
	a            *Antiquary
	blocks       *retirementStep
	blobs        *retirementStep
	blocksRuns   int
	blobsRuns    int
	blocksFails  bool
	blobsFails   bool
	cancel       context.CancelFunc
	denebEnabled bool
}

func newTickHarness(t *testing.T) *tickHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	h := &tickHarness{cancel: cancel, denebEnabled: true}
	backfilled, blobBackfilled := &atomic.Bool{}, &atomic.Bool{}
	backfilled.Store(true)
	blobBackfilled.Store(true)

	h.a = &Antiquary{
		ctx:            ctx,
		backfilled:     backfilled,
		blobBackfilled: blobBackfilled,
		cfg:            &clparams.BeaconChainConfig{DenebForkEpoch: 0},
	}
	h.blocks = &retirementStep{
		run: func() error {
			h.blocksRuns++
			if h.blocksFails {
				return errors.New("blocks failed")
			}
			return nil
		},
		onError: func(error) {},
	}
	h.blobs = &retirementStep{
		run: func() error {
			h.blobsRuns++
			if h.blobsFails {
				return errors.New("blobs failed")
			}
			return nil
		},
		onError: func(error) {},
	}
	return h
}

func (h *tickHarness) tick(n int) {
	for range n {
		h.a.retirementTick(h.blocks, h.blobs)
	}
}

// A step that keeps failing must not drag its sibling down with it.
func TestRetirementTickBacksOffEachStepIndependently(t *testing.T) {
	h := newTickHarness(t)
	h.blocksFails = true

	h.tick(6)

	require.Equal(t, 6, h.blobsRuns, "a succeeding step must run on every tick")
	require.Less(t, h.blocksRuns, 6, "a failing step must be skipped on some ticks")
	require.Positive(t, h.blocksRuns)
}

func TestRetirementTickBacksOffBlobsIndependently(t *testing.T) {
	h := newTickHarness(t)
	h.blobsFails = true

	h.tick(6)

	require.Equal(t, 6, h.blocksRuns)
	require.Less(t, h.blobsRuns, 6)
	require.Positive(t, h.blobsRuns)
}

// Success on one step must not clear the other's accumulated backoff.
func TestRetirementTickSuccessResetsOnlyItsOwnStep(t *testing.T) {
	h := newTickHarness(t)
	h.blobsFails = true
	h.tick(8)
	blobsAfterFailures := h.blobsRuns

	h.blocksFails = false
	h.tick(2)

	require.Equal(t, blobsAfterFailures, h.blobsRuns,
		"blocks succeeding must not reset the blobs backoff")
}

func TestRetirementTickRecoversAfterSuccess(t *testing.T) {
	h := newTickHarness(t)
	h.blobsFails = true
	h.tick(8)

	h.blobsFails = false
	// Enough ticks for the accumulated gap to elapse, then it must run every tick.
	h.tick(8)
	before := h.blobsRuns
	h.tick(3)

	require.Equal(t, before+3, h.blobsRuns)
}

func TestRetirementTickSkipsBlobsBeforeDeneb(t *testing.T) {
	h := newTickHarness(t)
	h.a.cfg = &clparams.BeaconChainConfig{DenebForkEpoch: math.MaxUint64}

	h.tick(3)

	require.Equal(t, 3, h.blocksRuns)
	require.Zero(t, h.blobsRuns, "blobs must not be antiquated before Deneb")
}

func TestRetirementTickWaitsForBackfill(t *testing.T) {
	h := newTickHarness(t)
	h.a.backfilled.Store(false)
	h.tick(3)
	require.Zero(t, h.blocksRuns)
	require.Zero(t, h.blobsRuns)

	h.a.backfilled.Store(true)
	h.a.blobBackfilled.Store(false)
	h.tick(3)
	require.Equal(t, 3, h.blocksRuns)
	require.Zero(t, h.blobsRuns)
}

// A failure during shutdown is not the step's fault, so it must not be counted.
func TestRetirementTickDoesNotBackOffOnShutdown(t *testing.T) {
	h := newTickHarness(t)
	h.blocksFails = true
	h.blobsFails = true
	h.cancel()

	h.tick(4)

	require.Equal(t, 4, h.blocksRuns)
	require.Equal(t, 4, h.blobsRuns)
}
