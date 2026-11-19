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

package network

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
)

type slowFailingRanges struct {
	inFlight, maxInFlight, calls atomic.Int32
}

func (s *slowFailingRanges) SendBeaconBlocksByRangeReq(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
	s.calls.Add(1)
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		m := s.maxInFlight.Load()
		if n <= m || s.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	select {
	case <-time.After(time.Second):
	case <-ctx.Done():
	}
	return nil, "peer", errors.New("i/o deadline exceeded")
}

func TestRequestMoreKeepsOneRangeRequestInFlight(t *testing.T) {
	ranges := &slowFailingRanges{}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	f := &ForwardBeaconDownloader{ctx: ctx, ranges: ranges}

	f.RequestMore(ctx)

	require.EqualValues(t, 1, ranges.maxInFlight.Load(), "concurrent range requests to the peer")
	require.Positive(t, ranges.calls.Load())
}
