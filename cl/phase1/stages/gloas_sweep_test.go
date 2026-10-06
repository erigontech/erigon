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

package stages

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestSweepUntilHeadSettlesRepeatsWhileHeadAdvances(t *testing.T) {
	heads := []common.Hash{{1}, {2}, {2}, {3}, {3}, {3}}
	head := func() (common.Hash, error) {
		h := heads[0]
		if len(heads) > 1 {
			heads = heads[1:]
		}
		return h, nil
	}
	sweeps := 0
	sweepUntilHeadSettles(context.Background(), head, func(context.Context) { sweeps++ })
	require.Equal(t, 3, sweeps)
}

func TestSweepUntilHeadSettlesStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sweeps := 0
	sweepUntilHeadSettles(ctx, func() (common.Hash, error) { return common.Hash{1}, nil }, func(context.Context) { sweeps++ })
	require.Equal(t, 0, sweeps)
}

func TestSweepUntilHeadSettlesStopsOnHeadError(t *testing.T) {
	sweeps := 0
	sweepUntilHeadSettles(context.Background(), func() (common.Hash, error) { return common.Hash{}, errors.New("no head") }, func(context.Context) { sweeps++ })
	require.Equal(t, 0, sweeps)
}
