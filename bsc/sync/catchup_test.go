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

package bscsync

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
)

func TestCatchUpFollowsMovingAnnouncedHead(t *testing.T) {
	t.Parallel()
	network := fakeChain(6000)
	chainRW := newFakeChainRW(network[:101])
	cfg := Config{ChainRW: chainRW, Svc: &fakeP2P{peers: 2, chain: network}, FcuInterval: 1000}

	announced := func() (uint64, bool) { return min(5999, chainRW.headNum()+2000), true }
	head, err := catchUp(t.Context(), testlog.Logger(t, log.LvlCrit), cfg, announced)
	require.NoError(t, err)

	require.Equal(t, uint64(5999-tipHandoverDistance), head.Number.Uint64())
	require.Equal(t, network[5999-tipHandoverDistance].Hash(), chainRW.lastFcu().head)
	require.Equal(t, head.Hash(), chainRW.CurrentHeader(t.Context()).Hash())
}

func TestCatchUpNearHeadDoesNothing(t *testing.T) {
	t.Parallel()
	network := fakeChain(1000)
	chainRW := newFakeChainRW(network[:970])
	cfg := Config{ChainRW: chainRW, Svc: &fakeP2P{peers: 2, chain: network}}

	head, err := catchUp(t.Context(), testlog.Logger(t, log.LvlCrit), cfg, func() (uint64, bool) { return 999, true })
	require.NoError(t, err)
	require.Equal(t, uint64(969), head.Number.Uint64())
	require.Zero(t, chainRW.numInserted())
}

func TestCatchUpWaitsForAnnouncedHead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		network := fakeChain(3000)
		chainRW := newFakeChainRW(network[:1])
		cfg := Config{ChainRW: chainRW, Svc: &fakeP2P{peers: 1, chain: network}}

		start := time.Now()
		announced := func() (uint64, bool) {
			if time.Since(start) < 10*time.Second {
				return 0, false
			}
			return 2999, true
		}
		head, err := catchUp(t.Context(), testlog.Logger(t, log.LvlCrit), cfg, announced)
		require.NoError(t, err)
		require.Equal(t, uint64(2999-tipHandoverDistance), head.Number.Uint64())
		require.GreaterOrEqual(t, time.Since(start), 10*time.Second)
	})
}

func TestRunParallelStopsAtTargetBlock(t *testing.T) {
	t.Parallel()
	network := fakeChain(4000)
	chainRW := newFakeChainRW(network[:1])
	cfg := Config{ChainRW: chainRW, Svc: &fakeP2P{peers: 2, chain: network}, TargetBlock: 2500, FcuInterval: 1000}

	require.NoError(t, runParallel(t.Context(), testlog.Logger(t, log.LvlCrit), cfg))
	require.Equal(t, uint64(2500), chainRW.headNum())
	require.Equal(t, 2500, chainRW.numInserted())
}

func TestCatchUpAnnouncedHeadDropsBelowCursor(t *testing.T) {
	t.Parallel()
	network := fakeChain(4000)
	chainRW := newFakeChainRW(network[:1])
	cfg := Config{ChainRW: chainRW, Svc: &fakeP2P{peers: 2, chain: network}}

	calls := 0
	announced := func() (uint64, bool) {
		calls++
		if calls < 5 {
			return 3999, true
		}
		return 1000, true
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	head, err := catchUp(ctx, testlog.Logger(t, log.LvlCrit), cfg, announced)
	require.NoError(t, err)
	require.Equal(t, uint64(3999-tipHandoverDistance), head.Number.Uint64())
}
