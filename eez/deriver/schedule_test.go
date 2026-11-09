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

package deriver

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/execution/types"
)

// scheduleFixture is the fixture of eez-rollupX a50c326
// crates/eez-protocol/src/l2/derivation.rs: reference and genesis at time
// 1000.
func scheduleFixture(chainID, step, positions uint64) (*config.RollupXConfig, *types.Header) {
	seed := common.Hash{31: 7}
	return &config.RollupXConfig{
			BlockIntervalSeconds: step,
			SyncIntervalBlocks:   positions,
			L1Reference:          config.L1Reference{ChainID: chainID, Number: 10, Hash: common.Hash{31: 10}, Timestamp: 1000, PrevRandao: seed},
		},
		&types.Header{Time: 1000, MixDigest: seed}
}

func header(number, time uint64) *types.Header {
	return &types.Header{Number: *uint256.NewInt(number), Time: time}
}

func TestScheduleIntervals(t *testing.T) {
	for _, tc := range []struct{ chain, step, positions uint64 }{{1, 2, 6}, {10200, 1, 5}} {
		x, genesis := scheduleFixture(tc.chain, tc.step, tc.positions)
		s, err := NewSchedule(x, genesis)
		require.NoError(t, err)
		slot := tc.step * tc.positions
		for n := uint64(1); n <= tc.positions*3; n++ {
			ts, err := s.Timestamp(n)
			require.NoError(t, err)
			require.Equal(t, 1000+n*tc.step, ts)
		}
		end, kind, err := s.Candidate(genesis, tc.positions, 1000+slot, true)
		require.NoError(t, err)
		require.Equal(t, tc.positions, end)
		require.Equal(t, LiveAnchor, kind)
		_, kind, err = s.Candidate(genesis, tc.positions*2, 1000+slot*3, false)
		require.NoError(t, err)
		require.Equal(t, CatchUpAnchor, kind)

		_, _, err = s.Candidate(genesis, tc.positions*2, 1000+slot*3, true)
		require.ErrorIs(t, err, ErrSchedule, "catch-up with actions")
		for _, blocks := range []uint64{0, 1, tc.positions - 1, tc.positions + 1} {
			_, _, err = s.Candidate(genesis, blocks, 1000+slot*3, false)
			require.ErrorIs(t, err, ErrSchedule, "%d blocks", blocks)
		}
		_, _, err = s.Candidate(genesis, tc.positions*2, 1000+slot, false)
		require.ErrorIs(t, err, ErrSchedule, "endpoint after its L1 block")
		_, _, err = s.Candidate(genesis, tc.positions, 1001+slot, false)
		require.ErrorIs(t, err, ErrSchedule, "L1 block off the slots")
		parent := header(1, 1000+tc.step)
		_, _, err = s.Candidate(parent, tc.positions, 1000+slot*2, false)
		require.ErrorIs(t, err, ErrSchedule, "parent not a Sync block")
		require.ErrorIs(t, s.CheckPosition(header(1, 1001+tc.step)), ErrSchedule)
	}
}

func TestScheduleLimits(t *testing.T) {
	x, genesis := scheduleFixture(10200, 1, 5)
	s, err := NewSchedule(x, genesis)
	require.NoError(t, err)
	// A u32 run of empty blocks is a valid wire value.
	end := uint64(1<<32 - 1)
	got, kind, err := s.Candidate(genesis, end, 1000+end, false)
	require.NoError(t, err)
	require.Equal(t, end, got)
	require.Equal(t, LiveAnchor, kind)
	_, err = s.Timestamp(^uint64(0))
	require.ErrorIs(t, err, ErrSchedule)

	for _, mod := range []func(*types.Header){
		func(h *types.Header) { h.Time++ },
		func(h *types.Header) { h.Time-- },
		func(h *types.Header) { h.Number.SetOne() },
		func(h *types.Header) { h.MixDigest = common.Hash{} },
	} {
		_, g := scheduleFixture(10200, 1, 5)
		mod(g)
		_, err := NewSchedule(x, g)
		require.ErrorIs(t, err, config.ErrInvalid)
	}
	other := types.CopyHeader(genesis)
	other.Extra = []byte{1}
	require.ErrorIs(t, s.CheckPosition(other), ErrSchedule, "another block 0")
}
