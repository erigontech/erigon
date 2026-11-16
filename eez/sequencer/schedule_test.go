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

package sequencer_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/sequencer"
)

// TestSchedule is T-SEQ-1 for the schedule: each L1 head anchors the slot
// whose Sync block has the next L1 block's timestamp, its trigger at the end
// of the Live blocks; a newer head supersedes an armed trigger; ticks wait
// for the first anchor.
func TestSchedule(t *testing.T) {
	const genesis = 1_000
	s := sequencer.NewSchedule(mainnet(), genesis)
	_, ok := s.Tick()
	require.False(t, ok, "no tick before the first anchor")
	_, ok = s.Next()
	require.False(t, ok)

	// Sync timestamp 1000+120+12 = 1132, height 66, trigger at 1126.
	require.Nil(t, s.Head(l1.Header{Number: 7, Time: genesis + 120}))
	at, ok := s.Next()
	require.True(t, ok)
	require.Equal(t, time.Unix(genesis+126, 0), at)
	tick, ok := s.Tick()
	require.True(t, ok)
	require.Equal(t, sequencer.LiveTick{SyncHeight: 66}, tick)

	stale := s.Head(l1.Header{Number: 8, Time: genesis + 132})
	require.Equal(t, &sequencer.SyncSlot{Height: 66, Time: genesis + 132, L1Head: 7}, stale)
	require.Equal(t, sequencer.SyncSlot{Height: 72, Time: genesis + 144, L1Head: 8}, s.Fire())
	_, ok = s.Next()
	require.False(t, ok, "fired")
	tick, _ = s.Tick()
	require.Equal(t, uint64(72), tick.SyncHeight, "ticks keep the latest anchor")

	// A genesis off the L1 slot boundary puts the Sync heights off K's
	// multiples.
	s = sequencer.NewSchedule(mainnet(), genesis+2)
	s.Head(l1.Header{Number: 7, Time: genesis + 120})
	require.Equal(t, sequencer.SyncSlot{Height: 65, Time: genesis + 132, L1Head: 7}, s.Fire())
}

// TestScheduleRun: a late head's trigger fires at once, and Live ticks
// follow on the L2 cadence.
func TestScheduleRun(t *testing.T) {
	tm := timing(2_000, 1_000, 500, 100)
	now := uint64(time.Now().Unix())
	s := sequencer.NewSchedule(tm, now-100)
	heads := make(chan l1.Header, 1)
	out := make(chan sequencer.Event, 4)
	go func() { _ = s.Run(t.Context(), heads, out, log.New()) }()

	heads <- l1.Header{Number: 3, Time: now - 10}
	next := func() sequencer.Event {
		select {
		case e := <-out:
			return e
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
			return nil
		}
	}
	require.Equal(t, sequencer.SyncSlot{Height: 92, Time: now - 8, L1Head: 3}, next())
	require.Equal(t, sequencer.LiveTick{SyncHeight: 92}, next())
}

// TestScheduleRefusesGenesisOffTheL1Grid: an L1 head off the slot grid of the
// genesis stops the schedule, whose Sync blocks would miss the L1 slots; one
// on it arms a trigger.
func TestScheduleRefusesGenesisOffTheL1Grid(t *testing.T) {
	const genesis = 1_000
	for _, c := range []struct {
		time uint64
		ok   bool
	}{{genesis + 120, true}, {genesis + 122, false}} {
		ctx, cancel := context.WithCancel(t.Context())
		heads := make(chan l1.Header, 1)
		heads <- l1.Header{Number: 7, Time: c.time}
		out := make(chan sequencer.Event, 4)
		done := make(chan error, 1)
		go func() { done <- sequencer.NewSchedule(mainnet(), genesis).Run(ctx, heads, out, log.New()) }()
		if c.ok {
			select {
			case e := <-out:
				require.IsType(t, sequencer.SyncSlot{}, e)
			case <-time.After(5 * time.Second):
				t.Fatal("no trigger")
			}
			cancel()
			require.NoError(t, <-done)
		} else {
			require.ErrorIs(t, <-done, sequencer.ErrTiming)
			cancel()
		}
	}
}
