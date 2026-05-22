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

package snapshotsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func verdictNamed(name string) *MinorityVerdict {
	return &MinorityVerdict{Adopt: []AdvertisementMismatch{{Name: name}}}
}

// TestAdoptionGraceGate_FiresAfterGrace: a verdict that persists for the
// grace window triggers exactly once.
func TestAdoptionGraceGate_FiresAfterGrace(t *testing.T) {
	t.Parallel()
	fired := make(chan *MinorityVerdict, 1)
	g := NewAdoptionGraceGate(60*time.Millisecond, func(v *MinorityVerdict) { fired <- v })

	require.True(t, g.Arm(verdictNamed("a")), "first Arm starts the grace window")

	select {
	case v := <-fired:
		require.Equal(t, "a", v.Adopt[0].Name)
	case <-time.After(2 * time.Second):
		t.Fatal("grace window elapsed but the trigger never fired")
	}
}

// TestAdoptionGraceGate_ClearCancels: a Clear within the window cancels
// the pending trigger — the transient disagreement resolved.
func TestAdoptionGraceGate_ClearCancels(t *testing.T) {
	t.Parallel()
	fired := make(chan *MinorityVerdict, 1)
	// A long grace so the timer cannot fire during the negative wait.
	g := NewAdoptionGraceGate(10*time.Second, func(v *MinorityVerdict) { fired <- v })

	require.True(t, g.Arm(verdictNamed("a")))
	require.True(t, g.Clear(), "Clear cancels the running window")

	select {
	case <-fired:
		t.Fatal("trigger fired despite Clear cancelling the window")
	case <-time.After(200 * time.Millisecond):
	}

	require.False(t, g.Clear(), "Clear is a no-op once nothing is pending")
}

// TestAdoptionGraceGate_ReArmKeepsDeadlineAndLatestVerdict: a second Arm
// inside the window does not restart the timer, and the trigger fires
// with the most recent verdict.
func TestAdoptionGraceGate_ReArmKeepsDeadlineAndLatestVerdict(t *testing.T) {
	t.Parallel()
	fired := make(chan *MinorityVerdict, 1)
	g := NewAdoptionGraceGate(80*time.Millisecond, func(v *MinorityVerdict) { fired <- v })

	require.True(t, g.Arm(verdictNamed("first")))
	require.False(t, g.Arm(verdictNamed("second")), "re-Arm within the window does not start a new one")

	select {
	case v := <-fired:
		require.Equal(t, "second", v.Adopt[0].Name, "the trigger fires with the latest verdict")
	case <-time.After(2 * time.Second):
		t.Fatal("trigger never fired")
	}
}

// TestAdoptionGraceGate_ZeroGraceFiresSynchronously: grace 0 disables the
// wait — Arm fires inline, the eager pre-grace behaviour.
func TestAdoptionGraceGate_ZeroGraceFiresSynchronously(t *testing.T) {
	t.Parallel()
	var got *MinorityVerdict
	g := NewAdoptionGraceGate(0, func(v *MinorityVerdict) { got = v })

	require.False(t, g.Arm(verdictNamed("a")), "zero grace starts no window")
	require.NotNil(t, got, "zero grace fires synchronously")
	require.Equal(t, "a", got.Adopt[0].Name)
	require.False(t, g.Clear(), "nothing pending after a synchronous fire")
}
