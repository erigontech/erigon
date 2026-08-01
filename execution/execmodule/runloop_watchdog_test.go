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

package execmodule

import "testing"

func TestWatchdogStep_ProgressResetsCounter(t *testing.T) {
	stuck, last, action := watchdogStep(500, 100, 100)
	if stuck != 501 || last != 100 || action != watchdogContinue {
		t.Fatalf("no-progress step: got (stuck=%d last=%d action=%d), want (501, 100, continue)", stuck, last, action)
	}

	stuck, last, action = watchdogStep(500, 100, 101)
	if stuck != 0 || last != 101 || action != watchdogContinue {
		t.Fatalf("progress advance did not reset: got (stuck=%d last=%d action=%d), want (0, 101, continue)", stuck, last, action)
	}
}

func TestWatchdogStep_WarnFiresExactlyAtThreshold(t *testing.T) {
	stuck, _, action := watchdogStep(runLoopStuckWarn-1, 100, 100)
	if action != watchdogWarn {
		t.Fatalf("expected WARN at threshold-1 → threshold, got action=%d (stuck=%d)", action, stuck)
	}
	if stuck != runLoopStuckWarn {
		t.Fatalf("stuck counter mismatch at warn: got %d, want %d", stuck, runLoopStuckWarn)
	}

	stuck, _, action = watchdogStep(runLoopStuckWarn, 100, 100)
	if action != watchdogContinue {
		t.Fatalf("expected single-fire WARN (no re-warn on next step), got action=%d (stuck=%d)", action, stuck)
	}
}

func TestWatchdogStep_AbortFiresAndStaysFiring(t *testing.T) {
	stuck, _, action := watchdogStep(runLoopStuckAbort-1, 100, 100)
	if action != watchdogAbort {
		t.Fatalf("expected ABORT at threshold-1 → threshold, got action=%d (stuck=%d)", action, stuck)
	}
	if stuck != runLoopStuckAbort {
		t.Fatalf("stuck counter mismatch at abort: got %d, want %d", stuck, runLoopStuckAbort)
	}

	stuck, _, action = watchdogStep(runLoopStuckAbort, 100, 100)
	if action != watchdogAbort {
		t.Fatalf("expected ABORT to stay firing (caller may not have handled it), got action=%d (stuck=%d)", action, stuck)
	}
}

func TestWatchdogStep_G6Repro(t *testing.T) {
	var stuck, last uint64
	last = 3228203
	warns := 0
	for i := range uint64(20_000) {
		var action watchdogAction
		stuck, last, action = watchdogStep(stuck, last, 3228203)
		switch action {
		case watchdogWarn:
			warns++
		case watchdogAbort:
			if i != runLoopStuckAbort-1 {
				t.Fatalf("ABORT fired at iteration %d, want first at %d", i, runLoopStuckAbort-1)
			}
			if warns != 1 {
				t.Fatalf("expected exactly 1 WARN before ABORT, got %d", warns)
			}
			return
		}
	}
	t.Fatal("ABORT never fired despite 20000 no-progress iterations")
}
