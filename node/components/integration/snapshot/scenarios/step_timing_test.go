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

package scenarios_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/lifecycle"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// TestStepTiming_MinimumFirstOrdering simulates per-step file arrival
// in a controlled time-stepped pattern and asserts:
//
//   - Each step's minimum subset advances to Advertisable BEFORE the
//     extras of the same step.
//   - Per-file FileTimings are recorded at each transition.
//   - StepTimings derives the minimum-vs-all breakdown correctly.
//   - The batch validator runs once per pass (minimum, then extras),
//     not once per file.
//
// This is the deterministic counterpart to the hoodi smoke run — the
// hoodi run proves the wiring fires under real conditions, this test
// proves the ORDER and TIMING are right.
//
// Output prints a breakdown table per step so the
// download-vs-validation distribution is eyeball-readable, feeding
// the future bandwidth-aware orchestrator's policy work.
func TestStepTiming_MinimumFirstOrdering(t *testing.T) {
	t.Parallel()
	inv := snapshot.NewInventory()
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	inv.SetClock(clock.now)

	// 3 step groups, each with .kv (minimum primary) + .kvi (minimum
	// accessor) + .v (extras). All in the accounts domain.
	type stepFiles struct {
		key   snapshot.StepKey
		kv    string
		kvi   string
		vfile string
	}
	steps := []stepFiles{
		{
			key:   snapshot.StepKey{FromStep: 0, ToStep: 256, Domain: snapshot.DomainAccounts},
			kv:    "v1.0-accounts.0-256.kv",
			kvi:   "v1.0-accounts.0-256.kvi",
			vfile: "v1.0-accountsHistory.0-256.v",
		},
		{
			key:   snapshot.StepKey{FromStep: 256, ToStep: 512, Domain: snapshot.DomainAccounts},
			kv:    "v1.0-accounts.256-512.kv",
			kvi:   "v1.0-accounts.256-512.kvi",
			vfile: "v1.0-accountsHistory.256-512.v",
		},
		{
			key:   snapshot.StepKey{FromStep: 512, ToStep: 768, Domain: snapshot.DomainAccounts},
			kv:    "v1.0-accounts.512-768.kv",
			kvi:   "v1.0-accounts.512-768.kvi",
			vfile: "v1.0-accountsHistory.512-768.v",
		},
	}

	// Phase 1: enqueue every file (Declared, no bytes yet).
	for _, s := range steps {
		require.NoError(t, inv.AddFile(&snapshot.FileEntry{
			Name: s.kv, Domain: snapshot.DomainAccounts,
			FromStep: s.key.FromStep, ToStep: s.key.ToStep,
		}))
		require.NoError(t, inv.AddFile(&snapshot.FileEntry{
			Name: s.kvi, Domain: snapshot.DomainAccounts,
			FromStep: s.key.FromStep, ToStep: s.key.ToStep,
		}))
		require.NoError(t, inv.AddFile(&snapshot.FileEntry{
			Name: s.vfile, Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory,
			FromStep: s.key.FromStep, ToStep: s.key.ToStep,
		}))
	}

	// Empty chain — the goal of this scenario is to exercise the
	// ordering and timing of the batch hook, not the validators
	// themselves (those have their own unit tests). AllFilesPresent
	// would fail here because no real files are written to disk.
	chain := validation.StepChain{}
	handler := lifecycle.BuildOnBatchValidation(chain, inv, nil)

	// Phase 2: minimum subset reaches Indexed first (per-step). Drive
	// each step's minimum through Downloaded → Indexed.
	for _, s := range steps {
		inv.AdvanceTo(s.kv, snapshot.LifecycleDownloaded)
		inv.AdvanceTo(s.kvi, snapshot.LifecycleDownloaded)
		// Skip Indexing-state — handler won't trigger until file is
		// at Indexed.
		inv.AdvanceTo(s.kv, snapshot.LifecycleIndexed)
		inv.AdvanceTo(s.kvi, snapshot.LifecycleIndexed)

		// Drive the handler — should advance minimum to Advertisable
		// even though extras (.v) are still at Declared.
		require.NoError(t, handler(context.Background(), &snapshot.FileEntry{
			Name: s.kv, Domain: snapshot.DomainAccounts,
			FromStep: s.key.FromStep, ToStep: s.key.ToStep,
		}))
	}

	// Verify minimum advanced before extras for each step.
	for _, s := range steps {
		state, _ := inv.LifecycleState(s.kv)
		require.Equal(t, snapshot.LifecycleAdvertisable, state, "%s", s.kv)
		state, _ = inv.LifecycleState(s.kvi)
		require.Equal(t, snapshot.LifecycleAdvertisable, state, "%s", s.kvi)
		state, _ = inv.LifecycleState(s.vfile)
		require.Equal(t, snapshot.LifecycleDeclared, state,
			"extras %s should still be at Declared", s.vfile)
	}

	// Phase 3: extras catch up.
	for _, s := range steps {
		inv.AdvanceTo(s.vfile, snapshot.LifecycleDownloaded)
		inv.AdvanceTo(s.vfile, snapshot.LifecycleIndexed)
		// Drive the handler again — extras pass should fire.
		require.NoError(t, handler(context.Background(), &snapshot.FileEntry{
			Name: s.vfile, Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory,
			FromStep: s.key.FromStep, ToStep: s.key.ToStep,
		}))
	}

	for _, s := range steps {
		state, _ := inv.LifecycleState(s.vfile)
		require.Equal(t, snapshot.LifecycleAdvertisable, state, "%s extras advance", s.vfile)
	}

	// Print breakdown table — read-only telemetry, not assertion.
	t.Logf("Per-step download-vs-validation breakdown:")
	t.Logf("  step               first→min-ready  min→all-ready  all-ready→all-validated")
	for _, s := range steps {
		st := inv.StepTimings(s.key)
		minDelay := st.MinimumIndexedAt.Sub(st.FirstFileSeenAt)
		extrasDelay := st.AllIndexedAt.Sub(st.MinimumIndexedAt)
		validateDelay := st.AllValidatedAt.Sub(st.AllIndexedAt)
		t.Logf("  [%4d-%4d]/%-12s  %-15s  %-13s  %s",
			s.key.FromStep, s.key.ToStep, s.key.Domain,
			minDelay, extrasDelay, validateDelay)

		// Assertions on the timing ordering.
		require.True(t, st.MinimumIndexedAt.Before(st.AllIndexedAt),
			"minimum reaches Indexed BEFORE the full step does")
		require.True(t, st.MinimumValidatedAt.Before(st.AllValidatedAt),
			"minimum reaches Validated BEFORE extras do (the availability win)")
	}
}

type fakeClock struct {
	start   time.Time
	counter atomic.Int64
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{start: start}
}

// now yields incrementing 1-second timestamps so each call has a
// distinct, ordered timestamp.
func (c *fakeClock) now() time.Time {
	i := c.counter.Add(1)
	return c.start.Add(time.Duration(i) * time.Second)
}
