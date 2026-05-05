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

package snapshot

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock returns a callback that yields incrementing timestamps
// 1 second apart, so each successive call to inv.now() differs.
// Lets tests assert ordering without relying on real wall-clock.
func fakeClock(start time.Time) func() time.Time {
	var counter atomic.Int64
	return func() time.Time {
		i := counter.Add(1)
		return start.Add(time.Duration(i) * time.Second)
	}
}

func TestFileTimings_RecordedOnAddFile(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inv.SetClock(fakeClock(t0))

	require.NoError(t, inv.AddFile(&FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts,
		FromStep: 0, ToStep: 256,
		Local: true,
	}))

	tt, ok := inv.FileTimings("v1.0-accounts.0-256.kv")
	require.True(t, ok)
	require.False(t, tt.EnqueuedAt.IsZero(), "EnqueuedAt set on AddFile")
	require.False(t, tt.DownloadCompletedAt.IsZero(),
		"file added with Local=true → DownloadCompletedAt set")
	require.True(t, tt.IndexedAt.IsZero(),
		"not yet at Indexed")
	require.True(t, tt.ValidatedAt.IsZero(),
		"not yet at Advertisable")
}

func TestFileTimings_RecordedOnAdvanceTo(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inv.SetClock(fakeClock(t0))

	require.NoError(t, inv.AddFile(&FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts,
		FromStep: 0, ToStep: 256,
		Local: true,
	}))
	inv.AdvanceTo("v1.0-accounts.0-256.kv", LifecycleIndexed)
	inv.AdvanceTo("v1.0-accounts.0-256.kv", LifecycleAdvertisable)

	tt, _ := inv.FileTimings("v1.0-accounts.0-256.kv")
	require.False(t, tt.IndexedAt.IsZero(), "IndexedAt set")
	require.False(t, tt.ValidatedAt.IsZero(), "ValidatedAt set")
	require.True(t, tt.IndexedAt.Before(tt.ValidatedAt),
		"IndexedAt < ValidatedAt — recorded in transition order")
	require.True(t, tt.DownloadCompletedAt.Before(tt.IndexedAt),
		"DownloadCompletedAt < IndexedAt")
}

func TestFileTimings_RecordedOnAdvanceStep(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inv.SetClock(fakeClock(t0))

	files := []*FileEntry{
		{Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts, FromStep: 0, ToStep: 256, State: LifecycleIndexed},
		{Name: "v1.0-accounts.0-256.kvi", Domain: DomainAccounts, FromStep: 0, ToStep: 256, State: LifecycleIndexed},
	}
	for _, f := range files {
		require.NoError(t, inv.AddFile(f))
	}

	inv.AdvanceStep(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts}, LifecycleAdvertisable)

	for _, f := range files {
		tt, _ := inv.FileTimings(f.Name)
		require.False(t, tt.ValidatedAt.IsZero(),
			"AdvanceStep stamps ValidatedAt for each advanced file")
	}
}

func TestStepTimings_DerivedFromFiles(t *testing.T) {
	t.Parallel()
	inv := NewInventory()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inv.SetClock(fakeClock(t0))

	// Add 3 files, advance them through the lifecycle. Minimum is
	// .kv + .kvi; extras is .v.
	primary := &FileEntry{Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts, FromStep: 0, ToStep: 256, Local: true}
	primaryAcc := &FileEntry{Name: "v1.0-accounts.0-256.kvi", Domain: DomainAccounts, FromStep: 0, ToStep: 256, Local: true}
	extras := &FileEntry{Name: "v1.0-accountsHistory.0-256.v", Domain: DomainAccounts, Kind: KindHistory, FromStep: 0, ToStep: 256, Local: true}
	require.NoError(t, inv.AddFile(primary))
	require.NoError(t, inv.AddFile(primaryAcc))
	require.NoError(t, inv.AddFile(extras))

	// Minimum reaches Indexed first; extras still at Downloaded.
	inv.AdvanceTo(primary.Name, LifecycleIndexed)
	inv.AdvanceTo(primaryAcc.Name, LifecycleIndexed)

	st := inv.StepTimings(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts})
	require.False(t, st.FirstFileSeenAt.IsZero(), "FirstFileSeenAt set")
	require.False(t, st.MinimumIndexedAt.IsZero(),
		"MinimumIndexedAt set when minimum subset reaches Indexed")
	require.True(t, st.AllIndexedAt.IsZero(),
		"AllIndexedAt zero — extras not yet at Indexed")
	require.True(t, st.AllValidatedAt.IsZero(),
		"AllValidatedAt zero — nothing at Advertisable yet")

	// Extras catches up.
	inv.AdvanceTo(extras.Name, LifecycleIndexed)
	st = inv.StepTimings(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts})
	require.False(t, st.AllIndexedAt.IsZero(),
		"AllIndexedAt set once extras at Indexed")
	require.True(t, st.MinimumIndexedAt.Before(st.AllIndexedAt) || st.MinimumIndexedAt.Equal(st.AllIndexedAt),
		"MinimumIndexedAt <= AllIndexedAt")

	// Validation advances all.
	inv.AdvanceStep(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts}, LifecycleAdvertisable)
	st = inv.StepTimings(StepKey{FromStep: 0, ToStep: 256, Domain: DomainAccounts})
	require.False(t, st.MinimumValidatedAt.IsZero())
	require.False(t, st.AllValidatedAt.IsZero())
}

func TestFileTimings_NotRecordedTwice(t *testing.T) {
	t.Parallel()
	// Subsequent transitions through the SAME state must not overwrite
	// the original timestamp — the orchestrator wants the FIRST time
	// each milestone was reached.
	inv := NewInventory()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inv.SetClock(fakeClock(t0))

	require.NoError(t, inv.AddFile(&FileEntry{
		Name: "v1.0-accounts.0-256.kv", Domain: DomainAccounts,
		FromStep: 0, ToStep: 256,
	}))
	inv.AdvanceTo("v1.0-accounts.0-256.kv", LifecycleDownloaded)
	first, _ := inv.FileTimings("v1.0-accounts.0-256.kv")

	// Try to advance to Downloaded again (no-op since AdvanceTo rejects
	// already-at-state). Trigger a hypothetical reset and re-advance.
	inv.AdvanceTo("v1.0-accounts.0-256.kv", LifecycleDeclared)
	inv.AdvanceTo("v1.0-accounts.0-256.kv", LifecycleDownloaded)
	second, _ := inv.FileTimings("v1.0-accounts.0-256.kv")

	require.Equal(t, first.DownloadCompletedAt, second.DownloadCompletedAt,
		"DownloadCompletedAt records the FIRST transition, not the reset+re-advance")
}
