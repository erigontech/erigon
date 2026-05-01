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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddFile_DerivesStateFromFlags(t *testing.T) {
	cases := []struct {
		name     string
		entry    *FileEntry
		expected LifecycleState
	}{
		{
			name:     "fresh entry no flags",
			entry:    &FileEntry{Name: "a.kv", Domain: DomainAccounts},
			expected: LifecycleDeclared,
		},
		{
			name:     "Local-only flag derives Downloaded",
			entry:    &FileEntry{Name: "b.kv", Domain: DomainAccounts, Local: true},
			expected: LifecycleDownloaded,
		},
		{
			name:     "Advertisable flag derives Advertisable (regardless of Local)",
			entry:    &FileEntry{Name: "c.kv", Domain: DomainAccounts, Local: true, Advertisable: true},
			expected: LifecycleAdvertisable,
		},
		{
			name:     "explicit State preserved",
			entry:    &FileEntry{Name: "d.kv", Domain: DomainAccounts, State: LifecycleIndexed},
			expected: LifecycleIndexed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv := NewInventory()
			inv.AddFile(tc.entry)
			got, ok := inv.LifecycleState(tc.entry.Name)
			require.True(t, ok)
			require.Equal(t, tc.expected, got, "expected %s, got %s", tc.expected, got)
		})
	}
}

func TestAdvanceTo_ForwardTransitionUpdatesFlags(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts})

	require.True(t, inv.AdvanceTo("a.kv", LifecycleDownloaded))
	e, ok := inv.GetByName("a.kv")
	require.True(t, ok)
	require.Equal(t, LifecycleDownloaded, e.State)
	require.True(t, e.Local)
	require.False(t, e.Advertisable)

	require.True(t, inv.AdvanceTo("a.kv", LifecycleAdvertisable))
	e, _ = inv.GetByName("a.kv")
	require.Equal(t, LifecycleAdvertisable, e.State)
	require.True(t, e.Local)
	require.True(t, e.Advertisable)
}

func TestAdvanceTo_SkippingIntermediateStatesIsAllowed(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts})

	// Startup-scan case: discovered fully validated, jumps to Advertisable.
	require.True(t, inv.AdvanceTo("a.kv", LifecycleAdvertisable))
	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, LifecycleAdvertisable, state)
}

func TestAdvanceTo_BackwardTransitionRejected(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, State: LifecycleAdvertisable, Local: true, Advertisable: true})

	// Backward (Advertisable → Indexed) rejected.
	require.False(t, inv.AdvanceTo("a.kv", LifecycleIndexed))
	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, LifecycleAdvertisable, state, "state must not have moved backward")
}

func TestAdvanceTo_ResetToDeclaredAlwaysAllowed(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, State: LifecycleAdvertisable, Local: true, Advertisable: true})

	require.True(t, inv.AdvanceTo("a.kv", LifecycleDeclared),
		"reset to Declared (corruption-detected re-download) must be allowed from any state")
	e, ok := inv.GetByName("a.kv")
	require.True(t, ok)
	require.Equal(t, LifecycleDeclared, e.State)
	require.False(t, e.Local)
	require.False(t, e.Advertisable)
}

func TestAdvanceTo_NoOpAtTargetReturnsFalse(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, State: LifecycleAdvertisable, Local: true, Advertisable: true})

	// No-op: already at target. Returns false (no work to do; no notification).
	require.False(t, inv.AdvanceTo("a.kv", LifecycleAdvertisable))
}

func TestAdvanceTo_UnknownEntryReturnsFalse(t *testing.T) {
	inv := NewInventory()
	require.False(t, inv.AdvanceTo("ghost.kv", LifecycleDownloaded))
}

func TestMarkLocal_DrivesStateMachine(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts})

	require.True(t, inv.MarkLocal("a.kv"))
	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, LifecycleDownloaded, state,
		"MarkLocal must advance LifecycleState to Downloaded")
}

func TestMarkNotLocal_ResetsToDeclared(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, State: LifecycleAdvertisable, Local: true, Advertisable: true})

	require.True(t, inv.MarkNotLocal("a.kv"))
	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, LifecycleDeclared, state,
		"MarkNotLocal must reset to Declared — corruption needs re-fetch from scratch")
	e, _ := inv.GetByName("a.kv")
	require.False(t, e.Advertisable, "Advertisable must clear on MarkNotLocal")
}

func TestMarkAdvertisable_AdvancesState(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, Local: true})
	state, _ := inv.LifecycleState("a.kv")
	require.Equal(t, LifecycleDownloaded, state)

	require.True(t, inv.MarkAdvertisable("a.kv"))
	state, _ = inv.LifecycleState("a.kv")
	require.Equal(t, LifecycleAdvertisable, state)
}

func TestDependencies_StoredOnEntry(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{
		Name:         "v1.0-accounts.0-256.kv",
		Domain:       DomainAccounts,
		Dependencies: []string{"v1.0-accounts.0-256.kvi"},
	})
	e, ok := inv.GetByName("v1.0-accounts.0-256.kv")
	require.True(t, ok)
	require.Equal(t, []string{"v1.0-accounts.0-256.kvi"}, e.Dependencies)
}
