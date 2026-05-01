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

package lifecycle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestDriver_StartStopIsClean(t *testing.T) {
	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv, SweepInterval: 10 * time.Millisecond}

	require.NoError(t, d.Start(context.Background()))
	d.Stop()
	// Multi-call safe.
	d.Stop()
}

func TestDriver_StartIsIdempotent(t *testing.T) {
	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv, SweepInterval: 10 * time.Millisecond}
	defer d.Stop()

	require.NoError(t, d.Start(context.Background()))
	require.NoError(t, d.Start(context.Background()), "second Start must be no-op")
}

func TestDriver_SweepNoOpOnEmptyInventory(t *testing.T) {
	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv}
	// Direct call — no Start needed for the invariant we're checking.
	d.Sweep(context.Background(), nil)
}

func TestDriver_SweepDispatchesByState(t *testing.T) {
	inv := snapshot.NewInventory()

	inv.AddFile(&snapshot.FileEntry{
		Name: "decl.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleDeclared,
	})
	inv.AddFile(&snapshot.FileEntry{
		Name: "down.kv", Domain: snapshot.DomainAccounts,
		Local: true, // → derives LifecycleDownloaded
	})
	inv.AddFile(&snapshot.FileEntry{
		Name: "idx.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleIndexed,
	})
	inv.AddFile(&snapshot.FileEntry{
		Name: "adv.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleAdvertisable, Local: true, Advertisable: true,
	})

	var indexingCount, validationCount atomic.Int32
	d := &Driver{
		Inv: inv,
		OnIndexing: func(_ context.Context, _ *snapshot.FileEntry) error {
			indexingCount.Add(1)
			return nil
		},
		OnValidation: func(_ context.Context, _ *snapshot.FileEntry) error {
			validationCount.Add(1)
			return nil
		},
	}

	d.Sweep(context.Background(), nil)
	require.Equal(t, int32(1), indexingCount.Load(),
		"Downloaded → OnIndexing dispatch")
	require.Equal(t, int32(1), validationCount.Load(),
		"Indexed → OnValidation dispatch")
	// Declared and Advertisable have no handler — sweep skips them.
}

func TestDriver_NilHandlersAreSafe(t *testing.T) {
	// Step 3 invariant: a Driver constructed with no handlers (the
	// default for the not-yet-cutover production wiring) does not
	// panic when sweeping a populated inventory.
	inv := snapshot.NewInventory()
	inv.AddFile(&snapshot.FileEntry{
		Name: "down.kv", Domain: snapshot.DomainAccounts, Local: true,
	})
	inv.AddFile(&snapshot.FileEntry{
		Name: "idx.kv", Domain: snapshot.DomainAccounts,
		State: snapshot.LifecycleIndexed,
	})

	d := &Driver{Inv: inv} // no handlers wired
	d.Sweep(context.Background(), nil)
}

func TestDriver_SubscriptionWakesSweep(t *testing.T) {
	inv := snapshot.NewInventory()

	var indexingCount atomic.Int32
	d := &Driver{
		Inv:           inv,
		SweepInterval: time.Hour, // long: only ChangeSet wakes the sweep
		OnIndexing: func(_ context.Context, _ *snapshot.FileEntry) error {
			indexingCount.Add(1)
			return nil
		},
	}
	require.NoError(t, d.Start(context.Background()))
	defer d.Stop()

	// Initial sweep on Start. Add a Downloaded file BEFORE Start to
	// guarantee the initial sweep observes it; the assertion below is
	// the proof that Start fired a sweep.
	// Actually — Start fires the first sweep immediately, but the
	// add must come BEFORE Start to be guaranteed visible. Restructure:
	// add file first, then Start, then a second file post-Start to
	// assert the ChangeSet wake.

	// Re-do.
	d.Stop()
	indexingCount.Store(0)

	inv.AddFile(&snapshot.FileEntry{
		Name: "first.kv", Domain: snapshot.DomainAccounts, Local: true,
	})

	require.NoError(t, d.Start(context.Background()))
	defer d.Stop()

	// Initial sweep should see "first.kv".
	require.Eventually(t, func() bool {
		return indexingCount.Load() >= 1
	}, time.Second, 10*time.Millisecond,
		"initial sweep on Start must process pre-existing Downloaded files")

	beforeAdd := indexingCount.Load()

	// AddFile triggers ChangeSet, which wakes the sweep loop.
	inv.AddFile(&snapshot.FileEntry{
		Name: "second.kv", Domain: snapshot.DomainAccounts, Local: true,
	})

	require.Eventually(t, func() bool {
		return indexingCount.Load() > beforeAdd
	}, time.Second, 10*time.Millisecond,
		"AddFile must wake the sweep via ChangeSet subscription")
}

func TestDriver_HandlerErrorDoesNotKillSweep(t *testing.T) {
	inv := snapshot.NewInventory()
	inv.AddFile(&snapshot.FileEntry{
		Name: "a.kv", Domain: snapshot.DomainAccounts, Local: true,
	})
	inv.AddFile(&snapshot.FileEntry{
		Name: "b.kv", Domain: snapshot.DomainStorage, Local: true,
	})

	var calls atomic.Int32
	d := &Driver{
		Inv: inv,
		OnIndexing: func(_ context.Context, _ *snapshot.FileEntry) error {
			calls.Add(1)
			return errOnIndexing
		},
	}

	d.Sweep(context.Background(), nil)
	require.Equal(t, int32(2), calls.Load(),
		"sweep must continue past handler errors")
}

// errOnIndexing is the test-side sentinel returned by the failing
// handler in TestDriver_HandlerErrorDoesNotKillSweep.
var errOnIndexing = errSentinel("simulated indexing failure")

type errSentinel string

func (e errSentinel) Error() string { return string(e) }
