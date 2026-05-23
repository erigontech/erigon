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

package storage

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/lifecycle"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// newRestartTestProvider constructs the minimum-viable Provider for
// Restart tests: inventory + lifecycle driver pointed at a real
// temp dir + an event bus. No orchestrator, no full Initialize.
func newRestartTestProvider(t *testing.T) (*Provider, string) {
	t.Helper()
	dir := t.TempDir()
	inv := snapshot.NewInventory()
	bus := event.NewEventBus(nil)
	driver := &lifecycle.Driver{
		Inv: inv, SnapDir: dir, SweepInterval: 24 * time.Hour, // disable auto-sweep for deterministic tests
	}
	require.NoError(t, driver.Start(context.Background()))
	t.Cleanup(driver.Stop)
	return &Provider{
		Inventory:       inv,
		LifecycleDriver: driver,
		eventBus:        bus,
		logger:          log.New(),
	}, dir
}

// TestRestart_FiresBeginThenEndAndPreservesInventoryPointer pins the
// Restart contract: RestartBegin fires before any inventory mutation
// (synchronous subscribers see the pre-drain state); RestartEnd fires
// after the rescan completes (synchronous subscribers see the
// post-rescan state); the Inventory pointer survives so existing
// ChangeSet subscriptions don't disconnect.
func TestRestart_FiresBeginThenEndAndPreservesInventoryPointer(t *testing.T) {
	p, _ := newRestartTestProvider(t)

	originalInvPtr := p.Inventory

	var mu sync.Mutex
	events := []string{}
	require.NoError(t, p.eventBus.Subscribe(func(e flow.RestartBegin) {
		mu.Lock()
		events = append(events, "begin:"+e.Reason)
		mu.Unlock()
	}))
	require.NoError(t, p.eventBus.Subscribe(func(_ flow.RestartEnd) {
		mu.Lock()
		events = append(events, "end")
		mu.Unlock()
	}))

	require.NoError(t, p.Restart(context.Background(), RestartOpts{Reason: "test"}))
	require.Same(t, originalInvPtr, p.Inventory, "Restart preserves the *Inventory pointer")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"begin:test", "end"}, events,
		"RestartBegin must fire before RestartEnd")
}

// TestRestart_DrainsThenRescansFromDisk pins the drain + rescan
// sequence. Pre-Restart: inventory holds one stale entry whose file
// does not exist on disk. Post-Restart: stale entry is gone; the new
// file written between Stop and Restart is picked up by the Sweep
// inside Restart and appears in the inventory.
func TestRestart_DrainsThenRescansFromDisk(t *testing.T) {
	p, dir := newRestartTestProvider(t)

	// Stale in-memory entry — file does not exist on disk.
	p.Inventory.AddFile(&snapshot.FileEntry{
		Name: "v1.1-stale.seg", Local: true, Trust: snapshot.TrustVerified,
	})
	// Fresh file on disk that the pre-Restart inventory does not know about.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "v1.1-fresh.seg"), []byte{}, 0o644))

	require.NoError(t, p.Restart(context.Background(), RestartOpts{Reason: "rescan-test"}))

	_, staleStillThere := p.Inventory.GetByName("v1.1-stale.seg")
	require.False(t, staleStillThere, "stale entry must be drained")

	_, freshPickedUp := p.Inventory.GetByName("v1.1-fresh.seg")
	require.True(t, freshPickedUp, "freshly-on-disk file must be picked up by the rescan")
}

// TestRestart_ChangeSetSubscriberStaysConnected confirms that an
// existing inventory.Subscribe() channel keeps receiving events
// across a Restart — the contract that lets the orchestrator's
// watchInventoryForPostIndexed avoid reconnection logic.
func TestRestart_ChangeSetSubscriberStaysConnected(t *testing.T) {
	p, dir := newRestartTestProvider(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "post-restart.seg"), []byte{}, 0o644))

	sub, unsub := p.Inventory.Subscribe()
	defer unsub()

	require.NoError(t, p.Restart(context.Background(), RestartOpts{}))

	// The first ChangeSet on `sub` should be the Drain's removal-of-nothing
	// (skipped because inventory was empty) or the Sweep's discovery of
	// post-restart.seg. Either way, the subscriber must remain connected.
	select {
	case _, ok := <-sub:
		require.True(t, ok, "subscriber channel must stay open across Restart")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber received no event after Restart's rescan picked up a new file")
	}
}

// TestRestart_RejectsWhenUnitialized confirms the validity gates fail
// fast (no inventory / no driver / no bus → error, not panic).
func TestRestart_RejectsWhenUnitialized(t *testing.T) {
	t.Run("nil inventory", func(t *testing.T) {
		p := &Provider{eventBus: event.NewEventBus(nil), LifecycleDriver: &lifecycle.Driver{}}
		require.Error(t, p.Restart(context.Background(), RestartOpts{}))
	})
	t.Run("nil driver", func(t *testing.T) {
		p := &Provider{Inventory: snapshot.NewInventory(), eventBus: event.NewEventBus(nil)}
		require.Error(t, p.Restart(context.Background(), RestartOpts{}))
	})
	t.Run("nil bus", func(t *testing.T) {
		p := &Provider{Inventory: snapshot.NewInventory(), LifecycleDriver: &lifecycle.Driver{}}
		require.Error(t, p.Restart(context.Background(), RestartOpts{}))
	})
}
