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

package component_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/app"
	"github.com/erigontech/erigon/node/app/component"
)

// --- Test Infrastructure ---

// orderTracker records the order of lifecycle calls across components.
type orderTracker struct {
	mu     sync.Mutex
	events []string
}

func (o *orderTracker) record(event string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
}

func (o *orderTracker) get() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, len(o.events))
	copy(out, o.events)
	return out
}

// trackedProvider implements all lifecycle interfaces and records call order.
type trackedProvider struct {
	name    string
	tracker *orderTracker
}

func (p *trackedProvider) Configure(ctx context.Context, opts ...app.Option) error {
	p.tracker.record(p.name + ":configure")
	return nil
}

func (p *trackedProvider) Initialize(ctx context.Context, opts ...app.Option) error {
	p.tracker.record(p.name + ":initialize")
	return nil
}

func (p *trackedProvider) Recover(ctx context.Context) error {
	p.tracker.record(p.name + ":recover")
	return nil
}

func (p *trackedProvider) Activate(ctx context.Context) error {
	p.tracker.record(p.name + ":activate")
	return nil
}

func (p *trackedProvider) Deactivate(ctx context.Context) error {
	p.tracker.record(p.name + ":deactivate")
	return nil
}

// --- Hierarchy Tests ---

// TestIsolatedDomain verifies that components in an isolated domain
// don't interfere with the shared root domain.
func TestIsolatedDomain(t *testing.T) {
	domain, err := component.NewComponentDomain(t.Context(), "isolated")
	require.NoError(t, err)

	tracker := &orderTracker{}
	c, err := component.NewComponent[trackedProvider](t.Context(),
		component.WithId("comp"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "comp", tracker: tracker}))
	require.NoError(t, err)
	require.Equal(t, "isolated:comp", c.Id().String())

	err = c.Activate(t.Context())
	require.NoError(t, err)

	state, err := c.AwaitState(t.Context(), component.Active)
	require.NoError(t, err)
	require.Equal(t, component.Active, state)

	events := tracker.get()
	require.Contains(t, events, "comp:configure")
	require.Contains(t, events, "comp:initialize")
	require.Contains(t, events, "comp:activate")

	err = c.Deactivate(t.Context())
	require.NoError(t, err)

	state, err = c.AwaitState(t.Context(), component.Deactivated)
	require.NoError(t, err)
	require.Equal(t, component.Deactivated, state)

	events = tracker.get()
	require.Contains(t, events, "comp:deactivate")
}

// TestDependencyActivationOrder verifies that dependencies activate before
// their dependents — modeling the Storage → Downloader → SnapshotManager
// hierarchy.
func TestDependencyActivationOrder(t *testing.T) {
	domain, err := component.NewComponentDomain(t.Context(), "order-test")
	require.NoError(t, err)

	tracker := &orderTracker{}

	// Storage — no dependencies (foundation)
	storage, err := component.NewComponent[trackedProvider](t.Context(),
		component.WithId("storage"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "storage", tracker: tracker}))
	require.NoError(t, err)

	// Downloader — depends on Storage
	downloader, err := component.NewComponent[trackedProvider](t.Context(),
		component.WithId("downloader"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "downloader", tracker: tracker}),
		component.WithDependencies(storage))
	require.NoError(t, err)

	// SnapshotManager — depends on Downloader (and transitively on Storage)
	snapMgr, err := component.NewComponent[trackedProvider](t.Context(),
		component.WithId("snapshot-manager"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "snapshot-manager", tracker: tracker}),
		component.WithDependencies(downloader))
	require.NoError(t, err)

	// Activate the leaf — should cascade to activate dependencies first
	err = snapMgr.Activate(t.Context())
	require.NoError(t, err)

	state, err := snapMgr.AwaitState(t.Context(), component.Active)
	require.NoError(t, err)
	require.Equal(t, component.Active, state)

	// All three should be active
	require.Equal(t, component.Active, storage.State())
	require.Equal(t, component.Active, downloader.State())
	require.Equal(t, component.Active, snapMgr.State())

	// Verify activation order: storage before downloader before snapshot-manager
	events := tracker.get()
	storageActivateIdx := indexOf(events, "storage:activate")
	downloaderActivateIdx := indexOf(events, "downloader:activate")
	snapMgrActivateIdx := indexOf(events, "snapshot-manager:activate")

	require.True(t, storageActivateIdx >= 0, "storage:activate not found")
	require.True(t, downloaderActivateIdx >= 0, "downloader:activate not found")
	require.True(t, snapMgrActivateIdx >= 0, "snapshot-manager:activate not found")
	require.True(t, storageActivateIdx < downloaderActivateIdx,
		"storage must activate before downloader: storage=%d, downloader=%d", storageActivateIdx, downloaderActivateIdx)
	require.True(t, downloaderActivateIdx < snapMgrActivateIdx,
		"downloader must activate before snapshot-manager: downloader=%d, snapMgr=%d", downloaderActivateIdx, snapMgrActivateIdx)
}

// TestDependencyDeactivationOrder verifies that dependents deactivate before
// their dependencies (reverse order of activation).
//
// KNOWN ISSUE: The framework's deactivation cascade has a timing bug where
// awaitDeactivationChannels blocks indefinitely when deactivating a component
// that has dependencies. The deactivateDependencies check at component.go:1015
// may see stale state because deactivation is async (goroutine at line 1001).
// This test is skipped until the framework bug is fixed.
func TestDependencyDeactivationOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	domain, err := component.NewComponentDomain(ctx, "deactivation-test")
	require.NoError(t, err)

	tracker := &orderTracker{}

	storage, err := component.NewComponent[trackedProvider](ctx,
		component.WithId("storage"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "storage", tracker: tracker}))
	require.NoError(t, err)

	downloader, err := component.NewComponent[trackedProvider](ctx,
		component.WithId("downloader"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "downloader", tracker: tracker}),
		component.WithDependencies(storage))
	require.NoError(t, err)

	// Activate both
	err = downloader.Activate(ctx)
	require.NoError(t, err)
	_, err = downloader.AwaitState(ctx, component.Active)
	require.NoError(t, err)
	require.Equal(t, component.Active, storage.State())

	// Deactivate the leaf — should cascade to deactivate dependencies
	err = downloader.Deactivate(t.Context())
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()

	state, err := downloader.AwaitState(waitCtx, component.Deactivated)
	require.NoError(t, err)
	require.Equal(t, component.Deactivated, state)

	state, err = storage.AwaitState(waitCtx, component.Deactivated)
	require.NoError(t, err)
	require.Equal(t, component.Deactivated, state)
	cancel() // cleanup

	// Verify deactivation order: downloader before storage (reverse of activation)
	events := tracker.get()
	downloaderDeactivateIdx := indexOf(events, "downloader:deactivate")
	storageDeactivateIdx := indexOf(events, "storage:deactivate")

	require.True(t, downloaderDeactivateIdx >= 0, "downloader:deactivate not found")
	require.True(t, storageDeactivateIdx >= 0, "storage:deactivate not found")
	require.True(t, downloaderDeactivateIdx < storageDeactivateIdx,
		"downloader must deactivate before storage: downloader=%d, storage=%d",
		downloaderDeactivateIdx, storageDeactivateIdx)
}

// TestThreeLevelHierarchy models the full Erigon plugin hierarchy:
// Storage → Downloader → SnapshotManager, all in an isolated domain.
// Verifies clean startup, all states correct, clean shutdown.
func TestThreeLevelHierarchy(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	domain, err := component.NewComponentDomain(ctx, "hierarchy")
	require.NoError(t, err)

	tracker := &orderTracker{}

	storage, err := component.NewComponent[trackedProvider](ctx,
		component.WithId("storage"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "storage", tracker: tracker}))
	require.NoError(t, err)

	downloader, err := component.NewComponent[trackedProvider](ctx,
		component.WithId("downloader"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "downloader", tracker: tracker}),
		component.WithDependencies(storage))
	require.NoError(t, err)

	snapMgr, err := component.NewComponent[trackedProvider](ctx,
		component.WithId("snap-mgr"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "snap-mgr", tracker: tracker}),
		component.WithDependencies(downloader))
	require.NoError(t, err)

	// Activate leaf — cascades through the full tree
	err = snapMgr.Activate(ctx)
	require.NoError(t, err)
	_, err = snapMgr.AwaitState(ctx, component.Active)
	require.NoError(t, err)

	// All active
	require.Equal(t, component.Active, storage.State())
	require.Equal(t, component.Active, downloader.State())
	require.Equal(t, component.Active, snapMgr.State())

	// Verify activation ordering
	events := tracker.get()
	sAct := indexOf(events, "storage:activate")
	dAct := indexOf(events, "downloader:activate")
	smAct := indexOf(events, "snap-mgr:activate")
	require.True(t, sAct < dAct && dAct < smAct,
		"activation order wrong: storage=%d, downloader=%d, snap-mgr=%d", sAct, dAct, smAct)

	// Deactivate from the leaf — should cascade through the full tree
	err = snapMgr.Deactivate(t.Context())
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()

	_, err = snapMgr.AwaitState(waitCtx, component.Deactivated)
	require.NoError(t, err)
	_, err = downloader.AwaitState(waitCtx, component.Deactivated)
	require.NoError(t, err)
	_, err = storage.AwaitState(waitCtx, component.Deactivated)
	require.NoError(t, err)

	// Verify deactivation order: snap-mgr → downloader → storage
	events = tracker.get()
	smDeact := indexOf(events, "snap-mgr:deactivate")
	dDeact := indexOf(events, "downloader:deactivate")
	sDeact := indexOf(events, "storage:deactivate")
	require.True(t, smDeact < dDeact && dDeact < sDeact,
		"deactivation order wrong: snap-mgr=%d, downloader=%d, storage=%d", smDeact, dDeact, sDeact)

	cancel()
}

// TestMultipleDependentsOnSameBase verifies that multiple components can
// depend on the same base component (e.g., both Downloader and TxPool
// depend on Storage).
func TestMultipleDependentsOnSameBase(t *testing.T) {
	domain, err := component.NewComponentDomain(t.Context(), "multi-dep")
	require.NoError(t, err)

	tracker := &orderTracker{}

	storage, err := component.NewComponent[trackedProvider](t.Context(),
		component.WithId("storage"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "storage", tracker: tracker}))
	require.NoError(t, err)

	downloader, err := component.NewComponent[trackedProvider](t.Context(),
		component.WithId("downloader"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "downloader", tracker: tracker}),
		component.WithDependencies(storage))
	require.NoError(t, err)

	txpool, err := component.NewComponent[trackedProvider](t.Context(),
		component.WithId("txpool"),
		component.WithDomain(domain),
		component.WithProvider(&trackedProvider{name: "txpool", tracker: tracker}),
		component.WithDependencies(storage))
	require.NoError(t, err)

	// Activate both leaves
	err = downloader.Activate(t.Context())
	require.NoError(t, err)
	_, err = downloader.AwaitState(t.Context(), component.Active)
	require.NoError(t, err)

	err = txpool.Activate(t.Context())
	require.NoError(t, err)
	_, err = txpool.AwaitState(t.Context(), component.Active)
	require.NoError(t, err)

	// All three active
	require.Equal(t, component.Active, storage.State())
	require.Equal(t, component.Active, downloader.State())
	require.Equal(t, component.Active, txpool.State())

	// Deactivate downloader — storage should stay active (txpool still depends on it)
	err = downloader.Deactivate(t.Context())
	require.NoError(t, err)
	_, err = downloader.AwaitState(t.Context(), component.Deactivated)
	require.NoError(t, err)

	require.Equal(t, component.Deactivated, downloader.State())
	require.Equal(t, component.Active, storage.State(), "storage should stay active — txpool still depends on it")
	require.Equal(t, component.Active, txpool.State())
}

// --- Helpers ---

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}
