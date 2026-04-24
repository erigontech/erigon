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

package harness

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/app/workerpool"
	"github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/ethconfig"
)

// RealNode wires the production downloader.Provider and sentry.Provider
// components against a MockStorage and an in-process Coordinator. Scenarios
// that need to prove the flow event contract against real components use
// RealNode in place of Node.
//
// Lifecycle:
//   - NewRealNode constructs the components but does not start them. Callers
//     populate the inventory via Storage.Inventory() (or NewMockStorageWithInventory)
//     before Start so InventoryLoaded carries the fixture state.
//   - Start installs the downloader and sentry bus bindings and calls
//     Orch.Start (which publishes InventoryLoaded).
//   - Close tears down the orchestrator, unbinds the components (Close on
//     each Provider calls UnbindBus internally), drains async handlers, and
//     releases the worker pool.
type RealNode struct {
	Bus        *CapturedBus
	Inventory  *snapshot.Inventory
	Storage    *MockStorage
	Orch       *flow.Orchestrator
	Downloader *downloader.Provider
	Sentry     *sentry.Provider
	Coord      *Coordinator

	pool    *testPool
	workers *workerpool.WorkerPool
}

// NewRealNode constructs a RealNode sharing the given Coordinator with any
// InprocPeers set up in the scenario. Pass nil for logger to discard
// orchestrator logs; pass nil for coord to auto-create one (useful for
// single-node tests that don't exercise peer transfers).
func NewRealNode(t *testing.T, logger log.Logger, coord *Coordinator) *RealNode {
	t.Helper()
	if logger == nil {
		logger = log.New()
		logger.SetHandler(log.DiscardHandler())
	}
	if coord == nil {
		coord = NewCoordinator()
	}

	workers := workerpool.New(perNodePoolSize)
	pool := &testPool{workers: workers}
	innerBus := event.NewEventBus(pool)
	bus := NewCapturedBus(innerBus)

	storage := NewMockStorage()
	orch := flow.NewWithStorage(bus, storage, logger)

	dirs := datadir.New(t.TempDir())
	dl := &downloader.Provider{}
	dl.Configure(nil, ethconfig.BlocksFreezing{}, dirs, logger, nil)
	dl.Client = NewInprocClient(coord, dirs.Snap)

	sn := &sentry.Provider{}

	return &RealNode{
		Bus:        bus,
		Inventory:  storage.Inventory(),
		Storage:    storage,
		Orch:       orch,
		Downloader: dl,
		Sentry:     sn,
		Coord:      coord,
		pool:       pool,
		workers:    workers,
	}
}

// Start binds the downloader and sentry to the bus and starts the
// orchestrator. After this returns, InventoryLoaded has been published with
// the current inventory state.
func (n *RealNode) Start(ctx context.Context) error {
	if err := n.Downloader.BindBus(ctx, n.Bus); err != nil {
		return err
	}
	if err := n.Sentry.BindBus(n.Bus); err != nil {
		return err
	}
	return n.Orch.Start(ctx)
}

// Close tears down the node. Orch.Close unsubscribes the orchestrator;
// Downloader.Close and Sentry.Close each call their UnbindBus. Drains any
// async handlers still running on the bus worker pool.
func (n *RealNode) Close() error {
	orchErr := n.Orch.Close()
	n.Downloader.Close()
	sentryErr := n.Sentry.Close()

	n.Bus.WaitAsync()
	n.pool.wg.Wait()
	n.workers.StopWait()

	if orchErr != nil {
		return orchErr
	}
	return sentryErr
}
