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

// Package harness provides the in-process test wiring for snapshot-flow
// integration scenarios. A harness owns an EventBus, an Inventory, a flow
// Orchestrator, and (in later milestones) Downloader / peer shims.
//
// The harness is deliberately thin: it composes the production component
// packages (node/components/storage/snapshot, node/components/storage/flow)
// against test fixtures, without running a full erigon binary.
package harness

import (
	"context"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/app/util"
	"github.com/erigontech/erigon/node/app/workerpool"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// perNodePoolSize bounds the worker pool each harness Node creates. Kept
// small because multi-node scenarios instantiate several pools.
const perNodePoolSize = 4

// Node is a single in-process node wired for integration tests.
//
// After New, call Start to kick off the orchestrator (which publishes
// InventoryLoaded). Close stops the orchestrator and drains async handlers.
type Node struct {
	Bus       *CapturedBus
	Inventory *snapshot.Inventory
	Orch      *flow.Orchestrator

	pool    *testPool
	workers *workerpool.WorkerPool
}

// NewNode constructs a node wired around the given inventory. The caller
// populates the inventory with fixture data before calling Start.
func NewNode(logger log.Logger) *Node {
	if logger == nil {
		logger = log.New()
		logger.SetHandler(log.DiscardHandler())
	}

	workers := workerpool.New(perNodePoolSize)
	pool := &testPool{workers: workers}
	innerBus := event.NewEventBus(pool)
	bus := NewCapturedBus(innerBus)

	inv := snapshot.NewInventory()
	orch := flow.New(bus, inv, logger)

	return &Node{
		Bus:       bus,
		Inventory: inv,
		Orch:      orch,
		pool:      pool,
		workers:   workers,
	}
}

// Start invokes the orchestrator startup sequence. Publishes InventoryLoaded
// as its first effect.
func (n *Node) Start(ctx context.Context) error {
	return n.Orch.Start(ctx)
}

// Close stops the orchestrator, waits for any in-flight async work to drain,
// and releases the worker pool.
func (n *Node) Close() error {
	err := n.Orch.Close()
	n.Bus.WaitAsync()
	n.pool.wg.Wait()
	n.workers.StopWait()
	return err
}

// --- internal exec pool ---

type testPool struct {
	workers *workerpool.WorkerPool
	wg      sync.WaitGroup
}

func (p *testPool) Exec(task func()) {
	p.wg.Add(1)
	p.workers.Submit(func() {
		defer p.wg.Done()
		task()
	})
}

func (p *testPool) PoolSize() int  { return p.workers.Size() }
func (p *testPool) QueueSize() int { return p.workers.WaitingQueueSize() }

var _ util.ExecPool = (*testPool)(nil)
