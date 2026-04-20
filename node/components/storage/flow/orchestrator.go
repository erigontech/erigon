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

package flow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Orchestrator mediates between the Inventory, the event bus, and the
// Storage / Downloader / Execution components. It subscribes to external
// signals (BlocksFlushed, PeerManifestReceived, DownloadComplete) and
// translates them into Inventory mutations or outbound event publications.
type Orchestrator struct {
	bus       event.EventBus
	inventory *snapshot.Inventory
	log       log.Logger

	mu               sync.Mutex
	started          bool
	lastFlushedBlock atomic.Uint64
}

// New creates a flow orchestrator bound to the given bus and inventory.
// Neither is owned — the caller manages their lifecycles.
func New(bus event.EventBus, inv *snapshot.Inventory, logger log.Logger) *Orchestrator {
	if logger == nil {
		logger = log.Root()
	}
	return &Orchestrator{
		bus:       bus,
		inventory: inv,
		log:       logger,
	}
}

// Start performs the startup sequence:
//  1. Registers subscriptions for inbound events.
//  2. Publishes InventoryLoaded with the current inventory state.
//
// Safe to call once per lifecycle. Returns an error on double-start.
func (o *Orchestrator) Start(_ context.Context) error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return fmt.Errorf("flow orchestrator already started")
	}
	if err := o.bus.Subscribe(o.onBlocksFlushed); err != nil {
		o.mu.Unlock()
		return fmt.Errorf("subscribe BlocksFlushed: %w", err)
	}
	o.started = true
	o.mu.Unlock()

	// Publish without holding the lock — handlers may re-enter the orchestrator.
	o.bus.Publish(InventoryLoaded{Inventory: o.inventory})
	return nil
}

// Close drains any in-flight async handlers, unsubscribes, and marks the
// orchestrator stopped.
func (o *Orchestrator) Close() error {
	o.mu.Lock()
	if !o.started {
		o.mu.Unlock()
		return nil
	}
	o.started = false
	o.mu.Unlock()

	// Drain async handlers before unsubscribing so onBlocksFlushed can't be
	// racing with our teardown.
	o.bus.WaitAsync()
	if err := o.bus.Unsubscribe(o.onBlocksFlushed); err != nil {
		o.log.Warn("[flow] unsubscribe BlocksFlushed", "err", err)
	}
	return nil
}

// onBlocksFlushed enforces ordering invariant O3 (monotonically non-decreasing
// LatestBlock). Out-of-order events produce a warning and are dropped.
func (o *Orchestrator) onBlocksFlushed(e BlocksFlushed) {
	prev := o.lastFlushedBlock.Load()
	if e.LatestBlock < prev {
		o.log.Warn("[flow] out-of-order BlocksFlushed dropped", "prev", prev, "got", e.LatestBlock)
		return
	}
	o.lastFlushedBlock.Store(e.LatestBlock)
}
