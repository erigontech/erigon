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

// Package nodebuilder is the central component registry for an Erigon node.
//
// Components are extracted from backend.go incrementally and registered here.
// The registry provides:
//   - A single field in the Ethereum struct instead of N individual provider fields
//   - Centralized Start and Close lifecycle across all registered components
//   - A clear inventory of what has been componentized vs what remains in backend.go
//
// Usage pattern:
//
//	// Construction (replaces N individual provider allocations in backend.go)
//	backend.components = nodebuilder.New()
//
//	// Per-component configuration and initialization (deps differ; stays in backend.go)
//	backend.components.TxPool.Configure(cfg.TxPool, chainConfig, logger)
//	backend.components.TxPool.Initialize(ctx, txpoolDeps)
//	backend.components.Downloader.Configure(cfg.Downloader, ...)
//	backend.components.Downloader.Initialize(ctx)
//
//	// Unified start (replaces N individual Start calls)
//	backend.components.Start(ctx, &bg)
//
//	// Unified close (replaces N individual Close calls)
//	backend.components.Close()
//
// Adding a new component:
//  1. Extract Provider to node/components/<name>/
//  2. Add a field to Registry and allocate it in New()
//  3. Wire Configure/Initialize in backend.go (same as before, just under backend.components)
//  4. Add to Start/Close if the component has background goroutines or needs cleanup
package nodebuilder

import (
	"context"

	downloadercomp "github.com/erigontech/erigon/node/components/downloader"
	txpoolcomp "github.com/erigontech/erigon/node/components/txpool"
)

// Builder holds all extracted node component providers and manages their
// shared lifecycle. Access components directly: b.TxPool.GrpcServer, etc.
//
// Fields are added here as components graduate from backend.go.
// Currently extracted:
//   - Downloader  (node/components/downloader)
//   - TxPool      (node/components/txpool)
//   - Shutter     (node/components/txpool — Shutter encrypted mempool wrapper)
//
// Not yet extracted (still inline in backend.go):
//   - Sentry / P2P
//   - ExecModule / StagedSync
//   - RPC servers / clients
//   - Mining
//   - Caplin (has its own internal stage machine)
type Builder struct {
	Downloader *downloadercomp.Provider
	TxPool     *txpoolcomp.Provider
	Shutter    *txpoolcomp.ShutterProvider
}

// New allocates a Builder with all current providers pre-initialized.
// Call once during node construction (replaces individual &Provider{} allocations).
func New() *Builder {
	return &Builder{
		Downloader: &downloadercomp.Provider{},
		TxPool:     &txpoolcomp.Provider{},
		Shutter:    &txpoolcomp.ShutterProvider{},
	}
}

// ErrGroup is satisfied by errgroup.Group and similar constructs.
type ErrGroup interface {
	Go(func() error)
}

// Start launches all component background goroutines.
// Call after all components have been initialized.
// Add new Start calls here when a component with background work is registered.
func (b *Builder) Start(ctx context.Context, eg ErrGroup) {
	b.TxPool.Start(ctx, eg)
	b.Shutter.Start(ctx, eg)
	// Downloader has no Start — it manages its own goroutines internally.
}

// Close shuts down all closeable components.
// Add new Close calls here when a closeable component is registered.
func (b *Builder) Close() {
	b.Downloader.Close()
}
