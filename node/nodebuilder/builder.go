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

// Package nodebuilder assembles an Erigon node from its extracted components.
//
// Components are extracted from backend.go incrementally. Each component lands
// here as a Build* method that owns the full Configure → Initialize lifecycle
// for that component. backend.go calls Build* methods and reads the resulting
// provider fields; it no longer needs to know about Configure/Initialize directly.
//
// Usage pattern in backend.go:
//
//	b := nodebuilder.New()
//
//	// Each Build* call configures and fully initializes the component.
//	if err := b.BuildDownloader(ctx, cfg.Downloader, cfg.Snapshot, dirs, logger, mux); err != nil {
//	    return err
//	}
//	// ...later components added by downstream branches...
//
//	// Unified lifecycle
//	b.Start(ctx, &eg)
//	defer b.Close()
//
// Adding a new component:
//  1. Extract Provider to node/components/<name>/
//  2. Add a <Name> field to Builder and allocate it in New()
//  3. Add Build<Name>(ctx, ...) that calls Provider.Configure + Provider.Initialize
//  4. Add to Start/Close if the component has background goroutines or cleanup
package nodebuilder

import (
	"context"
	"net/http"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	downloadercomp "github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Builder assembles an Erigon node from its extracted components.
// Fields are the live provider instances after Build* methods have been called.
//
// Components on this branch (feat/componentization):
//   - Downloader — snapshot BitTorrent client
//
// Components added by downstream branches:
//   - TxPool / Shutter — added by feat/txpool
//   - (future) Sentry, ExecModule, RPC, Mining, Caplin
type Builder struct {
	Downloader *downloadercomp.Provider
}

// New allocates a Builder with all providers pre-initialized.
func New() *Builder {
	return &Builder{
		Downloader: &downloadercomp.Provider{},
	}
}

// BuildDownloader configures and initializes the snapshot downloader component.
// After this call, b.Downloader.Client is ready for consumers.
func (b *Builder) BuildDownloader(
	ctx context.Context,
	cfg *downloadercfg.Cfg,
	snapshotCfg ethconfig.BlocksFreezing,
	dirs datadir.Dirs,
	logger log.Logger,
	debugMux *http.ServeMux,
) error {
	b.Downloader.Configure(cfg, snapshotCfg, dirs, logger, debugMux)
	return b.Downloader.Initialize(ctx)
}

// ErrGroup is satisfied by errgroup.Group and similar constructs.
type ErrGroup interface {
	Go(func() error)
}

// Start launches all component background goroutines.
// Call after all Build* methods have completed.
func (b *Builder) Start(_ context.Context, _ ErrGroup) {
	// Downloader manages its own goroutines internally — no Start needed.
	// Downstream branches add their components' Start calls here.
}

// Close shuts down all components that hold resources.
func (b *Builder) Close() {
	b.Downloader.Close()
}
