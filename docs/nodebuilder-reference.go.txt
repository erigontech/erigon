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
//	if err := b.BuildDownloader(ctx, cfg.Downloader, cfg.Snapshot, dirs, logger, mux); err != nil {
//	    return err
//	}
//	if err := b.BuildSentry(ctx, p2pCfg, syncCfg, networkID, urls, chainName, genesisHash, deps); err != nil {
//	    return err
//	}
//	if err := b.BuildRpc(ctx, httpCfg, mcpAddress, deps); err != nil {
//	    return err
//	}
//
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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	blockbuildingcomp "github.com/erigontech/erigon/node/components/blockbuilding"
	caplincomp "github.com/erigontech/erigon/node/components/caplin"
	downloadercomp "github.com/erigontech/erigon/node/components/downloader"
	execcomp "github.com/erigontech/erigon/node/components/exec"
	polygoncomp "github.com/erigontech/erigon/node/components/polygon"
	rpccomp "github.com/erigontech/erigon/node/components/rpc"
	sentrycomp "github.com/erigontech/erigon/node/components/sentry"
	storagecomp "github.com/erigontech/erigon/node/components/storage"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/p2p"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
)

// Builder assembles an Erigon node from its extracted components.
// Fields are the live provider instances after Build* methods have been called.
//
// Build ordering on this branch (feat/componentization):
//  1. Downloader     — snapshot BitTorrent client (no DB needed)
//  2. Storage        — DB open, snapshots, blockReader/Writer, KV RPC, notifications
//  3. Polygon        — Heimdall/Bridge services, PolygonSyncService (Bor-only)
//  4. Sentry         — P2P networking, sentry servers, execution P2P pipeline
//  5. Rpc            — embedded RPC services, JSON-RPC APIs, HTTP server
//  6. BlockBuilding  — block construction and mined/pending-block broadcast
//  7. Caplin         — embedded consensus layer (RunCaplinService)
//
// Not yet extracted (still inline in backend.go):
//   - TxPool / Shutter — added by feat/txpool
//   - ExecModule / StagedSync
type Builder struct {
	Storage       *storagecomp.Provider
	Downloader    *downloadercomp.Provider
	Polygon       *polygoncomp.Provider
	Sentry        *sentrycomp.Provider
	Rpc           *rpccomp.Provider
	BlockBuilding *blockbuildingcomp.Provider
	Exec          *execcomp.Provider
	Caplin        *caplincomp.Provider
}

// New allocates a Builder with all providers pre-initialized.
func New() *Builder {
	return &Builder{
		Storage:       &storagecomp.Provider{},
		Downloader:    &downloadercomp.Provider{},
		Polygon:       &polygoncomp.Provider{},
		Sentry:        &sentrycomp.Provider{},
		Rpc:           &rpccomp.Provider{},
		BlockBuilding: &blockbuildingcomp.Provider{},
		Exec:          &execcomp.Provider{},
		Caplin:        &caplincomp.Provider{},
	}
}

// BuildStorage initializes the storage component.
// Must be called after BuildDownloader (needs DownloaderClient for file-change callbacks).
// After this call, b.Storage.ChainDB, BlockReader, Notifications, etc. are ready.
func (b *Builder) BuildStorage(deps storagecomp.Deps) error {
	return b.Storage.Initialize(deps)
}

// BuildPolygon initializes the Polygon (Heimdall/Bridge) component.
// Must be called after BuildStorage (needs HeimdallStore, BridgeStore).
// For non-Bor chains this is a no-op and b.Polygon fields remain nil.
// After this call (on Bor chains), b.Polygon.Bridge, HeimdallService,
// BridgeRPC, and HeimdallRPC are ready.
func (b *Builder) BuildPolygon(deps polygoncomp.InitDeps) error {
	return b.Polygon.Initialize(deps)
}

// BuildDownloader configures and initializes the snapshot downloader component.
// Must be the first Build* call — it requires no database.
// After this call, b.Downloader.Client is ready and can be passed into BuildStorage.
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

// BuildSentry configures and initializes the P2P/sentry component.
// After this call, b.Sentry.SentriesClient, SentryServers, Publisher etc.
// are ready for consumers.
func (b *Builder) BuildSentry(
	ctx context.Context,
	p2pCfg p2p.Config,
	syncCfg ethconfig.Sync,
	networkID uint64,
	discoveryURLs []string,
	chainName string,
	genesisHash common.Hash,
	deps sentrycomp.Deps,
) error {
	b.Sentry.Configure(p2pCfg, syncCfg, networkID, discoveryURLs, chainName, genesisHash)
	return b.Sentry.Initialize(ctx, deps)
}

// BuildRpc configures and initializes the embedded RPC services component.
// deps.EthBackendRPC, deps.MiningRPC, and deps.StateDiffClient must be
// created by the caller before the txpool is initialised.
// After this call, b.Rpc.EthRpcClient, TxPoolRpcClient, EthApi etc. are ready.
func (b *Builder) BuildRpc(
	ctx context.Context,
	httpCfg *httpcfg.HttpCfg,
	mcpAddress string,
	deps rpccomp.Deps,
) error {
	b.Rpc.Configure(httpCfg, mcpAddress)
	return b.Rpc.Initialize(ctx, deps)
}

// BuildBlockBuilding initializes the block-building component.
// Must be called after TxPool (for TxnProvider) and Sentry (for SentriesClient.Hd).
// After this call, b.BlockBuilding.Builder and b.BlockBuilding.PendingBlocks are ready.
func (b *Builder) BuildBlockBuilding(deps blockbuildingcomp.Deps) {
	b.BlockBuilding.Initialize(deps)
}

// BuildExec initializes the execution pipeline component.
// Must be called after Sentry, Rpc, BlockBuilding, and TxPool components.
// After this call, b.Exec.ExecModule, ExecutionRpc, ExecutionEngine, PipelineSync,
// and EngineServer are ready for consumers.
func (b *Builder) BuildExec(deps execcomp.Deps) error {
	return b.Exec.Initialize(deps)
}

// StartCaplin launches the embedded Caplin consensus layer in a background goroutine.
// Must be called after the execution engine and TLS credentials are available.
// Returns an error only when the EnableEngineAPI execution client cannot be created.
func (b *Builder) StartCaplin(deps caplincomp.Deps) error {
	return b.Caplin.Start(deps)
}

// ErrGroup is satisfied by errgroup.Group and similar constructs.
type ErrGroup interface {
	Go(func() error)
}

// Start launches all component background goroutines.
// Call after all Build* methods have completed.
func (b *Builder) Start(_ context.Context, _ ErrGroup) {
	// Downloader and Sentry manage their own goroutines internally.
	// Rpc goroutines are started explicitly via Rpc.Start() from Init().
}

// Close shuts down all components that hold resources.
func (b *Builder) Close() {
	b.Downloader.Close()
}
