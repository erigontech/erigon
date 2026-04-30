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

// Package downloader provides the DownloaderComponent — the first component
// extracted from backend.go as part of the Erigon componentization effort.
//
// The Downloader manages BitTorrent-based snapshot distribution. It supports
// two modes:
//   - Local: in-process torrent client (default)
//   - Remote: connects to external downloader via gRPC
//
// Consumers access it through the downloader.Client interface, which abstracts
// both modes behind Download/Seed/Delete operations.
package downloader

import (
	"context"
	"fmt"
	"net/http"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/downloader/downloadergrpc"
	"github.com/erigontech/erigon/node/ethconfig"
	downloaderproto "github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// Provider holds the Downloader's runtime state. It implements the component
// lifecycle: Configure → Initialize → Activate → Deactivate.
//
// After Initialize, the Client field is ready for consumers to use.
type Provider struct {
	// Public fields — accessible by consumers via the component dependency graph.
	Downloader *dl.Downloader // nil when using external downloader
	Client     dl.Client      // always set after Initialize (local or remote)

	// Configuration
	cfg         *downloadercfg.Cfg
	snapshotCfg ethconfig.BlocksFreezing
	dirs        datadir.Dirs
	logger      log.Logger
	debugMux    *http.ServeMux // for debug handler registration

	// Internal state
	grpcClient downloaderproto.DownloaderClient // raw gRPC client before wrapping
}

// Configure applies configuration. Call before Initialize.
func (p *Provider) Configure(
	cfg *downloadercfg.Cfg,
	snapshotCfg ethconfig.BlocksFreezing,
	dirs datadir.Dirs,
	logger log.Logger,
	debugMux *http.ServeMux,
) {
	p.cfg = cfg
	p.snapshotCfg = snapshotCfg
	p.dirs = dirs
	p.logger = logger
	p.debugMux = debugMux
}

// Initialize creates the downloader (local or remote) and wraps it as a Client.
// After this call, p.Client is ready for use.
func (p *Provider) Initialize(ctx context.Context) error {
	if p.snapshotCfg.NoDownloader {
		p.logger.Info("[downloader] disabled via config")
		return nil
	}

	// Load snapshot hashes
	if p.cfg != nil && p.cfg.ChainName != "" {
		if err := downloadercfg.LoadSnapshotsHashes(ctx, p.cfg.Dirs, p.cfg.ChainName); err != nil {
			return fmt.Errorf("load snapshot hashes: %w", err)
		}
	}

	// Create the downloader (local or remote)
	grpcClient, err := p.initDownloader(ctx)
	if err != nil {
		return fmt.Errorf("init downloader: %w", err)
	}

	if grpcClient != nil {
		p.grpcClient = grpcClient
		p.Client = dl.NewRpcClient(grpcClient, p.dirs.Snap)
	}

	return nil
}

// initDownloader creates either a local in-process downloader or connects
// to an external one via gRPC.
func (p *Provider) initDownloader(ctx context.Context) (downloaderproto.DownloaderClient, error) {
	// External downloader
	if p.snapshotCfg.DownloaderAddr != "" {
		p.logger.Info("[downloader] connecting to external", "addr", p.snapshotCfg.DownloaderAddr)
		return downloadergrpc.NewClient(ctx, p.snapshotCfg.DownloaderAddr)
	}

	// No config or no chain name — skip
	if p.cfg == nil || p.cfg.ChainName == "" {
		return nil, nil
	}

	// Local in-process downloader
	d, err := dl.New(ctx, p.cfg, p.logger)
	if err != nil {
		return nil, err
	}
	p.Downloader = d

	if p.debugMux != nil {
		d.HandleTorrentClientStatus(p.debugMux)
	}

	bittorrentServer, err := dl.NewGrpcServer(d)
	if err != nil {
		return nil, fmt.Errorf("new grpc server: %w", err)
	}
	d.InitBackgroundLogger(true)

	return dl.DirectGrpcServerClient(bittorrentServer), nil
}

// Close shuts down the downloader. Safe to call multiple times.
func (p *Provider) Close() {
	if p.Downloader != nil {
		p.Downloader.Close()
		p.Downloader = nil
	}
}

// IsEnabled returns true if the downloader is configured and active.
func (p *Provider) IsEnabled() bool {
	return p.Client != nil
}
