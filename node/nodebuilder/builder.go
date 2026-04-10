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
// The builder provides:
//   - A single field in the Ethereum struct instead of N individual provider fields
//   - A clear inventory of what has been componentized vs what remains in backend.go
//
// Build ordering:
//  1. BuildDownloader — torrent client, gRPC proxy
//  2. (future) BuildStorage — DB, snapshots, blockReader, notifications
//
// Components are configured and initialized in backend.go (deps differ per component),
// but the builder owns allocation and provides a single access point.
package nodebuilder

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	downloadercomp "github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/ethconfig"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"

	"net/http"
)

// Builder holds all extracted node component providers.
// Fields are added here as components graduate from backend.go.
type Builder struct {
	Downloader *downloadercomp.Provider
}

// New allocates a Builder with all providers pre-initialized.
func New() *Builder {
	return &Builder{
		Downloader: &downloadercomp.Provider{},
	}
}

// BuildDownloader configures and initializes the downloader component.
func (b *Builder) BuildDownloader(ctx context.Context, dlCfg *downloadercfg.Cfg, snapCfg ethconfig.BlocksFreezing, dirs datadir.Dirs, logger log.Logger, debugMux *http.ServeMux) error {
	b.Downloader.Configure(dlCfg, snapCfg, dirs, logger, debugMux)
	return b.Downloader.Initialize(ctx)
}
