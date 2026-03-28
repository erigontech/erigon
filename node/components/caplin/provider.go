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

// Package caplin provides the Caplin Provider — the component extracted from
// backend.go responsible for running the embedded Caplin consensus layer.
//
// Lifecycle: Start (single step; no separate Configure/Initialize since Caplin
// manages its own internal lifecycle via RunCaplinService).
//
// Sequencing note: Start must be called after the execution engine and engine
// API server are available, and after TLS credentials have been set up.
package caplin

import (
	"context"
	"os"
	"strings"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/credentials"

	"github.com/erigontech/erigon/cl/clparams"
	executionclient "github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format/getters"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
)

// Provider holds the Caplin component runtime state.
// It has no public output fields — Caplin manages its own internal state
// via RunCaplinService.
type Provider struct{}

// Deps holds the external dependencies needed by Start.
type Deps struct {
	Ctx             context.Context
	ExecutionEngine executionclient.ExecutionEngine
	CaplinConfig    clparams.CaplinConfig
	NetworkID       uint64
	LoopBlockLimit  int
	Dirs            datadir.Dirs
	BlockReader     services.FullBlockReader
	ChainDB         kv.TemporalRoDB
	DownloaderClient downloader.Client
	Creds           credentials.TransportCredentials
	SnBuildSema     *semaphore.Weighted
	// For the EnableEngineAPI path: read JWT and dial execution engine over RPC.
	JWTSecretPath  string
	AuthRpcAddress string
	AuthRpcPort    int
	CtxCancel      context.CancelFunc
	Logger         log.Logger
}

// Start configures Caplin and launches it in a background goroutine.
// Returns an error if the optional engine-API execution client cannot be created.
// The goroutine calls CtxCancel when RunCaplinService exits.
func (p *Provider) Start(deps Deps) error {
	logger := deps.Logger
	cfg := deps.CaplinConfig
	cfg.NetworkId = clparams.NetworkType(deps.NetworkID)
	cfg.LoopBlockLimit = uint64(deps.LoopBlockLimit)

	executionEngine := deps.ExecutionEngine

	if cfg.EnableEngineAPI {
		jwtSecretHex, err := os.ReadFile(deps.JWTSecretPath)
		if err != nil {
			logger.Error("failed to read jwt secret", "err", err, "path", deps.JWTSecretPath)
			return err
		}
		jwtSecret := common.FromHex(strings.TrimSpace(string(jwtSecretHex)))
		executionEngine, err = executionclient.NewExecutionClientRPC(jwtSecret, deps.AuthRpcAddress, deps.AuthRpcPort)
		if err != nil {
			logger.Error("failed to create execution client", "err", err)
			return err
		}
	}

	go func() {
		eth1Getter := getters.NewExecutionSnapshotReader(deps.Ctx, deps.BlockReader, deps.ChainDB)
		if err := caplin1.RunCaplinService(deps.Ctx, executionEngine, cfg, deps.Dirs, eth1Getter, deps.DownloaderClient, deps.Creds, deps.SnBuildSema); err != nil {
			logger.Error("could not start caplin", "err", err)
		}
		deps.CtxCancel()
	}()

	return nil
}
