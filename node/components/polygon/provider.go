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

// Package polygon provides the PolygonProvider — the component extracted from
// backend.go responsible for Heimdall/Bridge services and PolygonSyncService.
//
// For non-Bor chains, Initialize and InitializeSyncService are no-ops and all
// public fields remain nil.
package polygon

import (
	"context"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/shards"
	sentryserver "github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
	polygonsync "github.com/erigontech/erigon/polygon/sync"
)

// Provider holds the Polygon component runtime state.
//
// All public fields remain nil for non-Bor chains.
type Provider struct {
	// Public outputs — available after Initialize (Bor chains only).
	Bridge          *bridge.Service
	HeimdallService *heimdall.Service
	BridgeRPC       *bridge.BackendServer
	HeimdallRPC     *heimdall.BackendServer

	// Available after InitializeSyncService (Bor chains only).
	SyncService *polygonsync.Service
}

// InitDeps holds all external dependencies needed by Initialize.
type InitDeps struct {
	Ctx             context.Context
	Config          *ethconfig.Config
	ChainConfig     *chain.Config
	HeimdallStore   heimdall.Store
	BridgeStore     bridge.Store
	AllBorSnapshots *heimdall.RoSnapshots
	Logger          log.Logger
}

// SyncServiceDeps holds all external dependencies needed by InitializeSyncService.
type SyncServiceDeps struct {
	Ctx                context.Context
	Config             *ethconfig.Config
	ChainConfig        *chain.Config
	Sentries           []sentryproto.SentryClient
	MaxPeers           int
	StatusDataProvider *sentryserver.StatusDataProvider
	ExecutionRpc       executionproto.ExecutionClient
	Notifications      *shards.Notifications
	EngineServer       *engineapi.EngineServer
	Backend            polygonsync.MinedBlockObserverRegistrar
	Logger             log.Logger
}

// Initialize creates Heimdall/Bridge services and RPC servers.
// For non-Bor chains this is a no-op.
func (p *Provider) Initialize(deps InitDeps) error {
	if deps.ChainConfig.Bor == nil {
		return nil
	}

	ctx := deps.Ctx
	config := deps.Config
	chainConfig := deps.ChainConfig
	logger := deps.Logger
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)

	var heimdallClient heimdall.Client
	var bridgeClient bridge.Client

	if !config.WithoutHeimdall {
		heimdallClient = heimdall.NewHttpClient(config.HeimdallURL, logger, poshttp.WithApiVersioner(ctx))
		bridgeClient = bridge.NewHttpClient(config.HeimdallURL, logger, poshttp.WithApiVersioner(ctx))
	} else {
		heimdallClient = heimdall.NewIdleClient(config.Builder)
		bridgeClient = bridge.NewIdleClient()
	}

	polygonBridge := bridge.NewService(bridge.ServiceConfig{
		Store:        deps.BridgeStore,
		Logger:       logger,
		BorConfig:    borConfig,
		EventFetcher: bridgeClient,
	})

	if err := deps.HeimdallStore.Milestones().Prepare(ctx); err != nil {
		return err
	}
	if _, err := deps.HeimdallStore.Milestones().DeleteFromBlockNum(ctx, 0); err != nil {
		return err
	}

	heimdallSvc := heimdall.NewService(heimdall.ServiceConfig{
		Store:       deps.HeimdallStore,
		ChainConfig: chainConfig,
		BorConfig:   borConfig,
		Client:      heimdallClient,
		Logger:      logger,
	})

	p.Bridge = polygonBridge
	p.HeimdallService = heimdallSvc
	p.BridgeRPC = bridge.NewBackendServer(ctx, polygonBridge)
	p.HeimdallRPC = heimdall.NewBackendServer(ctx, heimdallSvc)

	return nil
}

// InitializeSyncService creates the PolygonSyncService and sets range extractors
// on AllBorSnapshots. Must be called after Initialize and after the exec component
// is ready (ExecutionRpc available).
// For non-Bor chains this is a no-op.
func (p *Provider) InitializeSyncService(deps SyncServiceDeps, allBorSnapshots *heimdall.RoSnapshots, heimdallStore heimdall.Store, bridgeStore bridge.Store) {
	if p.Bridge == nil {
		return
	}

	sentryClient := libsentry.NewSentryMultiplexer(deps.Sentries)

	p.SyncService = polygonsync.NewService(
		deps.Config,
		deps.Logger,
		deps.ChainConfig,
		sentryClient,
		deps.MaxPeers,
		deps.StatusDataProvider,
		deps.ExecutionRpc,
		deps.Config.LoopBlockLimit,
		p.Bridge,
		p.HeimdallService,
		deps.Notifications,
		deps.EngineServer,
		deps.Backend,
		deps.Config.Dirs.Tmp,
	)

	// Set range extractors so that Bor snapshot retirement uses the local stores.
	// These are set on allBorSnapshots after the sync service is created so that
	// the retire stage loop can extract data from the local MDBX stores rather
	// than the chain DB.
	type extractableStore interface {
		RangeExtractor() snaptype.RangeExtractor
	}

	if withRE, ok := heimdallStore.Spans().(extractableStore); ok {
		allBorSnapshots.SetRangeExtractor(heimdall.Spans, withRE.RangeExtractor())
	}
	if withRE, ok := heimdallStore.Checkpoints().(extractableStore); ok {
		allBorSnapshots.SetRangeExtractor(heimdall.Checkpoints, withRE.RangeExtractor())
	}
	if withRE, ok := heimdallStore.Milestones().(extractableStore); ok {
		allBorSnapshots.SetRangeExtractor(heimdall.Milestones, withRE.RangeExtractor())
	}
	if withRE, ok := bridgeStore.(extractableStore); ok {
		allBorSnapshots.SetRangeExtractor(heimdall.Events, withRE.RangeExtractor())
	}
}
