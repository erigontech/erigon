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

// Package exec provides the Exec Provider — the component extracted from
// backend.go responsible for the execution pipeline, execution module,
// execution RPC, and the engine API server.
//
// Lifecycle: Initialize only; Start is driven by backend.go because the
// launch path differs between PoS, Bor, and PoW.
//
// Sequencing note: Initialize must be called after:
//   - Sentry (for SentriesClient, BackwardBlockDownloader)
//   - Rpc (for ExecModuleCache)
//   - BlockBuilding (for BuilderFunc)
//   - TxPool (for TxPoolRpcClient)
package exec

import (
	"context"

	executionclient "github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	directpkg "github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/shards"
	sentry_multi_client "github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
)

// Provider holds the exec component runtime state.
//
// After Initialize, ExecModule, ExecutionRpc, ExecutionEngine, PipelineSync,
// and EngineServer are ready for consumers.
type Provider struct {
	// Public outputs — available after Initialize.
	ExecModule      *execmodule.ExecModule
	ExecutionRpc    executionproto.ExecutionClient
	ExecutionEngine executionclient.ExecutionEngine
	EngineServer    *engineapi.EngineServer
	PipelineSync    *stagedsync.Sync
}

// Deps holds all external dependencies needed by Initialize.
type Deps struct {
	Ctx                context.Context
	DB                 kv.TemporalRwDB
	BlockReader        services.FullBlockReader
	BlockWriter        *blockio.BlockWriter
	Config             *ethconfig.Config
	ChainConfig        *chain.Config
	Notifications      *shards.Notifications
	SentriesClient     *sentry_multi_client.MultiClient
	DownloaderClient   downloader.Client
	BlockRetire        services.BlockRetire
	Tracer             *tracers.Tracer
	BuilderFunc        builder.BlockBuilderFunc
	TxPoolRpcClient    txpoolproto.TxpoolClient
	BackwardBlockDl    *execp2p.BackwardBlockDownloader
	ExecModuleCache    *execmodule.Cache
	CurrentBlockNumber uint64
	Logger             log.Logger
}

// Initialize creates all exec-pipeline objects and populates the public
// output fields on the provider.
func (p *Provider) Initialize(deps Deps) error {
	ctx := deps.Ctx
	config := deps.Config
	chainConfig := deps.ChainConfig

	pipelineStages := stageloop.NewPipelineStages(ctx, deps.DB, config, deps.SentriesClient,
		deps.Notifications, deps.DownloaderClient, deps.BlockReader, deps.BlockRetire, deps.Tracer)
	p.PipelineSync = stagedsync.New(config.Sync, pipelineStages,
		stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder,
		deps.Logger, stages.ModeApplyingBlocks)

	validationNotifications := shards.NewNotifications(nil)
	validationSync := stageloop.NewInMemoryExecution(ctx, deps.DB, config, deps.SentriesClient,
		validationNotifications, deps.BlockReader, deps.BlockWriter, deps.Logger)
	pipelineExecutor := execmodule.NewPipelineExecutor(p.PipelineSync, deps.DB, deps.BlockReader,
		chainConfig, deps.SentriesClient.Engine, validationSync, validationNotifications, deps.Logger)

	onlySnapDownloadOnStart := chainConfig.Bor != nil
	p.ExecModule = execmodule.NewExecModule(
		ctx,
		deps.BlockReader,
		deps.DB,
		pipelineExecutor,
		deps.CurrentBlockNumber,
		chainConfig,
		deps.BuilderFunc,
		deps.Notifications,
		deps.Notifications.Accumulator,
		deps.Notifications.RecentReceipts,
		deps.ExecModuleCache,
		deps.Notifications.StateChangesConsumer,
		deps.Logger,
		deps.SentriesClient.Engine,
		config.Sync,
		config.FcuBackgroundPrune,
		config.FcuBackgroundCommit,
		onlySnapDownloadOnStart,
	)

	p.ExecutionRpc = directpkg.NewExecutionClientDirect(p.ExecModule)

	var err error
	p.ExecutionEngine, err = executionclient.NewExecutionClientDirect(
		chainreader.NewChainReaderEth1(chainConfig, p.ExecutionRpc, config.FcuTimeout),
		deps.TxPoolRpcClient,
	)
	if err != nil {
		return err
	}

	p.EngineServer = engineapi.NewEngineServer(
		deps.Logger,
		chainConfig,
		p.ExecutionRpc,
		engine_block_downloader.NewEngineBlockDownloader(
			ctx,
			deps.Logger,
			p.ExecutionRpc,
			deps.BlockReader,
			deps.DB,
			chainConfig,
			config.Sync,
			deps.BackwardBlockDl,
		),
		config.InternalCL && !config.CaplinConfig.EnableEngineAPI,
		config.Builder.EnabledPOS,
		!config.PolygonPosSingleSlotFinality,
		deps.TxPoolRpcClient,
		config.FcuTimeout,
		config.MaxReorgDepth,
	)

	return nil
}
