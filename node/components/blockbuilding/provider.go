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

// Package blockbuilding provides the BlockBuilding Provider — the component
// extracted from backend.go responsible for block construction (mining) and
// the mined/pending-block broadcast goroutine.
//
// Lifecycle: Initialize → Start.
// No separate Configure step: all configuration is passed directly to Initialize.
//
// Sequencing note: Initialize must be called after the TxPool component (for
// TxnProvider) and after the Sentry component (for SentriesClient.Hd, used
// by the broadcast goroutine's PoW-mining quit channel).
package blockbuilding

import (
	"context"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/txnprovider"
)

// Provider holds the block-building component runtime state.
//
// After Initialize, Builder and PendingBlocks are ready.
// Start launches the mined/pending-block broadcast goroutine.
type Provider struct {
	// Public outputs — available after Initialize.
	Builder       *builder.Builder
	PendingBlocks chan *types.Block
}

// Deps holds the external dependencies needed by Initialize.
type Deps struct {
	Ctx                   context.Context
	ChainDB               kv.TemporalRoDB
	BuilderCfg            *buildercfg.BuilderConfig
	ChainConfig           *chain.Config
	Engine                rules.Engine
	BlockReader           services.FullBlockReader
	ExecuteBlocksCfg      stagedsync.ExecuteBlockCfg
	NotificationsEvents   stagedsync.ChainEventNotifier
	Tmpdir                string
	TxnProvider           txnprovider.TxnProvider
	MiningSealingQuit     chan struct{}
	LatestBlockBuiltStore *builder.LatestBlockBuiltStore
	Logger                log.Logger
}

// StartDeps holds the dependencies needed by Start.
type StartDeps struct {
	MinedBlocks         chan *types.Block
	MinedBlockObservers *event.Observers[*types.Block]
	MiningRPC           *privateapi.MiningServer
	QuitPoWMining       chan struct{}
	Logger              log.Logger
}

// Initialize creates the block builder.
func (p *Provider) Initialize(deps Deps) {
	p.Builder = builder.NewBuilder(
		deps.Ctx,
		deps.ChainDB,
		deps.BuilderCfg,
		deps.ChainConfig,
		deps.Engine,
		deps.BlockReader,
		deps.ExecuteBlocksCfg,
		deps.NotificationsEvents,
		&vm.Config{},
		deps.Tmpdir,
		deps.TxnProvider,
		deps.MiningSealingQuit,
		deps.LatestBlockBuiltStore,
		deps.Logger,
	)
	p.PendingBlocks = p.Builder.PendingBlockCh()
}

// Start launches the mined/pending-block broadcast goroutine.
// Call after Initialize and after the mining RPC is available.
func (p *Provider) Start(deps StartDeps) {
	logger := deps.Logger
	go func() {
		defer dbg.LogPanic()
		for {
			select {
			case b := <-deps.MinedBlocks:
				deps.MinedBlockObservers.Notify(b)
				if err := deps.MiningRPC.BroadcastMinedBlock(b); err != nil {
					logger.Error("txpool rpc mined block broadcast", "err", err)
				}
			case b := <-p.PendingBlocks:
				if err := deps.MiningRPC.BroadcastPendingBlock(b); err != nil {
					logger.Error("txpool rpc pending block broadcast", "err", err)
				}
			case <-deps.QuitPoWMining:
				return
			}
		}
	}()
}
