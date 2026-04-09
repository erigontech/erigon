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

package builder

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/txnprovider"
)

// SDProvider returns the latest published SharedDomains from FCU, or nil if none.
// Used by the builder to read uncommitted state during background commits.
type SDProvider func() *execctx.SharedDomains

// Builder runs the three block-building steps (createBlock, execBlock, finishBlock) directly
// without staged-sync machinery. Its Build method satisfies BlockBuilderFunc and can
// be passed directly to ExecModule.
type Builder struct {
	ctx                   context.Context
	db                    kv.TemporalRoDB
	pendingBlockCh        chan *types.Block
	builderCfg            *buildercfg.BuilderConfig
	chainConfig           *chain.Config
	engine                rules.Engine
	blockReader           services.FullBlockReader
	executeBlockCfg       stagedsync.ExecuteBlockCfg
	notifier              stagedsync.ChainEventNotifier
	vmConfig              *vm.Config
	tmpdir                string
	txnProvider           txnprovider.TxnProvider
	sealCancel            chan struct{}
	latestBlockBuiltStore *LatestBlockBuiltStore
	sdProvider            SDProvider
	logger                log.Logger
}

func NewBuilder(
	ctx context.Context,
	db kv.TemporalRoDB,
	builderCfg *buildercfg.BuilderConfig,
	chainConfig *chain.Config,
	engine rules.Engine,
	blockReader services.FullBlockReader,
	executeBlockCfg stagedsync.ExecuteBlockCfg,
	notifier stagedsync.ChainEventNotifier,
	vmConfig *vm.Config,
	tmpdir string,
	txnProvider txnprovider.TxnProvider,
	sealCancel chan struct{},
	latestBlockBuiltStore *LatestBlockBuiltStore,
	sdProvider SDProvider,
	logger log.Logger,
) *Builder {
	return &Builder{
		ctx:                   ctx,
		db:                    db,
		pendingBlockCh:        make(chan *types.Block, 1),
		builderCfg:            builderCfg,
		chainConfig:           chainConfig,
		engine:                engine,
		blockReader:           blockReader,
		executeBlockCfg:       executeBlockCfg,
		notifier:              notifier,
		vmConfig:              vmConfig,
		tmpdir:                tmpdir,
		txnProvider:           txnProvider,
		sealCancel:            sealCancel,
		latestBlockBuiltStore: latestBlockBuiltStore,
		sdProvider:            sdProvider,
		logger:                logger,
	}
}

// PendingBlockCh returns the channel that receives pending (pre-seal) blocks.
// Wire this to the node's pending-block broadcaster.
func (b *Builder) PendingBlockCh() chan *types.Block {
	return b.pendingBlockCh
}

// Build satisfies BlockBuilderFunc. Pass b.Build directly to ExecModule.
func (b *Builder) Build(param *Parameters, interrupt *atomic.Bool) (result *types.BlockWithReceipts, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}()

	// Per-build state: fresh AssembledBlock and result channel, shared pendingBlockCh.
	perBuildCfg := *b.builderCfg
	perBuildCfg.Etherbase = param.SuggestedFeeRecipient
	state := BuilderState{
		BuilderConfig:   &perBuildCfg,
		PendingResultCh: b.pendingBlockCh,
		BuilderResultCh: make(chan *types.BlockWithReceipts, 1),
		BuiltBlock:      &exec.AssembledBlock{},
	}

	tx, err := b.db.BeginTemporalRo(b.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// When a published SD is available (background commit in progress), create
	// a child SD that reads domain state from the parent's mem batch and table
	// data from the parent's overlay. The child's own writes are local and
	// discarded after block construction.
	var compositeTx kv.TemporalTx = tx
	var parentSD *execctx.SharedDomains
	if b.sdProvider != nil {
		parentSD = b.sdProvider()
	}
	if parentSD != nil {
		if overlay := parentSD.BlockOverlay(); overlay != nil {
			compositeTx = overlay.NewReadView(tx)
		}
	}

	sd, err := execctx.NewSharedDomains(b.ctx, compositeTx, b.logger)
	if err != nil {
		return nil, err
	}
	defer sd.Close()

	if parentSD != nil {
		sd.SetParent(parentSD)
	}

	executionAt, err := stages.GetStageProgress(compositeTx, stages.Execution)
	if err != nil {
		return nil, err
	}
	createCfg := StageBuilderCreateBlockCfg(state, b.chainConfig, b.engine, param, b.blockReader)
	execCfg := StageBuilderExecCfg(state, b.notifier, b.chainConfig, b.engine, b.vmConfig, b.tmpdir, interrupt, param.PayloadId, b.txnProvider, b.blockReader)
	finishCfg := StageBuilderFinishCfg(b.chainConfig, b.engine, state, b.sealCancel, b.blockReader, b.latestBlockBuiltStore)

	if err := createBlock(b.ctx, sd, compositeTx, executionAt, createCfg, b.logger); err != nil {
		return nil, err
	}
	if err := execBlock(b.ctx, sd, compositeTx, executionAt, execCfg, b.executeBlockCfg, b.logger); err != nil {
		return nil, err
	}
	if err := finishBlock(compositeTx, finishCfg, b.logger); err != nil {
		return nil, err
	}

	return <-state.BuilderResultCh, nil
}
