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
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/txnprovider"
)

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

	// Per-build state: fresh BuiltBlock and result channel, shared pendingBlockCh.
	perBuildCfg := *b.builderCfg
	perBuildCfg.Etherbase = param.SuggestedFeeRecipient
	state := BuilderState{
		BuilderConfig:   &perBuildCfg,
		PendingResultCh: b.pendingBlockCh,
		BuilderResultCh: make(chan *types.BlockWithReceipts, 1),
		BuiltBlock:      &BuiltBlock{},
	}

	tx, err := b.db.BeginTemporalRo(b.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(b.ctx, tx, b.logger)
	if err != nil {
		return nil, err
	}
	defer sd.Close()

	executionAt, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nil, err
	}
	createCfg := StageBuilderCreateBlockCfg(state, b.chainConfig, b.engine, param, b.blockReader)
	execCfg := StageBuilderExecCfg(state, b.notifier, b.chainConfig, b.engine, b.vmConfig, b.tmpdir, interrupt, param.PayloadId, b.txnProvider, b.blockReader)
	finishCfg := StageBuilderFinishCfg(b.chainConfig, b.engine, state, b.sealCancel, b.blockReader, b.latestBlockBuiltStore)

	if err := createBlock(b.ctx, sd, tx, executionAt, createCfg, b.logger); err != nil {
		return nil, err
	}
	if err := execBlock(b.ctx, sd, tx, executionAt, execCfg, b.executeBlockCfg, b.logger); err != nil {
		return nil, err
	}
	if err := finishBlock(tx, finishCfg, b.logger); err != nil {
		return nil, err
	}

	return <-state.BuilderResultCh, nil
}
