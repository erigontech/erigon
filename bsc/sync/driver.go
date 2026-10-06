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

// Package bscsync drives BSC (Parlia) block acquisition over devp2p without a
// consensus layer. It is an execution-module client: it fetches headers+bodies
// from peers and hands them to the exec module via InsertBlocks + UpdateForkChoice.
// It performs no execution itself (the exec module runs blocks-only for BSC until
// Parlia execution exists) and no seal verification.
package bscsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	bscp2p "github.com/erigontech/erigon/bsc/p2p"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
)

const (
	defaultFcuBlocks = 8192 // fallback FCU interval when LoopBlockLimit is unset
	peerBackoff      = 3 * time.Second
)

// Config parameterizes the BSC download driver.
type Config struct {
	ChainRW     chainreader.ChainReaderWriterEth1
	Svc         *bscp2p.Service
	TargetBlock uint64 // upper bound for the parallel downloader; required (> 0)
	FcuInterval uint64 // blocks between UpdateForkChoice calls during catch-up
}

// RunBlockDownloader runs the p2p service and the forward download loop until ctx
// is cancelled, the target is reached, or a fatal error occurs.
func RunBlockDownloader(ctx context.Context, logger log.Logger, cfg Config) error {
	if cfg.TargetBlock == 0 {
		return fmt.Errorf("bsc/sync: --sync.target-block is required (tip-following not implemented)")
	}
	if cfg.FcuInterval == 0 {
		cfg.FcuInterval = defaultFcuBlocks
	}
	errCh := make(chan error, 2)
	go func() { errCh <- cfg.Svc.Run(ctx) }()
	go func() { errCh <- runParallel(ctx, logger, cfg) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// runParallel downloads [head+1, TargetBlock] with the parallel multi-peer range
// downloader, persisting each assembled batch and advancing the head via FCU.
func runParallel(ctx context.Context, logger log.Logger, cfg Config) error {
	head, parent := computeResume(ctx, cfg.ChainRW)
	logger.Info("[bsc] block downloader started", "resumeFrom", head, "target", cfg.TargetBlock, "rangeSize", rangeSize)
	if head >= cfg.TargetBlock {
		logger.Info("[bsc] reached target", "block", head)
		return nil
	}
	lastFcu := head
	insert := func(ctx context.Context, blocks []*types.Block) error {
		if err := cfg.ChainRW.InsertBlocks(ctx, blocks); err != nil {
			return err
		}
		last := blocks[len(blocks)-1]
		head = last.NumberU64()
		if head-lastFcu >= cfg.FcuInterval || head >= cfg.TargetBlock {
			if err := commitHead(ctx, logger, cfg, last); err != nil {
				return err
			}
			lastFcu = head
		}
		return nil
	}
	if err := downloadRanges(ctx, logger, cfg.Svc, head+1, cfg.TargetBlock, parent, insert); err != nil {
		return err
	}
	logger.Info("[bsc] reached target", "block", cfg.TargetBlock)
	return nil
}

// commitHead advances the canonical head via the exec module, retrying while the
// module is busy (e.g. startup snapshot catch-up holding the semaphore).
func commitHead(ctx context.Context, logger log.Logger, cfg Config, head *types.Block) error {
	for {
		status, valErr, _, err := cfg.ChainRW.UpdateForkChoice(ctx, head.Hash(), common.Hash{}, common.Hash{})
		if err != nil {
			return err
		}
		switch status {
		case execmodule.ExecutionStatusSuccess:
			return nil
		case execmodule.ExecutionStatusBusy:
			if err := common.Sleep(ctx, peerBackoff); err != nil {
				return err
			}
			continue
		default:
			msg := ""
			if valErr != nil {
				msg = *valErr
			}
			logger.Error("[bsc] forkchoice rejected head", "block", head.NumberU64(), "hash", head.Hash(), "status", status, "err", msg)
			return errors.New("forkchoice rejected head: " + status.String())
		}
	}
}
