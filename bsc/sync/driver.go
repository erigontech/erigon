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
	"time"

	bscp2p "github.com/erigontech/erigon/bsc/p2p"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
)

const (
	fetchChunk       = 1024 // headers per FetchHeaders round (eth.MaxHeadersServe)
	defaultFcuBlocks = 8192 // fallback FCU interval when LoopBlockLimit is unset
	peerBackoff      = 3 * time.Second
)

// Config parameterizes the BSC download driver.
type Config struct {
	ChainRW     chainreader.ChainReaderWriterEth1
	Svc         *bscp2p.Service
	TargetBlock uint64 // exclusive-of-nothing upper bound; sync stops once head >= TargetBlock. 0 = no bound (until later phases add tip-following).
	FcuInterval uint64 // blocks between UpdateForkChoice calls during catch-up
}

// RunBlockDownloader runs the p2p service and the forward download loop until ctx
// is cancelled, the target is reached, or a fatal error occurs.
func RunBlockDownloader(ctx context.Context, logger log.Logger, cfg Config) error {
	if cfg.FcuInterval == 0 {
		cfg.FcuInterval = defaultFcuBlocks
	}
	errCh := make(chan error, 2)
	go func() { errCh <- cfg.Svc.Run(ctx) }()
	go func() { errCh <- runLoop(ctx, logger, cfg) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func runLoop(ctx context.Context, logger log.Logger, cfg Config) error {
	head, parent := computeResume(ctx, cfg.ChainRW)
	logger.Info("[bsc] block downloader started", "resumeFrom", head, "target", cfg.TargetBlock)

	lastFcu := head
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if cfg.TargetBlock != 0 && head >= cfg.TargetBlock {
			logger.Info("[bsc] reached target", "block", head)
			return nil
		}

		to := head + fetchChunk
		if cfg.TargetBlock != 0 && to > cfg.TargetBlock {
			to = cfg.TargetBlock
		}

		blocks, err := fetchForwardRange(ctx, cfg.Svc, head+1, to)
		if err != nil {
			if errors.Is(err, errNoPeers) || ctx.Err() == nil {
				logger.Debug("[bsc] fetch failed, backing off", "from", head+1, "to", to, "err", err)
				if !sleep(ctx, peerBackoff) {
					return ctx.Err()
				}
				continue
			}
			return err
		}

		if err := verifyChain(parent, blocks); err != nil {
			// A parent-hash break within bulk history is unexpected; stop rather than
			// auto-reorg (tip-following/reorg is a later phase).
			logger.Error("[bsc] chain verification failed, stopping", "from", head+1, "err", err)
			return err
		}

		if err := cfg.ChainRW.InsertBlocks(ctx, blocks); err != nil {
			return err
		}
		last := blocks[len(blocks)-1]
		head = last.NumberU64()
		parent = last.HeaderNoCopy()

		if head-lastFcu >= cfg.FcuInterval || (cfg.TargetBlock != 0 && head >= cfg.TargetBlock) {
			if err := commitHead(ctx, logger, cfg, last); err != nil {
				return err
			}
			lastFcu = head
		}

		logger.Info("[bsc] downloaded blocks", "to", head, "hash", last.Hash())
	}
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
			if !sleep(ctx, peerBackoff) {
				return ctx.Err()
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

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
