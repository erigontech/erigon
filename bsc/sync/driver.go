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
// Parlia execution exists). Catch-up does no seal verification; the tip loop
// verifies every header with the Parlia verifier.
package bscsync

import (
	"context"
	"errors"
	"time"

	bscp2p "github.com/erigontech/erigon/bsc/p2p"
	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

const (
	defaultFcuBlocks    = 8192 // fallback FCU interval when LoopBlockLimit is unset
	peerBackoff         = 3 * time.Second
	tipHandoverDistance = 32
	snapshotCacheSize   = 1024
	announceWait        = time.Second
)

type chainReaderWriter interface {
	CurrentHeader(ctx context.Context) *types.Header
	GetHeader(ctx context.Context, hash common.Hash, number uint64) *types.Header
	FrozenBlocks(ctx context.Context) (uint64, bool)
	GetHeaderByNumber(ctx context.Context, number uint64) *types.Header
	InsertBlocks(ctx context.Context, blocks []*types.Block) error
	UpdateForkChoice(ctx context.Context, headHash, safeHash, finalizeHash common.Hash, timeoutOverride ...uint64) (execmodule.ExecutionStatus, *string, common.Hash, error)
}

type rangeFetcher interface {
	ListPeersMayHaveBlockNum(blockNum uint64) []*p2p.PeerId
	FetchHeaders(ctx context.Context, start, end uint64, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error)
	FetchBodies(ctx context.Context, headers []*types.Header, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error)
}

type p2pService interface {
	rangeFetcher
	p2pObserverRegistrar
	backwardDownloader
	peerPenalizer
	Run(ctx context.Context) error
}

var (
	_ chainReaderWriter = chainreader.ChainReaderWriterEth1{}
	_ p2pService        = (*bscp2p.Service)(nil)
)

// Config parameterizes the BSC download driver.
type Config struct {
	ChainConfig *chain.Config
	ChainRW     chainReaderWriter
	Svc         p2pService
	TargetBlock uint64 // upper bound for the parallel downloader; 0 follows the chain tip
	FcuInterval uint64 // blocks between UpdateForkChoice calls during catch-up
}

// RunBlockDownloader runs the p2p service and the download loop until ctx is
// cancelled or a fatal error occurs. With a target block it stops at the target,
// otherwise it follows the chain tip.
func RunBlockDownloader(ctx context.Context, logger log.Logger, cfg Config) error {
	if cfg.FcuInterval == 0 {
		cfg.FcuInterval = defaultFcuBlocks
	}
	errCh := make(chan error, 3)
	go func() { errCh <- cfg.Svc.Run(ctx) }()
	if cfg.TargetBlock > 0 {
		go func() { errCh <- runParallel(ctx, logger, cfg) }()
	} else {
		tipEvents := NewTipEvents(logger, cfg.Svc)
		go func() { errCh <- tipEvents.Run(ctx) }()
		store, err := parlia.NewSnapshotStore(cfg.ChainConfig, chainHeaderReader{ctx: ctx, chainRW: cfg.ChainRW}, snapshotCacheSize)
		if err != nil {
			return err
		}
		ts := newTipSync(logger, cfg.ChainRW, cfg.Svc, fetchBackwardsWith(cfg.Svc), tipEvents, store, NewParliaVerifier(cfg.ChainConfig))
		go func() { errCh <- followTip(ctx, logger, cfg, ts) }()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// followTip catches up to the announced head, then follows the tip until the tip loop
// falls back to catch-up.
func followTip(ctx context.Context, logger log.Logger, cfg Config, ts *tipSync) error {
	for {
		head, err := catchUp(ctx, logger, cfg, ts.tipEvents.AnnouncedHead)
		if err != nil {
			return err
		}
		if head == nil {
			return errors.New("bsc/sync: no local head to follow the tip from")
		}
		announced, _ := ts.tipEvents.AnnouncedHead()
		logger.Info("[bsc] switching to tip mode", "head", head.Number.Uint64(), "announced", announced)
		err = ts.run(ctx, head)
		if !errors.Is(err, errFallBack) {
			return err
		}
		logger.Info("[bsc] falling back to catch-up", "reason", err)
	}
}

type chainHeaderReader struct {
	ctx     context.Context
	chainRW chainReaderWriter
}

func (r chainHeaderReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	return r.chainRW.GetHeader(r.ctx, hash, number)
}

// runParallel downloads [head+1, TargetBlock] with the parallel multi-peer range
// downloader, persisting each assembled batch and advancing the head via FCU.
func runParallel(ctx context.Context, logger log.Logger, cfg Config) error {
	head, _ := computeResume(ctx, cfg.ChainRW)
	logger.Info("[bsc] block downloader started", "resumeFrom", head, "target", cfg.TargetBlock, "rangeSize", rangeSize)
	if _, err := downloadTo(ctx, logger, cfg, func() uint64 { return cfg.TargetBlock }); err != nil {
		return err
	}
	logger.Info("[bsc] reached target", "block", max(head, cfg.TargetBlock))
	return nil
}

// catchUp downloads forward until the local head is at most tipHandoverDistance
// blocks behind the announced head. The target is recomputed every round, so it
// follows the chain while the download runs.
func catchUp(ctx context.Context, logger log.Logger, cfg Config, announced func() (uint64, bool)) (*types.Header, error) {
	waitLogged := false
	for {
		if _, ok := announced(); ok {
			break
		}
		if !waitLogged {
			logger.Info("[bsc] waiting for peers to announce a head")
			waitLogged = true
		}
		if err := common.Sleep(ctx, announceWait); err != nil {
			return nil, err
		}
	}
	target := func() uint64 {
		head, _ := announced()
		if head < tipHandoverDistance {
			return 0
		}
		return head - tipHandoverDistance
	}
	announcedHead, _ := announced()
	logger.Info("[bsc] catching up to the announced head", "announced", announcedHead, "handoverDistance", tipHandoverDistance)
	return downloadTo(ctx, logger, cfg, target)
}

// downloadTo downloads from the resume point up to target, evaluated every round,
// and commits the last inserted block. It returns the head.
func downloadTo(ctx context.Context, logger log.Logger, cfg Config, target func() uint64) (*types.Header, error) {
	head, parent := computeResume(ctx, cfg.ChainRW)
	fcuInterval := cfg.FcuInterval
	if fcuInterval == 0 {
		fcuInterval = defaultFcuBlocks
	}
	lastFcu := head
	var last *types.Block
	insert := func(ctx context.Context, blocks []*types.Block) error {
		if err := cfg.ChainRW.InsertBlocks(ctx, blocks); err != nil {
			return err
		}
		last = blocks[len(blocks)-1]
		head = last.NumberU64()
		if head-lastFcu >= fcuInterval {
			if err := commitHead(ctx, logger, cfg, last); err != nil {
				return err
			}
			lastFcu = head
		}
		return nil
	}
	if err := downloadRanges(ctx, logger, cfg.Svc, head+1, target, parent, insert); err != nil {
		return nil, err
	}
	if last == nil {
		return parent, nil
	}
	if head > lastFcu {
		if err := commitHead(ctx, logger, cfg, last); err != nil {
			return nil, err
		}
	}
	return last.Header(), nil
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
