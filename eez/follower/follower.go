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

// Package follower runs derivation against an L1 chain: boot catch-up, the
// watcher-driven loop, and the policy that tells a halt from a retry.
package follower

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/execution/chain"
)

// ErrL1ChainID reports an L1 source on another chain than the configured
// one.
var ErrL1ChainID = errors.New("L1 source serves another chain")

// Config tunes a Follower.
type Config struct {
	// Dir holds the reconcile checkpoint; empty keeps none.
	Dir string
	// Poll is the interval between L1 head reads.
	Poll time.Duration
	// FinalityEvery is the number of head reads per finality read.
	FinalityEvery int
	// ReorgDepth is the deepest L1 reorg the follower survives.
	ReorgDepth int
	// LogChunk is the widest block range of one log query; zero is
	// l1.LogChunk.
	LogChunk uint64
	// Retry and MaxRetry bound the pause before retrying a failed step,
	// which doubles from Retry up to MaxRetry.
	Retry, MaxRetry time.Duration
	// BootAttempts is how many consecutive failures boot tolerates.
	BootAttempts int
}

// DefaultConfig is the reference client's tuning.
var DefaultConfig = Config{
	Poll:          2 * time.Second,
	FinalityEvery: 6,
	ReorgDepth:    l1.DefaultReorgDepth,
	Retry:         2 * time.Second,
	MaxRetry:      30 * time.Second,
	BootAttempts:  32,
}

// Follower derives an EEZ chain from its settlements on L1.
type Follower struct {
	cfg     Config
	l2      *config.L2Config
	reader  l1.ChainReader
	scanner *l1.Scanner
	watcher *l1.Watcher
	chain   deriver.Chain
	deriver *deriver.Deriver
	logger  log.Logger
	// scanned is the last L1 block whose batches were reconciled, target
	// the L1 head to scan to and l1Finalized the latest finalized L1 block.
	scanned, target, l1Finalized uint64
	resumed                      bool
}

// New returns a follower of the EEZ chain cfg, local in chain, settled on
// the L1 that reader serves.
func New(reader l1.ChainReader, chain deriver.Chain, cfg *chain.Config, c Config, logger log.Logger) (*Follower, error) {
	l2, ok := cfg.L2.(*config.L2Config)
	if !ok {
		return nil, fmt.Errorf("chain %s is not an EEZ chain", cfg.ChainName)
	}
	d, err := deriver.New(chain, cfg, c.Dir, logger)
	if err != nil {
		return nil, err
	}
	scanner := l1.NewScanner(reader, l2.Registry, l2.RollupID)
	if c.LogChunk > 0 {
		scanner.SetChunk(c.LogChunk)
	}
	return &Follower{
		cfg:     c,
		l2:      l2,
		reader:  reader,
		scanner: scanner,
		watcher: l1.NewWatcher(reader, c.ReorgDepth),
		chain:   chain,
		deriver: d,
		logger:  logger,
	}, nil
}

// Deriver is the follower's deriver.
func (f *Follower) Deriver() *deriver.Deriver { return f.deriver }

// Run boots and follows L1 until ctx ends or the follower must halt: on a
// divergence from canonical L1, an L1 reorg it cannot follow, or a boot
// that keeps failing.
func (f *Follower) Run(ctx context.Context) error {
	if err := f.Boot(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := make(chan l1.Event, 64)
	watched := make(chan error, 1)
	go func() { watched <- f.watcher.Run(ctx, f.cfg.Poll, f.cfg.FinalityEvery, events) }()

	var retry <-chan time.Time
	pause := f.cfg.Retry
	for {
		var err error
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-watched:
			return err
		case e := <-events:
			err = f.Handle(ctx, e)
		case <-retry:
			err = f.Sync(ctx)
		}
		if err = f.recover(ctx, err); err != nil {
			if halts(err) {
				return err
			}
			f.logger.Warn("[eez] follower step failed, retrying", "in", pause, "err", err)
			retry, pause = time.After(pause), min(2*pause, f.cfg.MaxRetry)
			continue
		}
		retry, pause = nil, f.cfg.Retry
	}
}

// Boot resumes derivation where the local chain left it and catches up with
// the L1 head, retrying failures that may pass.
func (f *Follower) Boot(ctx context.Context) error {
	pause := f.cfg.Retry
	for attempt := 1; ; attempt++ {
		err := f.recover(ctx, f.boot(ctx))
		if err == nil || halts(err) || attempt >= f.cfg.BootAttempts {
			return err
		}
		f.logger.Warn("[eez] boot catch-up failed, retrying", "attempt", attempt, "in", pause, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pause):
		}
		pause = min(2*pause, f.cfg.MaxRetry)
	}
}

func (f *Follower) boot(ctx context.Context) error {
	id, err := f.reader.ChainID(ctx)
	if err != nil {
		return err
	}
	if id != f.l2.L1ChainID {
		return fmt.Errorf("%w: chain %d, want %d", ErrL1ChainID, id, f.l2.L1ChainID)
	}
	head, err := f.reader.Head(ctx)
	if err != nil {
		return err
	}
	if !f.resumed {
		from, err := f.resume(ctx, head.Number)
		if err != nil {
			return err
		}
		f.scanned, f.resumed = from-1, true
	}
	f.target = head.Number
	if err := f.Sync(ctx); err != nil {
		return err
	}
	return f.watcher.Seed(ctx, f.scanned)
}

// resume seeds the deriver and returns the L1 block to scan from: the
// checkpoint's when L1 and the local chain still hold it, else the newest
// settlement whose block the local chain holds, else the deploy block.
func (f *Follower) resume(ctx context.Context, head uint64) (uint64, error) {
	r, err := f.checkpointSeed(ctx)
	if err != nil {
		return 0, err
	}
	if r != nil {
		f.logger.Info("[eez] resuming from checkpoint", "l1Block", r.L1Block, "l2Block", r.LastL2Block)
		f.deriver.Seed(*r)
		return r.L1Block, nil
	}
	deploy := f.l2.RegistryDeployBlock
	if r, err = deriver.FindResumePoint(ctx, f.reader, f.l2.Registry, f.l2.RollupID, deploy, head, f.chain.CanonicalNumber); err != nil {
		return 0, err
	}
	if r != nil {
		f.logger.Info("[eez] resuming from local settled block", "l1Block", r.L1Block, "l2Block", r.LastL2Block)
		f.deriver.Seed(*r)
		return r.L1Block, nil
	}
	f.logger.Info("[eez] deriving from the registry deploy block", "l1Block", deploy)
	return max(deploy, 1), nil
}

// checkpointSeed returns the checkpoint's settlement when L1 and the local
// chain still hold it. A checkpoint that cannot be read is not used.
func (f *Follower) checkpointSeed(ctx context.Context) (*deriver.Record, error) {
	if f.cfg.Dir == "" {
		return nil, nil
	}
	cp, err := deriver.LoadCheckpoint(f.cfg.Dir)
	if err != nil {
		f.logger.Warn("[eez] checkpoint unreadable", "err", err)
		return nil, nil
	}
	if cp == nil {
		return nil, nil
	}
	var canonicalL1, localL2 *common.Hash
	if h, err := f.reader.HeaderByNumber(ctx, cp.L1Block); err == nil {
		canonicalL1 = &h.Hash
	} else if !errors.Is(err, l1.ErrNotFound) {
		return nil, err
	}
	if h, ok, err := f.chain.CanonicalHash(ctx, cp.L2Cursor); err != nil {
		return nil, err
	} else if ok {
		localL2 = &h
	}
	if err := cp.UsableWith(canonicalL1, localL2); err != nil {
		f.logger.Info("[eez] checkpoint unusable", "err", err)
		return nil, nil
	}
	return &deriver.Record{L1Block: cp.L1Block, L1BlockHash: cp.L1BlockHash, TxHash: cp.TxHash, LastL2Block: cp.L2Cursor}, nil
}

// Handle applies an L1 event.
func (f *Follower) Handle(ctx context.Context, e l1.Event) error {
	switch e := e.(type) {
	case l1.NewHead:
		f.target = max(f.target, e.Head.Number)
	case l1.Reorg:
		if err := f.rewind(ctx, e.Ancestor.Number); err != nil {
			return err
		}
		f.target = e.Ancestor.Number
	case l1.Finalized:
		f.l1Finalized = max(f.l1Finalized, e.Head.Number)
	}
	return f.Sync(ctx)
}

// Sync reconciles the batches of the L1 blocks up to the target and applies
// L1 finality.
func (f *Follower) Sync(ctx context.Context) error {
	if err := f.revalidate(ctx); err != nil {
		return err
	}
	for f.scanned < f.target {
		batches, reached, err := f.scanner.Scan(ctx, f.scanned+1, f.target)
		if err != nil {
			return err
		}
		if err := f.deriver.Reconcile(ctx, batches); err != nil {
			return err
		}
		f.scanned = reached
	}
	return f.deriver.OnFinalized(ctx, f.l1Finalized)
}

// revalidate rewinds to below the latest settlement whose L1 block is no
// longer canonical, which a reorg the watcher did not see can leave.
func (f *Follower) revalidate(ctx context.Context) error {
	r, ok := f.deriver.Last()
	if !ok {
		return nil
	}
	h, err := f.reader.HeaderByNumber(ctx, r.L1Block)
	if err != nil && !errors.Is(err, l1.ErrNotFound) {
		return err
	}
	if err == nil && h.Hash == r.L1BlockHash {
		return nil
	}
	return f.rewind(ctx, r.L1Block-1)
}

// rewind drops what derivation took from the L1 blocks above ancestor.
func (f *Follower) rewind(ctx context.Context, ancestor uint64) error {
	f.scanned = min(f.scanned, ancestor)
	return f.deriver.OnReorg(ctx, ancestor)
}

// recover turns a divergence found on an L1 block that is no longer
// canonical into a rewind: the evidence is gone, not contradicted.
func (f *Follower) recover(ctx context.Context, err error) error {
	var div *deriver.DivergenceError
	if !errors.As(err, &div) {
		return err
	}
	h, herr := f.reader.HeaderByNumber(ctx, div.L1Block)
	if herr != nil && !errors.Is(herr, l1.ErrNotFound) {
		// Canonicality unknown: retry rather than halt.
		return fmt.Errorf("%w (L1 block %d unreadable: %w)", retryable{err}, div.L1Block, herr)
	}
	if herr == nil && h.Hash == div.L1BlockHash {
		return err
	}
	f.logger.Warn("[eez] divergence on a reorged L1 block, rewinding", "l1Block", div.L1Block, "err", err)
	if err := f.rewind(ctx, div.L1Block-1); err != nil {
		return err
	}
	return retryable{err}
}

// retryable marks an error whose cause may pass.
type retryable struct{ error }

// halts reports whether err stops the follower.
func halts(err error) bool {
	if _, ok := errors.AsType[retryable](err); ok {
		return false
	}
	return errors.Is(err, deriver.ErrDiverged) || errors.Is(err, deriver.ErrFinalizedReorged) ||
		errors.Is(err, l1.ErrReorgTooDeep) || errors.Is(err, l1.ErrFinalityViolation) ||
		errors.Is(err, l1.ErrUndecodable) || errors.Is(err, ErrL1ChainID)
}
