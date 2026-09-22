// Copyright 2022 The Erigon Authors
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

package commitment

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type TrieContextFactory func(ctx context.Context) (PatriciaContext, func())

type WarmupKeyFunc func(hashedKey []byte, depth int, dst []byte) ([]byte, bool)

type WarmupStepFunc func(record, hashedKey []byte, depth int) (nextDepth int, stop bool)

type WarmupConfig struct {
	Enabled    bool
	CtxFactory TrieContextFactory
	NumWorkers int
	MaxDepth   int
	LogPrefix  string
	Key        WarmupKeyFunc
	Step       WarmupStepFunc
}

const WarmupMaxDepth = 128
const warmupKeyScratchLen = maxCompactKeyLen + 1

type WarmupStats struct {
	KeysProcessed uint64
	RecordsFound  uint64
	Duration      time.Duration
}

type Warmuper struct {
	ctx        context.Context
	cancel     context.CancelFunc
	ctxFactory TrieContextFactory
	maxDepth   int
	numWorkers int
	logPrefix  string
	key        WarmupKeyFunc
	step       WarmupStepFunc

	work chan warmupWorkItem
	g    *errgroup.Group

	keysProcessed atomic.Uint64
	recordsFound  atomic.Uint64
	startTime     time.Time

	outstanding [arenaRingSize]atomic.Int64
	mu          sync.Mutex
	cond        *sync.Cond

	started atomic.Bool
	closed  atomic.Bool
}

type warmupWorkItem struct {
	hashedKey  []byte
	startDepth int
	gen        uint64
}

func NewWarmuper(ctx context.Context, cfg WarmupConfig) *Warmuper {
	if cfg.Key == nil {
		panic("warmup key function is nil")
	}
	if cfg.Step == nil {
		panic("warmup step function is nil")
	}
	ctx, cancel := context.WithCancel(ctx)
	w := &Warmuper{
		ctx:        ctx,
		cancel:     cancel,
		ctxFactory: cfg.CtxFactory,
		maxDepth:   cfg.MaxDepth,
		numWorkers: cfg.NumWorkers,
		logPrefix:  cfg.LogPrefix,
		key:        cfg.Key,
		step:       cfg.Step,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *Warmuper) Start() {
	if w.started.Swap(true) {
		return
	}
	w.startTime = time.Now()
	if w.numWorkers <= 0 {
		return
	}

	w.work = make(chan warmupWorkItem, w.numWorkers*64)
	w.g, w.ctx = errgroup.WithContext(w.ctx)

	for i := 0; i < w.numWorkers; i++ {
		w.g.Go(func() error {
			trieCtx, cleanup := w.ctxFactory(w.ctx)
			if cleanup != nil {
				defer cleanup()
			}
			if trieCtx == nil {
				if err := w.ctx.Err(); err != nil {
					return err
				}
				return errors.New("warmup trie context factory returned nil PatriciaContext")
			}

			for {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				case item, ok := <-w.work:
					if !ok {
						return nil
					}
					w.warmupKey(trieCtx, item.hashedKey, item.startDepth)
					w.keysProcessed.Add(1)
					w.releaseGen(item.gen)
				}
			}
		})
	}

	// Wake WaitBufferFree on shutdown to avoid hanging on undrained items.
	w.g.Go(func() error {
		<-w.ctx.Done()
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
		return nil
	})
}

func (w *Warmuper) warmupKey(trieCtx PatriciaContext, hashedKey []byte, startDepth int) {
	depth := startDepth
	var compactBuf [warmupKeyScratchLen]byte
	for depth <= len(hashedKey) && depth <= w.maxDepth {
		prefix, ok := w.key(hashedKey, depth, compactBuf[:])
		if !ok {
			break
		}

		branchData, _, err := trieCtx.Branch(prefix)
		if err != nil {
			log.Debug(fmt.Sprintf("[%s][warmup] failed to get branch", w.logPrefix),
				"prefix", common.Bytes2Hex(prefix), "error", err)
		}
		if len(branchData) != 0 {
			w.recordsFound.Add(1)
		}

		nextDepth, stop := w.step(branchData, hashedKey, depth)
		if stop {
			break
		}
		if nextDepth <= depth {
			break
		}
		depth = nextDepth
	}
}

func (w *Warmuper) WarmKey(hashedKey []byte, startDepth int, gen uint64) {
	if !w.started.Load() || w.numWorkers <= 0 || w.closed.Load() {
		return
	}
	w.outstanding[gen%arenaRingSize].Add(1)
	select {
	case w.work <- warmupWorkItem{hashedKey: hashedKey, startDepth: startDepth, gen: gen}:
	case <-w.ctx.Done():
		w.releaseGen(gen)
	}
}

func (w *Warmuper) releaseGen(gen uint64) {
	if w.outstanding[gen%arenaRingSize].Add(-1) == 0 {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	}
}

func (w *Warmuper) WaitBufferFree(slot int) error {
	if slot < 0 || slot >= arenaRingSize {
		return fmt.Errorf("invalid arena slot %d", slot)
	}
	if w.outstanding[slot].Load() == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.outstanding[slot].Load() != 0 {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		w.cond.Wait()
	}
	return nil
}

func (w *Warmuper) Stats() WarmupStats {
	duration := time.Duration(0)
	if !w.startTime.IsZero() {
		duration = time.Since(w.startTime)
	}
	return WarmupStats{
		KeysProcessed: w.keysProcessed.Load(),
		RecordsFound:  w.recordsFound.Load(),
		Duration:      duration,
	}
}

func (w *Warmuper) DrainPending() {
	if !w.started.Load() || w.numWorkers <= 0 {
		return
	}
	for {
		select {
		case item := <-w.work:
			w.releaseGen(item.gen)
		default:
			return
		}
	}
}

func (w *Warmuper) CloseAndWait() {
	w.Close()
	if w.g != nil {
		_ = w.g.Wait()
	}
}

func (w *Warmuper) Close() {
	if w.closed.Swap(true) {
		return
	}
	// w.work is never closed: that would race a concurrent WarmKey send into a
	// panic and make DrainPending spin. ctx cancellation is the sole shutdown signal.
	w.cancel()
}
