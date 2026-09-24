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

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
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

func (w *Warmuper) begin() bool {
	if w.started.Swap(true) {
		return false
	}
	if w.numWorkers <= 0 {
		return false
	}
	w.work = make(chan warmupWorkItem, w.numWorkers*64)
	w.g, w.ctx = errgroup.WithContext(w.ctx)
	return true
}

func (w *Warmuper) goWorker(run func(trieCtx PatriciaContext, buf []byte) error) {
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
		return run(trieCtx, make([]byte, warmupKeyScratchLen))
	})
}

func (w *Warmuper) Start() {
	if !w.begin() {
		return
	}

	for range w.numWorkers {
		w.goWorker(func(trieCtx PatriciaContext, buf []byte) error {
			for {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				case item, ok := <-w.work:
					if !ok {
						return nil
					}
					w.warmupKey(trieCtx, item.hashedKey, item.startDepth, buf)
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

const warmSortedChunk = 256

func (w *Warmuper) WarmSorted(n int, key func(i int) []byte) {
	w.begin()
	if w.g == nil || w.closed.Load() {
		return
	}
	var next atomic.Int64
	for range w.numWorkers {
		w.goWorker(func(trieCtx PatriciaContext, buf []byte) error {
			for w.ctx.Err() == nil {
				lo := int(next.Add(warmSortedChunk)) - warmSortedChunk
				if lo >= n {
					return nil
				}
				var prev []byte
				for i := lo; i < min(lo+warmSortedChunk, n); i++ {
					hk := key(i)
					depth := nibbles.CommonPrefixLen(prev, hk)
					w.warmupKey(trieCtx, hk, depth, buf)
					w.keysProcessed.Add(1)
					prev = hk
				}
			}
			return nil
		})
	}
}

func (w *Warmuper) warmupKey(trieCtx PatriciaContext, hashedKey []byte, startDepth int, buf []byte) {
	depth := startDepth
	for depth <= len(hashedKey) && depth <= w.maxDepth {
		prefix, ok := w.key(hashedKey, depth, buf)
		if !ok {
			break
		}

		branchData, _, err := trieCtx.Branch(prefix)
		if err != nil {
			log.Debug(fmt.Sprintf("[%s][warmup] failed to get branch", w.logPrefix),
				"prefix", common.Bytes2Hex(prefix), "error", err)
		}

		nextDepth, stop := w.step(branchData, hashedKey, depth)
		if stop || nextDepth <= depth {
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
	return WarmupStats{KeysProcessed: w.keysProcessed.Load()}
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
	w.cancel()
}
