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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type TrieContextFactory func(ctx context.Context) (PatriciaContext, func())

type WarmupConfig struct {
	Enabled    bool
	CtxFactory TrieContextFactory
	NumWorkers int
	MaxDepth   int
	LogPrefix  string
}

const WarmupMaxDepth = 128

type WarmupStats struct {
	KeysProcessed uint64
	Duration      time.Duration
}

type Warmuper struct {
	ctx        context.Context
	cancel     context.CancelFunc
	ctxFactory TrieContextFactory
	maxDepth   int
	numWorkers int
	logPrefix  string

	work chan warmupWorkItem
	g    *errgroup.Group

	keysProcessed atomic.Uint64
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
	ctx, cancel := context.WithCancel(ctx)
	w := &Warmuper{
		ctx:        ctx,
		cancel:     cancel,
		ctxFactory: cfg.CtxFactory,
		maxDepth:   cfg.MaxDepth,
		numWorkers: cfg.NumWorkers,
		logPrefix:  cfg.LogPrefix,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *Warmuper) Start() {
	if w.started.Swap(true) {
		return
	}
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
	var compactBuf [maxCompactKeyLen]byte
	for depth <= len(hashedKey) && depth <= w.maxDepth {
		prefix := nibbles.HexToCompactInto(compactBuf[:], hashedKey[:depth])

		branchData, _, err := trieCtx.Branch(prefix)
		if err != nil {
			log.Debug(fmt.Sprintf("[%s][warmup] failed to get branch", w.logPrefix),
				"prefix", common.Bytes2Hex(prefix), "error", err)
		}

		if len(branchData) < 4 {
			break
		}

		if depth >= len(hashedKey) {
			break
		}
		nextNibble := int(hashedKey[depth])

		branchData = branchData[2:] // skip touch map

		bitmap := binary.BigEndian.Uint16(branchData[0:2])
		childBit := uint16(1) << nextNibble

		if bitmap&childBit == 0 {
			break
		}

		pos := 2
		for n := range nextNibble {
			if bitmap&(uint16(1)<<n) != 0 {
				if pos >= len(branchData) {
					break
				}
				fieldBits := branchData[pos]
				pos++
				pos = skipCellFields(branchData, pos, fieldBits)
			}
		}

		if pos >= len(branchData) {
			break
		}

		fieldBits := branchData[pos]
		pos++

		if cellFields(fieldBits)&(fieldAccountAddr|fieldStorageAddr) != 0 {
			break
		}

		hasExtension := (fieldBits & 1) != 0
		if hasExtension && pos < len(branchData) {
			extLen, n := binary.Uvarint(branchData[pos:])
			if n > 0 && extLen > 0 {
				depth += int(extLen)
				continue
			}
		}

		depth++
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

func (w *Warmuper) Wait() error {
	if !w.started.Load() || w.numWorkers <= 0 {
		return nil
	}
	w.Close()
	w.g.Wait()
	return nil
}

func (w *Warmuper) Stats() WarmupStats {
	duration := time.Duration(0)
	if !w.startTime.IsZero() {
		duration = time.Since(w.startTime)
	}
	return WarmupStats{
		KeysProcessed: w.keysProcessed.Load(),
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
