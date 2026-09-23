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

package v4

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errTrieReleased = errors.New("commitment v4: trie released")
	errTrieContext  = errors.New("commitment v4: missing Patricia context")
)

type Trie struct {
	ctx             commitment.PatriciaContext
	ctxFactory      commitment.TrieContextFactory
	root            []byte
	scheduleWorkers int
	scheduleStats   *scheduleStats
	deferUpdates    bool
	deferred        [][]recordDelta
}

type deferredPatriciaContext struct {
	commitment.PatriciaContext
	meter *meteredContext
	sink  *deferredPatriciaContext
	mu    sync.Mutex
	parts [][]recordDelta
}

func (c *deferredPatriciaContext) PutBranch(key, data, prev []byte) error {
	return c.putDeltas([]recordDelta{{key: key, data: data, prev: prev}})
}

func (c *deferredPatriciaContext) putDeltas(deltas []recordDelta) error {
	if c.sink != nil {
		return c.sink.putDeltas(deltas)
	}
	changed := deltas[:0]
	size := 0
	for _, d := range deltas {
		if bytes.Equal(d.prev, d.data) {
			continue
		}
		changed = append(changed, d)
		size += len(d.data)
	}
	if len(changed) == 0 {
		return nil
	}
	if c.meter != nil {
		c.meter.countWrites(len(changed), size)
	}
	c.mu.Lock()
	c.parts = append(c.parts, changed)
	c.mu.Unlock()
	return nil
}

func putDeltas(ctx commitment.PatriciaContext, deltas []recordDelta) error {
	if d, ok := ctx.(*deferredPatriciaContext); ok {
		return d.putDeltas(deltas)
	}
	return applyDeltas(deltas, ctx.PutBranch)
}

func (c *deferredPatriciaContext) wrapFactory(f commitment.TrieContextFactory) commitment.TrieContextFactory {
	if f == nil {
		return nil
	}
	return func(ctx context.Context) (commitment.PatriciaContext, func()) {
		inner, cleanup := f(ctx)
		if inner == nil {
			return nil, cleanup
		}
		return &deferredPatriciaContext{PatriciaContext: inner, sink: c}, cleanup
	}
}

func (c *deferredPatriciaContext) take() [][]recordDelta {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := c.parts
	c.parts = nil
	return parts
}

func NewTrie(tmpdir string, cfg commitment.TrieConfig) (commitment.Trie, *commitment.Updates) {
	if err := AssertV1Keyed(cfg.NibblesV2); err != nil {
		panic(err)
	}
	return &Trie{}, commitment.NewUpdates(commitment.ModeCollect, tmpdir, commitment.KeyToHexNibbleHash)
}

func init() {
	commitment.NewCommitmentV4Trie = NewTrie
}

func (t *Trie) RootHash() ([]byte, error) {
	if t == nil {
		return nil, errTrieReleased
	}
	if len(t.root) == 0 {
		return bytes.Clone(empty.RootHash[:]), nil
	}
	return bytes.Clone(t.root), nil
}

func (t *Trie) SetTraceWriter(io.Writer) {}

func (t *Trie) Variant() commitment.TrieVariant {
	return commitment.VariantCommitmentV4
}

func (*Trie) StateKey() []byte {
	return commitment.KeyCommitmentV4State
}

func (t *Trie) Reset() {
	if t != nil {
		t.root = nil
	}
}

func (t *Trie) SetTrieContextFactory(f commitment.TrieContextFactory) {
	if t != nil {
		t.ctxFactory = f
	}
}

func (t *Trie) ResetContext(ctx commitment.PatriciaContext) {
	if t != nil {
		t.ctx = ctx
	}
}

func (t *Trie) SetDeferCommitmentUpdates(deferUpdates bool) {
	if t != nil {
		t.deferUpdates = deferUpdates
	}
}

func (t *Trie) TakeDeferredUpdates() func(func(prefix, data, prevData []byte) error) error {
	if t == nil || len(t.deferred) == 0 {
		return nil
	}
	parts := t.deferred
	t.deferred = nil
	return func(putBranch func(prefix, data, prevData []byte) error) error {
		for _, deltas := range parts {
			for _, d := range deltas {
				if err := putBranch(d.key, d.data, d.prev); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func (t *Trie) Process(
	ctx context.Context,
	updates *commitment.Updates,
	logPrefix string,
	onProgress func(*commitment.CommitProgress),
	warmup commitment.WarmupConfig,
) ([]byte, error) {
	if t == nil {
		return nil, errTrieReleased
	}
	if t.ctx == nil {
		return nil, errTrieContext
	}
	if updates == nil {
		return nil, errors.New("commitment v4: nil updates")
	}
	if updates.Mode() != commitment.ModeCollect {
		return nil, errors.New("commitment v4: Process requires ModeCollect updates")
	}

	roundStart := time.Now()
	metered := newMeteredContext(t.ctx)
	var seen int
	defer func() { metered.publish(roundStart, uint64(seen)) }()

	var warmuper *commitment.Warmuper
	if warmup.Enabled {
		warmup.Key = warmupKeyV4
		warmup.Step = warmupStepV4
		warmuper = commitment.NewWarmuper(ctx, warmup)
		warmuper.Start()
		defer warmuper.CloseAndWait()
	}

	storage, accounts, seenKeys, err := partitionUpdates(ctx, updates, t.scheduleWorkers, warmuper)
	if err != nil {
		return nil, err
	}
	seen = seenKeys
	var processCtx commitment.PatriciaContext = metered
	factory := metered.wrapFactory(t.ctxFactory)
	var deferredCtx *deferredPatriciaContext
	if t.deferUpdates {
		if len(t.deferred) != 0 {
			return nil, errors.New("commitment v4: deferred updates were not taken")
		}
		deferredCtx = &deferredPatriciaContext{PatriciaContext: metered, meter: metered}
		processCtx = deferredCtx
		factory = deferredCtx.wrapFactory(factory)
	}
	root, err := runScheduledPhases(ctx, processCtx, factory, storage, accounts, t.scheduleWorkers, t.scheduleStats)
	if err != nil {
		return nil, err
	}
	if deferredCtx != nil {
		t.deferred = deferredCtx.take()
	}
	t.root = append(t.root[:0], root[:]...)
	if onProgress != nil {
		onProgress(&commitment.CommitProgress{KeyIndex: uint64(seen), UpdateCount: uint64(seen)})
	}
	return bytes.Clone(t.root), nil
}

func (t *Trie) Release() {
	if t != nil {
		t.ctx = nil
		t.root = nil
	}
}
