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
	traceW          io.Writer
	scheduleOrder   scheduleOrder
	scheduleWorkers int
	scheduleStats   *scheduleStats
	deferUpdates    bool
	deferred        []recordDelta
}

type deferredPatriciaContext struct {
	commitment.PatriciaContext
	mu     sync.Mutex
	deltas []recordDelta
}

func (c *deferredPatriciaContext) PutBranch(key, data, prev []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deltas = append(c.deltas, recordDelta{key: key, data: data, prev: prev})
	return nil
}

func (c *deferredPatriciaContext) take() []recordDelta {
	c.mu.Lock()
	defer c.mu.Unlock()
	deltas := c.deltas
	c.deltas = nil
	return deltas
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

func (t *Trie) SetTraceWriter(w io.Writer) {
	if t != nil {
		t.traceW = w
	}
}

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
	deltas := t.deferred
	t.deferred = nil
	return func(putBranch func(prefix, data, prevData []byte) error) error {
		return applyDeltas(deltas, putBranch)
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

	storage, accounts, seen, err := partitionUpdates(ctx, updates, t.scheduleWorkers)
	if err != nil {
		return nil, err
	}
	processCtx := t.ctx
	factory := t.ctxFactory
	var deferredCtx *deferredPatriciaContext
	if t.deferUpdates {
		factory = nil
		if len(t.deferred) != 0 {
			return nil, errors.New("commitment v4: deferred updates were not taken")
		}
		deferredCtx = &deferredPatriciaContext{PatriciaContext: t.ctx}
		processCtx = deferredCtx
	}
	root, err := runScheduledPhases(ctx, processCtx, factory, storage, accounts, t.scheduleOrder, t.scheduleWorkers, t.scheduleStats)
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
		t.traceW = nil
	}
}
