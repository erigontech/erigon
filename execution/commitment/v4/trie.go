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
	"time"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var errTrieContext = errors.New("commitment v4: missing Patricia context")

type Trie struct {
	ctx             commitment.PatriciaContext
	ctxFactory      commitment.TrieContextFactory
	root            []byte
	scheduleWorkers int
	fanOutMin       int
	deferUpdates    bool
	deferred        deltaParts
}

func NewTrie(tmpdir string, _ commitment.TrieConfig) (commitment.Trie, *commitment.Updates) {
	return &Trie{}, commitment.NewUpdates(commitment.ModeCollect, tmpdir, commitment.KeyToHexNibbleHash)
}

func init() {
	commitment.NewCommitmentV4Trie = NewTrie
}

func (t *Trie) RootHash() ([]byte, error) {
	if len(t.root) == 0 {
		return bytes.Clone(empty.RootHash[:]), nil
	}
	return bytes.Clone(t.root), nil
}

func (t *Trie) SetTraceWriter(io.Writer) {}

func (t *Trie) Variant() commitment.TrieVariant {
	return commitment.VariantCommitmentV4
}

func (t *Trie) Reset() { t.root = nil }

func (t *Trie) SetTrieContextFactory(f commitment.TrieContextFactory) { t.ctxFactory = f }

func (t *Trie) ResetContext(ctx commitment.PatriciaContext) { t.ctx = ctx }

func (t *Trie) SetDeferCommitmentUpdates(deferUpdates bool) { t.deferUpdates = deferUpdates }

func (t *Trie) SetStorageFanOutMin(n int) { t.fanOutMin = n }

func (t *Trie) TakeDeferredDeltas() [][]commitment.BranchDelta {
	parts := t.deferred
	t.deferred = nil
	return parts
}

func (t *Trie) Process(
	ctx context.Context,
	updates *commitment.Updates,
	logPrefix string,
	onProgress func(*commitment.CommitProgress),
	warmup commitment.WarmupConfig,
) ([]byte, error) {
	if updates == nil {
		return nil, errors.New("commitment v4: nil updates")
	}
	if updates.Mode() != commitment.ModeCollect {
		return nil, errors.New("commitment v4: Process requires ModeCollect updates")
	}
	var warmuper *commitment.Warmuper
	if warmup.Enabled {
		warmup.Key = warmupKeyV4
		warmup.Step = warmupStepV4
		warmuper = commitment.NewWarmuper(ctx, warmup)
		defer warmuper.CloseAndWait()
	}
	return t.round(ctx, onProgress, func() ([]storageTask, []accountEntry, int, error) {
		return partitionUpdates(ctx, updates, t.scheduleWorkers, warmuper)
	})
}

func (t *Trie) ProcessFeed(ctx context.Context, feed *commitment.Feed, onProgress func(*commitment.CommitProgress)) ([]byte, error) {
	if feed == nil {
		return nil, errors.New("commitment v4: nil feed")
	}
	return t.round(ctx, onProgress, func() ([]storageTask, []accountEntry, int, error) {
		storage, accounts := partitionAccounts(feed.Accounts, t.scheduleWorkers)
		return storage, accounts, feed.Keys, nil
	})
}

func (t *Trie) round(ctx context.Context, onProgress func(*commitment.CommitProgress), partition func() ([]storageTask, []accountEntry, int, error)) ([]byte, error) {
	if t.ctx == nil {
		return nil, errTrieContext
	}
	if t.deferUpdates && len(t.deferred) != 0 {
		return nil, errors.New("commitment v4: deferred updates were not taken")
	}
	roundStart := time.Now()
	metered := &meteredContext{t.ctx, new(meterCounts)}
	var seen int
	defer func() { metered.publish(roundStart, uint64(seen)) }()

	storage, accounts, seen, err := partition()
	if err != nil {
		return nil, err
	}
	root, parts, err := runScheduledPhases(ctx, metered, metered.wrapFactory(t.ctxFactory), storage, accounts, t.scheduleWorkers, t.fanOutMin)
	if err != nil {
		return nil, err
	}
	metered.countDeltas(parts)
	if t.deferUpdates {
		t.deferred = parts
	} else if err := applyDeltas(parts, t.ctx.PutBranch); err != nil {
		return nil, err
	}
	t.root = append(t.root[:0], root[:]...)
	if onProgress != nil {
		onProgress(&commitment.CommitProgress{KeyIndex: uint64(seen), UpdateCount: uint64(seen)})
	}
	return bytes.Clone(t.root), nil
}

func (t *Trie) Release() {
	t.ctx = nil
	t.root = nil
}
