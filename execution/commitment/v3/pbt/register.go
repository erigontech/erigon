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

package pbt

import (
	"context"
	"errors"
	"io"

	"github.com/erigontech/erigon/execution/commitment"
)

type registeredTrie struct {
	*Trie
	workers int
	stats   commitment.PBinCodeStats
}

func init() {
	commitment.NewCommitmentBinTrie = newRegisteredTrie
}

func newRegisteredTrie(tmpdir string, cfg commitment.TrieConfig) (commitment.Trie, *commitment.Updates) {
	return &registeredTrie{Trie: NewTrie(nil), workers: cfg.WarmupNumWorkersOrDefault()}, commitment.NewBinUpdates(tmpdir, nil)
}

func (t *registeredTrie) RootHash() ([]byte, error) {
	hash, err := t.Trie.RootHash()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), hash[:]...), nil
}

func (t *registeredTrie) SetTraceWriter(io.Writer) {}

func (t *registeredTrie) Variant() commitment.TrieVariant { return commitment.VariantBinPatriciaTrie }

func (t *registeredTrie) Reset() { t.Trie.Reset() }

func (t *registeredTrie) ResetContext(ctx commitment.PatriciaContext) { t.Trie.ResetContext(ctx) }

func (t *registeredTrie) Process(context.Context, *commitment.Updates, string, func(*commitment.CommitProgress), commitment.WarmupConfig) ([]byte, error) {
	return nil, errors.New("pbin: binary rows require a PBinFeed")
}

func (t *registeredTrie) Release() { t.Trie.Release() }

func (t *registeredTrie) SetTrieContextFactory(factory commitment.TrieContextFactory) {
	t.Trie.SetTrieContextFactory(factory)
}

func (t *registeredTrie) ProcessPBinFeed(ctx context.Context, feed *commitment.PBinFeed, onProgress func(*commitment.CommitProgress)) ([]byte, error) {
	ops, err := TranslateFeed(feed)
	if err != nil {
		return nil, err
	}
	t.stats = feedCodeStats(feed)
	hash, err := t.Trie.ProcessParallelContext(ctx, ops, t.workers)
	if err != nil {
		return nil, err
	}
	if onProgress != nil {
		onProgress(&commitment.CommitProgress{KeyIndex: uint64(len(ops)), UpdateCount: uint64(len(ops))})
	}
	return append([]byte(nil), hash[:]...), nil
}

func (t *registeredTrie) ProcessPBinOps(ctx context.Context, ops []Op, onProgress func(*commitment.CommitProgress)) ([]byte, error) {
	hash, err := t.Trie.ProcessParallelContext(ctx, ops, t.workers)
	if err != nil {
		return nil, err
	}
	if onProgress != nil {
		onProgress(&commitment.CommitProgress{KeyIndex: uint64(len(ops)), UpdateCount: uint64(len(ops))})
	}
	return append([]byte(nil), hash[:]...), nil
}

func (t *registeredTrie) CodeStats() commitment.PBinCodeStats { return t.stats }

func CodeStatsFromFeed(feed *commitment.PBinFeed) commitment.PBinCodeStats {
	return feedCodeStats(feed)
}
