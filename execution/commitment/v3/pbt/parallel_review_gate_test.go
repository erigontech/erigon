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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestReviewGateDelayedFactory(t *testing.T) {
	initial, ops := parallelRoundChainCollapseCorpus()
	seed := newTrieTestContext()
	requireProcess(t, seed, sortParallelRoundOps(initial))
	base := newParallelRoundBase(seed.records)
	trie := base.trie()
	holdFirstTaskForSecond(t, trie)
	hook := trie.phaseHook
	started := make(chan struct{})
	var calls atomic.Int32
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if op == nil && calls.Add(1) == 1 {
			close(started)
		}
		return hook(task, op)
	})
	var factories atomic.Int32
	trie.SetTrieContextFactory(func(ctx context.Context) (commitment.PatriciaContext, func()) {
		if factories.Add(1) == 2 {
			<-started
			time.Sleep(6 * time.Second)
		}
		return base.view(ctx)
	})
	_, err := trie.ProcessParallelContext(t.Context(), sortParallelRoundOps(ops), 2)
	require.NoError(t, err, "a delayed second factory must not classify a two-worker round as serial")
}
