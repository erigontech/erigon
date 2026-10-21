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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestParallelRoundServedViewsRejectsIdleReader(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	ops := corpus.updates(rand.New(rand.NewSource(102)), 200, 20)
	base := newParallelRoundBase(corpus.snapshot)
	trie := base.trie()
	holdFirstTaskForSecondWithin(trie, 5*time.Second)
	hook := trie.phaseHook
	joined := make(chan struct{})
	warmed := make(chan struct{})
	var factories atomic.Int32
	trie.SetTrieContextFactory(func(ctx context.Context) (commitment.PatriciaContext, func()) {
		view, cleanup := base.view(ctx)
		if factories.Add(1) == 1 {
			<-warmed
			return view, cleanup
		}
		_, _, _ = view.Branch(GlobalRootKey())
		close(warmed)
		select {
		case <-joined:
		case <-ctx.Done():
		}
		return view, cleanup
	})
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if op == nil && task.kind == phaseJoin {
			close(joined)
		}
		return hook(task, op)
	})
	_, err := trie.ProcessParallelContext(t.Context(), ops, 2)
	require.ErrorIs(t, err, errParallelRoundSerial)
}
