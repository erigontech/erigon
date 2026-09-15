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

package commitment

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// cancelAfterCtx reports itself done from the (after+1)th Done() call on. HashSort asks once per key, so
// the cancellation lands after `after` keys have been folded into the grid — mid-computation, every time.
type cancelAfterCtx struct {
	context.Context
	mu    sync.Mutex
	calls int
	after int
	done  chan struct{}
}

func newCancelAfterCtx(after int) *cancelAfterCtx {
	return &cancelAfterCtx{Context: context.Background(), after: after, done: make(chan struct{})}
}

func (c *cancelAfterCtx) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls > c.after {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
	}
	return c.done
}

func (c *cancelAfterCtx) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// A computation abandoned part way through must not poison the trie for the next one. A block round that is
// cut at its deadline while the commitment is folding returns early; the next round restores the trie from
// the stored state and computes again. Left with live rows from the abandoned fold, that restore refused
// every time ("target trie has active rows") and the chain could never seal another block.
func Test_HexPatriciaHashed_AbandonedProcessLeavesTrieRestorable(t *testing.T) {
	t.Parallel()

	firstKeys, firstUpdates := NewUpdateBuilder().
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Storage("02", "01", "0201").
		Storage("02", "56", "050505").
		Build()
	secondKeys, secondUpdates := NewUpdateBuilder().
		Balance("03", 7).
		Balance("04", 8).
		Storage("03", "57", "060606").
		Storage("04", "01", "0401").
		Balance("05", 9).
		Storage("05", "02", "8989").
		Build()

	// committed builds a state holding the first batch, as a block the chain has already sealed.
	committed := func() (*MockState, *HexPatriciaHashed, []byte) {
		ms := NewMockState(t)
		hph := NewHexPatriciaHashed(1, ms, DefaultTrieConfig())
		upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, firstKeys, firstUpdates)
		defer upds.Close()
		require.NoError(t, ms.applyPlainUpdates(firstKeys, firstUpdates))
		_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
		require.NoError(t, err)
		saved, err := hph.EncodeCurrentState(nil)
		require.NoError(t, err)
		return ms, hph, saved
	}
	second := func(t *testing.T, ms *MockState, hph *HexPatriciaHashed, ctx context.Context) ([]byte, error) {
		upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, secondKeys, secondUpdates)
		defer upds.Close()
		return hph.Process(ctx, upds, "", nil, WarmupConfig{})
	}

	// The round that is cut: it folds part of the second batch into the trie and returns.
	abandonedState, trie, saved := committed()
	require.NoError(t, abandonedState.applyPlainUpdates(secondKeys, secondUpdates))
	_, err := second(t, abandonedState, trie, newCancelAfterCtx(3))
	require.ErrorIs(t, err, context.Canceled, "the computation was meant to be cut part way")

	// The node discards the cut round's writes with its state, so the retry starts from the committed state
	// again — the SAME trie object, restored from the stored trie state.
	retryState, _, _ := committed()
	require.NoError(t, retryState.applyPlainUpdates(secondKeys, secondUpdates))
	trie.ResetContext(retryState)
	require.NoError(t, trie.SetState(saved), "an abandoned computation left the trie unrestorable")
	afterAbandon, err := second(t, retryState, trie, context.Background())
	require.NoError(t, err)

	cleanState, cleanTrie, _ := committed()
	require.NoError(t, cleanState.applyPlainUpdates(secondKeys, secondUpdates))
	clean, err := second(t, cleanState, cleanTrie, context.Background())
	require.NoError(t, err)

	require.Equal(t, clean, afterAbandon, "the retried computation differs from one that was never abandoned")
}
