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

package l1_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// TestLocalConformance is T-L1-1 for the native reader of an L1 chain in the
// same process, which also pushes its new heads.
func TestLocalConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	r := l1.NewLocal(m.DB, m.BlockReader, m.ReceiptsReader, m.Engine, m.ChainConfig, m.Notifications.Events)
	l1test.Conformance(t, r, model, l1test.SampleFilters)

	ctx, cancel := context.WithCancel(t.Context())
	heads, err := r.Heads(ctx)
	require.NoError(t, err)
	head, err := r.Head(t.Context())
	require.NoError(t, err)
	var parent *types.Block
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) (err error) {
		parent, err = m.BlockReader.BlockByNumber(t.Context(), tx, head.Number)
		return err
	}))
	next, err := m.GenerateChainFrom(parent, 1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), next.Blocks))
	select {
	case <-heads:
	case <-time.After(10 * time.Second):
		t.Fatal("no head event")
	}
	require.Eventually(t, func() bool {
		h, err := r.Head(context.Background())
		return err == nil && h.Hash == next.Blocks[0].Hash()
	}, 10*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case _, ok := <-heads:
		if ok {
			_, ok = <-heads
		}
		require.False(t, ok, "heads closes with its context")
	case <-time.After(10 * time.Second):
		t.Fatal("heads still open")
	}
}

// TestLocalHeadsAfterCommit: the chain announces a header before it commits
// its block; the reader delivers the head only once it reads that block as
// its head.
func TestLocalHeadsAfterCommit(t *testing.T) {
	m, _ := l1test.ErigonL1(t)
	r := l1.NewLocal(m.DB, m.BlockReader, m.ReceiptsReader, m.Engine, m.ChainConfig, m.Notifications.Events)
	heads, err := r.Heads(t.Context())
	require.NoError(t, err)
	head, err := r.Head(t.Context())
	require.NoError(t, err)
	var parent *types.Block
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) (err error) {
		parent, err = m.BlockReader.BlockByNumber(t.Context(), tx, head.Number)
		return err
	}))
	next, err := m.GenerateChainFrom(parent, 1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	announced, err := rlp.EncodeToBytes(next.Blocks[0].Header())
	require.NoError(t, err)

	m.Notifications.Events.OnNewHeader([][]byte{announced})
	select {
	case <-heads:
		t.Fatal("head delivered before its block is committed")
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), next.Blocks))
	select {
	case <-heads:
	case <-time.After(10 * time.Second):
		t.Fatal("no head event")
	}
	h, err := r.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, next.Blocks[0].Hash(), h.Hash, "the delivered head is readable")

	// An announced head that never becomes the head does not hold the next
	// one.
	stale, err := m.GenerateChainFrom(next.Blocks[0], 1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	staleHeader := types.CopyHeader(stale.Blocks[0].Header())
	staleHeader.Extra = []byte("never committed")
	never, err := rlp.EncodeToBytes(staleHeader)
	require.NoError(t, err)
	for drained := false; !drained; {
		select {
		case <-heads:
		case <-time.After(300 * time.Millisecond):
			drained = true
		}
	}
	m.Notifications.Events.OnNewHeader([][]byte{never})
	start := time.Now()
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), stale.Blocks))
	select {
	case <-heads:
	case <-time.After(10 * time.Second):
		t.Fatal("no head event")
	}
	require.Less(t, time.Since(start), time.Second, "a superseded announcement held the next head")
	h, err = r.Head(t.Context())
	require.NoError(t, err)
	require.Equal(t, stale.Blocks[0].Hash(), h.Hash)

	// One alone is delivered once the wait for it ends.
	for drained := false; !drained; {
		select {
		case <-heads:
		case <-time.After(300 * time.Millisecond):
			drained = true
		}
	}
	m.Notifications.Events.OnNewHeader([][]byte{never})
	start = time.Now()
	select {
	case <-heads:
	case <-time.After(10 * time.Second):
		t.Fatal("an announced head that never came held the event")
	}
	require.GreaterOrEqual(t, time.Since(start), time.Second, "delivered before the wait for the head ended")
}
