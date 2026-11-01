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

	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/execution/tests/blockgen"
)

// TestLocalConformance is T-L1-1 for the native reader of an L1 chain in the
// same process, which also pushes its new heads.
func TestLocalConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	r := l1.NewLocal(m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig, m.Notifications.Events)
	l1test.Conformance(t, r, model, l1test.SampleFilters)

	ctx, cancel := context.WithCancel(t.Context())
	heads, err := r.Heads(ctx)
	require.NoError(t, err)
	head, err := r.Head(t.Context())
	require.NoError(t, err)
	tx, err := m.DB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	parent, err := m.BlockReader.BlockByNumber(t.Context(), tx, head.Number)
	require.NoError(t, err)
	tx.Rollback()
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
