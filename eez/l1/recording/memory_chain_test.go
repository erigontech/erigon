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
package recording_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/eez/l1/recording"
)

// TestMemoryConformance is T-L1-1 against the in-memory chain.
func TestMemoryConformance(t *testing.T) {
	c := l1test.SampleChain(t)
	l1test.Conformance(t, c, c, l1test.SampleFilters)
	require.Len(t, c.Orphans(), 3)
}

func TestMemoryChainRejects(t *testing.T) {
	ctx := context.Background()
	c := l1test.SampleChain(t)
	head, err := c.Head(ctx)
	require.NoError(t, err)

	require.Error(t, c.Append(&recording.Block{Header: l1.Header{Number: head.Number + 1, Hash: common.Hash{1}}}))
	require.Error(t, c.Append(&recording.Block{Header: l1.Header{Number: head.Number + 2, Hash: common.Hash{1}, ParentHash: head.Hash}}))
	require.Error(t, c.Reorg(3), "below finalized")
	require.Error(t, c.Finalize(head.Number+1))

	c.SetMaxLogRange(4)
	_, err = c.Logs(ctx, 0, 4, l1test.EmitterA, nil)
	require.ErrorIs(t, err, l1.ErrRangeTooWide)
	_, err = c.Logs(ctx, 0, 3, l1test.EmitterA, nil)
	require.NoError(t, err)
	_, err = c.Logs(ctx, 0, head.Number+1, l1test.EmitterA, nil)
	require.ErrorIs(t, err, l1.ErrNotFound)
}

// TestMemoryChainHistory: blocks below the first one are before the
// chain's history; any other missing block is only not found.
func TestMemoryChainHistory(t *testing.T) {
	first := l1.Header{Number: 5, Hash: common.Hash{5}, ParentHash: common.Hash{4}}
	c, err := recording.NewMemoryChain(1, &recording.Block{Header: first})
	require.NoError(t, err)
	ctx := context.Background()

	_, err = c.HeaderByHash(ctx, first.ParentHash)
	require.ErrorIs(t, err, l1.ErrBeforeHistory)
	require.ErrorIs(t, err, l1.ErrNotFound)
	_, err = c.HeaderByNumber(ctx, 4)
	require.ErrorIs(t, err, l1.ErrBeforeHistory)

	_, err = c.HeaderByHash(ctx, common.Hash{9})
	require.ErrorIs(t, err, l1.ErrNotFound)
	require.NotErrorIs(t, err, l1.ErrBeforeHistory)
	_, err = c.HeaderByNumber(ctx, 6)
	require.NotErrorIs(t, err, l1.ErrBeforeHistory)
}
