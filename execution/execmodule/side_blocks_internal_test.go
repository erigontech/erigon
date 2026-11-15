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

package execmodule

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/types"
)

func TestSideBlocksToRetain(t *testing.T) {
	hash := func(i int) common.Hash { return common.Hash{byte(i)} }
	pending := make(map[common.Hash]pendingBlock, 20)
	for i := range 20 {
		pending[hash(i)] = pendingBlock{number: uint64(i)}
	}

	side := sideBlocksToRetain(pending, []common.Hash{hash(19)}, 10)

	require.Len(t, side, 8)
	for i := 11; i < 19; i++ {
		require.Contains(t, side, hash(i))
	}
	for i := range 11 {
		require.NotContains(t, side, hash(i))
	}
}

func TestDropBadChainWithUnknownLatestValidHashKeepsAncestors(t *testing.T) {
	c := common.Hash{1}
	p := common.Hash{2}
	s := common.Hash{3}
	b := common.Hash{4}

	e := &ExecModule{pendingBlocks: map[common.Hash]pendingBlock{
		p: {parent: c},
		s: {parent: p},
		b: {parent: s},
	}}
	e.dropBadChain(b, common.Hash{})
	require.NotContains(t, e.pendingBlocks, b)
	require.Contains(t, e.pendingBlocks, p)
	require.Contains(t, e.pendingBlocks, s)

	e.pendingBlocks = map[common.Hash]pendingBlock{
		c: {},
		p: {parent: c},
		s: {parent: p},
		b: {parent: s},
	}
	e.dropBadChain(b, c)
	require.NotContains(t, e.pendingBlocks, b)
	require.NotContains(t, e.pendingBlocks, p)
	require.NotContains(t, e.pendingBlocks, s)
	require.Contains(t, e.pendingBlocks, c)
}

func TestPurgeBadChainStopsAtMissingHeader(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	missing := common.Hash{1}
	e := &ExecModule{blockReader: nilHeaderReader{}}

	require.NotPanics(t, func() {
		require.NoError(t, e.purgeBadChain(t.Context(), tx, common.Hash{}, missing))
	})
}

type nilHeaderReader struct{ dbservices.FullBlockReader }

func (nilHeaderReader) Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error) {
	return nil, nil
}

func (nilHeaderReader) HeaderNumber(context.Context, kv.Getter, common.Hash) (*uint64, error) {
	number := uint64(1)
	return &number, nil
}
