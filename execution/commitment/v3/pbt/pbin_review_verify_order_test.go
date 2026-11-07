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
	"bytes"
	"context"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type pbinReviewDelayedRead struct {
	commitment.PatriciaContext
	ctx context.Context
	key []byte
}

func (v *pbinReviewDelayedRead) Branch(key []byte) ([]byte, kv.Step, error) {
	if bytes.Equal(key, v.key) {
		select {
		case <-v.ctx.Done():
		case <-time.After(time.Second):
		}
	}
	return v.PatriciaContext.Branch(key)
}

func TestPBinReviewVerifyErrorOrder(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	probe := NewTrie(&trieTestContext{records: corpus.snapshot}).newVerifier()
	root, err := probe.loadRoot()
	require.NoError(t, err)
	var paths []eip8297.Bitpath
	require.NoError(t, probe.collectChainPaths(root.row, &paths, make(map[string]struct{})))
	require.Greater(t, len(paths), 1)
	first, err := rowKeyForPath(&paths[0])
	require.NoError(t, err)
	last, err := rowKeyForPath(&paths[len(paths)-1])
	require.NoError(t, err)
	records := maps.Clone(corpus.snapshot)
	delete(records, string(first))
	delete(records, string(last))
	sequential := NewTrie(&trieTestContext{records: records}).Verify()
	require.ErrorContains(t, sequential, "is missing")
	base := newParallelRoundBase(records)
	parallel := NewTrie(base.main).VerifyContext(t.Context(), 4, func(ctx context.Context) (commitment.PatriciaContext, func()) {
		view, cleanup := base.view(ctx)
		return &pbinReviewDelayedRead{PatriciaContext: view, ctx: ctx, key: first}, cleanup
	})
	require.EqualError(t, parallel, sequential.Error(), "parallel verify must report the first corruption in sequential traversal order")
}

func TestPBinReviewUpperAndPartitionError(t *testing.T) {
	corpus := newParallelRoundCorpus(t, parallelRoundTestShape)
	records := maps.Clone(corpus.snapshot)
	rootKey := GlobalRootKey()
	root, err := DecodeRecord(rootKey, records[string(rootKey)])
	require.NoError(t, err)
	for i := range root.Cells {
		if root.Cells[i].Kind == BranchCell {
			root.Cells[i].Left[0] ^= 1
			break
		}
	}
	records[string(rootKey)], err = EncodeRecord(rootKey, &root)
	require.NoError(t, err)
	rows := verifyParallelRows(t, records)
	delete(records, rows[len(rows)-1])
	seq := NewTrie(&trieTestContext{records: records}).Verify()
	base := newParallelRoundBase(records)
	par := NewTrie(base.main).VerifyContext(t.Context(), 4, base.view)
	require.Error(t, seq)
	require.EqualError(t, par, seq.Error(), "upper and partition corruption must preserve sequential error priority")
}
