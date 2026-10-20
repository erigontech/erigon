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
package deriver

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

func record(l1Block uint64, tx byte, l2 uint64) Record {
	return Record{L1Block: l1Block, L1BlockHash: common.Hash{byte(l1Block)}, TxHash: common.Hash{tx}, LastL2Block: l2}
}

func TestIndex(t *testing.T) {
	var x Index
	require.Zero(t, x.Cursor())
	_, ok := x.SettledAt(100)
	require.False(t, ok)

	require.True(t, x.Append(record(10, 1, 6)))
	require.False(t, x.Append(record(11, 1, 12)), "deduplicated by transaction hash")
	require.True(t, x.Append(record(10, 2, 6)))
	require.True(t, x.Append(record(14, 3, 12)))
	require.True(t, x.Append(record(20, 4, 9)))
	require.Equal(t, uint64(9), x.Cursor(), "the latest settlement, not the highest")
	require.Equal(t, 2, x.CountAtL1Block(10))
	require.True(t, x.Contains(common.Hash{3}))

	settled, ok := x.SettledAt(15)
	require.True(t, ok)
	require.Equal(t, uint64(12), settled)
	_, ok = x.SettledAt(9)
	require.False(t, ok)

	require.Equal(t, 2, x.RetreatOnReorg(13))
	require.Equal(t, uint64(6), x.Cursor())
	require.False(t, x.Contains(common.Hash{3}))
	require.Equal(t, 2, x.RetreatOnReorg(0))
	require.Zero(t, x.Cursor())
}

func TestIndexPrune(t *testing.T) {
	var x Index
	for _, r := range []Record{record(10, 1, 3), record(12, 2, 5), record(12, 3, 6), record(15, 4, 8)} {
		require.True(t, x.Append(r))
	}
	x.Prune(9)
	require.True(t, x.Contains(common.Hash{1}), "nothing settled up to the finalized block")

	// The latest settlement up to finality stays, with the others of its L1
	// block.
	x.Prune(13)
	require.False(t, x.Contains(common.Hash{1}))
	require.True(t, x.Contains(common.Hash{2}))
	settled, ok := x.SettledAt(13)
	require.True(t, ok)
	require.Equal(t, uint64(6), settled)
	require.Equal(t, uint64(8), x.Cursor())

	x.Prune(15)
	require.False(t, x.Contains(common.Hash{3}))
	require.True(t, x.Contains(common.Hash{4}))
}

func TestCheckpoint(t *testing.T) {
	dir := t.TempDir()
	c, err := LoadCheckpoint(dir)
	require.NoError(t, err)
	require.Nil(t, c)

	want := &Checkpoint{L1Block: 23507949, L1BlockHash: common.Hash{1}, TxHash: common.Hash{2}, L2Cursor: 163050, L2BlockHash: common.Hash{3}}
	require.NoError(t, want.Save(dir))
	got, err := LoadCheckpoint(dir)
	require.NoError(t, err)
	require.Equal(t, want, got)
	_, err = os.Stat(filepath.Join(dir, CheckpointFile+".tmp"))
	require.True(t, os.IsNotExist(err))

	// The reference client's own rendering of the same values.
	ref := "eez-reconcile-checkpoint v2\nl1_block=23507949\n" +
		"l1_block_hash=0x0100000000000000000000000000000000000000000000000000000000000000\n" +
		"tx_hash=0x0200000000000000000000000000000000000000000000000000000000000000\n" +
		"l2_cursor=163050\n" +
		"l2_block_hash=0x0300000000000000000000000000000000000000000000000000000000000000\n"
	raw, err := os.ReadFile(filepath.Join(dir, CheckpointFile))
	require.NoError(t, err)
	require.Equal(t, ref, string(raw))

	for _, bad := range []string{
		"",
		strings.Replace(ref, "v2", "v1", 1),
		strings.Replace(ref, "l2_cursor=163050\n", "", 1),
		strings.Replace(ref, "l2_cursor=163050", "l2_cursor=-1", 1),
		strings.Replace(ref, "tx_hash=0x02", "tx_hash=0x2", 1),
		ref + "extra=1\n",
	} {
		_, err := decodeCheckpoint(bad)
		require.ErrorIs(t, err, ErrCheckpoint, bad)
	}
}

func TestCheckpointUsableWith(t *testing.T) {
	c := &Checkpoint{L1Block: 10, L1BlockHash: common.Hash{1}, L2Cursor: 6, L2BlockHash: common.Hash{2}}
	l1Hash, l2Hash, other := common.Hash{1}, common.Hash{2}, common.Hash{9}
	require.NoError(t, c.UsableWith(&l1Hash, &l2Hash))
	require.ErrorIs(t, c.UsableWith(nil, &l2Hash), ErrCheckpoint)
	require.ErrorIs(t, c.UsableWith(&other, &l2Hash), ErrCheckpoint)
	require.ErrorIs(t, c.UsableWith(&l1Hash, nil), ErrCheckpoint)
	require.ErrorIs(t, c.UsableWith(&l1Hash, &other), ErrCheckpoint)
}

var testRegistry = common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3")

func TestFindResumePoint(t *testing.T) {
	ctx := context.Background()
	chain, err := recording.NewMemoryChain(1, &recording.Block{Header: l1.Header{Hash: common.Hash{0x10}}})
	require.NoError(t, err)
	roots := map[uint64]common.Hash{3: {0xa}, 5: {0xb}, 8: {0xc}}
	for n := uint64(1); n <= 10; n++ {
		b := &recording.Block{Header: l1.Header{Number: n, Hash: common.Hash{0x10, byte(n)}, ParentHash: common.Hash{0x10, byte(n - 1)}}}
		if n == 1 {
			b.Header.ParentHash = common.Hash{0x10}
		}
		if root, ok := roots[n]; ok {
			b.Txs = []types.Transaction{types.NewTransaction(n, testRegistry, nil, 0, nil, nil)}
			l, err := protocol.L2ExecutionPerformedLog(testRegistry, 1, root, new(big.Int))
			require.NoError(t, err)
			b.Logs = []*types.Log{l}
		}
		if n == 5 {
			other, err := protocol.L2ExecutionPerformedLog(testRegistry, 2, common.Hash{0xc}, new(big.Int))
			require.NoError(t, err)
			b.Logs = append(b.Logs, other)
		}
		require.NoError(t, chain.Append(b))
	}
	local := func(known map[common.Hash]uint64) func(context.Context, common.Hash) (uint64, bool, error) {
		return func(_ context.Context, h common.Hash) (uint64, bool, error) {
			n, ok := known[h]
			return n, ok, nil
		}
	}

	r, err := FindResumePoint(ctx, chain, testRegistry, 1, 1, 10, local(map[common.Hash]uint64{{0xa}: 6, {0xb}: 12}))
	require.NoError(t, err)
	require.Equal(t, &Record{L1Block: 5, L1BlockHash: common.Hash{0x10, 5}, TxHash: chain.Canonical()[5].Txs[0].Hash(), LastL2Block: 12}, r)

	// Another rollup's root at a later block is not ours.
	chain.SetMaxLogRange(2)
	r, err = FindResumePoint(ctx, chain, testRegistry, 1, 1, 10, local(map[common.Hash]uint64{{0xa}: 6}))
	require.NoError(t, err)
	require.Equal(t, uint64(3), r.L1Block)

	r, err = FindResumePoint(ctx, chain, testRegistry, 1, 4, 10, local(map[common.Hash]uint64{{0xa}: 6}))
	require.NoError(t, err)
	require.Nil(t, r, "the match lies below the deploy block")
}
