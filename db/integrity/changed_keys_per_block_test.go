// Copyright 2024 The Erigon Authors
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

package integrity

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// pairKU64 is a minimal stream.KU64 backed by a slice of (key, txNum) pairs.
type pairKU64 struct {
	pairs []struct {
		key   []byte
		txNum uint64
	}
	pos int
}

func newPairKU64(pairs ...struct {
	key   []byte
	txNum uint64
}) *pairKU64 {
	return &pairKU64{pairs: pairs}
}

func (p *pairKU64) HasNext() bool { return p.pos < len(p.pairs) }
func (p *pairKU64) Close()        {}
func (p *pairKU64) Next() ([]byte, uint64, error) {
	e := p.pairs[p.pos]
	p.pos++
	return e.key, e.txNum, nil
}

func pair(key string, txNum uint64) struct {
	key   []byte
	txNum uint64
} {
	return struct {
		key   []byte
		txNum uint64
	}{[]byte(key), txNum}
}

// simpleTxNums returns a TxNumToBlock where block i covers txNums [i*10, i*10+9].
// Matches the txNum/10 mapping used in simple test cases.
func simpleTxNums(numBlocks int) *TxNumToBlock {
	m := &TxNumToBlock{
		maxTxNums:    make([]uint64, numBlocks),
		fromBlockNum: 0,
		toBlockNum:   uint64(numBlocks),
	}
	for i := range m.maxTxNums {
		m.maxTxNums[i] = uint64(i)*10 + 9
	}
	return m
}

func keysForBlock(idx *ChangedKeysPerBlock, blockNum uint64) []string {
	offsets := idx.Offsets(blockNum)
	if len(offsets) == 0 {
		return nil
	}
	result := make([]string, len(offsets))
	for i, off := range offsets {
		result[i] = idx.Key(off)
	}
	sort.Strings(result)
	return result
}

func TestChangedKeysPerBlock_Basic(t *testing.T) {
	// Keys sorted: "a" < "b" < "c" (iterator must be key-sorted)
	// "a" changes at txNums 5, 15, 25  -> blocks 0, 1, 2
	// "b" changes at txNums 10, 20     -> blocks 1, 2
	// "c" changes at txNum  7          -> block 0
	it := newPairKU64(
		pair("a", 5),
		pair("a", 15),
		pair("a", 25),
		pair("b", 10),
		pair("b", 20),
		pair("c", 7),
	)

	idx, err := changedKeysPerBlock(it, simpleTxNums(3))
	require.NoError(t, err)

	require.Equal(t, []string{"a", "c"}, keysForBlock(idx, 0))
	require.Equal(t, []string{"a", "b"}, keysForBlock(idx, 1))
	require.Equal(t, []string{"a", "b"}, keysForBlock(idx, 2))
	require.Nil(t, keysForBlock(idx, 3))

	require.Equal(t, 3, idx.NumKeys())   // "a", "b", "c" stored once each
	require.Equal(t, 3, idx.NumBlocks()) // blocks 0, 1, 2
}

func TestChangedKeysPerBlock_DeduplicatesSameKeyInBlock(t *testing.T) {
	// "a" changes 3 times within block 1 (txNums 10, 11, 19) — should appear once.
	// "b" changes once in block 1 (txNum 15).
	it := newPairKU64(
		pair("a", 10),
		pair("a", 11),
		pair("a", 19),
		pair("b", 15),
	)

	idx, err := changedKeysPerBlock(it, simpleTxNums(2))
	require.NoError(t, err)

	require.Equal(t, []string{"a", "b"}, keysForBlock(idx, 1))
	require.Equal(t, 2, idx.NumKeys())
}

func TestChangedKeysPerBlock_KeysSharedAcrossBlocks(t *testing.T) {
	// Same key "x" appears in many blocks — stored once, referenced many times.
	pairs := make([]struct {
		key   []byte
		txNum uint64
	}, 50)
	for i := range pairs {
		pairs[i] = pair("x", uint64(i*10+1)) // txNums 1,11,21,... -> blocks 0,1,2,...
	}
	it := &pairKU64{pairs: pairs}

	idx, err := changedKeysPerBlock(it, simpleTxNums(50))
	require.NoError(t, err)

	require.Equal(t, 1, idx.NumKeys()) // "x" stored exactly once
	require.Equal(t, 50, idx.NumBlocks())
	for blockNum := uint64(0); blockNum < 50; blockNum++ {
		require.Equal(t, []string{"x"}, keysForBlock(idx, blockNum))
	}
}

func TestChangedKeysPerBlock_TxNum2BlockError(t *testing.T) {
	// txNum 999 is beyond the single-block window [0,9] — BlockOf must error.
	it := newPairKU64(pair("a", 999))
	_, err := changedKeysPerBlock(it, simpleTxNums(1))
	require.Error(t, err)
}

func TestChangedKeysPerBlock_Empty(t *testing.T) {
	it := newPairKU64()
	idx, err := changedKeysPerBlock(it, simpleTxNums(1))
	require.NoError(t, err)
	require.Equal(t, 0, idx.NumKeys())
	require.Equal(t, 0, idx.NumBlocks())
	require.False(t, idx.Has(0))
}

func TestChangedKeysPerBlock_WithRealTxNumToBlock(t *testing.T) {
	// key "b" has txNum 7 (block 0) — lower than key "a"'s last txNum 22 (block 2).
	// Without cursor reset between keys, BlockOf(7) would fail (cursor stuck at 2).
	it := newPairKU64(
		pair("a", 2),
		pair("a", 12),
		pair("a", 22),
		pair("b", 7),
		pair("b", 17),
	)
	txNums := &TxNumToBlock{
		maxTxNums:    []uint64{9, 19, 29},
		fromBlockNum: 0,
		toBlockNum:   3,
	}
	idx, err := changedKeysPerBlock(it, txNums)
	require.NoError(t, err)

	require.Equal(t, []string{"a", "b"}, keysForBlock(idx, 0)) // a@2, b@7 → block 0
	require.Equal(t, []string{"a", "b"}, keysForBlock(idx, 1)) // a@12, b@17 → block 1
	require.Equal(t, []string{"a"}, keysForBlock(idx, 2))      // a@22 → block 2
}

func TestChangedKeysPerBlock_KeyAtLastBlock(t *testing.T) {
	// txNum 25 → block 2 (last in window), cursor must advance all the way to position 2.
	it := newPairKU64(pair("z", 25))
	idx, err := changedKeysPerBlock(it, simpleTxNums(3))
	require.NoError(t, err)
	require.Nil(t, keysForBlock(idx, 0))
	require.Nil(t, keysForBlock(idx, 1))
	require.Equal(t, []string{"z"}, keysForBlock(idx, 2))
}

func TestChangedKeysPerBlock_ManyKeysOneBlock(t *testing.T) {
	// 100 distinct keys all change within block 0 — verifies offset slice grows
	// correctly and all offsets are unique and valid.
	pairs := make([]struct {
		key   []byte
		txNum uint64
	}, 100)
	for i := range pairs {
		pairs[i] = pair(fmt.Sprintf("key%03d", i), uint64(i%10))
	}
	sort.Slice(pairs, func(a, b int) bool { return string(pairs[a].key) < string(pairs[b].key) })
	idx, err := changedKeysPerBlock(&pairKU64{pairs: pairs}, simpleTxNums(1))
	require.NoError(t, err)
	require.Equal(t, 100, idx.NumKeys())
	require.Equal(t, 1, idx.NumBlocks())
	require.Equal(t, 100, len(idx.Offsets(0)))
	seen := make(map[uint32]bool)
	for _, off := range idx.Offsets(0) {
		require.False(t, seen[off], "duplicate offset %d", off)
		seen[off] = true
	}
}

func TestTxNumToBlock_ToTxNum(t *testing.T) {
	// ToTxNum must return maxTxNums[last]+1 (exclusive upper bound), not maxTxNums[last].
	m := &TxNumToBlock{maxTxNums: []uint64{4, 9, 14}}
	require.Equal(t, uint64(15), m.ToTxNum())
}

func TestTxNumToBlock_BlockOf(t *testing.T) {
	// 3 blocks: block 10 has txNums [0,4], block 11 has [5,9], block 12 has [10,14].
	// maxTxNums: [4, 9, 14]
	m := &TxNumToBlock{
		maxTxNums:    []uint64{4, 9, 14},
		fromBlockNum: 10,
		toBlockNum:   13,
	}

	// Each sub-slice simulates one key's ascending txNums; reset cursor between keys.
	for _, tc := range [][2]uint64{
		{0, 10}, {4, 10}, // block 10 boundary
		{5, 11}, {9, 11}, // block 11 boundary
		{10, 12}, {14, 12}, // block 12 boundary
	} {
		m.ResetCursor()
		got, err := m.BlockOf(tc[0])
		require.NoError(t, err, "txNum=%d", tc[0])
		require.Equal(t, tc[1], got, "txNum=%d", tc[0])
	}

	// txNum beyond window must error.
	m.ResetCursor()
	_, err := m.BlockOf(15)
	require.Error(t, err)
}

func TestTxNumToBlock_NonMonotone(t *testing.T) {
	// Simulates the HistoryKeyTxNumRange ordering: txNums are sorted by key, not globally.
	// key "a" has txNums [2, 12, 22], key "b" has txNums [7, 17].
	// Iterator emits: (a,2),(a,12),(a,22),(b,7),(b,17) — NOT globally sorted.
	// All txNums must map to the correct block (10 txNums per block).
	m := &TxNumToBlock{
		maxTxNums:    []uint64{9, 19, 29},
		fromBlockNum: 0,
		toBlockNum:   3,
	}
	// key "a": ascending txNums, cursor advances forward
	for _, c := range [][2]uint64{{2, 0}, {12, 1}, {22, 2}} {
		got, err := m.BlockOf(c[0])
		require.NoError(t, err, "txNum=%d", c[0])
		require.Equal(t, c[1], got, "txNum=%d", c[0])
	}
	// key "b": new key — txNums restart from a lower value, cursor must reset
	m.ResetCursor()
	for _, c := range [][2]uint64{{7, 0}, {17, 1}} {
		got, err := m.BlockOf(c[0])
		require.NoError(t, err, "txNum=%d", c[0])
		require.Equal(t, c[1], got, "txNum=%d", c[0])
	}
}
