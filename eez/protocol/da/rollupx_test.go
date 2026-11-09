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
package da

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
)

// rollupXSpan is a one-run span: the counts tokens of blocks blocks, a zero
// beneficiary and empty extraData, then txs.
func rollupXSpan(blocks uint32, counts []byte, txs ...[]byte) []byte {
	out := append([]byte{SpanVersionV0}, encodeUvarint(blocks)...)
	out = append(out, counts...)
	out = append(out, encodeUvarint(blocks)...)
	out = append(out, make([]byte, 20)...)
	out = append(out, encodeUvarint(blocks)...)
	out = append(out, 0)
	for _, tx := range txs {
		out = append(out, encodeUvarint(uint32(len(tx)))...)
	}
	for _, tx := range txs {
		out = append(out, tx...)
	}
	return out
}

func encodeUvarint(n uint32) []byte {
	var out []byte
	for n >= 0x80 {
		out = append(out, byte(n)|0x80)
		n >>= 7
	}
	return append(out, byte(n))
}

// TestRollupXCountTokens checks the token encodings eez-rollupX a50c326 pins
// in crates/eez-payload-codec/src/v0/counts.rs.
func TestRollupXCountTokens(t *testing.T) {
	for _, tc := range []struct {
		counts []byte
		blocks uint32
		txs    uint32
	}{
		{[]byte{0x80, 0x80}, 1, 128},
		{[]byte{0x85, 0x94}, 1, 1428},
		{[]byte{0xbf, 0xff}, 1, MaxRollupXBlockTxs},
		{[]byte{0x00}, 1, 0},
		{[]byte{0xc0}, 2, 0},
		{[]byte{0xfe}, 64, 0},
		{[]byte{0xff, 0x41}, 65, 0},
		{[]byte{0xff, 0xd8, 4}, 600, 0},
	} {
		var r spanReader
		r.buf = tc.counts
		count, run, err := r.countToken()
		require.NoError(t, err, "%x", tc.counts)
		require.Zero(t, r.remaining())
		if tc.txs > 0 {
			require.Equal(t, tc.txs, count)
			require.Zero(t, run)
		} else {
			require.Equal(t, tc.blocks, run)
		}
		s := &Span{TxCounts: make([]uint32, tc.blocks)}
		s.TxCounts[0] = tc.txs
		require.Equal(t, tc.counts, s.EncodeRollupX()[1+len(encodeUvarint(tc.blocks)):][:len(tc.counts)])
	}
	for _, counts := range [][]byte{
		{},
		{0x80},
		{0x80, 0},
		{0x80, 127},
		{0xff},
		{0xff, 64},
		{0xff, 0xc1, 0},
		{0xff, 0x80, 0x80, 0x80, 0x80, 16},
		{0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 1},
	} {
		var r spanReader
		r.buf = counts
		_, _, err := r.countToken()
		require.Error(t, err, "%x", counts)
	}
}

func TestRollupXSpanRejects(t *testing.T) {
	tx := []byte{0x02, 0xc0}
	for _, tc := range []struct {
		name string
		enc  []byte
		err  error
	}{
		{"adjacent single empties", rollupXSpan(2, []byte{0, 0}), ErrSpanCountToken},
		{"empty run after empty", rollupXSpan(3, []byte{0, 0xc0}), ErrSpanCountToken},
		{"run beyond the span", rollupXSpan(2, []byte{0xc1}), ErrSpanRunCoverage},
		{"no blocks", []byte{0, 0}, ErrSpanNoBlocks},
		{"too long", append([]byte{0}, encodeUvarint(MaxRollupXSpanBlocks+1)...), ErrSpanTooLong},
		{"version", []byte{1, 1, 0}, ErrSpanVersion},
		{"system transaction", rollupXSpan(1, []byte{1}, []byte{0x76, 0xc0}), ErrSpanUnsupportedTx},
		{"blob transaction", rollupXSpan(1, []byte{1}, []byte{0x03, 0xc0}), ErrSpanUnsupportedTx},
		{"trailing byte", append(rollupXSpan(1, []byte{1}, tx), 0), ErrSpanTrailingByte},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRollupXSpan(tc.enc)
			require.ErrorIs(t, err, tc.err)
		})
	}
	s, err := DecodeRollupXSpan(rollupXSpan(3, []byte{0xc0, 1}, tx))
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 0, 1}, s.TxCounts)
	require.Equal(t, [][]byte{tx}, s.Transactions)
}

func TestRollupXSpanRoundTrip(t *testing.T) {
	a, b := common.HexToAddress("0xa"), common.HexToAddress("0xb")
	var blocks []spanBlock
	for i := range 200 {
		blk := spanBlock{beneficiary: a, extraData: []byte{}, txs: [][]byte{}}
		switch {
		case i == 3:
			blk.txs = [][]byte{{1}, bytes.Repeat([]byte{2}, 300)}
		case i == 150:
			blk.beneficiary, blk.extraData, blk.txs = b, []byte{5}, [][]byte{{4}}
		}
		blocks = append(blocks, blk)
	}
	s := &Span{}
	for _, blk := range blocks {
		s.TxCounts = append(s.TxCounts, uint32(len(blk.txs)))
		s.Beneficiaries = append(s.Beneficiaries, blk.beneficiary)
		s.ExtraData = append(s.ExtraData, blk.extraData)
		s.Transactions = append(s.Transactions, blk.txs...)
	}
	dec, err := DecodeRollupXSpan(s.EncodeRollupX())
	require.NoError(t, err)
	require.Equal(t, blocks, spanBlocks(dec))
}

// TestRollupXChiadoSpan decodes the span of the live eez.dev settlement in
// the anchor-23507949 blob, which DecodeSpan rejects.
func TestRollupXChiadoSpan(t *testing.T) {
	blob, err := testfixtures.LoadChiadoBlob()
	require.NoError(t, err)
	msgs, err := Decode([][]byte{blob}, nil)
	require.NoError(t, err)
	s, err := DecodeRollupXSpan(msgs[0].Data)
	require.NoError(t, err)
	require.NotEmpty(t, s.TxCounts)
	require.Empty(t, s.Transactions)
}

// FuzzDecodeRollupXSpan checks that every span DecodeRollupXSpan accepts is
// the unique encoding of what it decodes to.
func FuzzDecodeRollupXSpan(f *testing.F) {
	f.Add(rollupXSpan(3, []byte{0xc0, 1}, []byte{0x02, 0xc0}))
	f.Add(rollupXSpan(600, []byte{0xff, 0xd8, 4}))
	f.Fuzz(func(t *testing.T, operations []byte) {
		s, err := DecodeRollupXSpan(operations)
		if err != nil {
			return
		}
		require.Equal(t, operations, s.EncodeRollupX())
	})
}
