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

package rlp

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
)

var parseU64Tests = []struct {
	expectErr error
	payload   []byte
	expectPos int
	expectRes uint64
}{
	{payload: hexutil.MustDecodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: hexutil.MustDecodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: hexutil.MustDecodeHex("8107"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("B8020004"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("C0"), expectErr: fmt.Errorf("%w: uint64 must be a string, not isList", ErrParse)},
	{payload: hexutil.MustDecodeHex("00"), expectErr: fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: 00", ErrParse)},
	{payload: hexutil.MustDecodeHex("8AFFFFFFFFFFFFFFFFFF7C"), expectErr: fmt.Errorf("%w: uint64 must not be more than 8 bytes long, got 10", ErrParse)},
}

var parseU32Tests = []struct {
	expectErr error
	payload   []byte
	expectPos int
	expectRes uint32
}{
	{payload: hexutil.MustDecodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: hexutil.MustDecodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: hexutil.MustDecodeHex("8107"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("B8020004"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("C0"), expectErr: fmt.Errorf("%w: uint32 must be a string, not isList", ErrParse)},
	{payload: hexutil.MustDecodeHex("00"), expectErr: fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: 00", ErrParse)},
	{payload: hexutil.MustDecodeHex("85FF6738FF7C"), expectErr: fmt.Errorf("%w: uint32 must not be more than 4 bytes long, got 5", ErrParse)},
}

func TestPrimitives(t *testing.T) {
	for i, tt := range parseU64Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := ParseU64(tt.payload, 0)
			assert.Equal(tt.expectErr, err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
	for i, tt := range parseU32Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := ParseU32(tt.payload, 0)
			assert.Equal(tt.expectErr, err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
}

func TestCountItems(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		val  any
		want int
	}{
		{"empty", []uint{}, 0},
		{"single byte items", []uint{1, 2, 3}, 3},
		{"short strings", []string{"aa", "bb"}, 2},
		{"long string", []string{string(make([]byte, 200))}, 1},
		{"nested lists", [][]uint{{1}, {2, 3}, {}}, 3},
		{"mixed widths", []string{"", "a", string(make([]byte, 60))}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			enc, err := EncodeToBytes(tc.val)
			require.NoError(t, err)
			s := NewBytesStream(enc)
			defer PutStream(s)
			size, err := s.List()
			require.NoError(t, err)
			raw := s.Peek()
			require.GreaterOrEqual(t, uint64(len(raw)), size)
			assert.Equal(t, tc.want, countItems(raw[:size]))
		})
	}
}

// countItems sizes allocations from attacker-controlled bytes, so it must never
// report more items than the input can hold, and never panic.
func TestCountItemsBounded(t *testing.T) {
	t.Parallel()
	check := func(raw []byte) {
		t.Helper()
		got := countItems(raw)
		assert.GreaterOrEqual(t, got, 0)
		assert.LessOrEqual(t, got, len(raw), "%d bytes reported %d items", len(raw), got)
	}
	corrupt := []byte{0x00, 0x7f, 0x80, 0x81, 0xb7, 0xb8, 0xbf, 0xc0, 0xc1, 0xf7, 0xf8, 0xff}
	for _, n := range []int{1, 2, 7, 33, 260} {
		for _, b := range corrupt {
			raw := make([]byte, n)
			for i := range raw {
				raw[i] = b
			}
			check(raw)
		}
	}
	rnd := rand.New(rand.NewSource(1))
	for range 2000 {
		raw := make([]byte, rnd.Intn(64))
		rnd.Read(raw)
		check(raw)
	}
}

// The span scan skips the canonicality Prefix enforces, so pin that the two
// still agree on every well-formed payload.
func TestCountItemsMatchesPrefixWalk(t *testing.T) {
	t.Parallel()
	viaPrefix := func(payload []byte) int {
		n := 0
		for pos := 0; pos < len(payload); n++ {
			dataPos, dataLen, _, err := Prefix(payload, pos)
			if err != nil {
				return n
			}
			pos = dataPos + dataLen
		}
		return n
	}
	for _, val := range []any{
		[]uint{}, []uint{0, 1, 127, 128, 1 << 20, 1 << 60},
		[]string{"", "a", string(make([]byte, 55)), string(make([]byte, 56)), string(make([]byte, 70000))},
		[][]uint{{}, {1}, {2, 3, 4}},
		[][]byte{nil, {0x7f}, {0x80}, make([]byte, 300)},
	} {
		enc, err := EncodeToBytes(val)
		require.NoError(t, err)
		s := NewBytesStream(enc)
		size, err := s.List()
		require.NoError(t, err)
		payload := s.Peek()[:size]
		assert.Equal(t, viaPrefix(payload), countItems(payload), "%T", val)
		PutStream(s)
	}
}
