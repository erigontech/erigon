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

package eip8297

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func referencePath(t *testing.T, pattern byte, bitLen int16) Bitpath {
	t.Helper()
	p := PathFromBits(bytes.Repeat([]byte{pattern}, 66), bitLen)
	require.Equal(t, bitLen, p.BitLen)
	return p
}

func flipBit(p Bitpath, at int16) Bitpath {
	p.SetBitAt(at, p.Bit(at)^1)
	return p
}

func TestPBinCommonPrefixBits(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		aLen   int16
		bLen   int16
		flipAt int16 // -1: no divergence
		want   int16
	}{
		{"equal-271", 271, 271, -1, 271},
		{"equal-272", 272, 272, -1, 272},
		{"equal-273", 273, 273, -1, 273},
		{"equal-527", 527, 527, -1, 527},
		{"equal-528", 528, 528, -1, 528},
		{"diff-at-0", 528, 528, 0, 0},
		{"diff-at-63", 528, 528, 63, 63},
		{"diff-at-64", 528, 528, 64, 64},
		{"diff-at-270-len-271", 271, 271, 270, 270},
		{"diff-at-271-len-272", 272, 272, 271, 271},
		{"diff-at-272-len-273", 273, 273, 272, 272},
		{"diff-at-526-len-527", 527, 527, 526, 526},
		{"diff-at-527-len-528", 528, 528, 527, 527},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := referencePath(t, 0xA5, tc.aLen)
			b := referencePath(t, 0xA5, tc.bLen)
			if tc.flipAt >= 0 {
				b = flipBit(b, tc.flipAt)
			}
			require.Equal(t, tc.want, CommonPrefixBitsAt(&a, 0, &b))
			require.Equal(t, tc.want, CommonPrefixBitsAt(&b, 0, &a))
		})
	}
}

// Without clamping by min(aLen, bLen) the words keep agreeing past the shorter
// path's end, so an account key that prefixes a storage key over-reports.
func TestPBinCommonPrefixBits_ShorterPathIsPrefix(t *testing.T) {
	t.Parallel()

	long := referencePath(t, 0xAA, 528)
	short := referencePath(t, 0xAA, 272)

	require.Equal(t, int16(272), CommonPrefixBitsAt(&short, 0, &long))
	require.Equal(t, int16(272), CommonPrefixBitsAt(&long, 0, &short))
}

// Words carrying set bits beyond bitLen must not be read as real path bits.
func TestPBinCommonPrefixBits_IgnoresBitsBeyondBitLen(t *testing.T) {
	t.Parallel()

	long := referencePath(t, 0xAA, 528)

	dirty := referencePath(t, 0xAA, 272)
	dirty.Words[4] |= 0x0000FFFFFFFFFFFF // bits 272..319
	for i := 5; i < PathWords; i++ {
		dirty.Words[i] = ^uint64(0)
	}

	require.Equal(t, int16(272), CommonPrefixBitsAt(&dirty, 0, &long))
	require.Equal(t, int16(272), CommonPrefixBitsAt(&long, 0, &dirty))

	clean := referencePath(t, 0xAA, 272)
	dirty.MaskTail()
	require.Equal(t, clean.Words, dirty.Words)
}

func TestPBinBitpathAccessors(t *testing.T) {
	t.Parallel()

	p := PathFromBytes([]byte{0b10110001, 0b01000000})
	require.Equal(t, int16(16), p.BitLen)
	for i, want := range []uint64{1, 0, 1, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0} {
		require.Equalf(t, want, p.Bit(int16(i)), "bit %d", i)
	}

	mid := p.Slice(3, 11)
	require.Equal(t, int16(8), mid.BitLen)
	require.Equal(t, PathFromBytes([]byte{0b10001010}), mid)

	head, tail := p.Slice(0, 3), p.Slice(11, 16)
	head.Append(&mid)
	head.Append(&tail)
	require.Equal(t, p, head)

	empty, short := p.Slice(0, 0), p.Slice(0, 7)
	require.True(t, p.HasPrefix(&empty))
	require.True(t, p.HasPrefix(&short))
	require.True(t, p.HasPrefix(&p))

	flipped := flipBit(p, 5)
	other := flipped.Slice(0, 7)
	require.False(t, p.HasPrefix(&other))
	require.False(t, short.HasPrefix(&p))

	var appended Bitpath
	for i := int16(0); i < p.BitLen; i++ {
		appended.AppendBit(p.Bit(i))
	}
	require.Equal(t, p, appended)

	truncated := p
	truncated.Truncate(4)
	require.Equal(t, PathFromBits([]byte{0b10110000}, 4), truncated)
}

func TestPBinBitPathCodecRoundTrip(t *testing.T) {
	t.Parallel()

	src := make([]byte, 66)
	for i := range src {
		src[i] = byte(i*7 + 1)
	}

	for bitLen := int16(0); bitLen <= MaxPathBits; bitLen++ {
		p := PathFromBits(src, bitLen)
		enc := EncodeBitPath(&p)
		require.Equalf(t, (int(bitLen)+7)/8+1, len(enc), "bitLen %d", bitLen)
		require.LessOrEqual(t, len(enc), 67)

		got, err := DecodeBitPath(enc)
		require.NoErrorf(t, err, "bitLen %d", bitLen)
		require.Equalf(t, p, got, "bitLen %d", bitLen)
	}
}

func TestPBinBitPathCodecEmpty(t *testing.T) {
	t.Parallel()

	var empty Bitpath
	require.Equal(t, []byte{0x00}, EncodeBitPath(&empty))

	got, err := DecodeBitPath([]byte{0x00})
	require.NoError(t, err)
	require.Equal(t, empty, got)
}

func TestPBinBitPathCodecRejects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		buf  []byte
	}{
		{"empty-key", nil},
		{"tail-count-out-of-range", []byte{0xE0, 0x08}},
		{"tail-count-is-a-byte", []byte{0xE0, 0xFF}},
		{"tail-count-without-payload", []byte{0x05}},
		{"non-canonical-pad", []byte{0xFF, 0x03}},
		{"non-canonical-pad-single-bit", []byte{0x40, 0x01}},
		{"too-long", append(bytes.Repeat([]byte{0xAA}, 67), 0x00)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeBitPath(tc.buf)
			require.Error(t, err)
		})
	}

	got, err := DecodeBitPath([]byte{0xE0, 0x03})
	require.NoError(t, err)
	require.Equal(t, PathFromBits([]byte{0xE0}, 3), got)
}

// The commitment domain stores its state blob under the literal key "state", so
// no encoded bit path may collide with it.
func TestPBinBitPathNeverEncodesToStateKey(t *testing.T) {
	t.Parallel()

	_, err := DecodeBitPath([]byte("state"))
	require.Error(t, err)

	src := bytes.Repeat([]byte{0x74}, 66)
	for bitLen := int16(0); bitLen <= MaxPathBits; bitLen++ {
		p := PathFromBits(src, bitLen)
		require.NotEqualf(t, []byte("state"), EncodeBitPath(&p), "bitLen %d", bitLen)
	}
}

func FuzzPBinBitPathCodec(f *testing.F) {
	f.Add([]byte{}, uint16(0))
	f.Add([]byte{0x00}, uint16(1))
	f.Add(bytes.Repeat([]byte{0xFF}, 66), uint16(528))
	f.Add(bytes.Repeat([]byte{0xA5}, 34), uint16(272))
	f.Add([]byte{0xFF, 0x03}, uint16(3))

	f.Fuzz(func(t *testing.T, data []byte, n uint16) {
		bitLen := int16(int(n) % (MaxPathBits + 1))
		p := PathFromBits(data, bitLen)

		enc := EncodeBitPath(&p)
		got, err := DecodeBitPath(enc)
		require.NoError(t, err)
		require.Equal(t, p, got)

		// Decoding is total and canonical: anything that decodes must re-encode
		// to the very bytes it came from, so one bit path has one DB key.
		if q, err := DecodeBitPath(data); err == nil {
			require.Equal(t, data, EncodeBitPath(&q))
		}
	})
}

// The word-at-a-time scan must agree with a bit-by-bit walk at every offset,
// including the ones that straddle a word boundary.
func TestPBinCommonPrefixBitsAt_MatchesNaiveScan(t *testing.T) {
	t.Parallel()

	naive := func(key *Bitpath, from int16, prefix *Bitpath) int16 {
		limit := min(key.BitLen-from, prefix.BitLen)
		n := int16(0)
		for n < limit && key.Bit(from+n) == prefix.Bit(n) {
			n++
		}
		return n
	}

	key := referencePath(t, 0x6D, MaxPathBits)
	for _, from := range []int16{0, 1, 7, 63, 64, 65, 127, 128, 271, 272, 511, 512, 527, 528} {
		for _, want := range []int16{0, 1, 63, 64, 65, 128, 271} {
			p := key.Slice(from, min(from+want, key.BitLen))
			require.Equalf(t, naive(&key, from, &p), CommonPrefixBitsAt(&key, from, &p),
				"from %d, %d-bit prefix", from, p.BitLen)
			for flip := int16(0); flip < p.BitLen; flip++ {
				d := flipBit(p, flip)
				require.Equalf(t, naive(&key, from, &d), CommonPrefixBitsAt(&key, from, &d),
					"from %d, %d-bit prefix flipped at %d", from, p.BitLen, flip)
			}
		}
	}
}
