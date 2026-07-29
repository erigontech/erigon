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

package commitment

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func pbinTestPath(t *testing.T, pattern byte, bitLen int16) pbinBitpath {
	t.Helper()
	p := pbinPathFromBits(bytes.Repeat([]byte{pattern}, 66), bitLen)
	require.Equal(t, bitLen, p.bitLen)
	return p
}

func pbinFlipBit(p pbinBitpath, at int16) pbinBitpath {
	p.setBitAt(at, p.bit(at)^1)
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
			a := pbinTestPath(t, 0xA5, tc.aLen)
			b := pbinTestPath(t, 0xA5, tc.bLen)
			if tc.flipAt >= 0 {
				b = pbinFlipBit(b, tc.flipAt)
			}
			require.Equal(t, tc.want, pbinCommonPrefixBitsAt(&a, 0, &b))
			require.Equal(t, tc.want, pbinCommonPrefixBitsAt(&b, 0, &a))
		})
	}
}

// A 272-bit account key that is a bitwise prefix of a 528-bit storage key must
// report exactly 272 shared bits: without clamping by min(aLen, bLen) the words
// keep agreeing past the shorter path's end (guards H10).
func TestPBinCommonPrefixBits_ShorterPathIsPrefix(t *testing.T) {
	t.Parallel()

	long := pbinTestPath(t, 0xAA, 528)
	short := pbinTestPath(t, 0xAA, 272)

	require.Equal(t, int16(272), pbinCommonPrefixBitsAt(&short, 0, &long))
	require.Equal(t, int16(272), pbinCommonPrefixBitsAt(&long, 0, &short))
}

// Words carrying set bits beyond bitLen must not be read as real path bits
// (guards H10).
func TestPBinCommonPrefixBits_IgnoresBitsBeyondBitLen(t *testing.T) {
	t.Parallel()

	long := pbinTestPath(t, 0xAA, 528)

	dirty := pbinTestPath(t, 0xAA, 272)
	dirty.w[4] |= 0x0000FFFFFFFFFFFF // bits 272..319
	for i := 5; i < pbinPathWords; i++ {
		dirty.w[i] = ^uint64(0)
	}

	require.Equal(t, int16(272), pbinCommonPrefixBitsAt(&dirty, 0, &long))
	require.Equal(t, int16(272), pbinCommonPrefixBitsAt(&long, 0, &dirty))

	clean := pbinTestPath(t, 0xAA, 272)
	dirty.maskTail()
	require.Equal(t, clean.w, dirty.w)
}

func TestPBinBitpathAccessors(t *testing.T) {
	t.Parallel()

	p := pbinPathFromBytes([]byte{0b10110001, 0b01000000})
	require.Equal(t, int16(16), p.bitLen)
	for i, want := range []uint64{1, 0, 1, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0} {
		require.Equalf(t, want, p.bit(int16(i)), "bit %d", i)
	}

	mid := p.slice(3, 11)
	require.Equal(t, int16(8), mid.bitLen)
	require.Equal(t, pbinPathFromBytes([]byte{0b10001010}), mid)

	head, tail := p.slice(0, 3), p.slice(11, 16)
	head.append(&mid)
	head.append(&tail)
	require.Equal(t, p, head)

	empty, short := p.slice(0, 0), p.slice(0, 7)
	require.True(t, p.hasPrefix(&empty))
	require.True(t, p.hasPrefix(&short))
	require.True(t, p.hasPrefix(&p))

	flipped := pbinFlipBit(p, 5)
	other := flipped.slice(0, 7)
	require.False(t, p.hasPrefix(&other))
	require.False(t, short.hasPrefix(&p))

	var appended pbinBitpath
	for i := int16(0); i < p.bitLen; i++ {
		appended.appendBit(p.bit(i))
	}
	require.Equal(t, p, appended)

	truncated := p
	truncated.truncate(4)
	require.Equal(t, pbinPathFromBits([]byte{0b10110000}, 4), truncated)
}

func TestPBinBitPathCodecRoundTrip(t *testing.T) {
	t.Parallel()

	src := make([]byte, 66)
	for i := range src {
		src[i] = byte(i*7 + 1)
	}

	for bitLen := int16(0); bitLen <= pbinMaxPathBits; bitLen++ {
		p := pbinPathFromBits(src, bitLen)
		enc := pbinEncodeBitPath(&p)
		require.Equalf(t, (int(bitLen)+7)/8+1, len(enc), "bitLen %d", bitLen)
		require.LessOrEqual(t, len(enc), 67)

		got, err := pbinDecodeBitPath(enc)
		require.NoErrorf(t, err, "bitLen %d", bitLen)
		require.Equalf(t, p, got, "bitLen %d", bitLen)
	}
}

func TestPBinBitPathCodecEmpty(t *testing.T) {
	t.Parallel()

	var empty pbinBitpath
	require.Equal(t, []byte{0x00}, pbinEncodeBitPath(&empty))

	got, err := pbinDecodeBitPath([]byte{0x00})
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
			_, err := pbinDecodeBitPath(tc.buf)
			require.Error(t, err)
		})
	}

	got, err := pbinDecodeBitPath([]byte{0xE0, 0x03})
	require.NoError(t, err)
	require.Equal(t, pbinPathFromBits([]byte{0xE0}, 3), got)
}

// The commitment domain stores its state blob under the literal key "state", so
// no encoded bit path may collide with it (guards H5).
func TestPBinBitPathNeverEncodesToStateKey(t *testing.T) {
	t.Parallel()

	_, err := pbinDecodeBitPath(KeyCommitmentState)
	require.Error(t, err)

	src := bytes.Repeat([]byte{0x74}, 66)
	for bitLen := int16(0); bitLen <= pbinMaxPathBits; bitLen++ {
		p := pbinPathFromBits(src, bitLen)
		require.NotEqualf(t, KeyCommitmentState, pbinEncodeBitPath(&p), "bitLen %d", bitLen)
	}
}

func FuzzPBinBitPathCodec(f *testing.F) {
	f.Add([]byte{}, uint16(0))
	f.Add([]byte{0x00}, uint16(1))
	f.Add(bytes.Repeat([]byte{0xFF}, 66), uint16(528))
	f.Add(bytes.Repeat([]byte{0xA5}, 34), uint16(272))
	f.Add([]byte{0xFF, 0x03}, uint16(3))

	f.Fuzz(func(t *testing.T, data []byte, n uint16) {
		bitLen := int16(int(n) % (pbinMaxPathBits + 1))
		p := pbinPathFromBits(data, bitLen)

		enc := pbinEncodeBitPath(&p)
		got, err := pbinDecodeBitPath(enc)
		require.NoError(t, err)
		require.Equal(t, p, got)

		// Decoding is total and canonical: anything that decodes must re-encode
		// to the very bytes it came from, so one bit path has one DB key.
		if q, err := pbinDecodeBitPath(data); err == nil {
			require.Equal(t, data, pbinEncodeBitPath(&q))
		}
	})
}

// The word-at-a-time divergence scan must agree with a bit-by-bit walk at every
// offset, including the ones that straddle a word boundary.
func TestPBinCommonPrefixBitsAt_MatchesNaiveScan(t *testing.T) {
	t.Parallel()

	naive := func(key *pbinBitpath, from int16, prefix *pbinBitpath) int16 {
		limit := min(key.bitLen-from, prefix.bitLen)
		n := int16(0)
		for n < limit && key.bit(from+n) == prefix.bit(n) {
			n++
		}
		return n
	}

	key := pbinTestPath(t, 0x6D, pbinMaxPathBits)
	for _, from := range []int16{0, 1, 7, 63, 64, 65, 127, 128, 271, 272, 511, 512, 527, 528} {
		for _, want := range []int16{0, 1, 63, 64, 65, 128, 271} {
			p := key.slice(from, min(from+want, key.bitLen))
			require.Equalf(t, naive(&key, from, &p), pbinCommonPrefixBitsAt(&key, from, &p),
				"from %d, %d-bit prefix", from, p.bitLen)
			for flip := int16(0); flip < p.bitLen; flip++ {
				d := pbinFlipBit(p, flip)
				require.Equalf(t, naive(&key, from, &d), pbinCommonPrefixBitsAt(&key, from, &d),
					"from %d, %d-bit prefix flipped at %d", from, p.bitLen, flip)
			}
		}
	}
}
