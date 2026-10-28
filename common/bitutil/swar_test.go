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

package bitutil

import (
	"encoding/binary"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// swarWords covers the shapes a lane predicate can trip on: all-zero, all-set,
// a single lane at each of the eight offsets, and random noise.
func swarWords() []uint64 {
	words := []uint64{0, ^uint64(0), swarLow, swarHigh}
	for lane := range 8 {
		for _, b := range []byte{0x00, 0x01, 0x1f, 0x20, '"', '\\', 0x7f, 0x80, 0xff} {
			mask := uint64(0xff) << (8 * lane)
			v := uint64(b) << (8 * lane)
			words = append(words, v, ^mask|v)
		}
	}
	r := rand.New(rand.NewSource(7))
	for range 20000 {
		words = append(words, r.Uint64())
	}
	return words
}

func lanes(x uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], x)
	return b[:]
}

func TestHasZero(t *testing.T) {
	for _, x := range swarWords() {
		want := false
		for _, c := range lanes(x) {
			want = want || c == 0
		}
		require.Equal(t, want, HasZero(x) != 0, "x=%#016x", x)
	}
}

func TestHasByte(t *testing.T) {
	for _, x := range swarWords() {
		for _, b := range []byte{0x00, 0x01, '"', '\\', 0x7f, 0x80, 0xff} {
			want := false
			for _, c := range lanes(x) {
				want = want || c == b
			}
			require.Equal(t, want, HasByte(x, b) != 0, "x=%#016x b=%#02x", x, b)
		}
	}
}

// HasLess is exact only up to n == 128; the doc comment says so, and this pins
// both halves of that claim.
func TestHasLess(t *testing.T) {
	for _, x := range swarWords() {
		for _, n := range []byte{0x01, 0x20, 0x7f, 0x80} {
			want := false
			for _, c := range lanes(x) {
				want = want || c < n
			}
			require.Equal(t, want, HasLess(x, n) != 0, "x=%#016x n=%#02x", x, n)
		}
	}
}

// Above n == 128 the reported window narrows to [n-128, 128): both classes
// outside it go unreported, which is why the bound is a hard one.
func TestHasLessAboveBoundReportsOnlyTheMiddleWindow(t *testing.T) {
	require.Zero(t, HasLess(0, 129), "every lane is 0 < 129, and none is reported above n == 128")
	require.Zero(t, HasLess(0x47, 200), "71 < 200, but below n-128 it wraps with the high bit clear")
	require.Zero(t, HasLess(0xc7, 200), "199 < 200, but the &^ x term drops every lane at or above 128")
	require.NotZero(t, HasLess(0x48, 200), "72 is inside [n-128, 128) and is reported")
	require.NotZero(t, HasLess(0x7f, 200), "127 is inside [n-128, 128) and is reported")
}

// The lowest set bit is the one exception to the aggregate-only rule: no borrow
// reaches it from below, so it names the first matching lane.
func TestLowestSetBitLocatesFirstLane(t *testing.T) {
	for _, x := range swarWords() {
		for _, b := range []byte{0x00, 0x01, '"', 0x80, 0xff} {
			z := HasByte(x, b)
			if z == 0 {
				continue
			}
			first := -1
			for i, c := range lanes(x) {
				if c == b {
					first = i
					break
				}
			}
			require.Equal(t, first, bits.TrailingZeros64(z)>>3, "x=%#016x b=%#02x", x, b)
		}
	}
}
