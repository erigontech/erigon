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

package common

import (
	"bytes"
	"testing"
)

// SetBytes on all fixed-size byte types must follow Hash.SetBytes semantics:
// inputs longer than the type keep the last N bytes, shorter inputs are
// right-aligned with zero padding.
func TestBytesNSetBytes(t *testing.T) {
	seq := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i + 1)
		}
		return b
	}

	check := func(t *testing.T, got, in []byte) {
		t.Helper()
		n := len(got)
		want := make([]byte, n)
		if len(in) >= n {
			copy(want, in[len(in)-n:])
		} else {
			copy(want[n-len(in):], in)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("SetBytes(%d bytes) = %x, want %x", len(in), got, want)
		}
	}

	t.Run("Bytes4", func(t *testing.T) {
		for _, n := range []int{2, 4, 6} {
			var b Bytes4
			b.SetBytes(seq(n))
			check(t, b[:], seq(n))
		}
	})
	t.Run("Bytes48", func(t *testing.T) {
		for _, n := range []int{40, 48, 60} {
			var b Bytes48
			b.SetBytes(seq(n))
			check(t, b[:], seq(n))
		}
	})
	t.Run("Bytes64", func(t *testing.T) {
		for _, n := range []int{40, 64, 70} {
			var b Bytes64
			b.SetBytes(seq(n))
			check(t, b[:], seq(n))
		}
	})
	t.Run("Bytes96", func(t *testing.T) {
		for _, n := range []int{40, 96, 100} {
			var b Bytes96
			b.SetBytes(seq(n))
			check(t, b[:], seq(n))
		}
	})
	t.Run("Hash", func(t *testing.T) {
		for _, n := range []int{20, 32, 40} {
			var h Hash
			h.SetBytes(seq(n))
			check(t, h[:], seq(n))
		}
	})
	t.Run("Address", func(t *testing.T) {
		for _, n := range []int{12, 20, 28} {
			var a Address
			a.SetBytes(seq(n))
			check(t, a[:], seq(n))
		}
	})
}
