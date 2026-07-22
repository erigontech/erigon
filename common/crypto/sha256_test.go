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

package crypto_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/race"
)

func bytesOfLen(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i)
	}
	return b
}

// Sha256 must equal the digest of data concatenated with extras, at every size.
// Sizes straddle the internal stack-buffer boundary.
func TestSha256EqualsStdlibOfConcat(t *testing.T) {
	sizes := []int{0, 1, 31, 32, 33, 63, 64, 65, 127, 128, 1000}
	for _, dataLen := range sizes {
		for _, nExtras := range []int{0, 1, 2, 3} {
			extraLens := sizes
			if nExtras == 0 {
				extraLens = []int{0} // no extras are built, so their size is irrelevant
			}
			for _, extraLen := range extraLens {
				data := bytesOfLen(dataLen, 0x10)
				var extras [][]byte
				var concat bytes.Buffer
				concat.Write(data)
				for i := range nExtras {
					e := bytesOfLen(extraLen, byte(0x40+i))
					extras = append(extras, e)
					concat.Write(e)
				}
				want := sha256.Sum256(concat.Bytes())
				name := fmt.Sprintf("data=%d/extras=%dx%d", dataLen, nExtras, extraLen)
				t.Run(name, func(t *testing.T) {
					if got := crypto.Sha256(data, extras...); got != want {
						t.Errorf("Sha256 = %x, want %x", got, want)
					}
				})
			}
		}
	}
}

// Sha256 is shared by the shuffling hot paths, which previously each held a
// private hasher, so it must stay correct under concurrent use on both the
// stack and the pooled path.
func TestSha256Concurrent(t *testing.T) {
	small, smallExtra := bytesOfLen(32, 1), bytesOfLen(32, 2)
	big, bigExtra := bytesOfLen(500, 3), bytesOfLen(500, 4)
	wantSmall := crypto.Sha256(small, smallExtra)
	wantBig := crypto.Sha256(big, bigExtra)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 500 {
				if got := crypto.Sha256(small, smallExtra); got != wantSmall {
					t.Errorf("stack path = %x, want %x", got, wantSmall)
					return
				}
				if got := crypto.Sha256(big, bigExtra); got != wantBig {
					t.Errorf("pooled path = %x, want %x", got, wantBig)
					return
				}
			}
		})
	}
	wg.Wait()
}

// A nil extra must hash the same as no extra at all.
func TestSha256NilExtra(t *testing.T) {
	data := bytesOfLen(32, 1)
	if got, want := crypto.Sha256(data, nil), sha256.Sum256(data); got != want {
		t.Errorf("crypto.Sha256(data, nil) = %x, want %x", got, want)
	}
}

// Repeated calls must not leak state between invocations.
func TestSha256Repeatable(t *testing.T) {
	a, b := bytesOfLen(32, 2), bytesOfLen(32, 3)
	first := crypto.Sha256(a, b)
	for range 100 {
		if got := crypto.Sha256(a, b); got != first {
			t.Fatalf("Sha256 not repeatable: %x != %x", got, first)
		}
		crypto.Sha256(bytesOfLen(500, 9), bytesOfLen(500, 8)) // dirty the pooled join buffer
	}
}

// Joins too large for the stack buffer take the pooled scratch buffer, which keeps
// them allocation-free too.
func TestSha256JoinedPathAllocFree(t *testing.T) {
	// sync.Pool deliberately drops values under the race detector, so the pooled path
	// always allocates there and can't be measured.
	//goland:noinspection GoBoolExpressions
	if race.Enabled {
		t.Skip("sync.Pool does not pool under -race")
	}
	big, extra := bytesOfLen(4096, 7), bytesOfLen(32, 8)
	if n := testing.AllocsPerRun(200, func() { crypto.Sha256(big, extra) }); n != 0 {
		t.Errorf("crypto.Sha256(4096B, 32B) allocs = %v, want 0 (pooled join buffer)", n)
	}
}

func TestSha256AllocFree(t *testing.T) {
	a, b := bytesOfLen(32, 4), bytesOfLen(32, 5)
	if n := testing.AllocsPerRun(200, func() { crypto.Sha256(a, b) }); n != 0 {
		t.Errorf("crypto.Sha256(32B, 32B) allocs = %v, want 0", n)
	}
	if boundary := bytesOfLen(32, 9); testing.AllocsPerRun(200, func() { crypto.Sha256(boundary, boundary) }) != 0 {
		t.Errorf("Sha256 at the 64B boundary must stay on the stack path")
	}
	big := bytesOfLen(4096, 6)
	if n := testing.AllocsPerRun(200, func() { crypto.Sha256(big) }); n != 0 {
		t.Errorf("crypto.Sha256(4096B) allocs = %v, want 0", n)
	}
}
