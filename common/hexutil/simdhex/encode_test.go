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

package simdhex

import (
	"encoding/hex"
	"math/rand/v2"
	"testing"
)

func TestAppendEncodeMatchesStdlib(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for n := 0; n <= 300; n++ {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(r.Uint32())
		}
		prefix := []byte("0x")
		if got, want := string(AppendEncode(prefix, src)), "0x"+hex.EncodeToString(src); got != want {
			t.Fatalf("len %d: got %s want %s", n, got, want)
		}
	}
}
