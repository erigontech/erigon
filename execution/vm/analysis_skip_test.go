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

//go:build go1.27 && goexperiment.simd && amd64

package vm

import (
	"math/rand"
	"testing"
)

func TestCodeBitmapSkipEquivalence(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for iter := range 200000 {
		code := make([]byte, r.Intn(400))
		for i := range code {
			switch iter % 4 {
			case 0:
				code[i] = []byte{0x5b, 0x60, 0x7f, 0x00, 0x61}[r.Intn(5)]
			case 1:
				code[i] = byte(0x60 + r.Intn(32))
			default:
				code[i] = byte(r.Intn(256))
			}
		}
		if !equalBitvec(codeBitmapSkip(code), codeBitmapRef(code)) {
			t.Fatalf("mismatch len=%d code=%x", len(code), code)
		}
	}
}

func BenchmarkJumpdestAnalysisSkip(b *testing.B) {
	for name, alphabet := range map[string][]byte{
		"jumpdest":            {0x5b},
		"stop_jumpdest_push1": {0x00, 0x5b, 0x60},
		"jumpdest_push1":      {0x5b, 0x60},
		"push1to32":           {0x5b, 0x60, 0x6f, 0x70, 0x7f, 0x00},
		"push32":              {0x7f},
		"random":              nil,
	} {
		r := rand.New(rand.NewSource(1))
		codes := make([][]byte, 64)
		for k := range codes {
			codes[k] = make([]byte, 32*1024)
			for i := range codes[k] {
				if alphabet == nil {
					codes[k][i] = byte(r.Intn(256))
				} else {
					codes[k][i] = alphabet[r.Intn(len(alphabet))]
				}
			}
		}
		b.Run("skip/"+name, func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				codeBitmapSkip(codes[i%len(codes)])
			}
		})
	}
}
