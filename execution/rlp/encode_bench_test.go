// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"bytes"
	"runtime"
	"sync"
	"testing"

	"github.com/holiman/uint256"
)

func BenchmarkPutint(b *testing.B) {
	buf := make([]byte, 8)
	for b.Loop() {
		putint(buf, 0x12345678)
		sink = buf
	}
}

func BenchmarkEncodeUint256Ints(b *testing.B) {
	ints := make([]*uint256.Int, 200)
	for i := range ints {
		ints[i] = new(uint256.Int).Lsh(uint256.NewInt(1), uint(i))
	}
	out := bytes.NewBuffer(make([]byte, 0, 4096))

	b.ReportAllocs()

	for b.Loop() {
		out.Reset()
		if err := Encode(out, ints); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeConcurrentInterface(b *testing.B) {
	type struct1 struct {
		A string
		B *uint256.Int
		C [20]byte
	}
	value := []any{
		uint(999),
		&struct1{A: "hello", B: uint256.NewInt(0xFFFFFFFF)},
		[10]byte{1, 2, 3, 4, 5, 6},
		[]string{"yeah", "yeah", "yeah"},
	}

	var wg sync.WaitGroup
	for cpu := 0; cpu < runtime.NumCPU(); cpu++ {
		wg.Go(func() {
			var buffer bytes.Buffer
			for i := 0; i < b.N; i++ {
				buffer.Reset()
				err := Encode(&buffer, value)
				if err != nil {
					panic(err)
				}
			}
		})
	}
	wg.Wait()
}
