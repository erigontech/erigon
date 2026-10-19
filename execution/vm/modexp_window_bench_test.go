// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkModexpWindow(b *testing.B) {
	modulus := bytes.Repeat([]byte{0xff}, 32)
	modulus[31] = 0x43
	for _, size := range []int{31, 32, 64, 1024} {
		for _, dense := range []bool{false, true} {
			exponent := make([]byte, size)
			exponent[0] = 0x80
			if dense {
				exponent = bytes.Repeat([]byte{0xff}, size)
			}
			b.Run(fmt.Sprintf("bytes_%d/dense_%t", size, dense), func(b *testing.B) {
				var output [32]byte
				b.ReportAllocs()
				for b.Loop() {
					modexpU256(output[:], []byte{7}, exponent, modulus)
				}
			})
		}
	}
}
