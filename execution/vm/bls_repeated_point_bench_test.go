// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"bytes"
	"fmt"
	"testing"

	bls "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

func BenchmarkBLSRepeatedPoints(b *testing.B) {
	_, _, g1, g2 := bls.Generators()
	for _, size := range []int{1, 16, 128} {
		for _, tc := range []struct {
			name  string
			point []byte
			p     PrecompiledContract
		}{{"G1", encodePointG1(&g1), &bls12381G1MultiExp{}}, {"G2", encodePointG2(&g2), &bls12381G2MultiExp{}}} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, size), func(b *testing.B) {
				pair := make([]byte, len(tc.point)+32)
				copy(pair, tc.point)
				pair[len(pair)-1] = 1
				input := bytes.Repeat(pair, size)
				b.ReportAllocs()
				for b.Loop() {
					if _, err := tc.p.Run(input); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
