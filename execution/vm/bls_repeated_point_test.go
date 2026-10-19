// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

package vm

import (
	"bytes"
	"math/big"
	"testing"

	bls "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

func TestBLSRepeatedPointScalars(t *testing.T) {
	_, _, g1, g2 := bls.Generators()
	for _, tc := range []struct {
		name  string
		point []byte
		p     PrecompiledContract
		want  []byte
	}{
		{"G1", encodePointG1(&g1), &bls12381G1MultiExp{}, encodePointG1(new(bls.G1Affine).ScalarMultiplication(&g1, big.NewInt(136)))},
		{"G2", encodePointG2(&g2), &bls12381G2MultiExp{}, encodePointG2(new(bls.G2Affine).ScalarMultiplication(&g2, big.NewInt(136)))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := make([]byte, 0, 16*(len(tc.point)+32))
			for i := 1; i <= 16; i++ {
				input = append(input, tc.point...)
				var scalar [32]byte
				scalar[31] = byte(i)
				input = append(input, scalar[:]...)
			}
			got, err := tc.p.Run(input)
			if err != nil || !bytes.Equal(got, tc.want) {
				t.Fatalf("got %x, %v; want %x", got, err, tc.want)
			}
			input[len(tc.point)+32] = 1
			if _, err := tc.p.Run(input); err == nil {
				t.Fatal("accepted an invalid point after a repeated point")
			}
		})
	}
}
