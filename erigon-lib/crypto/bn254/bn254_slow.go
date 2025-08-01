// Copyright 2018 Péter Szilágyi. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

//go:build !amd64 && !arm64

// Package bn254 implements the Optimal Ate pairing over a 256-bit Barreto-Naehrig curve.
package bn254

import bn254 "github.com/erigontech/erigon-lib/crypto/bn254/google"

// G1 is an abstract cyclic group. The zero value is suitable for use as the
// output of an operation, but cannot be used as an input.
type G1 = bn254.G1

// G2 is an abstract cyclic group. The zero value is suitable for use as the
// output of an operation, but cannot be used as an input.
type G2 = bn254.G2

// PairingCheck calculates the Optimal Ate pairing for a set of points.
func PairingCheck(a []*G1, b []*G2) bool {
	return bn254.PairingCheck(a, b)
}
