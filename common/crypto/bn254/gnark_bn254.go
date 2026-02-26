// Copyright 2025 The Erigon Authors
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

package bn254

import (
	"encoding/binary"
	"errors"

	"github.com/consensys/gnark-crypto/ecc/bn254"
)

// UnmarshalCurvePointG1 unmarshals a given input [32-byte X | 32-byte Y] slice to a G1Affine point
func UnmarshalCurvePointG1(input []byte, point *bn254.G1Affine) error {
	if len(input) != 64 {
		return errors.New("invalid input size")
	}

	isAllZeroes := true
	for i := 0; i < 64; i += 8 {
		if 0 != binary.BigEndian.Uint64(input[i:i+8]) {
			isAllZeroes = false
			break
		}
	}
	if isAllZeroes {
		return nil
	}

	// read X and Y coordinates
	if err := point.X.SetBytesCanonical(input[:32]); err != nil {
		return err
	}
	if err := point.Y.SetBytesCanonical(input[32:64]); err != nil {
		return err
	}

	// subgroup check
	if !point.IsInSubGroup() {
		return errors.New("invalid point: subgroup check failed")
	}
	return nil
}

// MarshalCurvePointG1 marshals a given G1Affine point to byte slice with [32-byte X | 32-byte Y] form
func MarshalCurvePointG1(point *bn254.G1Affine) []byte {
	xBytes := point.X.Bytes()
	yBytes := point.Y.Bytes()
	ret := make([]byte, 0, 64)
	ret = append(ret, xBytes[:]...)
	ret = append(ret, yBytes[:]...)
	return ret
}

// UnmarshalCurvePointG2 unmarshals a given input [64-byte X | 64-byte Y] slice to a G2Affine point
func UnmarshalCurvePointG2(input []byte, point *bn254.G2Affine) error {
	if len(input) != 32*4 {
		return errors.New("invalid input size")
	}

	isAllZeroes := true
	for i := 0; i < 32*4; i += 8 {
		if 0 != binary.BigEndian.Uint64(input[i:i+8]) {
			isAllZeroes = false
			break
		}
	}
	if isAllZeroes {
		return nil
	}

	// read X and Y coordinates
	// p.X.A1 | p.X.A0
	if err := point.X.A1.SetBytesCanonical(input[:32]); err != nil {
		return err
	}
	if err := point.X.A0.SetBytesCanonical(input[32 : 32*2]); err != nil {
		return err
	}
	// p.Y.A1 | p.Y.A0
	if err := point.Y.A1.SetBytesCanonical(input[32*2 : 32*3]); err != nil {
		return err
	}
	if err := point.Y.A0.SetBytesCanonical(input[32*3 : 32*4]); err != nil {
		return err
	}
	// subgroup check
	if !point.IsInSubGroup() {
		return errors.New("invalid point: subgroup check failed")
	}
	return nil
}

// MarshalCurvePointG2 marshals a given G2Affine point to byte slice with [64-byte X | 64-byte Y] form
func MarshalCurvePointG2(point *bn254.G2Affine) []byte {
	x1Bytes := point.X.A1.Bytes()
	x0Bytes := point.X.A0.Bytes()
	y1Bytes := point.Y.A1.Bytes()
	y0Bytes := point.Y.A0.Bytes()
	ret := make([]byte, 0, 32*4)
	ret = append(ret, x1Bytes[:]...)
	ret = append(ret, x0Bytes[:]...)
	ret = append(ret, y1Bytes[:]...)
	ret = append(ret, y0Bytes[:]...)
	return ret
}
