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

package bn256

import (
	"encoding/binary"
	"errors"

	"github.com/consensys/gnark-crypto/ecc/bn254"
)

// UnmarshalCurvePoint unmarshals a given input [32-byte X | 32-byte Y] slice to a G1Affine point
func UnmarshalCurvePoint(input []byte, point *bn254.G1Affine) error {
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

// MarshalCurvePoint marshals a given G1Affine point to byte slice with [32-byte X | 32-byte Y] form
func MarshalCurvePoint(point *bn254.G1Affine, ret []byte) []byte {
	xBytes := point.X.Bytes()
	yBytes := point.Y.Bytes()
	ret = append(ret, xBytes[:]...)
	ret = append(ret, yBytes[:]...)
	return ret
}
