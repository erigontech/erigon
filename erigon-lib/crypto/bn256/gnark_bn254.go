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
func UnmarshalCurvePoint(input []byte) (*bn254.G1Affine, error) {
	if len(input) < 64 {
		return nil, errors.New("input too short")
	}

	isAllZeroes := true
	for i := 0; i < 64; i += 8 {
		if binary.LittleEndian.Uint64(input[i:i+8]) != 0 {
			isAllZeroes = false
			break
		}
	}
	point := &bn254.G1Affine{}
	if isAllZeroes {
		return point, nil
	}

	// read X and Y coordinates
	if err := point.X.SetBytesCanonical(input[:32]); err != nil {
		return nil, err
	}
	if err := point.Y.SetBytesCanonical(input[32:64]); err != nil {
		return nil, err
	}

	// subgroup check
	if !point.IsInSubGroup() {
		return nil, errors.New("invalid point: subgroup check failed")
	}
	return point, nil
}

// UnmarshalCurvePoint unmarshals a given input [32-byte X | 32-byte Y] slice to a G1Affine point
func MarshalCurvePoint(point *bn254.G1Affine) []byte {
	ret := make([]byte, 0, 64)
	xBytes := point.X.Bytes()
	yBytes := point.Y.Bytes()
	ret = make([]byte, 64)
	copy(ret[0:32], xBytes[:])
	copy(ret[32:64], yBytes[:])
	return ret
}
