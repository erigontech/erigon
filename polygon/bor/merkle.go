// Copyright 2024 The Erigon Authors
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

package bor

func AppendBytes32(data ...[]byte) []byte {
	var result []byte

	for _, v := range data {
		paddedV, err := ConvertTo32(v)
		if err == nil {
			result = append(result, paddedV[:]...)
		}
	}

	return result
}

func NextPowerOfTwo(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	// http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}

func ConvertTo32(input []byte) (output [32]byte, err error) {
	l := len(input)
	if l > 32 || l == 0 {
		return
	}

	copy(output[32-l:], input)

	return
}

func Convert(input [][32]byte) [][]byte {
	output := make([][]byte, 0, len(input))

	for _, in := range input {
		newInput := make([]byte, len(in[:]))
		copy(newInput, in[:])
		output = append(output, newInput)

	}

	return output
}
