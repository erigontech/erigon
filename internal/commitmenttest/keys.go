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

package commitmenttest

import (
	"bytes"
	"encoding/binary"
)

type KeySpec struct {
	Kind string
	Size int
}

func Key(spec KeySpec, number int) []byte {
	key := make([]byte, spec.Size)
	switch spec.Kind {
	case "integer":
		binary.BigEndian.PutUint64(key[len(key)-8:], uint64(number))
	case "bench-address", "bench-slot":
		multiplier := uint64(0x9E3779B97F4A7C15)
		if spec.Kind == "bench-slot" {
			multiplier = 0xC2B2AE3D27D4EB4F
		}
		binary.BigEndian.PutUint64(key[:8], uint64(number)*multiplier)
		binary.BigEndian.PutUint64(key[8:16], uint64(number))
	case "wrapping-address":
		for j := range key {
			key[j] = byte(number*17 + j)
		}
	case "wrapping-slot":
		for j := range key {
			key[j] = byte(number*29 + j*3)
		}
	default:
		panic("unknown key kind: " + spec.Kind)
	}
	return key
}

func Path(head []byte, fill byte) []byte {
	path := bytes.Repeat([]byte{fill}, 64)
	copy(path, head)
	return path
}
