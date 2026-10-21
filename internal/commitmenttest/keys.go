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
	"encoding/binary"
	"fmt"
)

type KeySpec struct {
	Kind  string
	Size  int
	Count int
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
	case "fold":
		for j := range key {
			key[j] = byte(number + j*17)
		}
	default:
		panic("unknown key kind: " + spec.Kind)
	}
	return key
}

func Keys(seed Seed, spec KeySpec) [][]byte {
	if spec.Count < 0 || spec.Size < 1 {
		panic(fmt.Sprintf("invalid key spec: %+v", spec))
	}
	keys := make([][]byte, 0, spec.Count)
	if spec.Kind != "random-distinct" {
		switch spec.Kind {
		case "integer":
			if spec.Size < 8 {
				panic(fmt.Sprintf("integer key size %d", spec.Size))
			}
		case "bench-address", "bench-slot":
			if spec.Size < 16 {
				panic(fmt.Sprintf("bench key size %d", spec.Size))
			}
		case "wrapping-address", "wrapping-slot", "fold":
		default:
			panic(fmt.Sprintf("unknown key kind: %s", spec.Kind))
		}
		for i := range spec.Count {
			keys = append(keys, Key(spec, i))
		}
		return keys
	}
	if spec.Size < 8 && uint64(spec.Count) > uint64(1)<<(8*spec.Size) {
		panic(fmt.Sprintf("too many distinct keys for size %d", spec.Size))
	}
	rng := seed.rand()
	seen := make(map[string]struct{}, spec.Count)
	for len(keys) < spec.Count {
		key := make([]byte, spec.Size)
		_, _ = rng.Read(key)
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

type Shape struct {
	Prefixes [][]byte
}

func Paths(spec Shape) [][]byte {
	paths := make([][]byte, 0, len(spec.Prefixes))
	for _, prefix := range spec.Prefixes {
		if len(prefix) > 64 {
			panic(fmt.Sprintf("prefix length %d", len(prefix)))
		}
		fill := byte(0xd)
		if len(prefix) != 0 {
			fill = (prefix[len(prefix)-1] + 7) & 15
		}
		path := make([]byte, 64)
		copy(path, prefix)
		for i := len(prefix); i < len(path); i++ {
			path[i] = fill
		}
		paths = append(paths, path)
	}
	for _, path := range paths {
		for _, nib := range path {
			if nib > 15 {
				panic(fmt.Sprintf("invalid nibble %d", nib))
			}
		}
	}
	return paths
}

func Path(head []byte, fill byte) []byte {
	path := make([]byte, 64)
	n := copy(path, head)
	for i := n; i < len(path); i++ {
		path[i] = fill
	}
	return path
}
