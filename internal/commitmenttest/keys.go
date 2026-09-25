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
	"fmt"
)

type KeySpec struct {
	Kind  string
	Size  int
	Start int
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

func Keys(seed Seed, spec KeySpec) ([][]byte, error) {
	if spec.Count < 0 || spec.Size < 1 {
		return nil, fmt.Errorf("invalid key spec: %+v", spec)
	}
	keys := make([][]byte, 0, spec.Count)
	if spec.Kind != "random-distinct" {
		switch spec.Kind {
		case "integer":
			if spec.Size < 8 {
				return nil, fmt.Errorf("integer key size %d", spec.Size)
			}
		case "bench-address", "bench-slot":
			if spec.Size < 16 {
				return nil, fmt.Errorf("bench key size %d", spec.Size)
			}
		case "wrapping-address", "wrapping-slot", "fold":
		default:
			return nil, fmt.Errorf("unknown key kind: %s", spec.Kind)
		}
		for i := range spec.Count {
			keys = append(keys, Key(spec, spec.Start+i))
		}
		return keys, nil
	}
	if spec.Size < 8 && uint64(spec.Count) > uint64(1)<<(8*spec.Size) {
		return nil, fmt.Errorf("too many distinct keys for size %d", spec.Size)
	}
	rng, err := seed.Rand()
	if err != nil {
		return nil, err
	}
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
	return keys, nil
}

type Shape struct {
	Plane    string
	Paths    [][]byte
	Prefixes [][]byte
}

func Paths(spec Shape) ([][]byte, error) {
	paths := make([][]byte, 0, len(spec.Paths)+len(spec.Prefixes))
	for _, path := range spec.Paths {
		if len(path) != 64 {
			return nil, fmt.Errorf("path length %d", len(path))
		}
		paths = append(paths, bytes.Clone(path))
	}
	for _, prefix := range spec.Prefixes {
		if len(prefix) > 64 {
			return nil, fmt.Errorf("prefix length %d", len(prefix))
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
				return nil, fmt.Errorf("invalid nibble %d", nib)
			}
		}
	}
	return paths, nil
}
