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

package v3

import "slices"

const (
	tagAccountNode byte = 0x40
	tagStorageNode byte = 0x41
)

func AccountNodeKey(path []byte, dst []byte) []byte {
	return nodeKey(tagAccountNode, nil, path, dst)
}

func StorageNodeKey(addrHash [32]byte, path []byte, dst []byte) []byte {
	return nodeKey(tagStorageNode, addrHash[:], path, dst)
}

func nodeKey(tag byte, addrHash, path, dst []byte) []byte {
	if len(path) > 64 {
		panic("commitment v3: path exceeds 64 nibbles")
	}
	packed := packedLen(len(path))
	dst = slices.Grow(dst, 2+len(addrHash)+packed)
	dst = append(dst, tag)
	dst = append(dst, addrHash...)
	start := len(dst)
	dst = dst[:start+packed]
	packPath(path, dst[start:start+packed:start+packed])
	return append(dst, byte(len(path)))
}
