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

import "github.com/erigontech/erigon/execution/commitment/nibbles"

func warmupStepV3(data, hashedKey []byte, depth int) (nextDepth int, stop bool) {
	if depth < 0 || depth >= len(hashedKey) {
		return 0, true
	}

	record := Record{data: data, depth: depth}
	l := record.layout()
	if !l.ok {
		return 0, true
	}

	if depth == 0 && l.selfExtLen != 0 {
		selfExt := record.SelfExt()
		end := depth + l.selfExtLen
		if end > len(hashedKey) || !packedMatches(selfExt[1:], hashedKey[depth:end]) {
			return 0, true
		}
		return end, false
	}
	bit := uint16(1) << hashedKey[depth]
	if l.tree()&bit == 0 {
		return 0, true
	}

	extLen := 0
	if l.ext&bit != 0 {
		ext := record.extAt(l, int(hashedKey[depth]))
		if len(ext) == 0 {
			return 0, true
		}
		extLen = int(ext[0])
	}
	nextDepth = depth + 1 + extLen
	if nextDepth > len(hashedKey) {
		return 0, true
	}
	return nextDepth, false
}

func PrefetchPath(read func(key []byte) []byte, addrHash, slotHash []byte) {
	plane, hash := byte(tagAccountNode), addrHash
	var owner []byte
	if len(slotHash) != 0 {
		plane, owner, hash = tagStorageNode, addrHash, slotHash
	}
	var path [64]byte
	hashedKey := unpackPath(hash, 64, path[:])
	depth := 0
	var buf [2 + 32 + 32 + 1]byte
	for depth < len(hashedKey) {
		data := read(nodeKey(plane, owner, hashedKey[:depth], buf[:0]))
		if len(data) == 0 {
			return
		}
		if split := splitDepthV3(data, hashedKey, depth); split != 0 {
			read(nodeKey(plane, owner, hashedKey[:split], buf[:0]))
			return
		}
		next, stop := warmupStepV3(data, hashedKey, depth)
		if stop {
			return
		}
		depth = next
	}
}

func splitDepthV3(data, hashedKey []byte, depth int) int {
	record := Record{data: data, depth: depth}
	l := record.layout()
	if !l.ok || depth == 0 && l.selfExtLen != 0 {
		return 0
	}
	nib := int(hashedKey[depth])
	bit := uint16(1) << nib
	rest := hashedKey[depth+1:]
	var packed []byte
	count := len(rest)
	switch {
	case l.leaf&bit != 0:
		packed, _ = record.leafAt(l, nib)
	case l.tree()&bit != 0 && l.ext&bit != 0:
		ext := record.extAt(l, nib)
		if len(ext) == 0 {
			return 0
		}
		packed, count = ext[1:], int(ext[0])
	default:
		return 0
	}
	if len(packed) == 0 || count > len(rest) {
		return 0
	}
	var scratch [64]byte
	common := nibbles.CommonPrefixLen(unpackPath(packed, count, scratch[:count:count]), rest)
	if common == count {
		return 0
	}
	return depth + 1 + common
}
