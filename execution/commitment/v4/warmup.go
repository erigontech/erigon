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

package v4

func warmupKeyV4(hashedKey []byte, depth int, dst []byte) ([]byte, bool) {
	if depth < 0 || depth > len(hashedKey) {
		return nil, false
	}

	if len(hashedKey) > 64 && depth >= 64 {
		addrHash := hashAddressPath(hashedKey[:64])
		return nodeKey(tagStorageNode, addrHash[:], hashedKey[64:depth], dst[:0]), true
	}
	return nodeKey(tagAccountNode, nil, hashedKey[:depth], dst[:0]), true
}

func warmupStepV4(data, hashedKey []byte, depth int) (nextDepth int, stop bool) {
	if depth < 0 || depth >= len(hashedKey) {
		return 0, true
	}

	planeDepth := depth
	if len(hashedKey) > 64 && depth >= 64 {
		planeDepth -= 64
	}
	record := NewRecord(data, planeDepth)
	l := record.layout()
	if !l.ok {
		return 0, true
	}

	branchPoint := depth
	if planeDepth == 0 && l.selfExtLen != 0 {
		selfExt := record.SelfExt()
		end := branchPoint + l.selfExtLen
		if len(selfExt) == 0 || end > len(hashedKey) || !packedMatches(selfExt[1:], hashedKey[branchPoint:end]) {
			return 0, true
		}
		branchPoint = end
	}
	if branchPoint >= len(hashedKey) {
		return 0, true
	}

	bit := uint16(1) << hashedKey[branchPoint]
	if l.tree()&bit == 0 {
		if len(hashedKey) > 64 && depth < 64 && l.leaf&bit != 0 {
			return 64, false
		}
		return 0, true
	}

	extLen := 0
	if l.ext&bit != 0 {
		ext := record.ExtAt(int(hashedKey[branchPoint]))
		if len(ext) == 0 {
			return 0, true
		}
		extLen = int(ext[0])
	}
	nextDepth = branchPoint + 1 + extLen
	if nextDepth > len(hashedKey) {
		return 0, true
	}
	return nextDepth, false
}

func PrefetchPath(read func(key []byte) []byte, addrHash, slotHash []byte, depth int) {
	var nibbles [128]byte
	hashedKey := unpackPath(addrHash, 64, nibbles[:64:64])
	if len(slotHash) != 0 {
		unpackPath(slotHash, 64, nibbles[64:128:128])
		hashedKey = nibbles[:128]
	}
	var buf [2 + 32 + 32 + 1]byte
	for depth < len(hashedKey) {
		key, ok := warmupKeyV4(hashedKey, depth, buf[:0])
		if !ok {
			return
		}
		data := read(key)
		if len(data) == 0 {
			return
		}
		next, stop := warmupStepV4(data, hashedKey, depth)
		if stop || next <= depth {
			return
		}
		depth = next
	}
}
