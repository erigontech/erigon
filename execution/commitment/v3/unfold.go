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

import (
	"fmt"
	"math/bits"
)

func unfold(ctx brancher, path []byte, plane byte, addrHash []byte) (*node, error) {
	if len(path) > 63 {
		return nil, fmt.Errorf("commitment v3: path depth %d", len(path))
	}

	key := nodeKey(plane, addrHash, path, nil)
	data, _, err := branchOwned(ctx, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	if len(data) == 0 {
		return fork(path), nil
	}
	if err := Validate(data, len(path)); err != nil {
		return nil, fmt.Errorf("commitment v3: record %x: %w", key, err)
	}

	n := fork(path)
	n.raw = data
	record := Record{data: n.raw, depth: len(path)}
	n.loaded = true
	n.plane = plane
	if data[0]&hdrHasSelfExt != 0 {
		n.path = decodeExtensionInto(record.SelfExt(), nil)
	}
	l := record.layout()
	if record.isLeafRoot() {
		fullPath := unpackPath(data[1:33], 64, nil)
		n.setLeaf(int(fullPath[0]), packPath(fullPath[1:], nil), data[34:])
		return n, nil
	}

	if len(path) != 0 {
		n.record, n.layout = record, l
		n.childMask, n.leafMask, n.hashMask = l.child, l.leaf, l.child&^l.leaf
		n.refs = leafRefsOf(ctx, key, data)
		return n, nil
	}
	n.slots = make([]childSlot, 0, bits.OnesCount16(l.child))
	for m := l.child; m != 0; m &= m - 1 {
		nib := bits.TrailingZeros16(m)
		bit := uint16(1) << nib
		if l.leaf&bit != 0 {
			suffix, value := record.leafAt(l, nib)
			n.setLeafShared(nib, suffix, value)
			continue
		}
		hash := record.slotAt(l, nib)
		n.setStoredChild(nib, hash, decodeExtensionInto(record.extAt(l, nib), nil))
	}
	return n, nil
}

func decodeExtensionInto(encoded, dst []byte) []byte {
	if len(encoded) == 0 {
		return nil
	}
	return unpackPath(encoded[1:], int(encoded[0]), dst)
}
