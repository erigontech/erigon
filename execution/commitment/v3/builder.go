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

import "math/bits"

type NodeBuilder struct {
	n node
}

func (b *NodeBuilder) Reset(path []byte, plane byte) { resetNode(&b.n, path, plane) }

func (b *NodeBuilder) SetLeaf(nib int, suffix, value []byte) { b.n.setLeafShared(nib, suffix, value) }

func (b *NodeBuilder) SetStoredChild(nib int, hash, ext []byte) { b.n.setStoredChild(nib, hash, ext) }

func (b *NodeBuilder) ChildCount() int { return bits.OnesCount16(b.n.childMask) }

func (b *NodeBuilder) Hash(depth int) ([32]byte, error) { return fold(&b.n, depth) }

func (b *NodeBuilder) AppendRecord(depth int, dst []byte) []byte {
	return encodeRecord(&b.n, depth, dst)
}

func AppendHashSuffix(scratch *[]byte, hashed *[32]byte, from int) []byte {
	start := len(*scratch)
	if from&1 == 0 {
		*scratch = append(*scratch, hashed[from/2:]...)
	} else {
		for i := from / 2; i < 31; i++ {
			*scratch = append(*scratch, hashed[i]<<4|hashed[i+1]>>4)
		}
		*scratch = append(*scratch, hashed[31]<<4)
	}
	return (*scratch)[start:]
}

func resetNode(n *node, path []byte, plane byte) {
	*n = node{path: path, slots: n.slots[:0], plane: plane}
}
