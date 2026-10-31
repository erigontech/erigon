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
	"math/bits"

	"github.com/erigontech/erigon/execution/commitment"
)

type leafRefSource interface {
	LeafRefs(key, data []byte) *commitment.LeafRefs
}

func leafRefsOf(ctx commitment.PatriciaContext, key, data []byte) *commitment.LeafRefs {
	if s, ok := ctx.(leafRefSource); ok {
		return s.LeafRefs(key, data)
	}
	return nil
}

func (c *meteredContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	return leafRefsOf(c.PatriciaContext, key, data)
}

func ComputeLeafRefs(key, data []byte) *commitment.LeafRefs {
	if len(key) < 2 || len(data) == 0 {
		return nil
	}
	plane, depth := key[0], int(key[len(key)-1])
	if depth == 0 || depth > 63 || plane != planeAccount && plane != planeStorage || Validate(data, depth) != nil {
		return nil
	}
	record := Record{data: data, depth: depth}
	l := record.layout()
	if l.leaf == 0 {
		return nil
	}
	var path [64]byte
	n := node{path: path[:depth], record: record, layout: l, plane: plane, childMask: l.child, leafMask: l.leaf, hashMask: l.child &^ l.leaf}
	refs := &commitment.LeafRefs{Refs: make([][32]byte, 0, bits.OnesCount16(l.leaf))}
	var out [32]byte
	for nib := range 16 {
		if l.leaf&(uint16(1)<<nib) == 0 {
			continue
		}
		ref, err := foldLeaf(&n, nib, false, out[:0])
		if err != nil || len(ref) != 32 {
			continue
		}
		refs.Mask |= uint16(1) << nib
		refs.Refs = append(refs.Refs, [32]byte(ref))
	}
	if refs.Mask == 0 {
		return nil
	}
	return refs
}

func (n *node) cachedLeafRef(nib int) ([]byte, bool) {
	r := n.refs
	bit := uint16(1) << nib
	if r == nil || r.Mask&bit == 0 || !n.stored(nib) {
		return nil, false
	}
	return r.Refs[bits.OnesCount16(r.Mask&(bit-1))][:], true
}
