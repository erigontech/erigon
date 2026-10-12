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

package pbt

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type leafRefSource interface {
	LeafRefs(key, data []byte) *commitment.LeafRefs
}

func leafRefsOf(ctx commitment.PatriciaContext, key, data []byte) *commitment.LeafRefs {
	if source, ok := ctx.(leafRefSource); ok {
		return source.LeafRefs(key, data)
	}
	return nil
}

func ComputeLeafRefs(key, data []byte) *commitment.LeafRefs {
	parsed, err := decodeRecordKey(key)
	if err != nil || len(data) == 0 {
		return nil
	}
	record, err := DecodeRecord(key, data)
	if err != nil || record.Form != RowRoot {
		return nil
	}
	slots := occupiedSlots(&record)
	if len(slots) == 0 {
		return nil
	}
	hashes := [maxCells]common.Hash{}
	prefixes := [maxCells][]byte{}
	mask := uint16(0)
	internal := make([]commitment.PBinInternalRef, 0, len(slots)-1)
	if _, err := computeCellRefs(parsed.path, &record, slots, 0, len(slots), parsed.path.BitLen, true, &hashes, &prefixes, &mask, &internal); err != nil {
		return nil
	}
	refs := &commitment.LeafRefs{Mask: mask, Refs: make([][32]byte, 0, len(slots)), Prefixes: make([][]byte, 0, len(slots)), PBinInternal: internal}
	for slot := range maxCells {
		if mask&(uint16(1)<<slot) != 0 {
			refs.Refs = append(refs.Refs, hashes[slot])
			refs.Prefixes = append(refs.Prefixes, prefixes[slot])
		}
	}
	return refs
}

func PrefetchPath(read func([]byte) []byte, key []byte) {
	read(GlobalRootKey())
	for bitLen := int16(4); bitLen <= int16(len(key)*8); bitLen += 4 {
		path := eip8297.PathFromBits(key, bitLen)
		rowKey, err := EncodeRowKey(&path)
		if err != nil {
			return
		}
		read(rowKey)
	}
}

func computeCellRefs(path eip8297.Bitpath, record *Record, slots []int, from, to int, parentSplit int16, root bool, hashes *[maxCells]common.Hash, prefixes *[maxCells][]byte, mask *uint16, internal *[]commitment.PBinInternalRef) (foldNode, error) {
	if to-from > 1 {
		split := firstSlotSplit(slots[from], slots[to-1], path.BitLen)
		middle := from
		for middle < to && slotBit(slots[middle], int(split-path.BitLen)) == 0 {
			middle++
		}
		if middle == from || middle == to {
			return foldNode{}, fmt.Errorf("row cells do not split at bit %d", split)
		}
		left, err := computeCellRefs(path, record, slots, from, middle, split, false, hashes, prefixes, mask, internal)
		if err != nil {
			return foldNode{}, err
		}
		right, err := computeCellRefs(path, record, slots, middle, to, split, false, hashes, prefixes, mask, internal)
		if err != nil {
			return foldNode{}, err
		}
		fromBit := parentSplit + 1
		if root {
			fromBit = path.BitLen
		}
		prefix := rowPrefix(&path, slots[from], fromBit, split)
		hash := branchHash(&prefix, &left.hash, &right.hash)
		*internal = append(*internal, commitment.PBinInternalRef{
			Mask:        slotsMask(slots, from, to),
			ParentSplit: parentSplit,
			Split:       split,
			Prefix:      eip8297.EncodeBitPath(&prefix),
			Left:        left.hash,
			Right:       right.hash,
			Hash:        hash,
		})
		return foldNode{split: split, left: left.hash, right: right.hash, hash: hash}, nil
	}
	slot := slots[from]
	cell := &record.Cells[slot]
	var hash common.Hash
	switch cell.Kind {
	case LeafCell:
		hash = leafHash(cell)
	case BranchCell:
		prefix := rowPrefix(&path, slot, parentSplit+1, path.BitLen+4)
		prefix.Append(&cell.Prefix)
		hash = branchHash(&prefix, &cell.Left, &cell.Right)
		prefixes[slot] = eip8297.EncodeBitPath(&prefix)
	default:
		return foldNode{}, fmt.Errorf("row cell %d is empty", slot)
	}
	hashes[slot] = hash
	*mask |= uint16(1) << slot
	return foldNode{hash: hash}, nil
}

func slotsMask(slots []int, from, to int) uint16 {
	var mask uint16
	for _, slot := range slots[from:to] {
		mask |= uint16(1) << slot
	}
	return mask
}

func (n *rowNode) cachedCellHash(slot int, prefix *eip8297.Bitpath) (common.Hash, bool) {
	bit := uint16(1) << slot
	if n.refs == nil || n.refMask&bit == 0 || n.dirtyCells&bit != 0 {
		return common.Hash{}, false
	}
	index := bits.OnesCount16(n.refMask & (bit - 1))
	if prefix != nil && (len(n.refs.Prefixes) <= index || !bytes.Equal(n.refs.Prefixes[index], eip8297.EncodeBitPath(prefix))) {
		return common.Hash{}, false
	}
	return common.Hash(n.refs.Refs[index]), true
}

func (n *rowNode) cachedInternalHash(mask uint16, parentSplit, split int16, prefix *eip8297.Bitpath) (foldNode, bool) {
	if n.refs == nil || n.dirtyCells&mask != 0 {
		return foldNode{}, false
	}
	wantPrefix := eip8297.EncodeBitPath(prefix)
	for i := range n.refs.PBinInternal {
		ref := &n.refs.PBinInternal[i]
		if ref.Mask != mask || ref.ParentSplit != parentSplit || ref.Split != split || !bytes.Equal(ref.Prefix, wantPrefix) {
			continue
		}
		return foldNode{
			split: split,
			left:  common.Hash(ref.Left),
			right: common.Hash(ref.Right),
			hash:  common.Hash(ref.Hash),
		}, true
	}
	return foldNode{}, false
}
