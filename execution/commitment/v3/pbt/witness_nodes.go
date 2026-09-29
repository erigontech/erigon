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
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/witness"
)

type PBinWitnessResolver struct {
	read    func([]byte) ([]byte, error)
	records map[string]pbinResolverRecord
}

type pbinParentBranchContext interface {
	ParentBranch([]byte) ([]byte, kv.Step, error)
}

type pbinResolverRecord struct {
	present bool
	record  Record
	err     error
}

func NewPBinWitnessResolver(ctx commitment.PatriciaContext) *PBinWitnessResolver {
	resolver := &PBinWitnessResolver{records: make(map[string]pbinResolverRecord)}
	if parent, ok := ctx.(pbinParentBranchContext); ok {
		resolver.read = func(key []byte) ([]byte, error) {
			data, _, err := parent.ParentBranch(key)
			return data, err
		}
	} else if ctx != nil {
		resolver.read = func(key []byte) ([]byte, error) {
			data, _, err := ctx.Branch(key)
			return data, err
		}
	}
	return resolver
}

func (r *PBinWitnessResolver) Resolve(path []byte) ([]byte, error) {
	walk, err := pbinDecodeWitnessPath(path)
	if err != nil {
		return nil, err
	}
	root, err := r.readRecord(GlobalRootKey())
	if err != nil {
		return nil, err
	}
	if !root.present {
		return nil, nil
	}
	blob, found, err := r.resolveRecord(eip8297.Bitpath{}, &root.record, walk, walk, nil)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	if err := pbinValidateWitnessBlob(path, blob); err != nil {
		return nil, err
	}
	return slices.Clone(blob), nil
}

func (r *PBinWitnessResolver) RootHash() (common.Hash, error) {
	root, err := r.readRecord(GlobalRootKey())
	if err != nil {
		return common.Hash{}, err
	}
	if !root.present {
		return eip8297.EmptyTreeHash, nil
	}
	return r.recordHash(GlobalRootKey(), eip8297.Bitpath{}, &root.record)
}

func (r *PBinWitnessResolver) readRecord(key []byte) (pbinResolverRecord, error) {
	cacheKey := string(key)
	if record, ok := r.records[cacheKey]; ok {
		return record, record.err
	}
	result := pbinResolverRecord{}
	if r.read == nil {
		result.err = fmt.Errorf("pbin witness: nil Patricia context")
		r.records[cacheKey] = result
		return result, result.err
	}
	data, err := r.read(key)
	if err != nil {
		result.err = err
		r.records[cacheKey] = result
		return result, err
	}
	if len(data) == 0 {
		r.records[cacheKey] = result
		return result, nil
	}
	record, err := DecodeRecord(key, data)
	if err != nil {
		result.err = err
		r.records[cacheKey] = result
		return result, err
	}
	result.present = true
	result.record = record
	r.records[cacheKey] = result
	return result, nil
}

func (r *PBinWitnessResolver) recordHash(key []byte, path eip8297.Bitpath, record *Record) (common.Hash, error) {
	switch record.Form {
	case LeafRoot:
		cell, ok := singleLeaf(record)
		if !ok {
			return common.Hash{}, fmt.Errorf("pbin witness: invalid leaf record at %x", key)
		}
		return leafHash(&cell), nil
	case ExtRoot:
		return branchHash(&record.SelfExt, &record.Left, &record.Right), nil
	case RowRoot:
		return pbinRowHash(key, path, record)
	default:
		return common.Hash{}, fmt.Errorf("pbin witness: unknown record form %d at %x", record.Form, key)
	}
}

func pbinRowHash(key []byte, path eip8297.Bitpath, record *Record) (common.Hash, error) {
	result, err := FoldRow(key, record)
	if err != nil {
		if len(occupiedSlots(record)) == 1 {
			cell := record.Cells[occupiedSlots(record)[0]]
			switch cell.Kind {
			case LeafCell:
				return leafHash(&cell), nil
			case BranchCell:
				prefix := rowPrefix(&path, occupiedSlots(record)[0], path.BitLen, path.BitLen+4)
				prefix.Append(&cell.Prefix)
				return branchHash(&prefix, &cell.Left, &cell.Right), nil
			}
		}
		return common.Hash{}, err
	}
	slots := occupiedSlots(record)
	prefix := rowPrefix(&path, slots[0], path.BitLen, result.Split)
	return branchHash(&prefix, &result.Left, &result.Right), nil
}

func (r *PBinWitnessResolver) resolveRecord(base eip8297.Bitpath, record *Record, node, target eip8297.Bitpath, expected *common.Hash) ([]byte, bool, error) {
	if !node.HasPrefix(&target) && !target.HasPrefix(&node) {
		return nil, false, nil
	}
	switch record.Form {
	case LeafRoot:
		cell, ok := singleLeaf(record)
		if !ok {
			return nil, false, fmt.Errorf("pbin witness: invalid leaf record")
		}
		blob, err := witness.PBinEncodeLeaf(cell.Key, cell.Value[:])
		if err != nil {
			return nil, false, err
		}
		if err := pbinCheckPointer(blob, expected); err != nil {
			return nil, false, err
		}
		if target != node {
			return nil, false, nil
		}
		return blob, true, nil
	case ExtRoot:
		return r.resolveExt(base, record, node, target, expected)
	case RowRoot:
		return r.resolveRow(base, record, node, target, expected)
	default:
		return nil, false, fmt.Errorf("pbin witness: unknown record form %d", record.Form)
	}
}

func (r *PBinWitnessResolver) resolveExt(base eip8297.Bitpath, record *Record, node, target eip8297.Bitpath, expected *common.Hash) ([]byte, bool, error) {
	absolute := base
	absolute.Append(&record.SelfExt)
	if !absolute.HasPrefix(&node) {
		return nil, false, fmt.Errorf("pbin witness: extension %x does not contain node %x", witness.PBinPath(&absolute), witness.PBinPath(&node))
	}
	relative := absolute.Slice(node.BitLen, absolute.BitLen)
	blob, err := witness.PBinEncodeBranch(&relative, &record.Left, &record.Right)
	if err != nil {
		return nil, false, err
	}
	if err := pbinCheckPointer(blob, expected); err != nil {
		return nil, false, err
	}
	if target == node {
		group, ok, err := r.tryGroupFromRecord(base, record, node, blob)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return group, true, nil
		}
		return blob, true, nil
	}
	if !absolute.HasPrefix(&target) || target.BitLen <= absolute.BitLen {
		return nil, false, nil
	}
	edge := target.Bit(absolute.BitLen)
	child := absolute
	child.AppendBit(edge)
	childHash := record.Left
	if edge != 0 {
		childHash = record.Right
	}
	window := absolute
	window.Truncate((absolute.BitLen / 4) * 4)
	return r.resolveChildAt(window, child, target, &childHash)
}

func (r *PBinWitnessResolver) resolveRow(base eip8297.Bitpath, record *Record, node, target eip8297.Bitpath, expected *common.Hash) ([]byte, bool, error) {
	path := base
	key, err := rowKeyForPath(&path)
	if err != nil {
		return nil, false, err
	}
	slots := occupiedSlots(record)
	if len(slots) == 0 {
		return nil, false, nil
	}
	return r.resolveRange(path, key, record, slots, 0, len(slots), path, node, target, expected)
}

func pbinRowRange(path, node eip8297.Bitpath, slots []int) (int, int, error) {
	if !node.HasPrefix(&path) {
		return 0, 0, fmt.Errorf("pbin witness: row path %x does not contain node path %x", witness.PBinPath(&path), witness.PBinPath(&node))
	}
	if node.BitLen == path.BitLen {
		return 0, len(slots), nil
	}
	from := 0
	consumed := min(int(node.BitLen-path.BitLen), 4)
	for from < len(slots) && !pbinSlotMatches(slots[from], &node, path.BitLen, consumed) {
		from++
	}
	to := from
	for to < len(slots) && pbinSlotMatches(slots[to], &node, path.BitLen, consumed) {
		to++
	}
	if from == to {
		return 0, 0, nil
	}
	return from, to, nil
}

func pbinSlotMatches(slot int, path *eip8297.Bitpath, start int16, count int) bool {
	for offset := range count {
		if slotBit(slot, offset) != path.Bit(start+int16(offset)) {
			return false
		}
	}
	return true
}

func (r *PBinWitnessResolver) resolveRange(path eip8297.Bitpath, key []byte, record *Record, slots []int, from, to int, node, wanted, target eip8297.Bitpath, expected *common.Hash) ([]byte, bool, error) {
	if from == to {
		return nil, false, nil
	}
	parentSplit := node.BitLen - 1
	if node.BitLen == 0 {
		parentSplit = 0
	}
	folded, err := foldRange(path, record, slots, from, to, parentSplit, nil)
	if err != nil {
		if to-from != 1 {
			return nil, false, err
		}
		return r.resolveSingle(path, key, record, slots[from], node, wanted, target, expected)
	}
	prefix := rowPrefix(&path, slots[from], parentSplit+1, folded.split)
	blob, err := witness.PBinEncodeBranch(&prefix, &folded.left, &folded.right)
	if err != nil {
		return nil, false, err
	}
	if node == wanted {
		if err := pbinCheckPointer(blob, expected); err != nil {
			return nil, false, err
		}
	}
	if node == wanted {
		group, ok, err := r.tryGroupRange(path, key, record, slots, from, to, node, blob)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if target == node {
				return group, true, nil
			}
			return nil, false, nil
		}
	}
	if target == node {
		return blob, true, nil
	}
	if !target.HasPrefix(&node) {
		return nil, false, nil
	}
	edgePath := node
	edgePath.Append(&prefix)
	if target.BitLen <= edgePath.BitLen {
		return nil, false, nil
	}
	edge := target.Bit(edgePath.BitLen)
	child := edgePath
	child.AppendBit(edge)
	middle := from
	for middle < to && slotBit(slots[middle], int(folded.split-path.BitLen)) == 0 {
		middle++
	}
	childHash := folded.left
	childFrom, childTo := from, middle
	if edge != 0 {
		childHash = folded.right
		childFrom, childTo = middle, to
	}
	if childFrom == childTo {
		return nil, false, nil
	}
	if childTo-childFrom > 1 {
		return r.resolveRange(path, key, record, slots, childFrom, childTo, child, wanted, target, &childHash)
	}
	return r.resolveSingle(path, key, record, slots[childFrom], child, wanted, target, &childHash)
}

func (r *PBinWitnessResolver) resolveSingle(path eip8297.Bitpath, key []byte, record *Record, slot int, node, wanted, target eip8297.Bitpath, expected *common.Hash) ([]byte, bool, error) {
	cell := &record.Cells[slot]
	parentSplit := node.BitLen - 1
	if node.BitLen == 0 {
		parentSplit = path.BitLen
	}
	prefix := rowPrefix(&path, slot, parentSplit+1, path.BitLen+4)
	if cell.Kind == BranchCell {
		prefix.Append(&cell.Prefix)
	}
	switch cell.Kind {
	case LeafCell:
		blob, err := witness.PBinEncodeLeaf(cell.Key, cell.Value[:])
		if err != nil {
			return nil, false, err
		}
		if node == wanted {
			if err := pbinCheckPointer(blob, expected); err != nil {
				return nil, false, err
			}
		}
		if target == node {
			return blob, true, nil
		}
		return nil, false, nil
	case BranchCell:
		blob, err := witness.PBinEncodeBranch(&prefix, &cell.Left, &cell.Right)
		if err != nil {
			return nil, false, err
		}
		if node == wanted {
			if err := pbinCheckPointer(blob, expected); err != nil {
				return nil, false, err
			}
		}
		if target == node {
			group, ok, err := r.tryGroupChild(path, slot, cell, node, blob)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return group, true, nil
			}
			return blob, true, nil
		}
		if !target.HasPrefix(&node) || target.BitLen <= node.BitLen+prefix.BitLen {
			return nil, false, nil
		}
		branchEnd := node
		branchEnd.Append(&prefix)
		edge := target.Bit(branchEnd.BitLen)
		childPath := branchEnd
		childPath.AppendBit(edge)
		childHash := cell.Left
		if edge != 0 {
			childHash = cell.Right
		}
		row := &rowNode{path: path}
		childRowPath, err := rowChildPath(row, slot, cell.Prefix, branchSplit(row, slot, &rowCell{Cell: cell, Kind: BranchCell}))
		if err != nil {
			return nil, false, err
		}
		return r.resolveChildAt(childRowPath, childPath, target, &childHash)
	default:
		return nil, false, fmt.Errorf("pbin witness: unknown row cell kind %d", cell.Kind)
	}
}

func (r *PBinWitnessResolver) resolveChildAt(rowPath, node, target eip8297.Bitpath, expected *common.Hash) ([]byte, bool, error) {
	key, err := rowKeyForPath(&rowPath)
	if err != nil {
		return nil, false, err
	}
	record, err := r.readRecord(key)
	if err != nil {
		return nil, false, err
	}
	if !record.present {
		return nil, false, fmt.Errorf("pbin witness: row %x is missing", key)
	}
	return r.resolveRecord(rowPath, &record.record, node, target, expected)
}

func pbinCheckPointer(blob []byte, expected *common.Hash) error {
	if expected == nil {
		return nil
	}
	hash, err := witness.PBinHashBlob(blob)
	if err != nil {
		return err
	}
	if hash != *expected {
		return fmt.Errorf("pbin witness: stored pointer does not match resolved blob")
	}
	return nil
}

func (r *PBinWitnessResolver) tryGroupFromRecord(base eip8297.Bitpath, record *Record, node eip8297.Bitpath, branch []byte) ([]byte, bool, error) {
	key, err := rowKeyForPath(&base)
	if err != nil {
		return nil, false, err
	}
	switch record.Form {
	case RowRoot:
		slots := occupiedSlots(record)
		from, to, err := pbinRowRange(base, node, slots)
		if err != nil {
			return nil, false, err
		}
		return r.tryGroupRange(base, key, record, slots, from, to, node, branch)
	case ExtRoot:
		return r.tryGroupExt(base, record, node, branch)
	default:
		return nil, false, nil
	}
}

func (r *PBinWitnessResolver) tryGroupRange(path eip8297.Bitpath, key []byte, record *Record, slots []int, from, to int, node eip8297.Bitpath, branch []byte) ([]byte, bool, error) {
	if to-from < 2 {
		return nil, false, nil
	}
	first, err := r.firstKeyRange(path, record, slots, from, to, node)
	if err != nil {
		return nil, false, err
	}
	if len(first) < 2 {
		return nil, false, nil
	}
	stem := first[:len(first)-1]
	values := make(map[byte][]byte)
	complete := true
	if err := r.collectRange(path, key, record, slots, from, to, node, stem, values, nil, &complete); err != nil {
		return nil, false, err
	}
	if !complete || len(values) < 2 {
		return nil, false, nil
	}
	subs := make([]byte, 0, len(values))
	for sub := range values {
		subs = append(subs, sub)
	}
	slices.Sort(subs)
	group := witness.PBinGroup{Position: uint16(node.BitLen), Stem: slices.Clone(stem), Subs: subs, Values: make([][]byte, len(subs))}
	for i, sub := range subs {
		group.Values[i] = values[sub]
	}
	blob, err := witness.PBinEncodeGroup(group)
	if err != nil {
		return nil, false, err
	}
	groupHash, err := witness.PBinHashBlob(blob)
	if err != nil {
		return nil, false, err
	}
	branchHashValue, err := witness.PBinHashBlob(branch)
	if err != nil {
		return nil, false, err
	}
	if groupHash != branchHashValue {
		return nil, false, fmt.Errorf("pbin witness: stored pointer does not match group blob at position %d", node.BitLen)
	}
	return blob, true, nil
}

func (r *PBinWitnessResolver) tryGroupExt(base eip8297.Bitpath, record *Record, node eip8297.Bitpath, branch []byte) ([]byte, bool, error) {
	absolute := base
	absolute.Append(&record.SelfExt)
	if absolute.BitLen == 0 {
		return nil, false, nil
	}
	first, err := r.firstKeyExt(base, record, node)
	if err != nil {
		return nil, false, err
	}
	if len(first) < 2 {
		return nil, false, nil
	}
	stem := first[:len(first)-1]
	values := make(map[byte][]byte)
	complete := true
	if err := r.collectExt(base, record, node, stem, values, nil, &complete); err != nil {
		return nil, false, err
	}
	if !complete || len(values) < 2 {
		return nil, false, nil
	}
	subs := make([]byte, 0, len(values))
	for sub := range values {
		subs = append(subs, sub)
	}
	slices.Sort(subs)
	group := witness.PBinGroup{Position: uint16(node.BitLen), Stem: slices.Clone(stem), Subs: subs, Values: make([][]byte, len(subs))}
	for i, sub := range subs {
		group.Values[i] = values[sub]
	}
	blob, err := witness.PBinEncodeGroup(group)
	if err != nil {
		return nil, false, err
	}
	groupHash, err := witness.PBinHashBlob(blob)
	if err != nil {
		return nil, false, err
	}
	branchHashValue, err := witness.PBinHashBlob(branch)
	if err != nil {
		return nil, false, err
	}
	if groupHash != branchHashValue {
		return nil, false, fmt.Errorf("pbin witness: stored pointer does not match group blob at position %d", node.BitLen)
	}
	return blob, true, nil
}

func (r *PBinWitnessResolver) tryGroupChild(path eip8297.Bitpath, slot int, cell *Cell, node eip8297.Bitpath, branch []byte) ([]byte, bool, error) {
	row := &rowNode{path: path}
	childRowPath, err := rowChildPath(row, slot, cell.Prefix, branchSplit(row, slot, &rowCell{Cell: cell, Kind: BranchCell}))
	if err != nil {
		return nil, false, err
	}
	branchEnd := node
	branchPrefix := rowPrefix(&path, slot, node.BitLen, path.BitLen+4)
	branchPrefix.Append(&cell.Prefix)
	branchEnd.Append(&branchPrefix)
	first, err := r.firstKeyChildAt(childRowPath, appendPathBit(branchEnd, 0))
	if err != nil {
		return nil, false, err
	}
	if len(first) < 2 {
		return nil, false, nil
	}
	stem := first[:len(first)-1]
	values := make(map[byte][]byte)
	complete := true
	if err := r.collectChildAt(childRowPath, appendPathBit(branchEnd, 0), stem, values, nil, &complete); err != nil {
		return nil, false, err
	}
	if err := r.collectChildAt(childRowPath, appendPathBit(branchEnd, 1), stem, values, nil, &complete); err != nil {
		return nil, false, err
	}
	if !complete || len(values) < 2 {
		return nil, false, nil
	}
	subs := make([]byte, 0, len(values))
	for sub := range values {
		subs = append(subs, sub)
	}
	slices.Sort(subs)
	group := witness.PBinGroup{Position: uint16(node.BitLen), Stem: slices.Clone(stem), Subs: subs, Values: make([][]byte, len(subs))}
	for i, sub := range subs {
		group.Values[i] = values[sub]
	}
	blob, err := witness.PBinEncodeGroup(group)
	if err != nil {
		return nil, false, err
	}
	groupHash, err := witness.PBinHashBlob(blob)
	if err != nil {
		return nil, false, err
	}
	branchHashValue, err := witness.PBinHashBlob(branch)
	if err != nil {
		return nil, false, err
	}
	if groupHash != branchHashValue {
		return nil, false, fmt.Errorf("pbin witness: stored pointer does not match group blob at position %d", node.BitLen)
	}
	return blob, true, nil
}

func (r *PBinWitnessResolver) firstKeyRange(path eip8297.Bitpath, record *Record, slots []int, from, to int, node eip8297.Bitpath) ([]byte, error) {
	if to-from > 1 {
		folded, err := foldRange(path, record, slots, from, to, node.BitLen-1, nil)
		if node.BitLen == path.BitLen {
			folded, err = foldRange(path, record, slots, from, to, path.BitLen, nil)
		}
		if err != nil {
			return nil, err
		}
		parentSplit := node.BitLen - 1
		if node.BitLen == 0 {
			parentSplit = path.BitLen
		}
		prefix := rowPrefix(&path, slots[from], parentSplit+1, folded.split)
		edgePath := node
		edgePath.Append(&prefix)
		middle := from
		for middle < to && slotBit(slots[middle], int(folded.split-path.BitLen)) == 0 {
			middle++
		}
		return r.firstKeyRange(path, record, slots, from, middle, appendPathBit(edgePath, 0))
	}
	cell := &record.Cells[slots[from]]
	if cell.Kind == LeafCell {
		return cell.Key, nil
	}
	if cell.Kind != BranchCell {
		return nil, fmt.Errorf("pbin witness: unknown row cell kind %d", cell.Kind)
	}
	parentSplit := node.BitLen - 1
	if node.BitLen == 0 {
		parentSplit = path.BitLen
	}
	prefix := rowPrefix(&path, slots[from], parentSplit+1, path.BitLen+4)
	prefix.Append(&cell.Prefix)
	branchNode := node
	branchNode.Append(&prefix)
	child := branchNode
	child.AppendBit(0)
	row := &rowNode{path: path}
	childRowPath, err := rowChildPath(row, slots[from], cell.Prefix, branchSplit(row, slots[from], &rowCell{Cell: cell, Kind: BranchCell}))
	if err != nil {
		return nil, err
	}
	return r.firstKeyChildAt(childRowPath, child)
}

func (r *PBinWitnessResolver) firstKeyChildAt(rowPath, node eip8297.Bitpath) ([]byte, error) {
	key, err := rowKeyForPath(&rowPath)
	if err != nil {
		return nil, err
	}
	record, err := r.readRecord(key)
	if err != nil {
		return nil, err
	}
	if !record.present {
		return nil, fmt.Errorf("pbin witness: row %x is missing", key)
	}
	return r.firstKeyRecord(rowPath, &record.record, node)
}

func (r *PBinWitnessResolver) firstKeyRecord(base eip8297.Bitpath, record *Record, node eip8297.Bitpath) ([]byte, error) {
	switch record.Form {
	case LeafRoot:
		cell, ok := singleLeaf(record)
		if !ok {
			return nil, fmt.Errorf("pbin witness: invalid leaf record")
		}
		return cell.Key, nil
	case ExtRoot:
		return r.firstKeyExt(base, record, node)
	case RowRoot:
		slots := occupiedSlots(record)
		from, to, err := pbinRowRange(base, node, slots)
		if err != nil {
			return nil, err
		}
		return r.firstKeyRange(base, record, slots, from, to, node)
	default:
		return nil, fmt.Errorf("pbin witness: unknown record form %d", record.Form)
	}
}

func (r *PBinWitnessResolver) firstKeyExt(base eip8297.Bitpath, record *Record, node eip8297.Bitpath) ([]byte, error) {
	absolute := base
	absolute.Append(&record.SelfExt)
	child := absolute
	child.AppendBit(0)
	window := absolute
	window.Truncate((absolute.BitLen / 4) * 4)
	return r.firstKeyChildAt(window, child)
}

func (r *PBinWitnessResolver) collectRange(path eip8297.Bitpath, key []byte, record *Record, slots []int, from, to int, node eip8297.Bitpath, stem []byte, values map[byte][]byte, expected *common.Hash, complete *bool) error {
	if from == to {
		return nil
	}
	parentSplit := node.BitLen - 1
	if node.BitLen == 0 {
		parentSplit = path.BitLen
	}
	if to-from > 1 {
		folded, err := foldRange(path, record, slots, from, to, parentSplit, nil)
		if err != nil {
			return err
		}
		prefix := rowPrefix(&path, slots[from], parentSplit+1, folded.split)
		stemPath := eip8297.PathFromBits(stem, int16(len(stem)*8))
		edgePath := node
		edgePath.Append(&prefix)
		if !stemPath.HasPrefix(&edgePath) && !edgePath.HasPrefix(&stemPath) {
			*complete = false
			return nil
		}
		middle := from
		for middle < to && slotBit(slots[middle], int(folded.split-path.BitLen)) == 0 {
			middle++
		}
		if edgePath.HasPrefix(&stemPath) {
			if err := r.collectRange(path, key, record, slots, from, middle, appendPathBit(edgePath, 0), stem, values, &folded.left, complete); err != nil {
				return err
			}
			return r.collectRange(path, key, record, slots, middle, to, appendPathBit(edgePath, 1), stem, values, &folded.right, complete)
		}
		edge := stemPath.Bit(edgePath.BitLen)
		*complete = false
		if edge == 0 {
			return r.collectRange(path, key, record, slots, from, middle, appendPathBit(edgePath, 0), stem, values, &folded.left, complete)
		}
		return r.collectRange(path, key, record, slots, middle, to, appendPathBit(edgePath, 1), stem, values, &folded.right, complete)
	}
	cell := &record.Cells[slots[from]]
	if cell.Kind == LeafCell {
		if bytes.Equal(cell.Key[:len(cell.Key)-1], stem) {
			values[cell.Key[len(cell.Key)-1]] = slices.Clone(cell.Value[:])
		} else {
			*complete = false
		}
		return nil
	}
	if cell.Kind != BranchCell {
		return fmt.Errorf("pbin witness: unknown row cell kind %d", cell.Kind)
	}
	prefix := rowPrefix(&path, slots[from], parentSplit+1, path.BitLen+4)
	prefix.Append(&cell.Prefix)
	branchNode := node
	stemPath := eip8297.PathFromBits(stem, int16(len(stem)*8))
	if !stemPath.HasPrefix(&branchNode) && !branchNode.HasPrefix(&stemPath) {
		*complete = false
		return nil
	}
	branchEnd := branchNode
	branchEnd.Append(&prefix)
	if branchNode.HasPrefix(&stemPath) {
		row := &rowNode{path: path}
		childRowPath, err := rowChildPath(row, slots[from], cell.Prefix, branchSplit(row, slots[from], &rowCell{Cell: cell, Kind: BranchCell}))
		if err != nil {
			return err
		}
		if err := r.collectChildAt(childRowPath, appendPathBit(branchEnd, 0), stem, values, &cell.Left, complete); err != nil {
			return err
		}
		return r.collectChildAt(childRowPath, appendPathBit(branchEnd, 1), stem, values, &cell.Right, complete)
	}
	if stemPath.BitLen < branchNode.BitLen+prefix.BitLen {
		*complete = false
		return nil
	}
	if stemPath.BitLen == branchEnd.BitLen {
		row := &rowNode{path: path}
		childRowPath, err := rowChildPath(row, slots[from], cell.Prefix, branchSplit(row, slots[from], &rowCell{Cell: cell, Kind: BranchCell}))
		if err != nil {
			return err
		}
		if err := r.collectChildAt(childRowPath, appendPathBit(branchEnd, 0), stem, values, &cell.Left, complete); err != nil {
			return err
		}
		return r.collectChildAt(childRowPath, appendPathBit(branchEnd, 1), stem, values, &cell.Right, complete)
	}
	edge := stemPath.Bit(branchEnd.BitLen)
	*complete = false
	childHash := cell.Left
	if edge != 0 {
		childHash = cell.Right
	}
	row := &rowNode{path: path}
	childRowPath, err := rowChildPath(row, slots[from], cell.Prefix, branchSplit(row, slots[from], &rowCell{Cell: cell, Kind: BranchCell}))
	if err != nil {
		return err
	}
	return r.collectChildAt(childRowPath, appendPathBit(branchEnd, edge), stem, values, &childHash, complete)
}

func (r *PBinWitnessResolver) collectChildAt(rowPath, node eip8297.Bitpath, stem []byte, values map[byte][]byte, expected *common.Hash, complete *bool) error {
	key, err := rowKeyForPath(&rowPath)
	if err != nil {
		return err
	}
	record, err := r.readRecord(key)
	if err != nil {
		return err
	}
	if !record.present {
		return fmt.Errorf("pbin witness: row %x is missing", key)
	}
	return r.collectRecord(rowPath, &record.record, node, stem, values, expected, complete)
}

func (r *PBinWitnessResolver) collectRecord(base eip8297.Bitpath, record *Record, node eip8297.Bitpath, stem []byte, values map[byte][]byte, expected *common.Hash, complete *bool) error {
	switch record.Form {
	case LeafRoot:
		cell, ok := singleLeaf(record)
		if !ok {
			return fmt.Errorf("pbin witness: invalid leaf record")
		}
		if bytes.Equal(cell.Key[:len(cell.Key)-1], stem) {
			values[cell.Key[len(cell.Key)-1]] = slices.Clone(cell.Value[:])
		} else {
			*complete = false
		}
		return nil
	case ExtRoot:
		return r.collectExt(base, record, node, stem, values, expected, complete)
	case RowRoot:
		slots := occupiedSlots(record)
		from, to, err := pbinRowRange(base, node, slots)
		if err != nil {
			return err
		}
		key, err := rowKeyForPath(&base)
		if err != nil {
			return err
		}
		return r.collectRange(base, key, record, slots, from, to, node, stem, values, expected, complete)
	default:
		return fmt.Errorf("pbin witness: unknown record form %d", record.Form)
	}
}

func (r *PBinWitnessResolver) collectExt(base eip8297.Bitpath, record *Record, node eip8297.Bitpath, stem []byte, values map[byte][]byte, expected *common.Hash, complete *bool) error {
	absolute := base
	absolute.Append(&record.SelfExt)
	stemPath := eip8297.PathFromBits(stem, int16(len(stem)*8))
	if !stemPath.HasPrefix(&node) || (!stemPath.HasPrefix(&absolute) && !absolute.HasPrefix(&stemPath)) {
		*complete = false
		return nil
	}
	if absolute.HasPrefix(&stemPath) {
		window := absolute
		window.Truncate((absolute.BitLen / 4) * 4)
		if err := r.collectChildAt(window, appendPathBit(absolute, 0), stem, values, &record.Left, complete); err != nil {
			return err
		}
		return r.collectChildAt(window, appendPathBit(absolute, 1), stem, values, &record.Right, complete)
	}
	if stemPath.BitLen == absolute.BitLen {
		window := absolute
		window.Truncate((absolute.BitLen / 4) * 4)
		if err := r.collectChildAt(window, appendPathBit(absolute, 0), stem, values, &record.Left, complete); err != nil {
			return err
		}
		return r.collectChildAt(window, appendPathBit(absolute, 1), stem, values, &record.Right, complete)
	}
	edge := stemPath.Bit(absolute.BitLen)
	*complete = false
	childHash := record.Left
	if edge != 0 {
		childHash = record.Right
	}
	window := absolute
	window.Truncate((absolute.BitLen / 4) * 4)
	return r.collectChildAt(window, appendPathBit(absolute, edge), stem, values, &childHash, complete)
}

func appendPathBit(path eip8297.Bitpath, bit uint64) eip8297.Bitpath {
	path.AppendBit(bit)
	return path
}

func pbinDecodeWitnessPath(path []byte) (eip8297.Bitpath, error) {
	if len(path) == 0 {
		return eip8297.Bitpath{}, nil
	}
	if len(path) < 2 {
		return eip8297.Bitpath{}, fmt.Errorf("pbin witness: path is truncated")
	}
	bitLen := int(binary.BigEndian.Uint16(path[:2]))
	packedLen := (bitLen + 7) / 8
	if bitLen > eip8297.MaxPathBits || len(path) != 2+packedLen {
		return eip8297.Bitpath{}, fmt.Errorf("pbin witness: invalid path length")
	}
	if bitLen%8 != 0 && path[len(path)-1]&((1<<uint(8-bitLen%8))-1) != 0 {
		return eip8297.Bitpath{}, fmt.Errorf("pbin witness: non-canonical path padding")
	}
	return eip8297.PathFromBits(path[2:], int16(bitLen)), nil
}

func pbinValidateWitnessBlob(path, blob []byte) error {
	walk, err := pbinDecodeWitnessPath(path)
	if err != nil {
		return err
	}
	decoded, err := witness.PBinDecodeBlob(blob)
	if err != nil {
		return err
	}
	if decoded.Group != nil && decoded.Group.Position != uint16(walk.BitLen) {
		return fmt.Errorf("pbin witness: group position %d does not match path length %d", decoded.Group.Position, walk.BitLen)
	}
	return nil
}
