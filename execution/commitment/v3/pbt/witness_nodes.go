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
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/witness"
)

type PBinWitnessResolver struct {
	read     func([]byte) ([]byte, error)
	nodes    map[string][]byte
	root     common.Hash
	loaded   bool
	loadErr  error
	rowsSeen map[string]struct{}
}

type pbinParentBranchContext interface {
	ParentBranch([]byte) ([]byte, kv.Step, error)
}

type pbinResolverEntry struct {
	key   []byte
	value []byte
}

func NewPBinWitnessResolver(ctx commitment.PatriciaContext) *PBinWitnessResolver {
	resolver := &PBinWitnessResolver{nodes: make(map[string][]byte), rowsSeen: make(map[string]struct{})}
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
	if err := r.load(); err != nil {
		return nil, err
	}
	walk, err := pbinDecodeWitnessPath(path)
	if err != nil {
		return nil, err
	}
	blob, ok := r.nodes[string(witness.PBinPath(&walk))]
	if !ok {
		return nil, nil
	}
	if err := pbinValidateWitnessBlob(path, blob); err != nil {
		return nil, err
	}
	return slices.Clone(blob), nil
}

func (r *PBinWitnessResolver) RootHash() (common.Hash, error) {
	if err := r.load(); err != nil {
		return common.Hash{}, err
	}
	return r.root, nil
}

func (r *PBinWitnessResolver) load() error {
	if r.loaded {
		return r.loadErr
	}
	r.loaded = true
	if r.read == nil {
		r.loadErr = fmt.Errorf("pbin witness: nil Patricia context")
		return r.loadErr
	}
	data, err := r.read(GlobalRootKey())
	if err != nil {
		r.loadErr = err
		return err
	}
	if len(data) == 0 {
		return nil
	}
	record, err := DecodeRecord(GlobalRootKey(), data)
	if err != nil {
		r.loadErr = err
		return err
	}
	entries := make(map[string]pbinResolverEntry)
	var rootPath eip8297.Bitpath
	if err := r.collectRecord(rootPath, GlobalRootKey(), &record, entries); err != nil {
		r.loadErr = err
		return err
	}
	ordered := make([]pbinResolverEntry, 0, len(entries))
	for _, entry := range entries {
		ordered = append(ordered, entry)
	}
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i].key, ordered[j].key) < 0 })
	if len(ordered) == 0 {
		return nil
	}
	var walk eip8297.Bitpath
	node := r.buildNode(ordered, walk)
	if node.err != nil {
		r.loadErr = node.err
		return node.err
	}
	r.root = node.hash
	return nil
}

func (r *PBinWitnessResolver) collectRecord(path eip8297.Bitpath, key []byte, record *Record, entries map[string]pbinResolverEntry) error {
	switch record.Form {
	case LeafRoot:
		return r.addLeaf(record.Cells[0], entries)
	case RowRoot:
		return r.collectRow(path, record, entries)
	case ExtRoot:
		fullPath := path
		fullPath.Append(&record.SelfExt)
		window := (fullPath.BitLen / 4) * 4
		rowPath := fullPath
		rowPath.Truncate(window)
		return r.collectRecordAt(rowPath, entries)
	default:
		return fmt.Errorf("pbin witness: unknown root form %d at %x", record.Form, key)
	}
}

func (r *PBinWitnessResolver) collectRecordAt(path eip8297.Bitpath, entries map[string]pbinResolverEntry) error {
	key, err := rowKeyForPath(&path)
	if err != nil {
		return err
	}
	rowKey := string(key)
	if _, ok := r.rowsSeen[rowKey]; ok {
		return nil
	}
	r.rowsSeen[rowKey] = struct{}{}
	data, err := r.read(key)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("pbin witness: row %x is missing", key)
	}
	record, err := DecodeRecord(key, data)
	if err != nil {
		return err
	}
	return r.collectRecord(path, key, &record, entries)
}

func (r *PBinWitnessResolver) collectRow(path eip8297.Bitpath, record *Record, entries map[string]pbinResolverEntry) error {
	if len(occupiedSlots(record)) >= 2 {
		key, err := rowKeyForPath(&path)
		if err != nil {
			return err
		}
		if _, err := FoldRow(key, record); err != nil {
			return err
		}
	}
	row := &rowNode{path: path}
	for slot := range record.Cells {
		cell := &record.Cells[slot]
		switch cell.Kind {
		case EmptyCell:
			continue
		case LeafCell:
			if err := r.addLeaf(*cell, entries); err != nil {
				return err
			}
		case BranchCell:
			split := branchSplit(row, slot, &rowCell{Cell: cell, Kind: BranchCell})
			childPath, err := rowChildPath(row, slot, cell.Prefix, split)
			if err != nil {
				return err
			}
			if err := r.collectRecordAt(childPath, entries); err != nil {
				return err
			}
		default:
			return fmt.Errorf("pbin witness: unknown row cell kind %d", cell.Kind)
		}
	}
	return nil
}

func (r *PBinWitnessResolver) addLeaf(cell Cell, entries map[string]pbinResolverEntry) error {
	if len(cell.Key) == 0 {
		return fmt.Errorf("pbin witness: leaf has an empty key")
	}
	key := string(cell.Key)
	if previous, ok := entries[key]; ok && !bytes.Equal(previous.value, cell.Value[:]) {
		return fmt.Errorf("pbin witness: leaf %x has conflicting values", cell.Key)
	}
	entries[key] = pbinResolverEntry{key: bytes.Clone(cell.Key), value: slices.Clone(cell.Value[:])}
	return nil
}

func (r *PBinWitnessResolver) buildNode(entries []pbinResolverEntry, walk eip8297.Bitpath) pbinResolverNode {
	if len(entries) == 1 {
		blob, err := witness.PBinEncodeLeaf(entries[0].key, entries[0].value)
		if err != nil {
			return pbinResolverNode{err: err}
		}
		return r.storeNode(walk, blob)
	}
	stem := entries[0].key[:len(entries[0].key)-1]
	allStem := true
	for _, entry := range entries[1:] {
		if !bytes.Equal(stem, entry.key[:len(entry.key)-1]) {
			allStem = false
			break
		}
	}
	if allStem {
		group := witness.PBinGroup{Position: uint16(walk.BitLen), Stem: bytes.Clone(stem), Subs: make([]byte, len(entries)), Values: make([][]byte, len(entries))}
		for i, entry := range entries {
			group.Subs[i] = entry.key[len(entry.key)-1]
			group.Values[i] = slices.Clone(entry.value)
		}
		blob, err := witness.PBinEncodeGroup(group)
		if err != nil {
			return pbinResolverNode{err: err}
		}
		return r.storeNode(walk, blob)
	}
	firstPath := eip8297.PathFromBytes(entries[0].key)
	lastPath := eip8297.PathFromBytes(entries[len(entries)-1].key)
	divergence := walk.BitLen
	for divergence < firstPath.BitLen && divergence < lastPath.BitLen && firstPath.Bit(divergence) == lastPath.Bit(divergence) {
		divergence++
	}
	prefix := firstPath.Slice(walk.BitLen, divergence)
	leftEntries := make([]pbinResolverEntry, 0, len(entries))
	rightEntries := make([]pbinResolverEntry, 0, len(entries))
	for _, entry := range entries {
		path := eip8297.PathFromBytes(entry.key)
		if path.Bit(divergence) == 0 {
			leftEntries = append(leftEntries, entry)
		} else {
			rightEntries = append(rightEntries, entry)
		}
	}
	leftWalk := walk
	leftWalk.Append(&prefix)
	leftWalk.AppendBit(0)
	rightWalk := walk
	rightWalk.Append(&prefix)
	rightWalk.AppendBit(1)
	left := r.buildNode(leftEntries, leftWalk)
	if left.err != nil {
		return left
	}
	right := r.buildNode(rightEntries, rightWalk)
	if right.err != nil {
		return right
	}
	blob, err := witness.PBinEncodeBranch(&prefix, &left.hash, &right.hash)
	if err != nil {
		return pbinResolverNode{err: err}
	}
	return r.storeNode(walk, blob)
}

type pbinResolverNode struct {
	blob []byte
	hash common.Hash
	err  error
}

func (r *PBinWitnessResolver) storeNode(walk eip8297.Bitpath, blob []byte) pbinResolverNode {
	hash, err := witness.PBinHashBlob(blob)
	if err != nil {
		return pbinResolverNode{err: err}
	}
	clone := slices.Clone(blob)
	r.nodes[string(witness.PBinPath(&walk))] = clone
	return pbinResolverNode{blob: clone, hash: hash}
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
