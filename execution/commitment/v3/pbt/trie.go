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
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type Op struct {
	Key   []byte
	Drop  []byte
	Value [eip8297.ValueLength]byte
}

func Drop(prefix []byte) Op { return Op{Drop: bytes.Clone(prefix)} }

type Trie struct {
	ctx        commitment.PatriciaContext
	root       *treeRoot
	rootLoaded bool
	rootDirty  bool
	rows       map[string]*rowNode
	dirtyRows  map[string]*rowNode
	deltas     []commitment.BranchDelta
}

func NewTrie(ctx commitment.PatriciaContext) *Trie {
	return &Trie{ctx: ctx, rows: make(map[string]*rowNode), dirtyRows: make(map[string]*rowNode)}
}

func (t *Trie) ResetContext(ctx commitment.PatriciaContext) {
	t.ctx = ctx
	t.root = nil
	t.rootLoaded = false
	t.rootDirty = false
	t.rows = make(map[string]*rowNode)
	t.dirtyRows = make(map[string]*rowNode)
	t.deltas = nil
}

func (t *Trie) Process(ops []Op) (common.Hash, error) {
	if t.ctx == nil {
		return common.Hash{}, fmt.Errorf("nil Patricia context")
	}
	if _, err := t.loadRoot(); err != nil {
		return common.Hash{}, err
	}
	t.deltas = nil
	ordered := make([]Op, len(ops))
	copy(ordered, ops)
	sort.SliceStable(ordered, func(i, j int) bool {
		key := func(op Op) []byte {
			switch {
			case len(op.Drop) != 0:
				return op.Drop
			default:
				return op.Key
			}
		}
		return bytes.Compare(key(ordered[i]), key(ordered[j])) < 0
	})
	for i := range ordered {
		var err error
		switch {
		case len(ordered[i].Drop) != 0:
			err = t.dropPrefix(ordered[i].Drop)
		case ordered[i].Value == ([eip8297.ValueLength]byte{}):
			err = t.remove(ordered[i].Key)
		default:
			err = t.insert(ordered[i].Key, ordered[i].Value)
		}
		if err != nil {
			return common.Hash{}, err
		}
		t.rootDirty = true
	}
	if err := t.normalize(); err != nil {
		return common.Hash{}, err
	}
	if err := t.write(); err != nil {
		return common.Hash{}, err
	}
	return t.rootHash()
}

func (t *Trie) RootHash() (common.Hash, error) {
	if t.ctx == nil {
		return common.Hash{}, fmt.Errorf("nil Patricia context")
	}
	if _, err := t.loadRoot(); err != nil {
		return common.Hash{}, err
	}
	return t.rootHash()
}

func (t *Trie) rootHash() (common.Hash, error) {
	if t.root == nil || t.root.row == nil && t.root.form == RowRoot {
		return eip8297.EmptyTreeHash, nil
	}
	switch t.root.form {
	case LeafRoot:
		return leafHash(&t.root.leaf), nil
	case ExtRoot:
		return branchHash(&t.root.self, &t.root.left, &t.root.right), nil
	case RowRoot:
		record := t.root.row.record()
		return Fold(GlobalRootKey(), &record)
	default:
		return common.Hash{}, fmt.Errorf("unknown root form %d", t.root.form)
	}
}

func (t *Trie) write() error {
	final := make(map[string][]byte, len(t.dirtyRows)+1)
	prev := make(map[string][]byte, len(t.dirtyRows)+1)
	rows := make(map[string]*rowNode, len(t.dirtyRows))
	for key, row := range t.dirtyRows {
		rows[key] = row
		prev[key] = bytes.Clone(row.prev)
		if row.tombstone {
			final[key] = nil
			continue
		}
		record := row.record()
		data, err := EncodeRecord(row.key, &record)
		if err != nil {
			return err
		}
		final[key] = data
	}
	rootKey := string(GlobalRootKey())
	if t.rootDirty && (t.root.form != RowRoot || t.root.row == nil) {
		var data []byte
		if t.root.form != RowRoot || t.root.row != nil {
			record := t.rootRecord()
			var err error
			data, err = EncodeRecord(GlobalRootKey(), &record)
			if err != nil {
				return err
			}
		}
		final[rootKey] = data
		prev[rootKey] = bytes.Clone(t.root.prev)
	}
	keys := make([]string, 0, len(final))
	for key := range final {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		data, old := final[key], prev[key]
		if bytes.Equal(data, old) {
			continue
		}
		if err := t.ctx.PutBranch([]byte(key), data, old); err != nil {
			return err
		}
		t.addDelta([]byte(key), data, old)
	}
	for key, row := range rows {
		data := final[key]
		row.raw = bytes.Clone(data)
		row.prev = bytes.Clone(data)
		row.dirty = false
		row.tombstone = len(data) == 0
	}
	if data, ok := final[rootKey]; ok {
		t.root.raw = bytes.Clone(data)
		t.root.prev = bytes.Clone(data)
	}
	t.dirtyRows = make(map[string]*rowNode)
	t.rootDirty = false
	return nil
}

func (t *Trie) rootRecord() Record {
	switch t.root.form {
	case LeafRoot:
		return Record{Form: LeafRoot, Cells: [maxCells]Cell{0: t.root.leaf}}
	case ExtRoot:
		return Record{Form: ExtRoot, SelfExt: t.root.self, Left: t.root.left, Right: t.root.right}
	case RowRoot:
		return t.root.row.record()
	default:
		return Record{}
	}
}

func (t *Trie) registerRow(row *rowNode) {
	if row.key == nil {
		key, err := rowKeyForPath(&row.path)
		if err != nil {
			return
		}
		row.key = key
	}
	t.rows[string(row.key)] = row
	if row.dirty {
		t.dirtyRows[string(row.key)] = row
	}
}

func (t *Trie) markDirty(row *rowNode) {
	for row != nil {
		row.markDirty()
		t.registerRow(row)
		row = row.parent
	}
}
