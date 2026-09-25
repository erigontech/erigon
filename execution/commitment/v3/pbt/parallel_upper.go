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
	"context"
	"fmt"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type phaseContext struct {
	base    commitment.PatriciaContext
	records map[string][]byte
}

func (c *phaseContext) Branch(key []byte) ([]byte, kv.Step, error) {
	if data, ok := c.records[string(key)]; ok {
		return bytes.Clone(data), 0, nil
	}
	data, step, err := c.base.Branch(key)
	if err == nil {
		c.records[string(key)] = bytes.Clone(data)
	}
	return bytes.Clone(data), step, err
}

func (c *phaseContext) PutBranch(key, data, prev []byte) error {
	current, ok := c.records[string(key)]
	if !ok {
		var err error
		current, _, err = c.base.Branch(key)
		if err != nil {
			return err
		}
	}
	if !bytes.Equal(current, prev) {
		return fmt.Errorf("previous record mismatch for %x", key)
	}
	c.records[string(key)] = bytes.Clone(data)
	return nil
}

func (c *phaseContext) Account(key []byte) (*commitment.Update, error) { return c.base.Account(key) }

func (c *phaseContext) Storage(key []byte) (*commitment.Update, error) { return c.base.Storage(key) }

type phaseBucketResult struct {
	deltas     []commitment.BranchDelta
	descriptor bucketDescriptor
	present    bool
}

type upperItem struct {
	key         []byte
	path        eip8297.Bitpath
	leaf        *Cell
	branch      bool
	left, right common.Hash
}

func (t *Trie) processParallelPhaseA(ctx context.Context, workers int, plan phasePlan, ops []Op) (common.Hash, error) {
	if _, err := t.loadRoot(); err != nil {
		return common.Hash{}, err
	}
	t.roundPrev = make(map[string][]byte)
	t.deltas = nil
	t.bucketDirty = make(map[string][]byte)
	t.rememberPrev(t.rootRecordKey(), t.root.prev)
	oldBuckets, err := t.expectedBucketRecords()
	if err != nil {
		return common.Hash{}, err
	}
	oldItems, err := t.upperItems()
	if err != nil {
		return common.Hash{}, err
	}
	bucketTasks := make([]phaseTask, 0)
	phaseB := make([]Op, 0, len(ops))
	for _, task := range plan.tasks {
		if task.kind == phaseBucket {
			bucketTasks = append(bucketTasks, task)
		}
	}
	for _, op := range ops {
		if len(op.Drop) == 0 && len(op.Key) == eip8297.StorageKeyLength && op.Key[0] == eip8297.StorageZone {
			continue
		}
		phaseB = append(phaseB, op)
	}
	sort.SliceStable(bucketTasks, func(i, j int) bool {
		if len(bucketTasks[i].ops) != len(bucketTasks[j].ops) {
			return len(bucketTasks[i].ops) > len(bucketTasks[j].ops)
		}
		return bytes.Compare([]byte(bucketTasks[i].key), []byte(bucketTasks[j].key)) < 0
	})
	results := make([]phaseBucketResult, len(bucketTasks))
	phasePlan := phasePlan{tasks: bucketTasks}
	phaseWorkers := workers
	factory := t.ctxFactory
	if factory == nil {
		phaseWorkers = 1
	}
	if err := runPhasePlanWithFactory(ctx, phaseWorkers, phasePlan, factory, func(workerCtx context.Context, workerContext commitment.PatriciaContext, task phaseTask) error {
		if err := t.phaseHookCall(task, nil); err != nil {
			return err
		}
		base := workerContext
		if base == nil {
			base = t.ctx
		}
		local := &phaseContext{base: base, records: make(map[string][]byte)}
		bucketTrie, err := newBucketTrie(local, []byte(task.key))
		if err != nil {
			return err
		}
		for i := range task.ops {
			if err := t.phaseHookCall(task, &task.ops[i]); err != nil {
				return err
			}
		}
		if _, err := bucketTrie.Process(task.ops); err != nil {
			return fmt.Errorf("process bucket %x: %w", []byte(task.key), err)
		}
		descriptor, present, err := bucketTrie.descriptorFromRoot()
		if err != nil {
			return err
		}
		index := 0
		for i := range bucketTasks {
			if bucketTasks[i].key == task.key {
				index = i
				break
			}
		}
		results[index] = phaseBucketResult{deltas: bucketTrie.TakeDeltas(), descriptor: descriptor, present: present}
		return workerCtx.Err()
	}); err != nil {
		return common.Hash{}, err
	}
	changedBuckets := make(map[string]phaseBucketResult, len(results))
	for i := range results {
		result := &results[i]
		key := bucketTasks[i].key
		changedBuckets[key] = *result
		for _, delta := range result.deltas {
			if err := t.ctx.PutBranch(delta.Key, delta.Data, delta.Prev); err != nil {
				return common.Hash{}, fmt.Errorf("apply phase A delta %x: %w", delta.Key, err)
			}
			t.rememberPrev(delta.Key, delta.Prev)
			t.addDelta(delta.Key, delta.Data, delta.Prev)
		}
	}
	for i := range phaseB {
		if err := t.phaseHookCall(phaseTask{kind: phaseChain, owner: ownerChain}, &phaseB[i]); err != nil {
			return common.Hash{}, err
		}
	}
	items, err := t.phaseBItems(oldItems, oldBuckets, changedBuckets, phaseB)
	if err != nil {
		return common.Hash{}, err
	}
	if err := t.rebuildUpper(items); err != nil {
		return common.Hash{}, err
	}
	return t.rootHash()
}

func (t *Trie) phaseHookCall(task phaseTask, op *Op) error {
	if t.phaseHook == nil {
		return nil
	}
	return t.phaseHook(task, op)
}

func (t *Trie) descriptorFromRoot() (bucketDescriptor, bool, error) {
	if _, err := t.loadRoot(); err != nil {
		return bucketDescriptor{}, false, err
	}
	switch t.root.form {
	case RowRoot:
		if t.root.row == nil {
			return bucketDescriptor{}, false, nil
		}
		return bucketDescriptor{form: RowRoot, row: t.root.row}, true, nil
	case LeafRoot:
		return bucketDescriptor{form: LeafRoot, leaf: t.root.leaf}, true, nil
	case ExtRoot:
		return bucketDescriptor{form: ExtRoot, self: t.root.self.Slice(t.rootPath.BitLen, t.root.self.BitLen), left: t.root.left, right: t.root.right}, true, nil
	default:
		return bucketDescriptor{}, false, fmt.Errorf("unknown root form %d", t.root.form)
	}
}

func (t *Trie) phaseBItems(oldItems []upperItem, oldBuckets map[string]bucketDescriptor, changed map[string]phaseBucketResult, ops []Op) ([]upperItem, error) {
	items := make([]upperItem, 0, len(oldItems)+len(oldBuckets))
	items = append(items, oldItems...)
	for key := range oldBuckets {
		descriptor := oldBuckets[key]
		if result, ok := changed[key]; ok {
			if !result.present {
				continue
			}
			descriptor = result.descriptor
		}
		item, err := upperItemForDescriptor([]byte(key), descriptor)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	for key := range changed {
		result := changed[key]
		if !result.present {
			continue
		}
		if _, ok := oldBuckets[key]; ok {
			continue
		}
		item, err := upperItemForDescriptor([]byte(key), result.descriptor)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	for _, op := range ops {
		index := -1
		for i := range items {
			if !items[i].branch && bytes.Equal(items[i].key, op.Key) {
				index = i
				break
			}
		}
		if op.Value == ([eip8297.ValueLength]byte{}) {
			if index >= 0 {
				items = append(items[:index], items[index+1:]...)
			}
			continue
		}
		path, err := keyPath(op.Key)
		if err != nil {
			return nil, err
		}
		cell := leafCell(op.Key, op.Value).Cell
		item := upperItem{key: bytes.Clone(op.Key), path: path, leaf: &cell}
		if index >= 0 {
			items[index] = item
		} else {
			items = append(items, item)
		}
	}
	return items, nil
}

func upperItemForDescriptor(key []byte, descriptor bucketDescriptor) (upperItem, error) {
	path, err := bucketPathForKey(key)
	if err != nil {
		return upperItem{}, err
	}
	switch descriptor.form {
	case LeafRoot:
		leafPath, err := keyPath(descriptor.leaf.Key)
		if err != nil {
			return upperItem{}, err
		}
		leaf := descriptor.leaf
		return upperItem{key: bytes.Clone(leaf.Key), path: leafPath, leaf: &leaf}, nil
	case RowRoot:
		result, err := rowFoldResult(descriptor.row)
		if err != nil {
			return upperItem{}, err
		}
		full, err := rowTopPrefix(descriptor.row, result.Split)
		if err != nil {
			return upperItem{}, err
		}
		return upperItem{key: bytes.Clone(key), path: full, branch: true, left: result.Left, right: result.Right}, nil
	case ExtRoot:
		full := path
		full.Append(&descriptor.self)
		return upperItem{key: bytes.Clone(key), path: full, branch: true, left: descriptor.left, right: descriptor.right}, nil
	default:
		return upperItem{}, fmt.Errorf("unknown bucket form %d", descriptor.form)
	}
}

func (t *Trie) upperItems() ([]upperItem, error) {
	items := make([]upperItem, 0)
	var visitRow func(*rowNode) error
	visitRow = func(row *rowNode) error {
		for slot := range row.cells {
			cell := row.cell(slot)
			switch cell.Kind {
			case LeafCell:
				if cell.Key[0] == eip8297.StorageZone {
					continue
				}
				path, err := keyPath(cell.Key)
				if err != nil {
					return err
				}
				leaf := cell.Cell
				items = append(items, upperItem{key: bytes.Clone(cell.Key), path: path, leaf: &leaf})
			case BranchCell:
				full := branchPath(row, slot, cell)
				if pathByte(&full, 0) == eip8297.StorageZone {
					continue
				}
				child, err := t.loadBranchChild(row, slot)
				if err != nil {
					return err
				}
				if err := visitRow(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	switch t.root.form {
	case LeafRoot:
		if t.root.leaf.Key[0] != eip8297.StorageZone {
			path, err := keyPath(t.root.leaf.Key)
			if err != nil {
				return nil, err
			}
			leaf := t.root.leaf
			items = append(items, upperItem{key: bytes.Clone(leaf.Key), path: path, leaf: &leaf})
		}
	case ExtRoot:
		row, err := t.extTopRow(t.root)
		if err != nil {
			return nil, err
		}
		if err := visitRow(row); err != nil {
			return nil, err
		}
	case RowRoot:
		if t.root.row != nil {
			if err := visitRow(t.root.row); err != nil {
				return nil, err
			}
		}
	}
	return items, nil
}

func (t *Trie) rebuildUpper(items []upperItem) error {
	oldRows := make(map[string][]byte)
	for key, row := range t.rows {
		if row.path.BitLen < 264 {
			oldRows[key] = bytes.Clone(row.raw)
		}
	}
	oldRoot := t.root
	newRoot, newRows, err := buildUpperRoot(items, oldRoot.raw)
	if err != nil {
		return err
	}
	final := make(map[string][]byte, len(newRows)+1)
	prev := make(map[string][]byte, len(newRows)+1)
	for key, row := range newRows {
		data, err := EncodeRecord(row.key, recordPointer(row))
		if err != nil {
			return err
		}
		final[key] = data
		prev[key] = oldRows[key]
	}
	for key := range oldRows {
		if _, ok := final[key]; !ok {
			final[key] = nil
			prev[key] = oldRows[key]
		}
	}
	rootKey := t.rootRecordKey()
	rootData := []byte(nil)
	if newRoot.form != RowRoot || newRoot.row != nil {
		rootRecord := rootRecordForTree(newRoot, t.rootRecordPath())
		rootData, err = EncodeRecord(rootKey, &rootRecord)
		if err != nil {
			return err
		}
	}
	final[string(rootKey)] = rootData
	prev[string(rootKey)] = oldRoot.raw
	keys := make([]string, 0, len(final))
	for key := range final {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if bytes.Equal(final[key], prev[key]) {
			continue
		}
		if err := t.ctx.PutBranch([]byte(key), final[key], prev[key]); err != nil {
			return fmt.Errorf("write upper record %x: %w", []byte(key), err)
		}
		t.addDelta([]byte(key), final[key], prev[key])
	}
	for key, row := range newRows {
		row.raw = bytes.Clone(final[key])
		row.prev = bytes.Clone(final[key])
		row.dirty = false
	}
	newRoot.raw = bytes.Clone(rootData)
	newRoot.prev = bytes.Clone(rootData)
	t.root = newRoot
	t.rootLoaded = true
	t.rows = newRows
	t.dirtyRows = make(map[string]*rowNode)
	t.rootDirty = false
	t.roundPrev = nil
	return nil
}

func rootRecordForTree(root *treeRoot, rootPath eip8297.Bitpath) Record {
	switch root.form {
	case LeafRoot:
		return Record{Form: LeafRoot, Cells: [maxCells]Cell{0: root.leaf}}
	case ExtRoot:
		self := root.self
		if self.BitLen > rootPath.BitLen {
			self = self.Slice(rootPath.BitLen, self.BitLen)
		}
		return Record{Form: ExtRoot, SelfExt: self, Left: root.left, Right: root.right}
	case RowRoot:
		if root.row == nil {
			return Record{}
		}
		return root.row.record()
	default:
		return Record{}
	}
}

func buildUpperRoot(items []upperItem, oldRoot []byte) (*treeRoot, map[string]*rowNode, error) {
	root := &treeRoot{raw: bytes.Clone(oldRoot), prev: bytes.Clone(oldRoot)}
	rows := make(map[string]*rowNode)
	if len(items) == 0 {
		return root, rows, nil
	}
	sort.Slice(items, func(i, j int) bool {
		return bytes.Compare(items[i].path.AppendPackedBits(nil), items[j].path.AppendPackedBits(nil)) < 0
	})
	if len(items) == 1 {
		item := items[0]
		if item.branch {
			root.form, root.self, root.left, root.right = ExtRoot, item.path, item.left, item.right
			return root, rows, nil
		}
		root.form, root.leaf = LeafRoot, *item.leaf
		return root, rows, nil
	}
	split := firstDifference(&items[0].path, &items[len(items)-1].path)
	window := (split / 4) * 4
	rowPath := items[0].path.Slice(0, window)
	row, err := buildUpperRow(items, rowPath, rows)
	if err != nil {
		return nil, nil, err
	}
	result, err := rowFoldResult(row)
	if err != nil {
		return nil, nil, err
	}
	if window == 0 {
		root.form, root.row = RowRoot, row
		return root, rows, nil
	}
	self, err := rowTopPrefix(row, result.Split)
	if err != nil {
		return nil, nil, err
	}
	root.form, root.self, root.left, root.right, root.topRow = ExtRoot, self, result.Left, result.Right, row
	return root, rows, nil
}

func buildUpperRow(items []upperItem, path eip8297.Bitpath, rows map[string]*rowNode) (*rowNode, error) {
	rowKey, err := rowKeyForPath(&path)
	if err != nil {
		return nil, err
	}
	row := newRow(path, rowKey, nil)
	for i := 0; i < len(items); {
		slot := int(items[i].path.Bit(path.BitLen)*8 + items[i].path.Bit(path.BitLen+1)*4 + items[i].path.Bit(path.BitLen+2)*2 + items[i].path.Bit(path.BitLen+3))
		j := i + 1
		for j < len(items) && int(items[j].path.Bit(path.BitLen)*8+items[j].path.Bit(path.BitLen+1)*4+items[j].path.Bit(path.BitLen+2)*2+items[j].path.Bit(path.BitLen+3)) == slot {
			j++
		}
		group := items[i:j]
		if len(group) == 1 {
			item := group[0]
			if item.branch {
				row.cells[slot] = branchCell(item.path.Slice(path.BitLen+4, item.path.BitLen), item.left, item.right)
			} else {
				row.cells[slot] = leafCell(item.leaf.Key, item.leaf.Value)
			}
		} else {
			split := firstDifference(&group[0].path, &group[len(group)-1].path)
			window := (split / 4) * 4
			childPath := group[0].path.Slice(0, window)
			child, err := buildUpperRow(group, childPath, rows)
			if err != nil {
				return nil, err
			}
			result, err := rowFoldResult(child)
			if err != nil {
				return nil, err
			}
			full, err := rowTopPrefix(child, result.Split)
			if err != nil {
				return nil, err
			}
			row.cells[slot] = branchCell(full.Slice(path.BitLen+4, result.Split), result.Left, result.Right)
			row.cells[slot].child = child
			child.parent = row
			child.parentSlot = slot
		}
		i = j
	}
	row.markDirty()
	rows[string(row.key)] = row
	return row, nil
}
