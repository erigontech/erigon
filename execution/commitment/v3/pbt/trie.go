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
	"maps"
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type Op struct {
	Key   []byte
	Drop  []byte
	Value [eip8297.ValueLength]byte
	merge *feedMerge
}

var errOperationOrder = fmt.Errorf("operation list must be sorted by key with at most one operation per key; drops must precede writes under their prefix")

func Drop(prefix []byte) Op { return Op{Drop: bytes.Clone(prefix)} }

func MergeGroupKey(op Op) []byte {
	if op.merge == nil {
		return nil
	}
	key := bytes.Clone(op.Key)
	key[len(key)-1] = eip8297.BasicDataLeafKey
	return key
}

type Trie struct {
	ctx                    commitment.PatriciaContext
	ctxFactory             commitment.TrieContextFactory
	phaseBase              commitment.PatriciaContext
	phaseReadMu            *sync.Mutex
	phaseHook              func(phaseTask, *Op) error
	coreApplyHook          func(phaseTask, *Op) error
	coreEncodeHook         func(phaseTask, []byte) error
	coreActivityHook       func(bool)
	foldHook               func([]byte)
	coreTask               *phaseTask
	ownedPrefix            *eip8297.Bitpath
	suppressRoot           bool
	suppressBucketRecords  bool
	rootKey                []byte
	rootPath               eip8297.Bitpath
	bucketMode             bool
	upperOnly              bool
	upperStops             []eip8297.Bitpath
	root                   *treeRoot
	rootLoaded             bool
	verifyOnly             bool
	roundPending           bool
	rootDirty              bool
	foldedRoot             common.Hash
	foldedRootReady        bool
	rows                   map[string]*rowNode
	dirtyRows              map[string]*rowNode
	routingRows            []*rowNode
	rowArena               arena[rowNode]
	cellArena              arena[Cell]
	bucketDirty            map[string][]byte
	scheduledBucketRecords map[string][]byte
	deltas                 []commitment.BranchDelta
	roundPrev              map[string][]byte
	originalLeafSeen       map[string]struct{}
	originalLeaves         map[string]*Cell
	droppedLeafKeys        map[string]struct{}
	mergeCreatedStems      map[string]struct{}
	verifiedBucketKeys     map[string]struct{}
}

func initTrieMaps(t *Trie) {
	t.rows = make(map[string]*rowNode)
	t.dirtyRows = make(map[string]*rowNode)
	t.bucketDirty = make(map[string][]byte)
	t.mergeCreatedStems = make(map[string]struct{})
}

func resetMap[K comparable, V any](m *map[K]V) {
	if *m == nil {
		*m = make(map[K]V)
		return
	}
	clear(*m)
}

func NewTrie(ctx commitment.PatriciaContext) *Trie {
	t := &Trie{ctx: ctx}
	initTrieMaps(t)
	return t
}

func (t *Trie) Reset() {
	t.mergeCreatedStems = make(map[string]struct{})
	t.ResetContext(t.ctx)
}

func newBucketTrie(ctx commitment.PatriciaContext, key []byte) (*Trie, error) {
	path, err := bucketPathForKey(key)
	if err != nil {
		return nil, err
	}
	t := &Trie{
		ctx:        ctx,
		rootKey:    bytes.Clone(key),
		rootPath:   path,
		bucketMode: true,
	}
	initTrieMaps(t)
	return t, nil
}

func newSubtreeTrie(ctx commitment.PatriciaContext, prefix eip8297.Bitpath, descriptor bucketDescriptor, present bool) (*Trie, error) {
	key, err := rowKeyForPath(&prefix)
	if err != nil {
		return nil, err
	}
	t := &Trie{
		ctx:                   ctx,
		rootKey:               key,
		rootPath:              prefix,
		ownedPrefix:           &prefix,
		suppressRoot:          true,
		suppressBucketRecords: true,
		rootLoaded:            true,
		root:                  &treeRoot{form: RowRoot},
	}
	initTrieMaps(t)
	if !present {
		return t, nil
	}
	switch descriptor.form {
	case LeafRoot:
		t.root.form = LeafRoot
		t.root.leaf = descriptor.leaf
	case ExtRoot:
		t.root.form = ExtRoot
		t.root.self = prefix
		t.root.self.Append(&descriptor.self)
		t.root.left = descriptor.left
		t.root.right = descriptor.right
	case RowRoot:
		if descriptor.row == nil {
			return nil, fmt.Errorf("subtree descriptor has no row")
		}
		record := descriptor.row.record()
		row := t.rowFromRecord(prefix, key, descriptor.row.raw, &record)
		t.root.form = RowRoot
		t.root.row = row
		t.root.raw = bytes.Clone(descriptor.row.raw)
		t.root.prev = bytes.Clone(descriptor.row.raw)
		t.rows[string(key)] = row
	default:
		return nil, fmt.Errorf("unknown subtree descriptor form %d", descriptor.form)
	}
	return t, nil
}

func (t *Trie) SetTrieContextFactory(factory commitment.TrieContextFactory) { t.ctxFactory = factory }

func (t *Trie) SetPhaseHook(hook func(phaseTask, *Op) error) { t.phaseHook = hook }

func (t *Trie) SetCoreHooks(apply func(phaseTask, *Op) error, encode func(phaseTask, []byte) error) {
	t.coreApplyHook = apply
	t.coreEncodeHook = encode
}

func (t *Trie) SetCoreActivityHook(hook func(bool)) { t.coreActivityHook = hook }

func (t *Trie) SetFoldHook(hook func([]byte)) { t.foldHook = hook }

func (t *Trie) foldRowResult(row *rowNode) (FoldResult, error) {
	if t.foldHook != nil {
		t.foldHook(row.key)
	}
	if foldRefHook != nil {
		foldRefHook(row.refs != nil)
	}
	record := row.record()
	node, err := foldRowWithRefs(row.path, &record, row)
	if err != nil {
		return FoldResult{}, err
	}
	return FoldResult{Split: node.split, Left: node.left, Right: node.right}, nil
}

func (t *Trie) ResetContext(ctx commitment.PatriciaContext) {
	t.ctx = ctx
	t.phaseBase = nil
	t.root = nil
	t.rootLoaded = false
	t.roundPending = false
	t.rootDirty = false
	t.foldedRoot = common.Hash{}
	t.foldedRootReady = false
	resetMap(&t.rows)
	resetMap(&t.dirtyRows)
	t.routingRows = t.routingRows[:0]
	t.rowArena.reset()
	t.cellArena.reset()
	resetMap(&t.bucketDirty)
	t.scheduledBucketRecords = nil
	t.deltas = nil
	t.roundPrev = nil
	t.originalLeafSeen = nil
	t.originalLeaves = nil
	t.droppedLeafKeys = nil
	if t.mergeCreatedStems == nil {
		t.mergeCreatedStems = make(map[string]struct{})
	}
}

func (t *Trie) rootRecordKey() []byte {
	if t.bucketMode {
		return t.rootKey
	}
	return GlobalRootKey()
}

func (t *Trie) rootRecordPath() eip8297.Bitpath {
	if t.bucketMode {
		return t.rootPath
	}
	return eip8297.Bitpath{}
}

func (t *Trie) Process(ops []Op) (common.Hash, error) {
	if t.ctx == nil {
		return common.Hash{}, fmt.Errorf("nil Patricia context")
	}
	if t.roundPending {
		t.ResetContext(t.ctx)
	}
	if err := validateOps(ops); err != nil {
		return common.Hash{}, err
	}
	t.roundPending = true
	resetMap(&t.roundPrev)
	resetMap(&t.originalLeafSeen)
	resetMap(&t.originalLeaves)
	resetMap(&t.droppedLeafKeys)
	resetMap(&t.bucketDirty)
	t.foldedRoot = common.Hash{}
	t.foldedRootReady = false
	if _, err := t.loadRoot(); err != nil {
		return common.Hash{}, err
	}
	t.rememberPrev(t.rootRecordKey(), t.root.prev)
	t.deltas = t.deltas[:0]
	for i := range ops {
		op := ops[i]
		op.Key = bytes.Clone(op.Key)
		if err := t.coreApply(&op); err != nil {
			return common.Hash{}, err
		}
		var err error
		switch {
		case len(op.Drop) != 0:
			if len(op.Drop) != 33 || (op.Drop[0] != eip8297.AccountZone && op.Drop[0] != eip8297.StorageZone) {
				err = errInsertKey
			} else if op.Drop[0] == eip8297.StorageZone {
				var bucketKey []byte
				bucketKey, err = bucketKeyForPrefix(op.Drop)
				if err == nil {
					t.touchBucket(bucketKey)
				}
			}
			if err != nil {
				err = errInsertKey
				break
			}
			err = t.dropPrefix(op.Drop)
		case op.merge != nil:
			err = t.applyMerge(op)
		case op.Value == ([eip8297.ValueLength]byte{}):
			if len(op.Key) == eip8297.StorageKeyLength && op.Key[0] == eip8297.StorageZone {
				var bucketKey []byte
				bucketKey, err = bucketKeyForStorage(op.Key)
				if err == nil {
					t.touchBucket(bucketKey)
				}
			}
			if err != nil {
				break
			}
			err = t.remove(op.Key)
		default:
			if len(op.Key) == eip8297.StorageKeyLength && op.Key[0] == eip8297.StorageZone {
				var bucketKey []byte
				bucketKey, err = bucketKeyForStorage(op.Key)
				if err == nil {
					t.touchBucket(bucketKey)
				}
			}
			if err != nil {
				break
			}
			err = t.insert(op.Key, op.Value)
		}
		if err != nil {
			return common.Hash{}, err
		}
		if err := t.refreshRouting(); err != nil {
			return common.Hash{}, err
		}
		t.rootDirty = true
	}
	if err := t.normalize(); err != nil {
		return common.Hash{}, err
	}
	if err := t.foldDirtyRows(); err != nil {
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

func (t *Trie) Release() { t.ctx = nil }

func (t *Trie) rootHash() (common.Hash, error) {
	if t.foldedRootReady {
		return t.foldedRoot, nil
	}
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
		return Fold(t.rootRecordKey(), &record)
	default:
		return common.Hash{}, fmt.Errorf("unknown root form %d", t.root.form)
	}
}

func (t *Trie) write() error {
	final := make(map[string][]byte, len(t.dirtyRows)+len(t.bucketDirty)+1)
	prev := make(map[string][]byte, len(t.dirtyRows)+len(t.bucketDirty)+1)
	rows := make(map[string]*rowNode, len(t.dirtyRows))
	for key, row := range t.dirtyRows {
		if !t.ownsRecordKey(row.key) {
			continue
		}
		rows[key] = row
		prev[key] = t.previousRecord([]byte(key), row.prev)
		if row.tombstone {
			if err := t.coreEncode(row.key); err != nil {
				return err
			}
			final[key] = []byte{}
			continue
		}
		record := row.record()
		if err := t.coreEncode(row.key); err != nil {
			return err
		}
		data, err := EncodeRecord(row.key, &record)
		if err != nil {
			return err
		}
		final[key] = data
	}
	rootKeyBytes := t.rootRecordKey()
	rootKey := string(rootKeyBytes)
	if !t.suppressRoot && t.rootDirty && t.ownsRecordKey(rootKeyBytes) && (t.root.form != RowRoot || t.root.row == nil) {
		data := []byte{}
		if t.root.form != RowRoot || t.root.row != nil {
			record := t.rootRecord()
			var err error
			if err := t.coreEncode(rootKeyBytes); err != nil {
				return err
			}
			data, err = EncodeRecord(rootKeyBytes, &record)
			if err != nil {
				return err
			}
		}
		final[rootKey] = data
		prev[rootKey] = t.previousRecord(rootKeyBytes, t.root.prev)
	}
	for key, bucketKey := range t.bucketDirty {
		if t.suppressBucketRecords || !t.ownsRecordKey(bucketKey) {
			continue
		}
		descriptor, ok, err := t.bucketDescriptor(bucketKey)
		if err != nil {
			return err
		}
		old, err := t.bucketRecordPrevious(bucketKey)
		if err != nil {
			return err
		}
		prev[key] = old
		if !ok {
			if err := t.coreEncode(bucketKey); err != nil {
				return err
			}
			final[key] = []byte{}
			continue
		}
		record := descriptor.record()
		if err := t.coreEncode(bucketKey); err != nil {
			return err
		}
		data, err := EncodeRecord(bucketKey, &record)
		if err != nil {
			return err
		}
		final[key] = data
	}
	keys := slices.Sorted(maps.Keys(final))
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
		stored := bytes.Clone(data)
		row.raw = stored
		row.prev = stored
		row.dirty = false
		row.tombstone = len(data) == 0
	}
	if data, ok := final[rootKey]; ok {
		stored := bytes.Clone(data)
		t.root.raw = stored
		t.root.prev = stored
	}
	clear(t.dirtyRows)
	clear(t.bucketDirty)
	t.clearRoutingRows()
	t.rootDirty = false
	return nil
}

func (t *Trie) rootRecord() Record {
	switch t.root.form {
	case LeafRoot:
		return Record{Form: LeafRoot, Cells: [maxCells]Cell{0: t.root.leaf}}
	case ExtRoot:
		self := t.root.self
		if self.BitLen > t.rootRecordPath().BitLen {
			self = self.Slice(t.rootRecordPath().BitLen, self.BitLen)
		}
		return Record{Form: ExtRoot, SelfExt: self, Left: t.root.left, Right: t.root.right}
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
		row.name = string(key)
	}
	if len(row.prev) != 0 {
		t.rememberPrevName(row.name, row.prev)
	}
	t.rows[row.name] = row
	if row.dirty {
		t.dirtyRows[row.name] = row
		if !row.routing {
			row.routing = true
			t.routingRows = append(t.routingRows, row)
		}
	}
}

func (t *Trie) rememberPrev(key, data []byte) {
	t.rememberPrevName(string(key), data)
}

func (t *Trie) rememberPrevName(name string, data []byte) {
	if t.roundPrev == nil {
		return
	}
	if _, ok := t.roundPrev[name]; !ok {
		t.roundPrev[name] = bytes.Clone(data)
	}
}

func (t *Trie) touchBucket(key []byte) {
	if t.bucketMode {
		return
	}
	t.bucketDirty[string(key)] = bytes.Clone(key)
}

func validateOps(ops []Op) error {
	var previous []byte
	for i, op := range ops {
		key := op.Key
		if len(op.Drop) != 0 {
			key = op.Drop
		}
		if i != 0 && bytes.Compare(previous, key) >= 0 {
			return errOperationOrder
		}
		previous = key
	}
	return nil
}

func (t *Trie) previousRecord(key, fallback []byte) []byte {
	if data, ok := t.roundPrev[string(key)]; ok {
		return bytes.Clone(data)
	}
	return bytes.Clone(fallback)
}

func (t *Trie) markDirty(row *rowNode) {
	for row != nil {
		row.markDirty()
		if !row.routing {
			row.routing = true
			t.routingRows = append(t.routingRows, row)
		}
		t.registerRow(row)
		row = row.parent
	}
}

func (t *Trie) clearRoutingRows() {
	for _, row := range t.routingRows {
		row.routing = false
	}
	t.routingRows = t.routingRows[:0]
}
