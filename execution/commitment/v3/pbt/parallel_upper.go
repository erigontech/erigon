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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type phaseContext struct {
	base    commitment.PatriciaContext
	records map[string][]byte
	prefix  *eip8297.Bitpath
	owners  []phaseOwner
}

type phaseOwner struct {
	task   int
	prefix eip8297.Bitpath
}

func phaseOwners(tasks []phaseTask, indexes []int) ([]phaseOwner, error) {
	owners := make([]phaseOwner, len(indexes))
	for i, index := range indexes {
		prefix, err := taskSubtreePrefix(tasks[index])
		if err != nil {
			return nil, err
		}
		owners[i] = phaseOwner{task: index, prefix: prefix}
	}
	return owners, nil
}

func (c *phaseContext) checkOwner(key []byte) error {
	if len(c.owners) == 0 {
		return nil
	}
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		return nil
	}
	for i := range c.owners {
		if pathHasPrefix(&path, &c.owners[i].prefix) {
			return fmt.Errorf("record %x is owned by task %d at %x", key, c.owners[i].task, eip8297.AppendBitPath(nil, &c.owners[i].prefix))
		}
	}
	return nil
}

func (t *Trie) checkRowOwner(key []byte) error {
	if ctx, ok := t.ctx.(*phaseContext); ok {
		return ctx.checkOwner(key)
	}
	return nil
}

func (c *phaseContext) Branch(key []byte) ([]byte, kv.Step, error) {
	if c.prefix != nil && !ownsRecordKey(key, c.prefix) {
		return nil, 0, fmt.Errorf("record %x is above subtree prefix", key)
	}
	if err := c.checkOwner(key); err != nil {
		return nil, 0, err
	}
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
	if c.prefix != nil && !ownsRecordKey(key, c.prefix) {
		return fmt.Errorf("record %x is above subtree prefix", key)
	}
	if err := c.checkOwner(key); err != nil {
		return err
	}
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

func (c *phaseContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	if source, ok := c.base.(leafRefSource); ok {
		return source.LeafRefs(key, data)
	}
	return nil
}

func (t *Trie) newPhaseContext(base commitment.PatriciaContext, prefix *eip8297.Bitpath) *phaseContext {
	return &phaseContext{base: base, records: make(map[string][]byte), prefix: prefix}
}

func (t *Trie) inheritHooks(dst *Trie) {
	dst.coreApplyHook = t.coreApplyHook
	dst.coreEncodeHook = t.coreEncodeHook
	dst.coreActivityHook = t.coreActivityHook
}

func descriptorWithFallback(descriptor bucketDescriptor, present bool, err error, deltas []commitment.BranchDelta, initial *phaseBucketResult) (bucketDescriptor, bool, error) {
	if err == nil {
		return descriptor, present, nil
	}
	if len(deltas) == 0 {
		return initial.descriptor, initial.present, nil
	}
	return bucketDescriptor{}, false, err
}

func (c *phaseContext) Account(key []byte) (*commitment.Update, error) {
	return c.base.Account(key)
}

func (c *phaseContext) Storage(key []byte) (*commitment.Update, error) {
	return c.base.Storage(key)
}

type phaseBucketResult struct {
	prefix     eip8297.Bitpath
	deltas     []commitment.BranchDelta
	descriptor bucketDescriptor
	present    bool
	unchanged  bool
}

//nolint:gocritic
func (t *Trie) runSubtreeTask(workerCtx context.Context, workerContext commitment.PatriciaContext, task phaseTask) (phaseBucketResult, error) {
	prefix, err := taskSubtreePrefix(task)
	if err != nil {
		return phaseBucketResult{}, err
	}
	for _, op := range task.ops {
		if err := opInSubtree(op, &prefix); err != nil {
			return phaseBucketResult{}, err
		}
	}
	base := cmp.Or(workerContext, t.phaseBase, t.ctx)
	local := t.newPhaseContext(base, nil)
	var subtreeTrie *Trie
	initial := task.initial
	switch {
	case task.kind == phaseBucketSubtask:
		bucket, err := newBucketTrie(local, []byte(task.key))
		if err != nil {
			return phaseBucketResult{}, err
		}
		initial.descriptor, initial.present, err = bucket.descriptorAtPrefix(&prefix)
		if err != nil {
			return phaseBucketResult{}, err
		}
		local.prefix = &prefix
		subtreeTrie, err = newSubtreeTrie(local, prefix, initial.descriptor, initial.present)
		if err != nil {
			return phaseBucketResult{}, err
		}
	case task.hasInitial:
		local.prefix = &prefix
		subtreeTrie, err = newSubtreeTrie(local, prefix, task.initial.descriptor, task.initial.present)
		if err != nil {
			return phaseBucketResult{}, err
		}
	case task.kind == phaseBucket && len(task.dependencies) == 0:
		subtreeTrie, err = newBucketTrie(local, []byte(task.key))
		if err != nil {
			return phaseBucketResult{}, err
		}
	default:
		subtreeTrie = NewTrie(local)
		subtreeTrie.ownedPrefix = &prefix
		subtreeTrie.suppressRoot = true
		subtreeTrie.suppressBucketRecords = true
	}
	subtreeTrie.coreTask = &task
	t.inheritHooks(subtreeTrie)
	if err := t.phaseHookOps(&task); err != nil {
		return phaseBucketResult{}, err
	}
	if _, err := subtreeTrie.Process(task.ops); err != nil {
		return phaseBucketResult{}, fmt.Errorf("process subtree %x: %w", []byte(task.key), err)
	}
	deltas := subtreeTrie.takeDeltaPart()
	if task.kind == phaseBucket {
		if len(deltas) == 0 {
			return phaseBucketResult{prefix: prefix, unchanged: true}, workerCtx.Err()
		}
	} else {
		deltas = ownedDeltas(deltas, &prefix)
	}
	descriptor, present, err := subtreeTrie.descriptorFromRoot()
	if task.kind != phaseBucket {
		descriptor, present, err = descriptorWithFallback(descriptor, present, err, deltas, &initial)
	}
	if err != nil {
		return phaseBucketResult{}, err
	}
	return phaseBucketResult{prefix: prefix, deltas: deltas, descriptor: descriptor, present: present}, workerCtx.Err()
}

func ownedDeltas(deltas []commitment.BranchDelta, prefix *eip8297.Bitpath) []commitment.BranchDelta {
	result := make([]commitment.BranchDelta, 0, len(deltas))
	for _, delta := range deltas {
		path, err := eip8297.DecodeBitPath(delta.Key)
		if err != nil || !pathHasPrefix(&path, prefix) {
			continue
		}
		result = append(result, delta)
	}
	return result
}

//nolint:gocritic
func taskSubtreePrefix(task phaseTask) (eip8297.Bitpath, error) {
	if task.prefix.BitLen != 0 {
		return task.prefix, nil
	}
	if task.kind == phaseBucket || task.kind == phaseBucketSubtask {
		return bucketPathForKey([]byte(task.key))
	}
	return eip8297.Bitpath{}, fmt.Errorf("subtree task has no prefix")
}

func opInSubtree(op Op, prefix *eip8297.Bitpath) error {
	var path eip8297.Bitpath
	var err error
	if len(op.Drop) != 0 {
		if len(op.Drop) > eip8297.MaxPathBits/8 {
			return fmt.Errorf("operation is outside subtree prefix")
		}
		path = eip8297.PathFromBits(op.Drop, int16(len(op.Drop)*8))
	} else {
		path, err = keyPath(op.Key)
		if err != nil {
			return err
		}
	}
	if !pathHasPrefix(&path, prefix) {
		return fmt.Errorf("operation is outside subtree prefix")
	}
	return nil
}

func (t *Trie) processParallelPhaseA(ctx context.Context, workers int, plan phasePlan) (common.Hash, error) {
	base := t.ctx
	round := t.newPhaseContext(base, nil)
	t.ctx = round
	t.phaseBase = base
	defer func() {
		t.ctx = base
		t.phaseBase = nil
	}()
	t.roundPrev = make(map[string][]byte)
	t.deltas = nil
	t.bucketDirty = make(map[string][]byte)
	if _, err := t.loadRoot(); err != nil {
		return common.Hash{}, err
	}
	if err := t.preparePhaseTasks(&plan); err != nil {
		return common.Hash{}, err
	}
	t.rememberPrev(t.rootRecordKey(), t.root.prev)
	results := make([]phaseBucketResult, len(plan.tasks))
	ready := make([]bool, len(plan.tasks))
	phaseWorkers := workers
	factory := t.ctxFactory
	if factory == nil {
		phaseWorkers = 1
	}
	var finalRoot common.Hash
	if err := runPhasePlanWithFactory(ctx, phaseWorkers, plan, factory, func(workerCtx context.Context, workerContext commitment.PatriciaContext, task phaseTask) error {
		if err := t.phaseHookCall(task, nil); err != nil {
			return err
		}
		var result phaseBucketResult
		var err error
		switch task.kind {
		case phaseBucketSubtask, phaseBucket:
			if task.kind == phaseBucketSubtask || len(task.dependencies) == 0 {
				result, err = t.runSubtreeTask(workerCtx, workerContext, task)
			} else {
				result, err = t.runBucketJoin(workerCtx, workerContext, task, plan.tasks, results, ready)
			}
		case phaseChain:
			result, err = t.runChainTask(workerCtx, workerContext, task, results, ready)
		case phaseJoin:
			changed := make(map[string]phaseBucketResult, len(task.dependencies))
			for _, dependency := range task.dependencies {
				result := &results[dependency]
				changed[string(eip8297.AppendBitPath(nil, &result.prefix))] = *result
			}
			if round.owners, err = phaseOwners(plan.tasks, task.dependencies); err != nil {
				return err
			}
			if t.joinHook != nil {
				if err := t.joinHook(task, round); err != nil {
					return err
				}
			}
			t.coreTask = &task
			finalRoot, err = t.processUpperOps(nil, changed)
			t.coreTask = nil
			result.deltas = t.takeDeltaPart()
		default:
			return fmt.Errorf("unknown phase task %d", task.kind)
		}
		if err != nil {
			return err
		}
		results[task.resultIndex] = result
		ready[task.resultIndex] = true
		return nil
	}); err != nil {
		return common.Hash{}, err
	}
	t.ctx = base
	var deltas []commitment.BranchDelta
	for index := range results {
		if plan.tasks[index].kind == phaseBucketSubtask {
			continue
		}
		for _, delta := range results[index].deltas {
			if err := base.PutBranch(delta.Key, delta.Data, delta.Prev); err != nil {
				return common.Hash{}, err
			}
		}
		deltas = append(deltas, results[index].deltas...)
	}
	t.deltas = deltas
	return finalRoot, nil
}

//nolint:gocritic
func (t *Trie) runBucketJoin(workerCtx context.Context, workerContext commitment.PatriciaContext, task phaseTask, tasks []phaseTask, results []phaseBucketResult, ready []bool) (phaseBucketResult, error) {
	base := cmp.Or(workerContext, t.phaseBase)
	local := t.newPhaseContext(base, nil)
	bucketTrie, err := newBucketTrie(local, []byte(task.key))
	if err != nil {
		return phaseBucketResult{}, err
	}
	bucketTrie.coreTask = &task
	t.inheritHooks(bucketTrie)
	bucketTrie.roundPrev = make(map[string][]byte)
	bucketTrie.bucketDirty = make(map[string][]byte)
	childDeltas := make([]commitment.BranchDelta, 0)
	changed := make(map[string]phaseBucketResult, len(task.dependencies))
	for _, dependency := range task.dependencies {
		if !ready[dependency] {
			return phaseBucketResult{}, fmt.Errorf("bucket %x missing subtree result", []byte(task.key))
		}
		result := results[dependency]
		childDeltas = append(childDeltas, result.deltas...)
		changed[string(eip8297.AppendBitPath(nil, &result.prefix))] = result
	}
	if local.owners, err = phaseOwners(tasks, task.dependencies); err != nil {
		return phaseBucketResult{}, err
	}
	if t.joinHook != nil {
		if err := t.joinHook(task, local); err != nil {
			return phaseBucketResult{}, err
		}
	}
	if _, err := bucketTrie.processUpperOps(nil, changed); err != nil {
		return phaseBucketResult{}, err
	}
	deltas := append(childDeltas, bucketTrie.takeDeltaPart()...)
	if len(deltas) == 0 {
		return phaseBucketResult{prefix: task.prefix, unchanged: true}, workerCtx.Err()
	}
	descriptor, present, err := bucketTrie.descriptorFromRoot()
	if err != nil {
		return phaseBucketResult{}, err
	}
	return phaseBucketResult{prefix: task.prefix, deltas: deltas, descriptor: descriptor, present: present}, workerCtx.Err()
}

//nolint:gocritic
func (t *Trie) runChainTask(workerCtx context.Context, workerContext commitment.PatriciaContext, task phaseTask, results []phaseBucketResult, ready []bool) (phaseBucketResult, error) {
	base := cmp.Or(workerContext, t.phaseBase)
	local := t.newPhaseContext(base, &task.prefix)
	chainTrie, err := newSubtreeTrie(local, task.prefix, task.initial.descriptor, task.initial.present)
	if err != nil {
		return phaseBucketResult{}, err
	}
	chainTrie.coreTask = &task
	t.inheritHooks(chainTrie)
	if task.zone == eip8297.StorageZone {
		changed := make(map[string]phaseBucketResult)
		for _, dependency := range task.dependencies {
			if !ready[dependency] {
				return phaseBucketResult{}, fmt.Errorf("storage chain missing bucket result")
			}
			if result := results[dependency]; !result.unchanged {
				changed[string(eip8297.AppendBitPath(nil, &result.prefix))] = result
			}
		}
		if _, err := chainTrie.processUpperOps(nil, changed); err != nil {
			return phaseBucketResult{}, err
		}
	} else {
		if err := t.phaseHookOps(&task); err != nil {
			return phaseBucketResult{}, err
		}
		if _, err := chainTrie.Process(task.ops); err != nil {
			return phaseBucketResult{}, err
		}
	}
	deltas := ownedDeltas(chainTrie.takeDeltaPart(), &task.prefix)
	descriptor, present, err := chainTrie.descriptorFromRoot()
	descriptor, present, err = descriptorWithFallback(descriptor, present, err, deltas, &task.initial)
	if err != nil {
		return phaseBucketResult{}, err
	}
	return phaseBucketResult{prefix: task.prefix, deltas: deltas, descriptor: descriptor, present: present}, workerCtx.Err()
}

func ownsRecordKey(key []byte, prefix *eip8297.Bitpath) bool {
	if bytes.Equal(key, GlobalRootKey()) {
		return prefix.BitLen == 0
	}
	path, err := eip8297.DecodeBitPath(key)
	return err == nil && pathHasPrefix(&path, prefix)
}

func (t *Trie) preparePhaseTasks(plan *phasePlan) error {
	for i := range plan.tasks {
		task := &plan.tasks[i]
		if task.kind != phaseChain {
			continue
		}
		descriptor, present, err := t.descriptorAtPrefix(&task.prefix)
		if err != nil {
			return err
		}
		task.initial = phaseBucketResult{prefix: task.prefix, descriptor: descriptor, present: present}
		task.hasInitial = true
	}
	return nil
}

func (t *Trie) phaseHookOps(task *phaseTask) error {
	if t.phaseHook == nil {
		return nil
	}
	task.ops = slices.Clone(task.ops)
	for i := range task.ops {
		task.ops[i].Key = bytes.Clone(task.ops[i].Key)
		task.ops[i].Drop = bytes.Clone(task.ops[i].Drop)
		if err := t.phaseHook(*task, &task.ops[i]); err != nil {
			return err
		}
	}
	return nil
}

//nolint:gocritic
func (t *Trie) phaseHookCall(task phaseTask, op *Op) error {
	if t.phaseHook == nil {
		return nil
	}
	return t.phaseHook(task, op)
}

func (t *Trie) coreApply(op *Op) error {
	if t.coreApplyHook == nil || t.coreTask == nil {
		if t.coreActivityHook != nil && t.coreTask != nil {
			t.coreActivityHook(true)
			defer t.coreActivityHook(false)
		}
		return nil
	}
	if t.coreActivityHook != nil {
		t.coreActivityHook(true)
		defer t.coreActivityHook(false)
	}
	return t.coreApplyHook(*t.coreTask, op)
}

func (t *Trie) coreEncode(key []byte) error {
	if t.coreEncodeHook == nil || t.coreTask == nil {
		return nil
	}
	return t.coreEncodeHook(*t.coreTask, key)
}

func (t *Trie) ownsRecordKey(key []byte) bool {
	if t.ownedPrefix == nil {
		return true
	}
	if len(key) == 0 {
		return false
	}
	return ownsRecordKey(key, t.ownedPrefix)
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
		if !t.root.row.folded {
			return bucketDescriptor{}, false, fmt.Errorf("row descriptor was not folded")
		}
		return bucketDescriptor{form: RowRoot, row: t.root.row, split: t.root.row.foldResult.Split, left: t.root.row.foldResult.Left, right: t.root.row.foldResult.Right}, true, nil
	case LeafRoot:
		return bucketDescriptor{form: LeafRoot, leaf: t.root.leaf}, true, nil
	case ExtRoot:
		return bucketDescriptor{form: ExtRoot, self: t.root.self.Slice(t.rootPath.BitLen, t.root.self.BitLen), left: t.root.left, right: t.root.right}, true, nil
	default:
		return bucketDescriptor{}, false, fmt.Errorf("unknown root form %d", t.root.form)
	}
}
