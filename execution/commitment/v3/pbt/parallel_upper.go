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

func (t *Trie) runSubtreeTask(workerCtx context.Context, workerContext commitment.PatriciaContext, task phaseTask) (phaseBucketResult, error) {
	base := workerContext
	if base == nil {
		base = t.phaseBase
		if base == nil {
			base = t.ctx
		}
	}
	local := &phaseContext{base: base, records: make(map[string][]byte)}
	bucketTrie, err := newBucketTrie(local, []byte(task.key))
	if err != nil {
		return phaseBucketResult{}, err
	}
	for i := range task.ops {
		if err := t.phaseHookCall(task, &task.ops[i]); err != nil {
			return phaseBucketResult{}, err
		}
	}
	if _, err := bucketTrie.Process(task.ops); err != nil {
		return phaseBucketResult{}, fmt.Errorf("process bucket %x: %w", []byte(task.key), err)
	}
	descriptor, present, err := bucketTrie.descriptorFromRoot()
	if err != nil {
		return phaseBucketResult{}, err
	}
	return phaseBucketResult{deltas: bucketTrie.TakeDeltas(), descriptor: descriptor, present: present}, workerCtx.Err()
}

func (t *Trie) processParallelPhaseA(ctx context.Context, workers int, plan phasePlan, ops []Op) (common.Hash, error) {
	base := t.ctx
	round := &phaseContext{base: base, records: make(map[string][]byte)}
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
	t.rememberPrev(t.rootRecordKey(), t.root.prev)
	bucketTasks := make([]phaseTask, 0)
	for _, task := range plan.tasks {
		if task.kind == phaseBucket {
			bucketTasks = append(bucketTasks, task)
		}
	}
	results := make([]phaseBucketResult, len(bucketTasks))
	bucketResultIndex := make(map[string]int, len(bucketTasks))
	for i := range bucketTasks {
		bucketResultIndex[bucketTasks[i].key] = i
	}
	subtaskResults := make([]bool, len(plan.tasks))
	chainResults := make([][]Op, len(plan.tasks))
	phaseWorkers := workers
	factory := t.ctxFactory
	if factory == nil {
		phaseWorkers = 1
	}
	var finalRoot common.Hash
	if err := runPhasePlanWithFactory(ctx, phaseWorkers, plan, factory, func(workerCtx context.Context, workerContext commitment.PatriciaContext, task phaseTask) error {
		switch task.kind {
		case phaseBucketSubtask:
			if err := t.phaseHookCall(task, nil); err != nil {
				return err
			}
			subtaskResults[task.resultIndex] = true
			return workerCtx.Err()
		case phaseBucket:
			if err := t.phaseHookCall(task, nil); err != nil {
				return err
			}
			for _, dependency := range task.dependencies {
				if !subtaskResults[dependency] {
					return fmt.Errorf("bucket %x missing subtask result", []byte(task.key))
				}
			}
			result, err := t.runSubtreeTask(workerCtx, workerContext, task)
			if err != nil {
				return err
			}
			results[bucketResultIndex[task.key]] = result
			return nil
		case phaseChain:
			if err := t.phaseHookCall(task, nil); err != nil {
				return err
			}
			if task.zone == eip8297.StorageZone {
				return workerCtx.Err()
			}
			for i := range task.ops {
				if err := t.phaseHookCall(task, &task.ops[i]); err != nil {
					return err
				}
			}
			chainResults[task.resultIndex] = append([]Op(nil), task.ops...)
			return workerCtx.Err()
		case phaseJoin:
			changedBuckets := make(map[string]phaseBucketResult, len(results))
			for i := range results {
				result := &results[i]
				key := bucketTasks[i].key
				changedBuckets[key] = *result
				for _, delta := range result.deltas {
					if _, ok := round.records[string(delta.Key)]; !ok {
						round.records[string(delta.Key)] = bytes.Clone(delta.Prev)
					}
					if err := t.ctx.PutBranch(delta.Key, delta.Data, delta.Prev); err != nil {
						return fmt.Errorf("apply phase A delta %x: %w", delta.Key, err)
					}
					t.rememberPrev(delta.Key, delta.Prev)
					t.addDelta(delta.Key, delta.Data, delta.Prev)
				}
			}
			if err := t.phaseHookCall(task, nil); err != nil {
				return err
			}
			var err error
			phaseB := make([]Op, 0, len(ops))
			for _, chain := range chainResults {
				phaseB = append(phaseB, chain...)
			}
			finalRoot, err = t.processUpperOps(phaseB, changedBuckets)
			return err
		default:
			return fmt.Errorf("unknown phase task %d", task.kind)
		}
	}); err != nil {
		return common.Hash{}, err
	}
	merged := mergeRoundDeltas(t.deltas)
	t.deltas = merged
	t.ctx = base
	for _, delta := range merged {
		if err := base.PutBranch(delta.Key, delta.Data, delta.Prev); err != nil {
			return common.Hash{}, err
		}
	}
	return finalRoot, nil
}

func mergeRoundDeltas(deltas []commitment.BranchDelta) []commitment.BranchDelta {
	byKey := make(map[string]commitment.BranchDelta, len(deltas))
	for _, delta := range deltas {
		key := string(delta.Key)
		if existing, ok := byKey[key]; ok {
			existing.Data = bytes.Clone(delta.Data)
			byKey[key] = existing
			continue
		}
		byKey[key] = commitment.BranchDelta{Key: bytes.Clone(delta.Key), Data: bytes.Clone(delta.Data), Prev: bytes.Clone(delta.Prev)}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]commitment.BranchDelta, 0, len(keys))
	for _, key := range keys {
		delta := byKey[key]
		if bytes.Equal(delta.Data, delta.Prev) {
			continue
		}
		result = append(result, delta)
	}
	return result
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
