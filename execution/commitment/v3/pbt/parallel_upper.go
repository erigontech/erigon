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
		base = t.ctx
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
	if _, err := t.loadRoot(); err != nil {
		return common.Hash{}, err
	}
	t.roundPrev = make(map[string][]byte)
	t.deltas = nil
	t.bucketDirty = make(map[string][]byte)
	t.rememberPrev(t.rootRecordKey(), t.root.prev)
	bucketTasks := make([]phaseTask, 0)
	phaseB := make([]Op, 0, len(ops))
	for _, task := range plan.tasks {
		if task.kind == phaseBucket {
			bucketTasks = append(bucketTasks, task)
		}
	}
	for _, op := range ops {
		if len(op.Drop) != 0 {
			continue
		}
		if len(op.Key) == eip8297.StorageKeyLength && op.Key[0] == eip8297.StorageZone {
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
		result, err := t.runSubtreeTask(workerCtx, workerContext, task)
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
		results[index] = result
		return nil
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
	return t.processUpperOps(phaseB, changedBuckets)
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
