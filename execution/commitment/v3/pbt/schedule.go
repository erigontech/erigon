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
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type phaseTaskKind uint8

const (
	phaseBucket phaseTaskKind = iota
	phaseBucketSubtask
	phaseChain
	phaseJoin
)

type phaseTask struct {
	kind         phaseTaskKind
	key          string
	prefix       eip8297.Bitpath
	zone         byte
	nibble       byte
	ops          []Op
	dependencies []int
	resultStart  int
	resultEnd    int
	batch        []phaseTask
	initial      phaseBucketResult
	hasInitial   bool
}

type phasePlan struct {
	tasks   []phaseTask
	results int
}

const fanOutMin = 8

func buildPhasePlan(ops []Op) (phasePlan, error) {
	return buildPhasePlanWithThreshold(ops, fanOutMin)
}

type phaseGroup struct {
	zone   byte
	nibble byte
	stem   []byte
}

func phaseOpGroup(op *Op) (phaseGroup, error) {
	if len(op.Drop) != 0 {
		if len(op.Drop) != 33 || (op.Drop[0] != eip8297.AccountZone && op.Drop[0] != eip8297.StorageZone) {
			return phaseGroup{}, errInsertKey
		}
		group := phaseGroup{zone: op.Drop[0], nibble: op.Drop[1] >> 4}
		if group.zone == eip8297.StorageZone {
			group.stem = op.Drop[:33]
		}
		return group, nil
	}
	if len(op.Key) == 0 {
		return phaseGroup{}, errInsertKey
	}
	if keyLen, ok := eip8297.ZoneKeyLength(op.Key[0]); !ok || len(op.Key) != keyLen {
		return phaseGroup{}, fmt.Errorf("invalid tree key")
	}
	group := phaseGroup{zone: op.Key[0], nibble: op.Key[1] >> 4}
	if group.zone == eip8297.StorageZone {
		group.stem = op.Key[:33]
	}
	return group, nil
}

func (g *phaseGroup) same(other *phaseGroup) bool {
	return g.zone == other.zone && g.nibble == other.nibble && bytes.Equal(g.stem, other.stem)
}

func buildPhasePlanWithThreshold(ops []Op, threshold int) (phasePlan, error) {
	return buildPhasePlanBatched(ops, threshold, 1)
}

func buildPhasePlanBatched(ops []Op, threshold, bucketBatch int) (phasePlan, error) {
	if err := validateOps(ops); err != nil {
		return phasePlan{}, err
	}
	var tasks []phaseTask
	var chainIndexes []int
	var buckets []phaseGroup
	var bucketOps [][]Op
	add := func(group phaseGroup, run []Op) {
		if group.stem == nil {
			chainIndexes = append(chainIndexes, len(tasks))
			tasks = append(tasks, phaseTask{kind: phaseChain, zone: group.zone, nibble: group.nibble, prefix: chainPrefix(group.zone, group.nibble), ops: run})
			return
		}
		buckets = append(buckets, group)
		bucketOps = append(bucketOps, run)
	}
	var group phaseGroup
	start := 0
	for i := range ops {
		next, err := phaseOpGroup(&ops[i])
		if err != nil {
			return phasePlan{}, err
		}
		if i > 0 && !next.same(&group) {
			add(group, ops[start:i:i])
			start = i
		}
		group = next
	}
	if len(ops) > 0 {
		add(group, ops[start:len(ops):len(ops)])
	}
	tasks = slices.Grow(tasks, len(buckets)+17)
	order := make([]int, len(buckets))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return len(bucketOps[b]) - len(bucketOps[a]) })
	bucketIndex := make([]int, len(buckets))
	open := -1
	for _, b := range order {
		bucketPath := eip8297.PathFromBits(buckets[b].stem, 264)
		key, err := EncodeRowKey(&bucketPath)
		if err != nil {
			return phasePlan{}, err
		}
		bucketKey := string(key)
		bucket := bucketOps[b]
		if threshold > 0 && len(bucket) >= threshold && !hasDrop(bucket) {
			dependencies := make([]int, 0, 16)
			start := 0
			for i := 1; i <= len(bucket); i++ {
				if i < len(bucket) && bucket[i].Key[33]>>4 == bucket[start].Key[33]>>4 {
					continue
				}
				dependencies = append(dependencies, len(tasks))
				tasks = append(tasks, phaseTask{kind: phaseBucketSubtask, key: bucketKey, prefix: eip8297.PathFromBits(bucket[start].Key, bucketPath.BitLen+4), ops: bucket[start:i:i]})
				start = i
			}
			bucketIndex[b] = len(tasks)
			tasks = append(tasks, phaseTask{kind: phaseBucket, key: bucketKey, prefix: bucketPath, dependencies: dependencies})
			continue
		}
		single := phaseTask{kind: phaseBucket, key: bucketKey, prefix: bucketPath, ops: bucket}
		if bucketBatch > 1 {
			if open < 0 || len(tasks[open].batch) == bucketBatch {
				open = len(tasks)
				tasks = append(tasks, phaseTask{kind: phaseBucket})
			}
			tasks[open].batch = append(tasks[open].batch, single)
			bucketIndex[b] = open
			continue
		}
		bucketIndex[b] = len(tasks)
		tasks = append(tasks, single)
	}
	for first := 0; first < len(buckets); {
		nibble := buckets[first].nibble
		task := phaseTask{kind: phaseChain, zone: eip8297.StorageZone, nibble: nibble, prefix: chainPrefix(eip8297.StorageZone, nibble)}
		for ; first < len(buckets) && buckets[first].nibble == nibble; first++ {
			task.dependencies = append(task.dependencies, bucketIndex[first])
		}
		slices.Sort(task.dependencies)
		task.dependencies = slices.Compact(task.dependencies)
		chainIndexes = append(chainIndexes, len(tasks))
		tasks = append(tasks, task)
	}
	tasks = append(tasks, phaseTask{kind: phaseJoin, dependencies: chainIndexes})
	results := 0
	for i := range tasks {
		tasks[i].resultStart = results
		results += max(1, len(tasks[i].batch))
		tasks[i].resultEnd = results
	}
	return phasePlan{tasks: tasks, results: results}, nil
}

func chainPrefix(zone, nibble byte) eip8297.Bitpath {
	return eip8297.PathFromBits([]byte{zone, nibble << 4}, 12)
}

func hasDrop(ops []Op) bool {
	for _, op := range ops {
		if len(op.Drop) != 0 {
			return true
		}
	}
	return false
}

func runPhasePlan(ctx context.Context, workers int, plan phasePlan, run func(phaseTask) error) error {
	return runPhasePlanWithFactory(ctx, workers, plan, nil, func(_ context.Context, _ commitment.PatriciaContext, task phaseTask) error {
		return run(task)
	})
}

func runPhasePlanWithFactory(ctx context.Context, workers int, plan phasePlan, factory commitment.TrieContextFactory, run func(context.Context, commitment.PatriciaContext, phaseTask) error) error {
	if len(plan.tasks) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	workers = min(workers, len(plan.tasks))
	done := make([]chan struct{}, len(plan.tasks))
	for i := range done {
		done[i] = make(chan struct{})
	}
	var next atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			var workerContext commitment.PatriciaContext
			var cleanup func()
			if factory != nil {
				workerContext, cleanup = factory(gctx)
				if cleanup != nil {
					defer cleanup()
				}
				if workerContext == nil {
					return errors.New("trie context factory returned a nil context")
				}
			}
			for {
				if err := gctx.Err(); err != nil {
					return err
				}
				i := int(next.Add(1)) - 1
				if i >= len(plan.tasks) {
					return nil
				}
				for _, dependency := range plan.tasks[i].dependencies {
					select {
					case <-done[dependency]:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
				if err := run(gctx, workerContext, plan.tasks[i]); err != nil {
					return err
				}
				close(done[i])
			}
		})
	}
	return g.Wait()
}

func (t *Trie) ProcessParallel(ops []Op, workers int) (common.Hash, error) {
	return t.ProcessParallelWithThreshold(ops, workers, fanOutMin)
}

func (t *Trie) ProcessParallelWithThreshold(ops []Op, workers, threshold int) (common.Hash, error) {
	return t.processParallelContext(context.Background(), ops, workers, threshold)
}

func (t *Trie) ProcessParallelContext(ctx context.Context, ops []Op, workers int) (common.Hash, error) {
	return t.processParallelContext(ctx, ops, workers, fanOutMin)
}

func (t *Trie) processParallelContext(ctx context.Context, ops []Op, workers, threshold int) (common.Hash, error) {
	if err := ctx.Err(); err != nil {
		return common.Hash{}, err
	}
	if t.roundPending {
		t.ResetContext(t.ctx)
	}
	if t.ctx == nil || workers == 1 || len(ops) < 2 {
		return t.Process(ops)
	}
	plan, err := buildPhasePlanBatched(ops, threshold, t.bucketBatch)
	if err != nil {
		return common.Hash{}, err
	}
	hash, err := t.processParallelPhaseA(ctx, workers, plan)
	if err == nil {
		t.roundPending = true
	}
	return hash, err
}
