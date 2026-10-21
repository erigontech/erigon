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

type phaseSchedule uint8

const (
	phaseScheduleGrain phaseSchedule = iota
	phaseScheduleOld
)

const (
	phaseGrainMin       = 128
	phaseGrainPerWorker = 4
)

func phaseGrainFor(roundKeys, workers int) int {
	return max(phaseGrainMin, roundKeys/(phaseGrainPerWorker*max(workers, 1)))
}

type phaseScan struct {
	tasks        []phaseTask
	chainIndexes []int
	buckets      []phaseGroup
	bucketOps    [][]Op
}

func scanPhaseOps(ops []Op) (phaseScan, error) {
	var scan phaseScan
	if err := validateOps(ops); err != nil {
		return scan, err
	}
	add := func(group phaseGroup, run []Op) {
		if group.stem == nil {
			scan.chainIndexes = append(scan.chainIndexes, len(scan.tasks))
			scan.tasks = append(scan.tasks, phaseTask{kind: phaseChain, zone: group.zone, nibble: group.nibble, prefix: chainPrefix(group.zone, group.nibble), ops: run})
			return
		}
		scan.buckets = append(scan.buckets, group)
		scan.bucketOps = append(scan.bucketOps, run)
	}
	var group phaseGroup
	start := 0
	for i := range ops {
		next, err := phaseOpGroup(&ops[i])
		if err != nil {
			return phaseScan{}, err
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
	scan.tasks = slices.Grow(scan.tasks, len(scan.buckets)+17)
	return scan, nil
}

func (s *phaseScan) bucketTask(b int) (phaseTask, error) {
	path := eip8297.PathFromBits(s.buckets[b].stem, 264)
	key, err := EncodeRowKey(&path)
	if err != nil {
		return phaseTask{}, err
	}
	return phaseTask{kind: phaseBucket, key: string(key), prefix: path, ops: s.bucketOps[b]}, nil
}

func (s *phaseScan) appendSplitBucket(bucket phaseTask) int {
	dependencies := make([]int, 0, 16)
	ops := bucket.ops
	start := 0
	for i := 1; i <= len(ops); i++ {
		if i < len(ops) && ops[i].Key[33]>>4 == ops[start].Key[33]>>4 {
			continue
		}
		dependencies = append(dependencies, len(s.tasks))
		s.tasks = append(s.tasks, phaseTask{kind: phaseBucketSubtask, key: bucket.key, prefix: eip8297.PathFromBits(ops[start].Key, bucket.prefix.BitLen+4), ops: ops[start:i:i]})
		start = i
	}
	s.tasks = append(s.tasks, phaseTask{kind: phaseBucket, key: bucket.key, prefix: bucket.prefix, dependencies: dependencies})
	return len(s.tasks) - 1
}

func (s *phaseScan) finish(bucketIndex []int) phasePlan {
	for first := 0; first < len(s.buckets); {
		nibble := s.buckets[first].nibble
		task := phaseTask{kind: phaseChain, zone: eip8297.StorageZone, nibble: nibble, prefix: chainPrefix(eip8297.StorageZone, nibble)}
		for ; first < len(s.buckets) && s.buckets[first].nibble == nibble; first++ {
			task.dependencies = append(task.dependencies, bucketIndex[first])
		}
		slices.Sort(task.dependencies)
		task.dependencies = slices.Compact(task.dependencies)
		s.chainIndexes = append(s.chainIndexes, len(s.tasks))
		s.tasks = append(s.tasks, task)
	}
	s.tasks = append(s.tasks, phaseTask{kind: phaseJoin, dependencies: s.chainIndexes})
	results := 0
	for i := range s.tasks {
		s.tasks[i].resultStart = results
		results += max(1, len(s.tasks[i].batch))
		s.tasks[i].resultEnd = results
	}
	return phasePlan{tasks: s.tasks, results: results}
}

func buildPhasePlanBatched(ops []Op, threshold, bucketBatch int) (phasePlan, error) {
	scan, err := scanPhaseOps(ops)
	if err != nil {
		return phasePlan{}, err
	}
	order := make([]int, len(scan.buckets))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return len(scan.bucketOps[b]) - len(scan.bucketOps[a]) })
	bucketIndex := make([]int, len(scan.buckets))
	open := -1
	for _, b := range order {
		single, err := scan.bucketTask(b)
		if err != nil {
			return phasePlan{}, err
		}
		if threshold > 0 && len(single.ops) >= threshold && !hasDrop(single.ops) {
			bucketIndex[b] = scan.appendSplitBucket(single)
			continue
		}
		if bucketBatch > 1 {
			if open < 0 || len(scan.tasks[open].batch) == bucketBatch {
				open = len(scan.tasks)
				scan.tasks = append(scan.tasks, phaseTask{kind: phaseBucket})
			}
			scan.tasks[open].batch = append(scan.tasks[open].batch, single)
			bucketIndex[b] = open
			continue
		}
		bucketIndex[b] = len(scan.tasks)
		scan.tasks = append(scan.tasks, single)
	}
	return scan.finish(bucketIndex), nil
}

func buildPhasePlanGrain(ops []Op, grain int) (phasePlan, error) {
	scan, err := scanPhaseOps(ops)
	if err != nil {
		return phasePlan{}, err
	}
	type unit struct{ first, last, ops int }
	units := make([]unit, 0, len(scan.buckets))
	for b := 0; b < len(scan.buckets); {
		u := unit{first: b, last: b + 1, ops: len(scan.bucketOps[b])}
		for u.ops <= grain && u.last < len(scan.buckets) && scan.buckets[u.last].nibble == scan.buckets[b].nibble && u.ops+len(scan.bucketOps[u.last]) <= grain {
			u.ops += len(scan.bucketOps[u.last])
			u.last++
		}
		units = append(units, u)
		b = u.last
	}
	slices.SortStableFunc(units, func(a, b unit) int { return b.ops - a.ops })
	bucketIndex := make([]int, len(scan.buckets))
	for _, u := range units {
		if u.last-u.first == 1 {
			single, err := scan.bucketTask(u.first)
			if err != nil {
				return phasePlan{}, err
			}
			if len(single.ops) > grain && !hasDrop(single.ops) {
				bucketIndex[u.first] = scan.appendSplitBucket(single)
				continue
			}
			bucketIndex[u.first] = len(scan.tasks)
			scan.tasks = append(scan.tasks, single)
			continue
		}
		batch := phaseTask{kind: phaseBucket, batch: make([]phaseTask, 0, u.last-u.first)}
		for b := u.first; b < u.last; b++ {
			single, err := scan.bucketTask(b)
			if err != nil {
				return phasePlan{}, err
			}
			batch.batch = append(batch.batch, single)
			bucketIndex[b] = len(scan.tasks)
		}
		scan.tasks = append(scan.tasks, batch)
	}
	return scan.finish(bucketIndex), nil
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
	return t.processParallelContext(context.Background(), ops, workers, t.defaultPhasePlan)
}

func (t *Trie) ProcessParallelWithThreshold(ops []Op, workers, threshold int) (common.Hash, error) {
	return t.processParallelContext(context.Background(), ops, workers, func(ops []Op, _ int) (phasePlan, error) {
		return buildPhasePlanBatched(ops, threshold, t.bucketBatch)
	})
}

func (t *Trie) ProcessParallelContext(ctx context.Context, ops []Op, workers int) (common.Hash, error) {
	return t.processParallelContext(ctx, ops, workers, t.defaultPhasePlan)
}

func (t *Trie) defaultPhasePlan(ops []Op, workers int) (phasePlan, error) {
	if t.phaseSchedule == phaseScheduleOld || t.bucketBatch > 1 {
		return buildPhasePlanBatched(ops, fanOutMin, t.bucketBatch)
	}
	return buildPhasePlanGrain(ops, phaseGrainFor(len(ops), workers))
}

func (t *Trie) processParallelContext(ctx context.Context, ops []Op, workers int, buildPlan func([]Op, int) (phasePlan, error)) (common.Hash, error) {
	if err := ctx.Err(); err != nil {
		return common.Hash{}, err
	}
	if err := t.beginDeferredRound(); err != nil {
		return common.Hash{}, err
	}
	if t.roundPending {
		t.ResetContext(t.ctx)
	}
	if t.ctx == nil || workers == 1 || len(ops) < 2 {
		return t.Process(ops)
	}
	plan, err := buildPlan(ops, workers)
	if err != nil {
		return common.Hash{}, err
	}
	hash, err := t.processParallelPhaseA(ctx, workers, plan)
	if err == nil {
		t.roundPending = true
	}
	return hash, err
}
