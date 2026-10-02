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
	"maps"
	"runtime"
	"slices"
	"sort"
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
	resultIndex  int
	initial      phaseBucketResult
	hasInitial   bool
	fallback     bucketDescriptor
	fallbackSeen bool
	fallbackOK   bool
}

type phasePlan struct {
	tasks []phaseTask
}

const fanOutMin = 8

func buildPhasePlan(ops []Op) (phasePlan, error) {
	return buildPhasePlanWithThreshold(ops, fanOutMin)
}

func buildPhasePlanWithThreshold(ops []Op, threshold int) (phasePlan, error) {
	if err := validateOps(ops); err != nil {
		return phasePlan{}, err
	}
	bucketOps := make(map[string][]Op)
	bucketKeys := make(map[string][]byte)
	chains := make(map[[2]byte]struct{})
	chainOps := make(map[[2]byte][]Op)
	for _, op := range ops {
		if len(op.Drop) != 0 {
			if len(op.Drop) == 33 && op.Drop[0] == eip8297.AccountZone {
				key := [2]byte{eip8297.AccountZone, op.Drop[1] >> 4}
				chainOps[key] = append(chainOps[key], op)
				chains[key] = struct{}{}
				continue
			}
			key, err := bucketKeyForPrefix(op.Drop)
			if err != nil {
				return phasePlan{}, err
			}
			bucketKeys[string(key)] = key
			bucketOps[string(key)] = append(bucketOps[string(key)], op)
			chains[[2]byte{eip8297.StorageZone, key[1] >> 4}] = struct{}{}
			continue
		}
		if len(op.Key) == 0 {
			return phasePlan{}, errInsertKey
		}
		if _, err := keyPath(op.Key); err != nil {
			return phasePlan{}, err
		}
		zone := op.Key[0]
		if zone == eip8297.StorageZone {
			key, err := bucketKeyForStorage(op.Key)
			if err != nil {
				return phasePlan{}, err
			}
			bucketKeys[string(key)] = key
			bucketOps[string(key)] = append(bucketOps[string(key)], op)
		} else {
			chainOps[[2]byte{zone, op.Key[1] >> 4}] = append(chainOps[[2]byte{zone, op.Key[1] >> 4}], op)
		}
		chains[[2]byte{zone, op.Key[1] >> 4}] = struct{}{}
	}
	bucketList := make([][]byte, 0, len(bucketKeys))
	for _, key := range bucketKeys {
		bucketList = append(bucketList, key)
	}
	sort.Slice(bucketList, func(i, j int) bool {
		if len(bucketOps[string(bucketList[i])]) != len(bucketOps[string(bucketList[j])]) {
			return len(bucketOps[string(bucketList[i])]) > len(bucketOps[string(bucketList[j])])
		}
		return bytes.Compare(bucketList[i], bucketList[j]) < 0
	})
	tasks := make([]phaseTask, 0, len(bucketList)+len(chains)+1)
	chainKeys := make([][2]byte, 0, len(chains))
	for key := range chains {
		chainKeys = append(chainKeys, key)
	}
	sort.Slice(chainKeys, func(i, j int) bool {
		if chainKeys[i][0] != chainKeys[j][0] {
			return chainKeys[i][0] < chainKeys[j][0]
		}
		return chainKeys[i][1] < chainKeys[j][1]
	})
	chainIndexes := make([]int, 0, len(chainKeys))
	for _, key := range chainKeys {
		if key[0] == eip8297.StorageZone {
			continue
		}
		index := len(tasks)
		chainIndexes = append(chainIndexes, index)
		tasks = append(tasks, phaseTask{kind: phaseChain, zone: key[0], nibble: key[1], prefix: chainPrefix(key[0], key[1]), ops: chainOps[key], resultIndex: index})
	}
	bucketIndex := make(map[string]int, len(bucketList))
	for _, key := range bucketList {
		bucketKey := string(key)
		bucket := bucketOps[bucketKey]
		if threshold > 0 && len(bucket) >= threshold && !hasDrop(bucket) {
			groups := make(map[string][]Op)
			prefixes := make(map[string]eip8297.Bitpath)
			bucketPath, err := bucketPathForKey(key)
			if err != nil {
				return phasePlan{}, err
			}
			for _, op := range bucket {
				path, err := keyPath(op.Key)
				if err != nil {
					return phasePlan{}, err
				}
				prefix := path.Slice(0, bucketPath.BitLen+4)
				name := string(eip8297.AppendBitPath(nil, &prefix))
				groups[name] = append(groups[name], op)
				prefixes[name] = prefix
			}
			groupNames := slices.Sorted(maps.Keys(groups))
			dependencies := make([]int, 0, len(groupNames))
			for _, name := range groupNames {
				index := len(tasks)
				dependencies = append(dependencies, index)
				tasks = append(tasks, phaseTask{kind: phaseBucketSubtask, key: bucketKey, prefix: prefixes[name], ops: groups[name], resultIndex: index})
			}
			bucketIndex[bucketKey] = len(tasks)
			tasks = append(tasks, phaseTask{kind: phaseBucket, key: bucketKey, prefix: bucketPath, dependencies: dependencies, resultIndex: len(tasks)})
			continue
		}
		bucketIndex[bucketKey] = len(tasks)
		bucketPath, err := bucketPathForKey(key)
		if err != nil {
			return phasePlan{}, err
		}
		tasks = append(tasks, phaseTask{kind: phaseBucket, key: bucketKey, prefix: bucketPath, ops: bucket, resultIndex: len(tasks)})
	}
	for _, key := range chainKeys {
		if key[0] != eip8297.StorageZone {
			continue
		}
		task := phaseTask{kind: phaseChain, zone: key[0], nibble: key[1], prefix: chainPrefix(key[0], key[1]), resultIndex: len(tasks)}
		for _, bucket := range bucketList {
			if bucket[1]>>4 == key[1] {
				task.dependencies = append(task.dependencies, bucketIndex[string(bucket)])
			}
		}
		sort.Ints(task.dependencies)
		chainIndexes = append(chainIndexes, len(tasks))
		tasks = append(tasks, task)
	}
	tasks = append(tasks, phaseTask{kind: phaseJoin, dependencies: chainIndexes, resultIndex: len(tasks)})
	return phasePlan{tasks: tasks}, nil
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
			workerCtx := gctx
			var workerContext commitment.PatriciaContext
			var cleanup func()
			if factory != nil {
				workerContext, cleanup = factory(gctx)
				if cleanup != nil {
					defer cleanup()
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
				if err := run(workerCtx, workerContext, plan.tasks[i]); err != nil {
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
	plan, err := buildPhasePlanWithThreshold(ops, threshold)
	if err != nil {
		return common.Hash{}, err
	}
	hash, err := t.processParallelPhaseA(ctx, workers, plan, ops)
	if err == nil {
		t.roundPending = true
	}
	return hash, err
}
