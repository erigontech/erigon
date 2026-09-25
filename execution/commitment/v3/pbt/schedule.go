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
	"runtime"
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
	phaseChain
	phaseJoin
)

type phaseTaskOwner uint8

const (
	ownerBucket phaseTaskOwner = iota
	ownerChain
	ownerJoin
)

type phaseTask struct {
	kind         phaseTaskKind
	owner        phaseTaskOwner
	key          string
	zone         byte
	nibble       byte
	ops          []Op
	dependencies []int
}

type phasePlan struct {
	tasks []phaseTask
}

func buildPhasePlan(ops []Op) (phasePlan, error) {
	if err := validateOps(ops); err != nil {
		return phasePlan{}, err
	}
	bucketOps := make(map[string][]Op)
	bucketKeys := make(map[string][]byte)
	chains := make(map[[2]byte]struct{})
	for _, op := range ops {
		if len(op.Drop) != 0 {
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
		}
		chains[[2]byte{zone, op.Key[1] >> 4}] = struct{}{}
	}
	bucketList := make([][]byte, 0, len(bucketKeys))
	for _, key := range bucketKeys {
		bucketList = append(bucketList, key)
	}
	sort.Slice(bucketList, func(i, j int) bool {
		if len(bucketList[i]) != len(bucketList[j]) {
			return len(bucketList[i]) > len(bucketList[j])
		}
		return bytes.Compare(bucketList[i], bucketList[j]) < 0
	})
	tasks := make([]phaseTask, 0, len(bucketList)+len(chains)+1)
	bucketIndex := make(map[string]int, len(bucketList))
	for _, key := range bucketList {
		bucketIndex[string(key)] = len(tasks)
		tasks = append(tasks, phaseTask{kind: phaseBucket, owner: ownerBucket, key: string(key), ops: bucketOps[string(key)]})
	}
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
		task := phaseTask{kind: phaseChain, owner: ownerChain, zone: key[0], nibble: key[1]}
		if key[0] == eip8297.StorageZone {
			for _, bucket := range bucketList {
				if bucket[1]>>4 == key[1] {
					task.dependencies = append(task.dependencies, bucketIndex[string(bucket)])
				}
			}
			sort.Ints(task.dependencies)
		}
		chainIndexes = append(chainIndexes, len(tasks))
		tasks = append(tasks, task)
	}
	if len(chainIndexes) != 0 {
		tasks = append(tasks, phaseTask{kind: phaseJoin, owner: ownerJoin, dependencies: chainIndexes})
	}
	return phasePlan{tasks: tasks}, nil
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
	return t.ProcessParallelContext(context.Background(), ops, workers)
}

func (t *Trie) ProcessParallelContext(ctx context.Context, ops []Op, workers int) (common.Hash, error) {
	plan, err := buildPhasePlan(ops)
	if err != nil {
		return common.Hash{}, err
	}
	if err := ctx.Err(); err != nil {
		return common.Hash{}, err
	}
	if t.ctx == nil {
		return t.Process(ops)
	}
	return t.processParallelPhaseA(ctx, workers, plan, ops)
}
