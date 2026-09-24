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

package v4

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

type accountPlan struct {
	entry    accountEntry
	oldValue []byte
	found    bool
	delete   bool
	skip     bool
}

type accountResult struct {
	plan  accountPlan
	value []byte
	err   error
}

const (
	storageOversubscribe = 4
	accountPlaneFanout   = 16
)

func runStoragePhase(ctx context.Context, rawCtx commitment.PatriciaContext, factory commitment.TrieContextFactory, storage []storageTask, roots [][32]byte, parts []deltaParts, workers, fanOutMin int) error {
	if len(storage) == 0 {
		return nil
	}
	if factory == nil || workers <= 1 {
		for i, task := range storage {
			if err := ctx.Err(); err != nil {
				return err
			}
			root, taskParts, err := runStorageTaskWithPlan(rawCtx, task, foldPlan{})
			if err != nil {
				return err
			}
			roots[i], parts[i] = root, taskParts
		}
		return nil
	}

	workers = min(workers, len(storage))
	var next atomic.Int64
	g, gCtx := errgroup.WithContext(ctx)
	if fanOutMin <= 0 {
		fanOutMin = defaultStorageFanOutMin
	}
	taskPlan := foldPlan{ctx: gCtx, factory: factory, workers: accountPlaneFanout, fanOutMin: fanOutMin}
	for range workers {
		g.Go(func() error {
			workerCtx, cleanup := factory(gCtx)
			if cleanup != nil {
				defer cleanup()
			}
			for {
				if err := gCtx.Err(); err != nil {
					return err
				}
				i := int(next.Add(1)) - 1
				if i >= len(storage) {
					return nil
				}
				root, taskParts, err := runStorageTaskWithPlan(workerCtx, storage[i], taskPlan)
				if err != nil {
					return err
				}
				roots[i], parts[i] = root, taskParts
			}
		})
	}
	return g.Wait()
}

func runScheduledPhases(ctx context.Context, rawCtx commitment.PatriciaContext, factory commitment.TrieContextFactory, storage []storageTask, accounts []accountEntry, workers, fanOutMin int) ([32]byte, deltaParts, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	storageWorkers, accountWorkers := workers, workers
	if workers <= 0 {
		workers = runtime.NumCPU()
		storageWorkers = workers * storageOversubscribe
		accountWorkers = min(workers, accountPlaneFanout)
	}
	storageRoots := make([][32]byte, len(storage))
	storageParts := make([]deltaParts, len(storage))
	accountFold := foldPlan{ctx: ctx, factory: factory, workers: accountWorkers}
	overlap := factory != nil && storageWorkers > 1
	storageDone := make(chan error, 1)
	if overlap {
		go func() {
			storageDone <- runStoragePhase(ctx, rawCtx, factory, storage, storageRoots, storageParts, storageWorkers, fanOutMin)
		}()
	} else {
		storageDone <- runStoragePhase(ctx, rawCtx, factory, storage, storageRoots, storageParts, storageWorkers, fanOutMin)
	}

	g := accountGraph()
	root, plans, planErr := g.planAccounts(rawCtx, accounts, accountFold)
	if err := <-storageDone; err != nil {
		return [32]byte{}, nil, err
	}
	if planErr != nil {
		return [32]byte{}, nil, planErr
	}
	results := make(map[[32]byte][32]byte, len(storage))
	for i, task := range storage {
		results[task.addrHash] = storageRoots[i]
	}

	accountResults := make([]accountResult, len(plans))
	accountValues := make([]byte, accountLeafScratch*len(plans))
	parallelFor(len(plans), workers, 1024, func(i int) {
		plan := plans[i]
		accountResults[i].plan = plan
		if plan.skip || plan.delete {
			return
		}
		storageRoot := empty.RootHash
		if plan.entry.storageDirty {
			var ok bool
			storageRoot, ok = results[hashAddressPath(plan.entry.hashedKey)]
			if !ok {
				accountResults[i].err = errPhaseBRecord
				return
			}
			if !plan.found && plan.entry.update == nil && storageRoot == empty.RootHash {
				accountResults[i].plan.skip = true
				return
			}
		} else if plan.found {
			_, _, _, existingRoot, decodeErr := decodeAccountLeaf(plan.oldValue)
			if decodeErr != nil {
				accountResults[i].err = fmt.Errorf("%w: %w", errPhaseBRecord, decodeErr)
				return
			}
			copy(storageRoot[:], existingRoot)
		}
		update, updateErr := accountUpdate(plan.oldValue, plan.found, plan.entry.update)
		if updateErr != nil {
			accountResults[i].err = updateErr
			return
		}
		at := i * accountLeafScratch
		accountResults[i].value = encodeAccountLeaf(update, storageRoot[:], accountValues[at:at:at+accountLeafScratch])
	})
	var groups [16][]int
	var rest []int
	for i := range accountResults {
		result := &accountResults[i]
		if result.err != nil {
			return [32]byte{}, nil, result.err
		}
		if result.plan.skip {
			continue
		}
		nib := result.plan.entry.hashedKey[0]
		if child := root.child(int(nib)); !result.plan.delete && len(root.path) == 0 && child != nil && len(child.path) == len(root.path)+1 {
			groups[nib] = append(groups[nib], i)
			continue
		}
		rest = append(rest, i)
	}
	var groupErrs [16]error
	parallelFor(16, workers, 1, func(nib int) {
		for _, i := range groups[nib] {
			if err := insert(root, accountResults[i].plan.entry.hashedKey, accountResults[i].value); err != nil {
				groupErrs[nib] = err
				return
			}
		}
	})
	for _, err := range groupErrs {
		if err != nil {
			return [32]byte{}, nil, err
		}
	}
	for _, i := range rest {
		result := &accountResults[i]
		if result.plan.delete {
			if err := removeRoot(root, result.plan.entry.hashedKey); err != nil {
				return [32]byte{}, nil, err
			}
			continue
		}
		if len(root.path) == 0 {
			if err := insert(root, result.plan.entry.hashedKey, result.value); err != nil {
				return [32]byte{}, nil, err
			}
		} else if err := insertRoot(root, result.plan.entry.hashedKey, result.value); err != nil {
			return [32]byte{}, nil, err
		}
	}
	accountParts, err := g.persistGraph(rawCtx, root, accountFold)
	if err != nil {
		return [32]byte{}, nil, err
	}
	hash, err := fold(root, 0)
	return hash, append(slices.Concat(storageParts...), accountParts...), err
}

func (g graph) planAccounts(ctx commitment.PatriciaContext, accounts []accountEntry, plan foldPlan) (*node, []accountPlan, error) {
	root, err := g.loadRoot(ctx)
	if err != nil {
		return nil, nil, err
	}
	plans, err := makeAccountPlans(ctx, g, root, accounts, plan)
	return root, plans, err
}

func parallelFor(n, workers, chunk int, fn func(i int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(workers, (n+chunk-1)/chunk) {
		wg.Go(func() {
			for {
				lo := int(next.Add(int64(chunk))) - chunk
				if lo >= n {
					return
				}
				for i := lo; i < min(lo+chunk, n); i++ {
					fn(i)
				}
			}
		})
	}
	wg.Wait()
}

func (g graph) accountPlanFor(ctx commitment.PatriciaContext, root *node, entry accountEntry) (accountPlan, error) {
	oldValue, found := accountLeafAt(root, entry.hashedKey)
	if !found && storedAccountPath(root, entry.hashedKey) {
		if err := g.ensurePath(ctx, root, entry.hashedKey); err != nil {
			return accountPlan{}, fmt.Errorf("%w: account path %x: %w", errPhaseBRecord, entry.hashedKey, err)
		}
		oldValue, found = accountLeafAt(root, entry.hashedKey)
	}
	plan := accountPlan{entry: entry, oldValue: oldValue, found: found}
	switch {
	case entry.update != nil && entry.update.Deleted():
		plan.delete = found
		plan.skip = !found
	case !entry.storageDirty && (entry.update == nil || entry.update.Flags == 0):
		plan.skip = true
	case !found && entry.update != nil && entry.update.Flags == 0:
		plan.skip = true
	}
	return plan, nil
}

func (g graph) ensureRootChildren(ctx commitment.PatriciaContext, root *node, nibs []int) error {
	for _, nib := range nibs {
		bit := uint16(1) << nib
		if root.childMask&bit == 0 || root.leafMask&bit != 0 || root.child(nib) != nil {
			continue
		}
		if !root.hasChildHash(nib) {
			return g.errNode
		}
		childPath := append(append([]byte(nil), root.path...), byte(nib))
		childPath = append(childPath, root.childExtAt(nib)...)
		child, err := g.unfoldChild(ctx, childPath)
		if err != nil {
			return err
		}
		root.setChild(nib, child)
	}
	return nil
}

func makeAccountPlans(ctx commitment.PatriciaContext, g graph, root *node, entries []accountEntry, plan foldPlan) ([]accountPlan, error) {
	plans := make([]accountPlan, len(entries))
	planOne := func(ctx commitment.PatriciaContext, i int) error {
		p, err := g.accountPlanFor(ctx, root, entries[i])
		plans[i] = p
		return err
	}
	fanned, err := g.fanOutRoot(ctx, root, len(entries), func(i int) byte { return entries[i].hashedKey[0] }, plan, planOne)
	if err != nil || fanned {
		return plans, err
	}
	for i := range entries {
		if err := planOne(ctx, i); err != nil {
			return nil, err
		}
	}
	return plans, nil
}

func (g graph) fanOutRoot(ctx commitment.PatriciaContext, root *node, n int, nibOf func(i int) byte, plan foldPlan, fn func(ctx commitment.PatriciaContext, i int) error) (bool, error) {
	if !plan.parallel() || len(root.path) != 0 {
		return false, nil
	}
	var groups [16][]int
	for i := range n {
		nib := nibOf(i)
		groups[nib] = append(groups[nib], i)
	}
	nibs := make([]int, 0, 16)
	for nib := range groups {
		if len(groups[nib]) != 0 {
			nibs = append(nibs, nib)
		}
	}
	if len(nibs) < 2 {
		return false, nil
	}
	if err := g.ensureRootChildren(ctx, root, nibs); err != nil {
		return true, err
	}
	eg, egCtx := errgroup.WithContext(plan.ctx)
	eg.SetLimit(min(plan.workers, len(nibs)))
	for _, nib := range nibs {
		eg.Go(func() error {
			workerCtx, cleanup := plan.factory(egCtx)
			if cleanup != nil {
				defer cleanup()
			}
			if workerCtx == nil {
				return g.errNode
			}
			for _, i := range groups[nib] {
				if err := fn(workerCtx, i); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return true, eg.Wait()
}
