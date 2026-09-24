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
	value []byte
	err   error
}

const (
	storageOversubscribe = 1
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
	storageWorkers, accountWorkers := workers, workers
	if workers <= 0 {
		workers = runtime.NumCPU()
		storageWorkers = workers * storageOversubscribe
		accountWorkers = min(workers, accountPlaneFanout)
	}
	storageRoots := make([][32]byte, len(storage))
	storageParts := make([]deltaParts, len(storage))
	accountFold := foldPlan{ctx: ctx, factory: factory, workers: accountWorkers}
	storageDone := make(chan error, 1)
	if factory != nil && storageWorkers > 1 {
		go func() {
			storageDone <- runStoragePhase(ctx, rawCtx, factory, storage, storageRoots, storageParts, storageWorkers, fanOutMin)
		}()
	} else {
		storageDone <- runStoragePhase(ctx, rawCtx, factory, storage, storageRoots, storageParts, storageWorkers, fanOutMin)
	}

	g := graph{plane: planeAccount}
	root, err := g.loadRoot(rawCtx)
	if err != nil {
		<-storageDone
		return [32]byte{}, nil, err
	}
	plans := make([]accountPlan, len(accounts))
	accountResults := make([]accountResult, len(plans))
	accountValues := make([]byte, accountLeafScratch*len(plans))
	storageReady := make(chan struct{})
	var storageErr error
	var results map[[32]byte][32]byte
	go func() {
		defer close(storageReady)
		if storageErr = <-storageDone; storageErr != nil {
			return
		}
		results = make(map[[32]byte][32]byte, len(storage))
		for i, task := range storage {
			results[task.addrHash] = storageRoots[i]
		}
	}()
	encode := func(i int) {
		plan := &plans[i]
		if plan.skip || plan.delete {
			return
		}
		storageRoot := empty.RootHash
		if plan.entry.storageDirty {
			var ok bool
			storageRoot, ok = results[hashAddressPath(plan.entry.hashedKey)]
			if !ok {
				accountResults[i].err = errNodeRecord
				return
			}
			if !plan.found && plan.entry.update == nil && storageRoot == empty.RootHash {
				plan.skip = true
				return
			}
		}
		update, existingRoot, updateErr := accountUpdate(plan.oldValue, plan.found, plan.entry.update)
		if updateErr != nil {
			accountResults[i].err = updateErr
			return
		}
		if !plan.entry.storageDirty && plan.found {
			copy(storageRoot[:], existingRoot)
		}
		at := i * accountLeafScratch
		accountResults[i].value = encodeAccountLeaf(update, storageRoot[:], accountValues[at:at:at+accountLeafScratch])
	}

	var pipelined [16]bool
	var groupHashes [16][32]byte
	var groupParts [16]deltaParts
	var pending [16]*pendingRemoval
	pipeline := func(ctx commitment.PatriciaContext, nib int, group []int) error {
		<-storageReady
		child := root.child(nib)
		if storageErr != nil || len(root.path) != 0 || child == nil || len(child.path) != 1 {
			return nil
		}
		for k, i := range group {
			encode(i)
			if err := accountResults[i].err; err != nil {
				return err
			}
			plan := &plans[i]
			switch {
			case plan.skip:
			case plan.delete:
				state, err := remove(child, plan.entry.hashedKey)
				if err != nil {
					return err
				}
				if state.kind != removalKeep {
					pending[nib] = &pendingRemoval{state: state, rest: group[k+1:]}
					return nil
				}
			default:
				if err := insert(child, plan.entry.hashedKey, accountResults[i].value); err != nil {
					return err
				}
			}
		}
		hash, err := g.materialize(ctx, child, root, &groupParts[nib])
		if err != nil {
			return err
		}
		groupHashes[nib], pipelined[nib] = hash, true
		return nil
	}
	planErr := g.planAccounts(rawCtx, root, accounts, plans, accountFold, pipeline)
	<-storageReady
	if storageErr != nil {
		return [32]byte{}, nil, storageErr
	}
	if planErr != nil {
		return [32]byte{}, nil, planErr
	}

	remaining := make([]int, 0, len(plans))
	for i := range plans {
		if nib := plans[i].entry.hashedKey[0]; !pipelined[nib] && pending[nib] == nil {
			remaining = append(remaining, i)
		}
	}
	for nib := range 16 {
		switch {
		case pipelined[nib]:
			root.setStoredChild(nib, groupHashes[nib][:], nil)
		case pending[nib] != nil:
			applyRemoval(root, nib, pending[nib].state)
			collapsedState(root)
			remaining = append(remaining, pending[nib].rest...)
		}
	}
	slices.Sort(remaining)
	parallelFor(len(remaining), workers, 1024, func(k int) { encode(remaining[k]) })
	var groups [16][]int
	var rest []int
	for _, i := range remaining {
		if err := accountResults[i].err; err != nil {
			return [32]byte{}, nil, err
		}
		plan := &plans[i]
		if plan.skip {
			continue
		}
		nib := plan.entry.hashedKey[0]
		if child := root.child(int(nib)); !plan.delete && len(root.path) == 0 && child != nil && len(child.path) == len(root.path)+1 {
			groups[nib] = append(groups[nib], i)
			continue
		}
		rest = append(rest, i)
	}
	var groupErrs [16]error
	parallelFor(16, workers, 1, func(nib int) {
		for _, i := range groups[nib] {
			if err := insert(root, plans[i].entry.hashedKey, accountResults[i].value); err != nil {
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
		hashedKey := plans[i].entry.hashedKey
		if plans[i].delete {
			if err := removeRoot(root, hashedKey); err != nil {
				return [32]byte{}, nil, err
			}
			continue
		}
		if len(root.path) == 0 {
			if err := insert(root, hashedKey, accountResults[i].value); err != nil {
				return [32]byte{}, nil, err
			}
		} else if err := insertRoot(root, hashedKey, accountResults[i].value); err != nil {
			return [32]byte{}, nil, err
		}
	}
	accountParts, err := g.persistGraph(rawCtx, root, accountFold)
	if err != nil {
		return [32]byte{}, nil, err
	}
	hash, err := fold(root, 0)
	return hash, slices.Concat(slices.Concat(storageParts...), slices.Concat(groupParts[:]...), accountParts), err
}

type pendingRemoval struct {
	state removalState
	rest  []int
}

func (g graph) planAccounts(ctx commitment.PatriciaContext, root *node, accounts []accountEntry, plans []accountPlan, plan foldPlan, after func(ctx commitment.PatriciaContext, nib int, group []int) error) error {
	planOne := func(ctx commitment.PatriciaContext, i int) error {
		p, err := g.accountPlanFor(ctx, root, accounts[i])
		plans[i] = p
		return err
	}
	fanned, err := g.fanOutRoot(ctx, root, len(accounts), func(i int) byte { return accounts[i].hashedKey[0] }, plan, planOne, after)
	if err != nil || fanned {
		return err
	}
	for i := range accounts {
		if err := planOne(ctx, i); err != nil {
			return err
		}
	}
	return nil
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
	oldValue, found, stored := accountLeafAt(root, entry.hashedKey)
	if stored {
		if err := g.ensurePath(ctx, root, entry.hashedKey); err != nil {
			return accountPlan{}, fmt.Errorf("%w: account path %x: %w", errNodeRecord, entry.hashedKey, err)
		}
		oldValue, found, _ = accountLeafAt(root, entry.hashedKey)
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
			return errNodeRecord
		}
		child, err := g.unfoldChild(ctx, root.childPath(nib, nil))
		if err != nil {
			return err
		}
		root.setChild(nib, child)
	}
	return nil
}

func (g graph) fanOutRoot(ctx commitment.PatriciaContext, root *node, n int, nibOf func(i int) byte, plan foldPlan, fn func(ctx commitment.PatriciaContext, i int) error, after func(ctx commitment.PatriciaContext, nib int, group []int) error) (bool, error) {
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
				return errNodeRecord
			}
			for _, i := range groups[nib] {
				if err := fn(workerCtx, i); err != nil {
					return err
				}
			}
			if after != nil {
				return after(workerCtx, nib, groups[nib])
			}
			return nil
		})
	}
	return true, eg.Wait()
}
