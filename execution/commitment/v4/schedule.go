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
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

type scheduleStats struct {
	inFlight atomic.Int64
	max      atomic.Int64
}

func (s *scheduleStats) enter() {
	if s == nil {
		return
	}
	current := s.inFlight.Add(1)
	for {
		maximum := s.max.Load()
		if current <= maximum || s.max.CompareAndSwap(maximum, current) {
			return
		}
	}
}

func (s *scheduleStats) leave() {
	if s != nil {
		s.inFlight.Add(-1)
	}
}

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

const storageOversubscribe = 4

func runStoragePhase(ctx context.Context, rawCtx commitment.PatriciaContext, factory commitment.TrieContextFactory, storage []storageTask, roots [][32]byte, workers int, stats *scheduleStats) error {
	if len(storage) == 0 {
		return nil
	}
	if factory == nil || workers <= 1 {
		for i, task := range storage {
			if err := ctx.Err(); err != nil {
				return err
			}
			root, err := runStorageTask(rawCtx, task)
			if err != nil {
				return err
			}
			roots[i] = root
		}
		return nil
	}

	workers = min(workers, len(storage))
	var next atomic.Int64
	g, gCtx := errgroup.WithContext(ctx)
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
				stats.enter()
				root, err := runStorageTask(workerCtx, storage[i])
				stats.leave()
				if err != nil {
					return err
				}
				roots[i] = root
			}
		})
	}
	return g.Wait()
}

func runScheduledPhases(ctx context.Context, rawCtx commitment.PatriciaContext, factory commitment.TrieContextFactory, storage []storageTask, accounts []accountEntry, workers int, stats *scheduleStats) ([32]byte, error) {
	if rawCtx == nil {
		return [32]byte{}, errors.New("commitment v4: nil scheduled context")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	storageWorkers := workers
	if workers <= 0 {
		workers = runtime.NumCPU()
		storageWorkers = workers * storageOversubscribe
	}
	storageRoots := make([][32]byte, len(storage))
	accountFold := foldPlan{ctx: ctx, factory: factory, workers: storageWorkers}
	if err := runStoragePhase(ctx, rawCtx, factory, storage, storageRoots, storageWorkers, stats); err != nil {
		return [32]byte{}, err
	}
	results := make(map[[32]byte][32]byte, len(storage))
	for i, task := range storage {
		results[task.addrHash] = storageRoots[i]
	}

	root, err := unfold(rawCtx, nil, planeAccount, nil)
	if err != nil {
		return [32]byte{}, err
	}
	if root == nil {
		root = fork(nil)
		root.loaded = true
	}
	root.plane = planeAccount
	before := new(keySet)
	g := accountGraph(before)
	g.reachableRecordKeys(root, before)
	if err := g.materializeRootExtension(rawCtx, root); err != nil {
		return [32]byte{}, err
	}
	plans, err := makeAccountPlans(rawCtx, g, root, accounts, accountFold)
	if err != nil {
		return [32]byte{}, err
	}

	accountResults := make([]accountResult, len(plans))
	accountValues := make([]byte, accountLeafScratch*len(plans))
	ag := new(errgroup.Group)
	ag.SetLimit(workers)
	for i := range plans {
		ag.Go(func() error {
			plan := plans[i]
			accountResults[i].plan = plan
			if plan.skip || plan.delete {
				return nil
			}
			storageRoot := empty.RootHash
			if plan.entry.storageDirty {
				var ok bool
				storageRoot, ok = results[hashAddressPath(plan.entry.hashedKey)]
				if !ok {
					accountResults[i].err = errPhaseBRecord
					return nil
				}
				if !plan.found && plan.entry.update == nil && storageRoot == empty.RootHash {
					accountResults[i].plan.skip = true
					return nil
				}
			} else if plan.found {
				_, _, _, existingRoot, decodeErr := decodeAccountLeaf(plan.oldValue)
				if decodeErr != nil {
					accountResults[i].err = fmt.Errorf("%w: %w", errPhaseBRecord, decodeErr)
					return nil
				}
				copy(storageRoot[:], existingRoot)
			}
			stats.enter()
			defer stats.leave()
			update, updateErr := accountUpdate(plan.oldValue, plan.found, plan.entry.update)
			if updateErr != nil {
				accountResults[i].err = updateErr
				return nil
			}
			at := i * accountLeafScratch
			accountResults[i].value = encodeAccountLeaf(update, storageRoot[:], accountValues[at:at:at+accountLeafScratch])
			return nil
		})
	}
	_ = ag.Wait()
	for i := range accountResults {
		result := &accountResults[i]
		if result.err != nil {
			return [32]byte{}, result.err
		}
		if result.plan.skip {
			continue
		}
		if result.plan.delete {
			if err := removeRoot(root, result.plan.entry.hashedKey); err != nil {
				return [32]byte{}, err
			}
			continue
		}
		if len(root.path) == 0 {
			if err := insert(root, result.plan.entry.hashedKey, result.value); err != nil {
				return [32]byte{}, err
			}
		} else if err := insertRoot(root, result.plan.entry.hashedKey, result.value); err != nil {
			return [32]byte{}, err
		}
	}
	if err := g.persistGraph(rawCtx, root, accountFold); err != nil {
		return [32]byte{}, err
	}
	return fold(root, 0)
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
	for i := range entries {
		if len(entries[i].hashedKey) != 64 {
			return nil, errPhaseBKey
		}
		for _, nib := range entries[i].hashedKey {
			if nib > 0x0f {
				return nil, errPhaseBKey
			}
		}
	}

	plans := make([]accountPlan, len(entries))
	groups := make(map[int][]int, 16)
	for i := range entries {
		nib := int(entries[i].hashedKey[0])
		groups[nib] = append(groups[nib], i)
	}

	if !plan.parallel() || len(root.path) != 0 || len(groups) < 2 {
		for i := range entries {
			p, err := g.accountPlanFor(ctx, root, entries[i])
			if err != nil {
				return nil, err
			}
			plans[i] = p
		}
		return plans, nil
	}

	nibs := make([]int, 0, len(groups))
	for nib := range groups {
		nibs = append(nibs, nib)
	}
	if err := g.ensureRootChildren(ctx, root, nibs); err != nil {
		return nil, err
	}

	unfolded := make([]keySet, len(nibs))
	eg, egCtx := errgroup.WithContext(plan.ctx)
	eg.SetLimit(min(plan.workers, len(nibs)))
	for k, nib := range nibs {
		eg.Go(func() error {
			workerCtx, cleanup := plan.factory(egCtx)
			if cleanup != nil {
				defer cleanup()
			}
			if workerCtx == nil {
				return g.errNode
			}
			wg := g
			wg.before = &unfolded[k]
			for _, i := range groups[nib] {
				p, err := wg.accountPlanFor(workerCtx, root, entries[i])
				if err != nil {
					return err
				}
				plans[i] = p
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	for k := range unfolded {
		g.before.addAll(&unfolded[k])
	}
	return plans, nil
}
