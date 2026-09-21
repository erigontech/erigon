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
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type scheduleOrder uint8

const (
	orderLongestFirst scheduleOrder = iota
	orderSequential
	orderReversed
)

type scheduleStats struct {
	inFlight atomic.Int64
	max      atomic.Int64
}

type workerSchedule struct {
	sem   chan struct{}
	stats *scheduleStats
}

func newWorkerSchedule(workers int, stats *scheduleStats) *workerSchedule {
	if workers <= 0 {
		workers = max(1, runtime.NumCPU())
	}
	return &workerSchedule{sem: make(chan struct{}, workers), stats: stats}
}

func (s *workerSchedule) run(ctx context.Context, fn func() error) error {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.stats != nil {
		current := s.stats.inFlight.Add(1)
		for {
			maximum := s.stats.max.Load()
			if current <= maximum || s.stats.max.CompareAndSwap(maximum, current) {
				break
			}
		}
	}
	defer func() {
		if s.stats != nil {
			s.stats.inFlight.Add(-1)
		}
		<-s.sem
	}()
	return fn()
}

type lockedPatriciaContext struct {
	mu  sync.Mutex
	ctx commitment.PatriciaContext
}

func (c *lockedPatriciaContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, step, err := c.ctx.Branch(key)
	return bytes.Clone(data), step, err
}

func (c *lockedPatriciaContext) PutBranch(key, data, prev []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctx.PutBranch(key, data, prev)
}

func (c *lockedPatriciaContext) Account(key []byte) (*commitment.Update, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update, err := c.ctx.Account(key)
	if update == nil {
		return nil, err
	}
	return update.Copy(), err
}

func (c *lockedPatriciaContext) Storage(key []byte) (*commitment.Update, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update, err := c.ctx.Storage(key)
	if update == nil {
		return nil, err
	}
	return update.Copy(), err
}

type storageResult struct {
	root [32]byte
	err  error
	done chan struct{}
}

func (r *storageResult) wait(ctx context.Context) ([32]byte, error) {
	select {
	case <-r.done:
		return r.root, r.err
	case <-ctx.Done():
		return [32]byte{}, ctx.Err()
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

func orderedStorageTasks(tasks []storageTask, order scheduleOrder) []storageTask {
	ordered := append([]storageTask(nil), tasks...)
	switch order {
	case orderReversed:
		slicesReverse(ordered)
	case orderLongestFirst:
		sort.SliceStable(ordered, func(i, j int) bool {
			return len(ordered[i].entries) > len(ordered[j].entries)
		})
	}
	return ordered
}

func slicesReverse[T any](items []T) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func runScheduledPhases(ctx context.Context, rawCtx commitment.PatriciaContext, storage []storageTask, accounts []accountEntry, order scheduleOrder, workers int, stats *scheduleStats) ([32]byte, error) {
	if rawCtx == nil {
		return [32]byte{}, errors.New("commitment v4: nil scheduled context")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	safeCtx := &lockedPatriciaContext{ctx: rawCtx}
	schedule := newWorkerSchedule(workers, stats)
	storage = orderedStorageTasks(storage, order)
	results := make(map[[32]byte]*storageResult, len(storage))
	for _, task := range storage {
		result := &storageResult{done: make(chan struct{})}
		results[task.addrHash] = result
		go func(task storageTask, result *storageResult) {
			err := schedule.run(ctx, func() error {
				var taskErr error
				result.root, taskErr = runStorageTask(safeCtx, task)
				return taskErr
			})
			result.err = err
			close(result.done)
		}(task, result)
	}
	storageWaited := false
	defer func() {
		if !storageWaited {
			waitStorageResults(results)
		}
	}()

	root, err := unfold(safeCtx, nil, planeAccount, nil)
	if err != nil {
		return [32]byte{}, err
	}
	if root == nil {
		root = fork(nil)
	}
	root.plane = planeAccount
	before := reachableAccountRecordKeys(root)
	plans, err := makeAccountPlans(safeCtx, root, accounts)
	if err != nil {
		return [32]byte{}, err
	}

	accountResults := make([]accountResult, len(plans))
	var wg sync.WaitGroup
	for i := range plans {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			plan := plans[i]
			accountResults[i].plan = plan
			if plan.skip || plan.delete {
				return
			}
			var storageRoot [32]byte = empty.RootHash
			if plan.entry.storageDirty {
				addrHash := hashAddressPath(plan.entry.hashedKey)
				result, ok := results[addrHash]
				if !ok {
					accountResults[i].err = errPhaseBRecord
					return
				}
				var waitErr error
				storageRoot, waitErr = result.wait(ctx)
				if waitErr != nil {
					accountResults[i].err = waitErr
					return
				}
				if !plan.found && plan.entry.update == nil && storageRoot == empty.RootHash {
					accountResults[i].plan.skip = true
					return
				}
			}
			accountResults[i].err = schedule.run(ctx, func() error {
				update, updateErr := accountUpdate(plan.oldValue, plan.found, plan.entry.update)
				if updateErr != nil {
					return updateErr
				}
				accountResults[i].value = encodeAccountLeaf(update, storageRoot[:], nil)
				return nil
			})
		}(i)
	}
	wg.Wait()
	waitStorageResults(results)
	storageWaited = true
	for _, result := range results {
		if result.err != nil {
			return [32]byte{}, result.err
		}
	}
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
		if len(root.path) != 0 && !bytes.HasPrefix(result.plan.entry.hashedKey, root.path) && rootBitsCount(root.childMask) == 1 && root.leafMask == 0 {
			if err := materializeAccountRootChild(safeCtx, root); err != nil {
				return [32]byte{}, err
			}
		}
		if len(root.path) == 0 {
			if err := insert(root, result.plan.entry.hashedKey, packPath(result.plan.entry.hashedKey[1:], nil), result.value); err != nil {
				return [32]byte{}, err
			}
		} else if err := insertRoot(root, result.plan.entry.hashedKey, result.value); err != nil {
			return [32]byte{}, err
		}
	}
	if err := persistAccountGraph(safeCtx, root, before); err != nil {
		return [32]byte{}, err
	}
	return fold(root, 0)
}

func makeAccountPlans(ctx commitment.PatriciaContext, root *node, entries []accountEntry) ([]accountPlan, error) {
	plans := make([]accountPlan, 0, len(entries))
	for _, entry := range entries {
		if len(entry.hashedKey) != 64 {
			return nil, errPhaseBKey
		}
		for _, nib := range entry.hashedKey {
			if nib > 0x0f {
				return nil, errPhaseBKey
			}
		}
		oldValue, found := accountLeafAt(root, entry.hashedKey)
		if !found && storedAccountPath(root, entry.hashedKey) {
			if err := ensureAccountPath(ctx, root, entry.hashedKey); err != nil {
				return nil, fmt.Errorf("%w: account path %x: %w", errPhaseBRecord, entry.hashedKey, err)
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
		plans = append(plans, plan)
	}
	return plans, nil
}

func waitStorageResults(results map[[32]byte]*storageResult) {
	for _, result := range results {
		<-result.done
	}
}
