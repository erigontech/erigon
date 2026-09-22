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
	"math/bits"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

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
	ordered := slices.Clone(tasks)
	switch order {
	case orderReversed:
		slices.Reverse(ordered)
	case orderLongestFirst:
		slices.SortStableFunc(ordered, func(a, b storageTask) int {
			return len(b.entries) - len(a.entries)
		})
	}
	return ordered
}

func runScheduledPhases(ctx context.Context, rawCtx commitment.PatriciaContext, storage []storageTask, accounts []accountEntry, order scheduleOrder, workers int, stats *scheduleStats) ([32]byte, error) {
	if rawCtx == nil {
		return [32]byte{}, errors.New("commitment v4: nil scheduled context")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	safeCtx := &lockedPatriciaContext{ctx: rawCtx}
	storage = orderedStorageTasks(storage, order)

	storageRoots := make([][32]byte, len(storage))
	sg, sgCtx := errgroup.WithContext(ctx)
	sg.SetLimit(workers)
	for i, task := range storage {
		sg.Go(func() error {
			if sgCtx.Err() != nil {
				return sgCtx.Err()
			}
			stats.enter()
			defer stats.leave()
			var err error
			storageRoots[i], err = runStorageTask(safeCtx, task)
			return err
		})
	}
	if err := sg.Wait(); err != nil {
		return [32]byte{}, err
	}
	results := make(map[[32]byte][32]byte, len(storage))
	for i, task := range storage {
		results[task.addrHash] = storageRoots[i]
	}

	g := accountGraph()
	root, err := unfold(safeCtx, nil, planeAccount, nil, g.scratch)
	if err != nil {
		return [32]byte{}, err
	}
	if root == nil {
		root = fork(nil)
	}
	root.plane = planeAccount
	before := g.reachableRecordKeys(root)
	plans, err := makeAccountPlans(safeCtx, g, root, accounts)
	if err != nil {
		return [32]byte{}, err
	}

	accountResults := make([]accountResult, len(plans))
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
			accountResults[i].value = encodeAccountLeaf(update, storageRoot[:], nil)
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
		if len(root.path) != 0 && !bytes.HasPrefix(result.plan.entry.hashedKey, root.path) && bits.OnesCount16(root.childMask) == 1 && root.leafMask == 0 {
			if err := materializeAccountRootChild(safeCtx, root); err != nil {
				return [32]byte{}, err
			}
		}
		if len(root.path) == 0 {
			if err := insert(root, result.plan.entry.hashedKey, result.value); err != nil {
				return [32]byte{}, err
			}
		} else if err := insertRoot(root, result.plan.entry.hashedKey, result.value); err != nil {
			return [32]byte{}, err
		}
	}
	if err := g.persistGraph(safeCtx, root, before); err != nil {
		return [32]byte{}, err
	}
	return fold(root, 0)
}

func makeAccountPlans(ctx commitment.PatriciaContext, g graph, root *node, entries []accountEntry) ([]accountPlan, error) {
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
			if err := g.ensurePath(ctx, root, entry.hashedKey); err != nil {
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
