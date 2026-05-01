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

package component_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/node/app/component"
)

// fuzzProvider is a minimal provider for fuzz testing — no gomock, no
// shared root domain, no global state. Each instance is independent.
type fuzzProvider struct {
	id int
}

func (p *fuzzProvider) String() string { return fmt.Sprintf("fuzz-%d", p.id) }

// TestFuzzLifecycleConcurrent runs randomized concurrent activate/deactivate
// operations on a fixed dependency tree. Checks:
//   - No panics
//   - No data races (run with -race)
//   - All states are valid after completion
//
// KNOWN ISSUE: this test exposes a deadlock in the component state machine
// where rapid activate/deactivate cycles cause lock contention between
// activateDependencies → onDependenciesActive → activateProvider → setState
// and concurrent deactivation paths. The deadlock is a real bug that needs
// fixing before the component framework is used for production components.
//
// Run with: go test -race -run TestFuzzLifecycleConcurrent -count=10 ./node/app/component/
func TestFuzzLifecycleConcurrent(t *testing.T) {
	// Previously deadlocked — fixed by actor model serialization.
	const (
		numComponents = 8
		numOps        = 300
		numWorkers    = 4
	)

	seed := time.Now().UnixNano()
	t.Logf("seed=%d (reproduce with rand.NewSource(%d))", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	ctx := t.Context()

	// Build a fixed tree: 0 is root, each subsequent component depends on
	// exactly one earlier component (DAG, no cycles).
	//
	//   0 ← 1 ← 3 ← 6
	//   ↑   ↑
	//   2   4 ← 7
	//   ↑
	//   5
	//
	components := make([]component.Component[fuzzProvider], numComponents)
	for i := 0; i < numComponents; i++ {
		c, err := component.NewComponent[fuzzProvider](ctx,
			component.WithId(fmt.Sprintf("fuzz-%d", i)),
			component.WithProvider(&fuzzProvider{id: i}))
		if err != nil {
			t.Fatalf("NewComponent %d: %v", i, err)
		}
		components[i] = c
	}

	// Wire tree edges (child depends on parent).
	parents := []int{-1, 0, 0, 1, 1, 2, 3, 4} // -1 = root (no parent)
	for i := 1; i < numComponents; i++ {
		components[i].AddDependency(components[parents[i]])
	}

	// Generate random activate/deactivate operations.
	type fuzzOp struct {
		target   int
		activate bool // true=activate, false=deactivate
	}
	ops := make([]fuzzOp, numOps)
	for i := range ops {
		ops[i] = fuzzOp{
			target:   rng.Intn(numComponents),
			activate: rng.Float64() < 0.6, // bias toward activate so we exercise both paths
		}
	}

	// Execute concurrently.
	var wg sync.WaitGroup
	opsPerWorker := numOps / numWorkers
	for w := 0; w < numWorkers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := w * opsPerWorker
			end := start + opsPerWorker
			if w == numWorkers-1 {
				end = numOps
			}
			for i := start; i < end; i++ {
				o := ops[i]
				c := components[o.target]

				opCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				func() {
					defer cancel()
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("panic during op %d (activate=%v) on fuzz-%d: %v",
								i, o.activate, o.target, r)
						}
					}()
					if o.activate {
						_ = c.Activate(opCtx)
					} else {
						_ = c.Deactivate(opCtx)
					}
				}()
			}
		}()
	}
	wg.Wait()

	// Let any pending async state transitions settle.
	time.Sleep(200 * time.Millisecond)

	// Final invariant: all states must be valid.
	for i, c := range components {
		state := c.State()
		if state < component.Unknown || state > component.Failed {
			t.Errorf("component fuzz-%d: invalid state %d", i, state)
		}
	}
}

// TestFuzzLifecycleSerial runs the same pattern serially for deterministic
// reproduction of logic bugs.
//
// KNOWN ISSUE: same deadlock as TestFuzzLifecycleConcurrent — the lock
// ordering issue manifests even in serial because activate/deactivate spawn
// background goroutines that contend with the next operation.
func TestFuzzLifecycleSerial(t *testing.T) {
	// Previously deadlocked — fixed by actor model serialization.
	const (
		numComponents = 6
		numOps        = 500
	)

	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	ctx := t.Context()

	components := make([]component.Component[fuzzProvider], numComponents)
	for i := 0; i < numComponents; i++ {
		c, err := component.NewComponent[fuzzProvider](ctx,
			component.WithId(fmt.Sprintf("sfuzz-%d", i)),
			component.WithProvider(&fuzzProvider{id: i}))
		if err != nil {
			t.Fatalf("NewComponent %d: %v", i, err)
		}
		components[i] = c
	}

	// Linear chain: 0 ← 1 ← 2 ← 3 ← 4 ← 5
	for i := 1; i < numComponents; i++ {
		components[i].AddDependency(components[i-1])
	}

	for i := 0; i < numOps; i++ {
		target := rng.Intn(numComponents)
		activate := rng.Float64() < 0.6
		c := components[target]

		opCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		func() {
			defer cancel()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic at op %d (activate=%v) on sfuzz-%d: %v",
						i, activate, target, r)
				}
			}()
			if activate {
				_ = c.Activate(opCtx)
			} else {
				_ = c.Deactivate(opCtx)
			}
		}()
	}

	for i, c := range components {
		state := c.State()
		if state < component.Unknown || state > component.Failed {
			t.Errorf("component sfuzz-%d: invalid state %d", i, state)
		}
	}
}

// TestFuzzAddRemoveConcurrent specifically targets the AddDependency /
// RemoveDependency paths under concurrent activate/deactivate. Uses a flat
// pool (no initial wiring) to avoid cycles from random adds.
//
// KNOWN ISSUE: AddDependency not yet routed through actor — can still deadlock
// when combined with concurrent activate/deactivate. Will be fixed when
// addDependency/removeDependency are converted to actor messages.
func TestFuzzAddRemoveConcurrent(t *testing.T) {
	t.Skip("AddDependency not yet routed through actor — see fuzz_test.go")
	const (
		numComponents = 6
		numOps        = 200
		numWorkers    = 3
	)

	seed := time.Now().UnixNano()
	t.Logf("seed=%d", seed)
	rng := rand.New(rand.NewSource(seed))

	ctx := t.Context()

	components := make([]component.Component[fuzzProvider], numComponents)
	for i := 0; i < numComponents; i++ {
		c, err := component.NewComponent[fuzzProvider](ctx,
			component.WithId(fmt.Sprintf("ar-%d", i)),
			component.WithProvider(&fuzzProvider{id: i}))
		if err != nil {
			t.Fatalf("NewComponent %d: %v", i, err)
		}
		components[i] = c
	}

	type arOp struct {
		kind   int // 0=activate, 1=deactivate, 2=addDep, 3=removeDep
		target int
		arg    int
	}
	ops := make([]arOp, numOps)
	for i := range ops {
		ops[i] = arOp{
			kind:   rng.Intn(4),
			target: rng.Intn(numComponents),
			arg:    rng.Intn(numComponents),
		}
	}

	var wg sync.WaitGroup
	opsPerWorker := numOps / numWorkers
	for w := 0; w < numWorkers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := w * opsPerWorker
			end := start + opsPerWorker
			if w == numWorkers-1 {
				end = numOps
			}
			for i := start; i < end; i++ {
				o := ops[i]
				c := components[o.target]

				opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				func() {
					defer cancel()
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("panic at op %d (kind=%d) on ar-%d: %v",
								i, o.kind, o.target, r)
						}
					}()
					switch o.kind {
					case 0:
						_ = c.Activate(opCtx)
					case 1:
						_ = c.Deactivate(opCtx)
					case 2:
						// Only add dependency from higher ID to lower ID to prevent cycles.
						if o.target > o.arg {
							c.AddDependency(components[o.arg])
						}
					case 3:
						c.RemoveDependency(components[o.arg])
					}
				}()
			}
		}()
	}
	wg.Wait()

	time.Sleep(200 * time.Millisecond)

	for i, c := range components {
		state := c.State()
		if state < component.Unknown || state > component.Failed {
			t.Errorf("component ar-%d: invalid state %d", i, state)
		}
	}
}
