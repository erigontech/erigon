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

package commitment

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// A zero-value field means "unset", so the constructor's fallbacks must resolve
// to DefaultAdaptivePinControllerConfig — one policy, not two that drift apart
// when the defaults are tuned.
func TestNewAdaptivePinController_ZeroConfigResolvesToDefaults(t *testing.T) {
	c := NewAdaptivePinController(NewBranchCache(64), AdaptivePinControllerConfig{}, log.Root())
	if want := DefaultAdaptivePinControllerConfig(); c.cfg != want {
		t.Fatalf("zero-value config resolved to %+v, want %+v", c.cfg, want)
	}
}

// An explicitly-set field must survive the fallbacks.
func TestNewAdaptivePinController_ExplicitConfigWins(t *testing.T) {
	cfg := AdaptivePinControllerConfig{
		PromoteThresholdMisses:    7,
		MaxPromotedContracts:      3,
		DemoteCooldownBlocks:      11,
		InitialViewBudgetBytes:    1 << 20,
		ExtensionBudgetBytes:      2 << 20,
		PerContractMaxBudgetBytes: 3 << 20,
	}
	c := NewAdaptivePinController(NewBranchCache(64), cfg, log.Root())
	if c.cfg != cfg {
		t.Fatalf("explicit config was overwritten: got %+v, want %+v", c.cfg, cfg)
	}
}

func TestAdaptivePinPlanDoesNotMutateCacheBeforePublication(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	publisher := branchCache.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.MaxPromotedContracts = 1
	controller := NewAdaptivePinController(branchCache, cfg, log.Root())

	var contractHash [32]byte
	contractHash[0] = 1
	prefix := nibbles.HexToCompact(ContractNibbles(contractHash[:]))
	controller.onCacheMiss(prefix)
	reader := func(key []byte) ([]byte, uint64, bool, error) {
		if !bytes.Equal(key, prefix) {
			return nil, 0, false, nil
		}
		return []byte{0, 0, 0, 0}, 1, true, nil
	}

	plan := controller.PlanBlock(1, testBranchGeneration(1), reader, nil, nil)
	_, _, ok := branchCache.Get(prefix)
	require.False(t, ok, "planning from an uncommitted transaction must not change BranchCache")
	plan.Abort()
	require.Empty(t, controller.states, "aborting the database transaction must restore controller state")

	plan = controller.PlanBlock(2, testBranchGeneration(1), reader, nil, nil)
	publication := publisher.Begin()
	publication.Publish(testBranchGeneration(2), nil, false, plan)
	plan.Commit()

	_, _, ok = branchCache.View(testBranchGeneration(2)).Get(prefix)
	require.True(t, ok, "publication must apply the staged pin")
}

func TestAdaptivePinPlanIsDiscardedAfterFilesPublication(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	publisher := branchCache.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.MaxPromotedContracts = 1
	controller := NewAdaptivePinController(branchCache, cfg, log.Root())

	var contractHash [32]byte
	contractHash[0] = 1
	prefix := nibbles.HexToCompact(ContractNibbles(contractHash[:]))
	controller.onCacheMiss(prefix)
	reader := func(key []byte) ([]byte, uint64, bool, error) {
		if !bytes.Equal(key, prefix) {
			return nil, 0, false, nil
		}
		return []byte{0, 0, 0, 0}, 1, true, nil
	}
	plan := controller.PlanBlock(1, testBranchGeneration(1), reader, nil, nil)

	change := branchCache.BeginFilesPublication(100)
	require.NotNil(t, change)
	change.Finish()

	committedKey := []byte{0x01}
	committedValue := []byte{0xaa}
	publication := publisher.Begin()
	publication.Publish(testBranchGeneration(2), []BranchUpdate{{
		Key:   committedKey,
		Value: committedValue,
		Step:  2,
		TxNum: 100,
	}}, false, plan)
	plan.Commit()

	current := branchCache.View(cache.BranchGeneration(2, 100))
	got, _, ok := current.Get(committedKey)
	require.True(t, ok, "commit publication must retain the files identity published while the plan was prepared")
	require.Equal(t, committedValue, got)
	_, _, ok = current.Get(prefix)
	require.False(t, ok, "a pin prepared from the previous files must not enter the new generation")
	require.Empty(t, controller.states, "discarding the stale plan must also discard its residency state")
}

func TestAdaptivePinPlanSkipsStaleSourceAfterFilesPublication(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	publisher := branchCache.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.MaxPromotedContracts = 1
	controller := NewAdaptivePinController(branchCache, cfg, log.Root())

	change := branchCache.BeginFilesPublication(100)
	require.NotNil(t, change)
	change.Finish()

	var contractHash [32]byte
	contractHash[0] = 1
	prefix := nibbles.HexToCompact(ContractNibbles(contractHash[:]))
	controller.onCacheMiss(prefix)
	readerCalls := 0
	reader := func(key []byte) ([]byte, uint64, bool, error) {
		readerCalls++
		if !bytes.Equal(key, prefix) {
			return nil, 0, false, nil
		}
		return []byte{0, 0, 0, 0}, 1, true, nil
	}

	plan := controller.PlanBlock(1, testBranchGeneration(1), reader, nil, nil)
	require.Nil(t, plan, "a transaction pinned to the old files cannot prepare a plan for the new cache generation")
	require.Zero(t, readerCalls, "a plan that cannot be published must not scan branches")
}

func TestAdaptivePinPlanPanicRestoresController(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	branchCache.Publisher().Initialize(testBranchGeneration(1))
	controller := NewAdaptivePinController(branchCache, DefaultAdaptivePinControllerConfig(), log.Root())

	var contractHash [32]byte
	contractHash[0] = 1
	previousState := &adaptiveContractState{contractHash: contractHash}
	controller.states[contractHash] = previousState
	controller.onCacheMiss(nibbles.HexToCompact(ContractNibbles(contractHash[:])))

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		controller.PlanBlock(1, testBranchGeneration(1), nil, func() (BatchBranchResolver, func(), error) {
			panic("factory failed")
		}, nil)
	}()

	require.Equal(t, "factory failed", recovered)
	require.True(t, controller.mu.TryLock(), "planning panic must release the controller")
	controller.mu.Unlock()
	require.Same(t, previousState, controller.states[contractHash])
	require.Equal(t, uint64(1), controller.snapshotMisses()[contractHash])
}

func TestAdaptivePinControllerForgetsPinsClearedByFilesPublication(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	publisher := branchCache.Publisher()
	publisher.Initialize(testBranchGeneration(1))

	cfg := DefaultAdaptivePinControllerConfig()
	cfg.PromoteThresholdMisses = 1
	cfg.MaxPromotedContracts = 1
	cfg.DemoteCooldownBlocks = 100
	controller := NewAdaptivePinController(branchCache, cfg, log.Root())

	var contractHash [32]byte
	contractHash[0] = 1
	prefix := nibbles.HexToCompact(ContractNibbles(contractHash[:]))
	controller.onCacheMiss(prefix)
	reader := func(key []byte) ([]byte, uint64, bool, error) {
		if !bytes.Equal(key, prefix) {
			return nil, 0, false, nil
		}
		return []byte{0, 0, 0, 0}, 1, true, nil
	}

	plan := controller.PlanBlock(1, testBranchGeneration(1), reader, nil, nil)
	publication := publisher.Begin()
	publication.Publish(testBranchGeneration(2), nil, false, plan)
	plan.Commit()
	require.NotEmpty(t, controller.states)

	change := branchCache.BeginFilesPublication(100)
	require.NotNil(t, change)
	change.Finish()

	currentGeneration := cache.BranchGeneration(2, 100)
	plan = controller.PlanBlock(2, currentGeneration, reader, nil, nil)
	publication = publisher.Begin()
	publication.Publish(cache.BranchGeneration(3, 100), nil, false, plan)
	plan.Commit()

	require.Empty(t, controller.states, "residency state must not outlive the BranchCache entries it describes")
}

func TestAdaptivePinControllerDelayedResetPreservesSynchronizedState(t *testing.T) {
	branchCache := NewBranchCache(64)
	t.Cleanup(branchCache.Close)
	controller := NewAdaptivePinController(branchCache, DefaultAdaptivePinControllerConfig(), log.Root())

	branchCache.Reset()
	controller.mu.Lock()
	controller.syncCacheClearLocked()
	var contractHash [32]byte
	contractHash[0] = 1
	state := &adaptiveContractState{contractHash: contractHash}
	controller.states[contractHash] = state
	controller.mu.Unlock()

	controller.ResetAfterCacheClear()

	require.Same(t, state, controller.states[contractHash], "a delayed reset must not discard state built after the cache clear")
}

// The trunk-preload counters are the only signal for how much work the adaptive
// pin controller is doing, so promotion must feed them. Asserting the byte
// counter is enough to prove recordPreload ran: nothing else writes it.
func TestAdaptivePin_PromoteRecordsPreloadMetrics(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	bytesBefore := mxPreloadBytesTotal.GetValue()

	c := NewAdaptivePinController(NewBranchCache(64), AdaptivePinControllerConfig{}, log.Root())
	var h [32]byte
	copy(h[:], hash)
	mutations := adaptiveCacheMutations{}

	c.mu.Lock()
	state, err := c.promoteLocked(h, resolve, nil, nil, &mutations)
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if state.usedBytes() == 0 {
		t.Fatal("promote pinned nothing, so the metric assertions below would be vacuous")
	}

	if got := mxPreloadBytesTotal.GetValue() - bytesBefore; got <= 0 {
		t.Errorf("commitment_trunk_preload_bytes_total advanced by %v after promoting a contract, want > 0", got)
	}
}

// Extensions are the dominant preload path in a running node, so they must be
// counted too, not just the one-off promote.
func TestAdaptivePin_ExtendRecordsPreloadMetrics(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	resolve := fakeResolver(tree, nil, 100, "")

	// Budget the initial view so the queue survives promotion and an extension
	// has something left to pin.
	cfg := AdaptivePinControllerConfig{InitialViewBudgetBytes: minEntryBytes + 1}
	c := NewAdaptivePinController(NewBranchCache(64), cfg, log.Root())
	var h [32]byte
	copy(h[:], hash)
	mutations := adaptiveCacheMutations{}

	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.promoteLocked(h, resolve, nil, nil, &mutations)
	if err != nil {
		t.Fatal(err)
	}
	if state.queueRemaining() == 0 {
		t.Fatal("initial view drained the queue, so there is no extension to measure")
	}

	bytesBefore := mxPreloadBytesTotal.GetValue()

	if err := c.runExtensionLocked(state, 1<<20, resolve, nil, nil, &mutations); err != nil {
		t.Fatal(err)
	}

	if got := mxPreloadBytesTotal.GetValue() - bytesBefore; got <= 0 {
		t.Errorf("commitment_trunk_preload_bytes_total advanced by %v after an extension, want > 0", got)
	}
}

// The elapsed time cannot be asserted through a real preload: the work takes
// microseconds, and a coarse platform timer rounds that to zero. Drive
// recordPreload with a known elapsed time instead.
func TestRecordPreload_RecordsElapsedAndBytes(t *testing.T) {
	const elapsed = 50 * time.Millisecond

	for _, tc := range []struct {
		name        string
		bytesPinned int
		wantBytes   float64
	}{
		{"pinned bytes are counted", 4096, 4096},
		// A rolled-back step reports the time it cost without the pins it lost.
		{"a step that pinned nothing still counts its time", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bytesBefore := mxPreloadBytesTotal.GetValue()
			secondsBefore := mxPreloadDurationSecondsTotal.GetValue()

			recordPreload(time.Now().Add(-elapsed), tc.bytesPinned)

			if got := mxPreloadBytesTotal.GetValue() - bytesBefore; got != tc.wantBytes {
				t.Errorf("commitment_trunk_preload_bytes_total advanced by %v, want %v", got, tc.wantBytes)
			}
			if got := mxPreloadDurationSecondsTotal.GetValue() - secondsBefore; got < elapsed.Seconds() {
				t.Errorf("commitment_trunk_preload_duration_seconds_total advanced by %v, want >= %v", got, elapsed.Seconds())
			}
		})
	}
}
