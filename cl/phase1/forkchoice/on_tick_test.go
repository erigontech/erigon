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

package forkchoice

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
)

// A stalled /eth/v1/events subscriber must not wedge fork choice: the finalized
// checkpoint event send has to run outside f.mu.
func TestFinalizedCheckpointEmittedOutsideLock(t *testing.T) {
	f := buildExAnteStore(t)

	delivered := make(chan *beaconevents.EventStream, 1)
	stalled := make(chan *beaconevents.EventStream)
	subA := f.emitters.State().Subscribe(delivered)
	subB := f.emitters.State().Subscribe(stalled)
	defer subA.Unsubscribe()
	defer subB.Unsubscribe()

	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	promoted := solid.Checkpoint{Epoch: 1, Root: root}
	f.unrealizedJustifiedCheckpoint.Store(promoted)
	f.unrealizedFinalizedCheckpoint.Store(promoted)

	tickDone := make(chan struct{})
	go func() {
		f.OnTick(f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot * 1000)
		close(tickDone)
	}()

	var ev *beaconevents.EventStream
	select {
	case ev = <-delivered:
	case <-time.After(10 * time.Second):
		t.Fatal("finalized checkpoint event was not sent")
	}
	require.Equal(t, beaconevents.StateFinalizedCheckpoint, ev.Event)

	// subB has not accepted the event, so the send is still in flight.
	lockFree := make(chan struct{})
	go func() {
		f.mu.Lock()
		_ = f.headHash
		f.mu.Unlock()
		close(lockFree)
	}()
	select {
	case <-lockFree:
	case <-time.After(10 * time.Second):
		t.Fatal("f.mu is held while the event send is blocked on a stalled subscriber")
	}

	select {
	case <-stalled:
	case <-time.After(10 * time.Second):
		t.Fatal("stalled subscriber never received the event")
	}
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("OnTick did not finish")
	}
	requireOperationPrunerIdle(t, f)
}

func TestTickBuildsUnrealizedJustifiedCheckpointState(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.JustifiedCheckpoint()
	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	next := solid.Checkpoint{Epoch: justified.Epoch + 1, Root: root}
	f.unrealizedJustifiedCheckpoint.Store(next)

	f.OnTick((f.beaconCfg.SlotsPerEpoch - 1) * f.beaconCfg.SecondsPerSlot * 1000)

	require.Equal(t, justified, f.JustifiedCheckpoint())
	require.Eventually(t, func() bool {
		_, ok := f.checkpointStates.Load(next)
		return ok
	}, 10*time.Second, 10*time.Millisecond)
}

func TestEpochStartTickBuildsPromotedCheckpointState(t *testing.T) {
	f := buildExAnteStore(t)
	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	next := solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch + 1, Root: root}
	f.unrealizedJustifiedCheckpoint.Store(next)

	f.OnTick(f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot * 1000)

	require.Equal(t, next, f.JustifiedCheckpoint())
	require.Eventually(t, func() bool {
		_, ok := f.checkpointStates.Load(next)
		return ok
	}, 10*time.Second, 10*time.Millisecond)
}

func TestTickSkipsCheckpointStateBuild(t *testing.T) {
	tests := []struct {
		name string
		tick func(f *ForkChoiceStore) uint64
	}{
		{name: "slot not advanced", tick: func(f *ForkChoiceStore) uint64 { return f.timeMs.Load() }},
		{name: "not synced", tick: func(f *ForkChoiceStore) uint64 {
			return (f.highestSeen.Load() + 2*f.beaconCfg.SlotsPerEpoch + 1) * f.beaconCfg.SecondsPerSlot * 1000
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := buildExAnteStore(t)
			_, root := decodeDiffBlock(t, diffBlockc2Enc)
			next := solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch + 1, Root: root}
			f.unrealizedJustifiedCheckpoint.Store(next)

			f.OnTick(tt.tick(f))

			require.Never(t, func() bool {
				_, ok := f.checkpointStates.Load(next)
				return ok
			}, time.Second, 10*time.Millisecond)
		})
	}
}

type blockingStateForkGraph struct {
	fork_graph.ForkGraph
	root    common.Hash
	calls   atomic.Int32
	release chan struct{}
}

func (g *blockingStateForkGraph) ReplayState(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	if blockRoot == g.root {
		g.calls.Add(1)
		<-g.release
	}
	return g.ForkGraph.ReplayState(blockRoot)
}

func TestConcurrentCheckpointStateMissesBuildOnce(t *testing.T) {
	f := buildExAnteStore(t)
	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	next := solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch + 1, Root: root}
	graph := &blockingStateForkGraph{ForkGraph: f.forkGraph, root: root, release: make(chan struct{})}
	f.forkGraph = graph
	release := sync.OnceFunc(func() { close(graph.release) })
	defer release()
	f.unrealizedJustifiedCheckpoint.Store(next)

	slot := f.Slot()
	var ticks sync.WaitGroup
	for i := uint64(1); i <= 3; i++ {
		ticks.Go(func() { f.OnTick((slot + i) * f.beaconCfg.SecondsPerSlot * 1000) })
	}
	ticks.Wait()
	built := make(chan error, 1)
	go func() {
		_, err := f.getCheckpointState(next)
		built <- err
	}()

	require.Eventually(t, func() bool { return graph.calls.Load() >= 1 }, 10*time.Second, time.Millisecond)
	require.Never(t, func() bool { return graph.calls.Load() > 1 }, 300*time.Millisecond, 5*time.Millisecond)
	release()

	require.NoError(t, <-built)
	_, ok := f.checkpointStates.Load(next)
	require.True(t, ok)
	require.Equal(t, int32(1), graph.calls.Load())
}

func TestFinalizationDropsCheckpointStateBelowIt(t *testing.T) {
	for _, tt := range []struct {
		name       string
		pruneFirst bool
	}{
		{name: "store then prune"},
		{name: "prune then store", pruneFirst: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := buildExAnteStore(t)
			_, root := decodeDiffBlock(t, diffBlockc2Enc)
			next := solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch + 1, Root: root}
			graph := &blockingStateForkGraph{ForkGraph: f.forkGraph, root: root, release: make(chan struct{})}
			f.forkGraph = graph
			release := sync.OnceFunc(func() { close(graph.release) })
			defer release()
			finalize := func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.updateCheckpoints(f.JustifiedCheckpoint(), solid.Checkpoint{Epoch: next.Epoch + 1, Root: root})
			}

			built := make(chan error, 1)
			go func() {
				_, err := f.getCheckpointState(next)
				built <- err
			}()
			if tt.pruneFirst {
				require.Eventually(t, func() bool { return graph.calls.Load() == 1 }, 10*time.Second, time.Millisecond)
				finalize()
				release()
				require.NoError(t, <-built)
			} else {
				release()
				require.NoError(t, <-built)
				_, ok := f.checkpointStates.Load(next)
				require.True(t, ok)
				finalize()
			}

			_, ok := f.checkpointStates.Load(next)
			require.False(t, ok)
		})
	}
}

type pausingHeaderForkGraph struct {
	*blockingStateForkGraph
	armed  atomic.Bool
	pause  sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (g *pausingHeaderForkGraph) GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if g.armed.Load() {
		g.pause.Do(func() {
			close(g.paused)
			<-g.resume
		})
	}
	return g.blockingStateForkGraph.GetHeader(blockRoot)
}

func TestCheckpointStateStoredDuringFinalizationIsDropped(t *testing.T) {
	f := buildExAnteStore(t)
	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	next := solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch + 1, Root: root}
	graph := &pausingHeaderForkGraph{
		blockingStateForkGraph: &blockingStateForkGraph{ForkGraph: f.forkGraph, root: root, release: make(chan struct{})},
		paused:                 make(chan struct{}),
		resume:                 make(chan struct{}),
	}
	f.forkGraph = graph
	f.unrealizedJustifications.Store(root, next)
	releaseBuild := sync.OnceFunc(func() { close(graph.release) })
	defer releaseBuild()
	resumeFinalization := sync.OnceFunc(func() { close(graph.resume) })
	defer resumeFinalization()

	built := make(chan error, 1)
	go func() {
		_, err := f.getCheckpointState(next)
		built <- err
	}()
	require.Eventually(t, func() bool { return graph.calls.Load() == 1 }, 10*time.Second, time.Millisecond)

	graph.armed.Store(true)
	finalized := make(chan struct{})
	go func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.updateCheckpoints(f.JustifiedCheckpoint(), solid.Checkpoint{Epoch: next.Epoch + 1, Root: root})
		close(finalized)
	}()
	select {
	case <-graph.paused:
	case <-time.After(10 * time.Second):
		t.Fatal("finalization did not reach a header lookup")
	}

	releaseBuild()
	require.NoError(t, <-built)
	resumeFinalization()
	<-finalized

	_, ok := f.checkpointStates.Load(next)
	require.False(t, ok)
}

func TestCheckpointStateStoreDelayedPastFinalizationIsDropped(t *testing.T) {
	f := buildExAnteStore(t)
	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	next := solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch + 1, Root: root}
	graph := &blockingStateForkGraph{ForkGraph: f.forkGraph, root: root, release: make(chan struct{})}
	f.forkGraph = graph
	release := sync.OnceFunc(func() { close(graph.release) })
	defer release()

	built := make(chan error, 1)
	go func() {
		_, err := f.getCheckpointState(next)
		built <- err
	}()
	require.Eventually(t, func() bool { return graph.calls.Load() == 1 }, 10*time.Second, time.Millisecond)

	f.mu.Lock()
	f.checkpointStatesMu.Lock()
	release()
	time.Sleep(500 * time.Millisecond)
	f.updateCheckpoints(f.JustifiedCheckpoint(), solid.Checkpoint{Epoch: next.Epoch + 1, Root: root})
	f.checkpointStatesMu.Unlock()
	f.mu.Unlock()

	require.NoError(t, <-built)
	_, ok := f.checkpointStates.Load(next)
	require.False(t, ok)
}

type blockingPruneForkGraph struct {
	fork_graph.ForkGraph
	started chan uint64
	release chan struct{}
}

func (g *blockingPruneForkGraph) Prune(slot uint64) error {
	g.started <- slot
	<-g.release
	return nil
}

func TestForkGraphPruneRunsOutsideLock(t *testing.T) {
	f := buildExAnteStore(t)
	base := f.forkGraph
	pruneGraph := &blockingPruneForkGraph{
		ForkGraph: base,
		started:   make(chan uint64, 1),
		release:   make(chan struct{}),
	}
	f.forkGraph = pruneGraph
	release := sync.OnceFunc(func() { close(pruneGraph.release) })
	defer release()

	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	promoted := solid.Checkpoint{Epoch: 4, Root: root}
	f.unrealizedJustifiedCheckpoint.Store(promoted)
	f.unrealizedFinalizedCheckpoint.Store(promoted)
	f.highestSeen.Store(f.beaconCfg.SlotsPerEpoch * promoted.Epoch)

	tickDone := make(chan struct{})
	go func() {
		f.OnTick(promoted.Epoch * f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot * 1000)
		close(tickDone)
	}()

	select {
	case <-pruneGraph.started:
	case <-time.After(10 * time.Second):
		t.Fatal("fork graph prune did not start")
	}

	lockFree := make(chan struct{})
	go func() {
		f.mu.Lock()
		_ = f.headHash
		f.mu.Unlock()
		close(lockFree)
	}()
	select {
	case <-lockFree:
	case <-time.After(10 * time.Second):
		t.Fatal("f.mu is held while fork graph prune is blocked")
	}

	release()
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("OnTick did not finish")
	}
	requireOperationPrunerIdle(t, f)
}

func TestOnTickBeforeGenesisIsNoop(t *testing.T) {
	f := buildExAnteStore(t)
	const shift = 1000
	f.genesisTime += shift
	f.timeMs.Store(f.timeMs.Load() + shift*1000)
	before := f.timeMs.Load()

	done := make(chan struct{})
	go func() {
		f.OnTick(f.genesisTime*1000 - 1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("OnTick before genesis did not return")
	}
	require.Equal(t, before, f.timeMs.Load())
}

func TestOnTickWithSaturatedGenesisIsNoop(t *testing.T) {
	genesisTime := uint64(math.MaxUint64/1000 + 1)
	f, _, _, _ := buildExAnteStoreBeforeBlocks(t, nil, &genesisTime)
	require.Zero(t, f.Slot())
	before := f.timeMs.Load()

	done := make(chan struct{})
	go func() {
		f.OnTick(uint64(time.Now().UnixMilli()))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("OnTick before a saturated genesis did not return")
	}
	require.Equal(t, uint64(math.MaxUint64), before)
	require.Zero(t, f.Slot())
	require.Equal(t, before, f.timeMs.Load())
}

func TestOnTickDoesNotMoveTimeBackwards(t *testing.T) {
	tests := []struct {
		name        string
		earlierTime func(slotStartMs uint64) uint64
	}{
		{name: "same slot", earlierTime: func(slotStartMs uint64) uint64 { return slotStartMs + 4000 }},
		{name: "earlier slot", earlierTime: func(slotStartMs uint64) uint64 { return slotStartMs - 1000 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := buildExAnteStore(t)
			laterSlot := f.Slot() + 2
			slotStartMs := (f.genesisTime + laterSlot*f.beaconCfg.SecondsPerSlot) * 1000
			laterTimeMs := slotStartMs + 5000
			f.OnTick(laterTimeMs)
			f.OnTick(tt.earlierTime(slotStartMs))

			assert.Equal(t, laterTimeMs/1000, f.Time())
			assert.Equal(t, laterSlot, f.Slot())
			assert.Equal(t, laterTimeMs, f.timeMs.Load())
		})
	}
}

func TestOnTickAdvancesSlotWhileForkChoiceLocked(t *testing.T) {
	f := buildExAnteStore(t)
	nextSlot := f.Slot() + 1
	nextSlotTimeMs := (f.genesisTime + nextSlot*f.beaconCfg.SecondsPerSlot) * 1000
	require.NotZero(t, f.ProposerBoostRoot())

	f.mu.Lock()
	tickDone := make(chan struct{})
	go func() {
		f.OnTick(nextSlotTimeMs)
		close(tickDone)
	}()

	assert.Eventually(t, func() bool {
		return f.Slot() == nextSlot
	}, time.Second, time.Millisecond)
	f.mu.Unlock()

	select {
	case <-tickDone:
	case <-time.After(5 * time.Second):
		t.Fatal("OnTick did not finish")
	}
	require.Zero(t, f.ProposerBoostRoot())
}

func TestConcurrentOnTickKeepsLatestTime(t *testing.T) {
	const (
		workers = 64
		rounds  = 1000
	)
	f := newTestForkChoiceStore(12, 3, 32, 0, 0, 0)

	for round := range rounds {
		f.timeMs.Store(0)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := range workers {
			timeMs := uint64((i+round)%workers + 1)
			go func() {
				defer wg.Done()
				<-start
				f.OnTick(timeMs)
			}()
		}
		close(start)
		wg.Wait()
		require.Equal(t, uint64(workers), f.timeMs.Load())
	}
}
