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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
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
		f.OnTick(f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot)
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

	f.OnTick((f.beaconCfg.SlotsPerEpoch - 1) * f.beaconCfg.SecondsPerSlot)

	require.Equal(t, justified, f.JustifiedCheckpoint())
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
		{name: "slot not advanced", tick: func(f *ForkChoiceStore) uint64 { return f.time.Load() }},
		{name: "not synced", tick: func(f *ForkChoiceStore) uint64 {
			return (f.highestSeen.Load() + 2*f.beaconCfg.SlotsPerEpoch + 1) * f.beaconCfg.SecondsPerSlot
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

func (g *blockingStateForkGraph) GetState(blockRoot common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	if blockRoot == g.root {
		g.calls.Add(1)
		<-g.release
	}
	return g.ForkGraph.GetState(blockRoot, alwaysCopy)
}

func TestConcurrentCheckpointStateMissesBuildOnce(t *testing.T) {
	f := buildExAnteStore(t)
	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	next := solid.Checkpoint{Epoch: f.JustifiedCheckpoint().Epoch + 1, Root: root}
	graph := &blockingStateForkGraph{ForkGraph: f.forkGraph, root: root, release: make(chan struct{})}
	f.forkGraph = graph
	released := false
	defer func() {
		if !released {
			close(graph.release)
		}
	}()
	f.unrealizedJustifiedCheckpoint.Store(next)

	slot := f.Slot()
	for i := uint64(1); i <= 3; i++ {
		f.OnTick((slot + i) * f.beaconCfg.SecondsPerSlot)
	}
	built := make(chan error, 1)
	go func() {
		_, err := f.getCheckpointState(next)
		built <- err
	}()

	require.Eventually(t, func() bool { return graph.calls.Load() >= 1 }, 10*time.Second, time.Millisecond)
	require.Never(t, func() bool { return graph.calls.Load() > 1 }, 300*time.Millisecond, 5*time.Millisecond)
	close(graph.release)
	released = true

	require.NoError(t, <-built)
	_, ok := f.checkpointStates.Load(next)
	require.True(t, ok)
	require.Equal(t, int32(1), graph.calls.Load())
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
	released := false
	defer func() {
		if !released {
			close(pruneGraph.release)
		}
	}()

	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	promoted := solid.Checkpoint{Epoch: 4, Root: root}
	f.unrealizedJustifiedCheckpoint.Store(promoted)
	f.unrealizedFinalizedCheckpoint.Store(promoted)
	f.highestSeen.Store(f.beaconCfg.SlotsPerEpoch * promoted.Epoch)

	tickDone := make(chan struct{})
	go func() {
		f.OnTick(promoted.Epoch * f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot)
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

	close(pruneGraph.release)
	released = true
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("OnTick did not finish")
	}
	requireOperationPrunerIdle(t, f)
}
