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
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
)

func TestGetHeadWithSyncedHeadStateMatchesAuxiliaryStateHead(t *testing.T) {
	wantStore := buildExAnteStore(t)
	gotStore := buildExAnteStore(t)
	require.NotZero(t, gotStore.ProposerBoostRoot())
	require.Greater(t, gotStore.latestMessages.latestMessagesCount(), 0)

	var wantRoot common.Hash
	var wantSlot uint64
	require.NoError(t, wantStore.syncedDataManager.ViewHeadState(func(headState *state.CachingBeaconState) error {
		var err error
		wantRoot, wantSlot, err = wantStore.GetHead(headState)
		return err
	}))

	gotRoot, gotSlot, err := gotStore.GetHeadWithSyncedHeadState()
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	require.Equal(t, wantSlot, gotSlot)
}

func TestGetHeadWithSyncedHeadStateDoesNotChangeCacheWhenNotSynced(t *testing.T) {
	store := buildExAnteStore(t)
	_, _, err := store.GetHeadWithSyncedHeadState()
	require.NoError(t, err)
	store.OnTick(store.time.Load() + store.beaconCfg.SecondsPerSlot)
	require.Zero(t, store.headHash)
	store.syncedDataManager.UnsetHeadState()

	wantSlot := store.headSlot
	wantPayloadStatus := store.headPayloadStatus
	wantWeights := maps.Clone(store.weights)

	_, _, err = store.GetHeadWithSyncedHeadState()

	require.ErrorIs(t, err, synced_data.ErrNotSynced)
	require.Zero(t, store.headHash)
	require.Equal(t, wantSlot, store.headSlot)
	require.Equal(t, wantPayloadStatus, store.headPayloadStatus)
	require.Equal(t, wantWeights, store.weights)
}

func TestGetHeadWithSyncedHeadStateTakesForkChoiceLockFirst(t *testing.T) {
	store := buildExAnteStore(t)
	store.OnTick(store.time.Load() + store.beaconCfg.SecondsPerSlot)
	require.Zero(t, store.headHash)
	graph := &pausingHeaderForkGraph{
		ForkGraph: store.forkGraph,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	store.forkGraph = graph
	released := false
	t.Cleanup(func() {
		if !released {
			close(graph.release)
		}
	})

	var headState *state.CachingBeaconState
	require.NoError(t, store.syncedDataManager.ViewHeadState(func(st *state.CachingBeaconState) error {
		headState = st
		return nil
	}))

	headDone := make(chan error, 1)
	go func() {
		_, _, err := store.GetHeadWithSyncedHeadState()
		headDone <- err
	}()
	select {
	case <-graph.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("head computation did not reach the justified header lookup")
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- store.syncedDataManager.OnHeadState(headState)
	}()
	select {
	case err := <-writerDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("synced-data lock held before the fork-choice lock")
	}

	store.mu.Lock()
	locked := true
	defer func() {
		if locked {
			store.mu.Unlock()
		}
	}()
	close(graph.release)
	released = true
	time.Sleep(250 * time.Millisecond)

	writerDone = make(chan error, 1)
	go func() {
		writerDone <- store.syncedDataManager.OnHeadState(headState)
	}()
	select {
	case err := <-writerDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("synced-data lock taken before the fork-choice lock")
	}

	store.mu.Unlock()
	locked = false
	select {
	case err := <-headDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("head computation did not finish after releasing the fork-choice lock")
	}
}

// pausingHeaderForkGraph blocks the first GetHeader call, which getHeadNode makes
// after releasing its read lock and before the head computation takes f.mu.
type pausingHeaderForkGraph struct {
	fork_graph.ForkGraph
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (g *pausingHeaderForkGraph) GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return g.ForkGraph.GetHeader(blockRoot)
}
