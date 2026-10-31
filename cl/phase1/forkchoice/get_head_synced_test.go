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
	"errors"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
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

	gotHead, gotSlot, err := gotStore.GetHeadWithSyncedHeadState()
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotHead.Root)
	require.Equal(t, wantSlot, gotSlot)
}

func TestGetHeadWithSyncedHeadStatePreservesPayloadStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status cltypes.PayloadStatus
	}{
		{"FULL", cltypes.PayloadStatusFull},
		{"EMPTY", cltypes.PayloadStatusEmpty},
		{"PENDING", cltypes.PayloadStatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := ForkChoiceNode{Root: common.Hash{0x41}, PayloadStatus: tc.status}
			store := &ForkChoiceStore{
				headHash:          want.Root,
				headSlot:          12,
				headPayloadStatus: want.PayloadStatus,
				syncedDataManager: synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true),
			}

			head, slot, err := store.GetHeadWithSyncedHeadState()

			require.NoError(t, err)
			require.Equal(t, want, head)
			require.Equal(t, uint64(12), slot)
		})
	}
}

func TestGetHeadWithSyncedHeadStateDoesNotChangeCacheWhenNotSynced(t *testing.T) {
	store := buildExAnteStore(t)
	_, _, err := store.GetHeadWithSyncedHeadState()
	require.NoError(t, err)
	store.OnTick(store.timeMs.Load() + store.beaconCfg.SecondsPerSlot*1000)
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
	store.OnTick(store.timeMs.Load() + store.beaconCfg.SecondsPerSlot*1000)
	require.Zero(t, store.headHash)
	store.checkpointStates.Delete(store.justifiedCheckpoint.Load())
	graph := &pausingCheckpointStateForkGraph{
		ForkGraph: store.forkGraph,
		entered:   make(chan struct{}, 1),
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
		t.Fatal("head computation did not reach checkpoint-state loading")
	}

	tickDone := make(chan struct{})
	go func() {
		store.OnTick(store.timeMs.Load() + store.beaconCfg.SecondsPerSlot*1000)
		close(tickDone)
	}()
	select {
	case <-tickDone:
	case <-time.After(5 * time.Second):
		t.Fatal("fork-choice lock held during checkpoint-state loading")
	}

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- store.syncedDataManager.OnHeadState(headState)
	}()
	select {
	case err := <-writerDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("synced-data lock held during checkpoint-state loading")
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

type pausingCheckpointStateForkGraph struct {
	fork_graph.ForkGraph
	entered chan struct{}
	release chan struct{}
}

func (g *pausingCheckpointStateForkGraph) CurrentStateLocked(common.Hash) *state.CachingBeaconState {
	return nil
}

func (g *pausingCheckpointStateForkGraph) ReplayState(root common.Hash) (*state.CachingBeaconState, error) {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	return g.ForkGraph.ReplayState(root)
}

type failingCheckpointStateForkGraph struct {
	fork_graph.ForkGraph
	calls atomic.Int32
}

func (g *failingCheckpointStateForkGraph) CurrentStateLocked(common.Hash) *state.CachingBeaconState {
	return nil
}

func (g *failingCheckpointStateForkGraph) ReplayState(common.Hash) (*state.CachingBeaconState, error) {
	g.calls.Add(1)
	return nil, errors.New("checkpoint state unavailable")
}

func TestGetHeadWithSyncedHeadStateIgnoresCheckpointStateLoadFailure(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	anchorState := state.New(&beaconCfg)
	baseGraph, err := fork_graph.NewForkGraphDisk(
		anchorState,
		nil,
		afero.NewMemMapFs(),
		beacon_router_configuration.RouterConfiguration{},
	)
	require.NoError(t, err)
	graph := &failingCheckpointStateForkGraph{ForkGraph: baseGraph}
	manager := synced_data.NewSyncedDataManager(&beaconCfg, true)
	anchorRoot := baseGraph.AnchorRoot()
	require.NoError(t, manager.OnHeadStateWithBlockRoot(anchorState, anchorRoot))
	store, err := NewForkChoiceStore(
		eth_clock.NewEthereumClock(0, common.Hash{}, &beaconCfg),
		anchorState,
		nil,
		pool.NewOperationsPool(&beaconCfg),
		graph,
		beaconevents.NewEventEmitter(),
		manager,
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)

	head, headSlot, err := store.GetHeadWithSyncedHeadState()

	require.NoError(t, err)
	require.Equal(t, anchorRoot, head.Root)
	require.Equal(t, baseGraph.AnchorSlot(), headSlot)
	require.Equal(t, int32(1), graph.calls.Load())
}
