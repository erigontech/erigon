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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestGetStateAtBlockRootHoldsReadLockDuringCurrentStateCopy(t *testing.T) {
	root := common.Hash{0x01}
	graph := &getFinalizedExecutionHashForkGraph{
		states:              map[common.Hash]*state.CachingBeaconState{root: state.New(&clparams.MainnetBeaconConfig)},
		currentStateRoot:    root,
		currentStateStarted: make(chan struct{}, 1),
		currentStateRelease: make(chan struct{}),
	}
	store := &ForkChoiceStore{forkGraph: graph}
	copyDone := make(chan error, 1)
	go func() {
		_, err := store.GetStateAtBlockRoot(root)
		copyDone <- err
	}()
	<-graph.currentStateStarted

	writerDone := make(chan struct{})
	go func() {
		store.mu.Lock()
		close(writerDone)
		store.mu.Unlock()
	}()
	select {
	case <-writerDone:
		t.Fatal("writer entered while current state copy was in progress")
	case <-time.After(20 * time.Millisecond):
	}

	close(graph.currentStateRelease)
	require.NoError(t, <-copyDone)
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not enter after current state copy completed")
	}
}

func TestViewCurrentStateAtBlockRoot(t *testing.T) {
	currentRoot := common.Hash{0x01}
	currentState := state.New(&clparams.MainnetBeaconConfig)
	graph := &getFinalizedExecutionHashForkGraph{
		states:           map[common.Hash]*state.CachingBeaconState{currentRoot: currentState},
		currentStateRoot: currentRoot,
	}
	store := &ForkChoiceStore{forkGraph: graph}
	for _, test := range []struct {
		name       string
		root       common.Hash
		wantCalled bool
	}{
		{name: "non-current root", root: common.Hash{0x02}},
		{name: "current root", root: currentRoot, wantCalled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			err := store.ViewCurrentStateAtBlockRoot(test.root, func(headState *state.CachingBeaconState) error {
				called = true
				require.Same(t, currentState, headState)
				return nil
			})

			require.NoError(t, err)
			require.Equal(t, test.wantCalled, called)
		})
	}
}

func TestViewCurrentStateAtBlockRootHoldsReadLockUntilCallbackReturns(t *testing.T) {
	root := common.Hash{0x01}
	currentState := state.New(&clparams.MainnetBeaconConfig)
	store := &ForkChoiceStore{forkGraph: &getFinalizedExecutionHashForkGraph{
		states:           map[common.Hash]*state.CachingBeaconState{root: currentState},
		currentStateRoot: root,
	}}
	callbackErr := errors.New("callback failed")
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- store.ViewCurrentStateAtBlockRoot(root, func(*state.CachingBeaconState) error {
			close(callbackEntered)
			<-callbackRelease
			return callbackErr
		})
	}()
	<-callbackEntered

	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		store.mu.Lock()
		close(writerDone)
		store.mu.Unlock()
	}()
	<-writerStarted
	select {
	case <-writerDone:
		t.Fatal("writer entered while the callback held the current state")
	case <-time.After(20 * time.Millisecond):
	}

	close(callbackRelease)
	require.ErrorIs(t, <-viewDone, callbackErr)
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not enter after the callback returned")
	}
}

func TestViewCurrentStateAtBlockRootWaitsForWriter(t *testing.T) {
	root := common.Hash{0x01}
	currentState := state.New(&clparams.MainnetBeaconConfig)
	replacementState := state.New(&clparams.MainnetBeaconConfig)
	graph := &getFinalizedExecutionHashForkGraph{
		states:           map[common.Hash]*state.CachingBeaconState{root: currentState},
		currentStateRoot: root,
	}
	store := &ForkChoiceStore{forkGraph: graph}
	store.mu.Lock()
	graph.states[root] = replacementState
	viewStarted := make(chan struct{})
	callbackEntered := make(chan struct{})
	viewDone := make(chan error, 1)
	go func() {
		close(viewStarted)
		viewDone <- store.ViewCurrentStateAtBlockRoot(root, func(headState *state.CachingBeaconState) error {
			close(callbackEntered)
			if headState != replacementState {
				return errors.New("viewed stale current state")
			}
			return nil
		})
	}()
	<-viewStarted
	select {
	case <-callbackEntered:
		store.mu.Unlock()
		t.Fatal("callback entered while the writer held the lock")
	case <-time.After(20 * time.Millisecond):
	}

	store.mu.Unlock()
	select {
	case <-callbackEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not enter after the writer released the lock")
	}
	require.NoError(t, <-viewDone)
}

// Each call already holds f.mu when it reads the current state; locking again would
// deadlock behind the queued writer.
func TestLockedStateReadersDoNotRelock(t *testing.T) {
	underReadLock := func(store *ForkChoiceStore, read func() error) func() error {
		return func() error {
			store.mu.RLock()
			defer store.mu.RUnlock()
			return read()
		}
	}
	currentCheckpointStore := func(t *testing.T) (*ForkChoiceStore, *getFinalizedExecutionHashForkGraph, solid.Checkpoint) {
		checkpoint := checkpointStateCacheTestCheckpoints(1)[0]
		store := newCheckpointStateCacheTestStore(t, []solid.Checkpoint{checkpoint}, checkpoint)
		graph := store.forkGraph.(*getFinalizedExecutionHashForkGraph)
		graph.currentStateRoot = checkpoint.Root
		return store, graph, checkpoint
	}
	tests := map[string]func(t *testing.T) (*ForkChoiceStore, *getFinalizedExecutionHashForkGraph, func() error){
		"state copy": func(t *testing.T) (*ForkChoiceStore, *getFinalizedExecutionHashForkGraph, func() error) {
			store, graph, checkpoint := currentCheckpointStore(t)
			return store, graph, underReadLock(store, func() error {
				_, err := store.copyStateAtBlockRootLocked(checkpoint.Root)
				return err
			})
		},
		"is head weak": func(t *testing.T) (*ForkChoiceStore, *getFinalizedExecutionHashForkGraph, func() error) {
			store, graph, checkpoint := currentCheckpointStore(t)
			return store, graph, underReadLock(store, func() error {
				store.isHeadWeak(checkpoint.Root)
				return nil
			})
		},
		"on attestation": func(t *testing.T) (*ForkChoiceStore, *getFinalizedExecutionHashForkGraph, func() error) {
			fixture := newTargetShufflingFixture(t)
			graph := fixture.forkGraph(false)
			graph.states[fixture.targetRoot] = fixture.targetState
			graph.currentStateRoot = fixture.targetRoot
			store := newAttestationTestStore(t, fixture.cfg, fixture.headState, fixture.headRoot, fixture.attestationSlot, graph)
			store.syncedDataManager = synced_data.NewSyncedDataManager(fixture.cfg, true)
			target := solid.Checkpoint{Epoch: fixture.targetEpoch, Root: fixture.targetRoot}
			attestation := &solid.Attestation{
				AggregationBits: aggregationBits(fixture.committeeSize(t, fixture.targetRoot), int(fixture.cfg.MaxValidatorsPerCommittee), fixture.committeePosition),
				Data: &solid.AttestationData{
					Slot:            fixture.attestationSlot,
					CommitteeIndex:  fixture.committeeIndex,
					BeaconBlockRoot: fixture.targetRoot,
					Target:          target,
				},
			}
			return store, graph, func() error { return store.OnAttestation(attestation, true, false) }
		},
		"on block with an empty checkpoint cache": func(t *testing.T) (*ForkChoiceStore, *getFinalizedExecutionHashForkGraph, func() error) {
			store, _, child := prepareEmbeddedPtcVoteBlock(t, clparams.MaxPtcSize, []uint64{42}, []int{0}, false, func(*ForkChoiceStore, common.Hash) {})
			store.checkpointStates.Delete(store.justifiedCheckpoint.Load().(solid.Checkpoint))
			graph := store.forkGraph.(*embeddedPtcVoteForkGraph).getFinalizedExecutionHashForkGraph
			return store, graph, func() error { return store.OnBlock(context.Background(), child, false, false, false) }
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			store, graph, call := setup(t)
			graph.currentStateStarted = make(chan struct{}, 1)
			graph.currentStateRelease = make(chan struct{})
			callDone := make(chan error, 1)
			go func() { callDone <- call() }()
			select {
			case <-graph.currentStateStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("call did not read the current state")
			}

			writerDone := make(chan struct{})
			go func() {
				store.mu.Lock()
				close(writerDone)
				store.mu.Unlock()
			}()
			time.Sleep(20 * time.Millisecond)
			close(graph.currentStateRelease)
			select {
			case err := <-callDone:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("call tried to reacquire the fork-choice lock")
			}
			select {
			case <-writerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("queued writer did not complete")
			}
		})
	}
}

func TestGetOwnedStateAtBlockRootCopiesReplayedState(t *testing.T) {
	root := common.Hash{0x01}
	replayed := state.New(&clparams.MainnetBeaconConfig)
	replayed.SetVersion(clparams.GloasVersion)
	blockEth1Data := &cltypes.Eth1Data{
		Root:         common.Hash{0x11},
		DepositCount: 22,
		BlockHash:    common.Hash{0x33},
	}
	replayed.SetEth1Data(blockEth1Data)
	require.NoError(t, replayed.AddEth1DataVote(blockEth1Data))
	blockBid := &cltypes.ExecutionPayloadBid{
		BlockHash:          common.Hash{0x44},
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}
	replayed.SetLatestExecutionPayloadBid(blockBid)
	graph := &getFinalizedExecutionHashForkGraph{
		states:           map[common.Hash]*state.CachingBeaconState{root: replayed},
		currentStateRoot: common.Hash{0xff},
	}
	store := &ForkChoiceStore{forkGraph: graph}

	owned, err := store.GetOwnedStateAtBlockRoot(root)
	require.NoError(t, err)
	require.NotSame(t, replayed, owned)

	blockEth1Data.Root = common.Hash{0xaa}
	blockEth1Data.DepositCount = 0xbb
	blockEth1Data.BlockHash = common.Hash{0xcc}
	blockBid.BlockHash = common.Hash{0xdd}

	require.Equal(t, common.Hash{0x11}, owned.Eth1Data().Root)
	require.Equal(t, uint64(22), owned.Eth1Data().DepositCount)
	require.Equal(t, common.Hash{0x33}, owned.Eth1Data().BlockHash)
	require.Equal(t, common.Hash{0x11}, owned.Eth1DataVotes().Get(0).Root)
	require.Equal(t, uint64(22), owned.Eth1DataVotes().Get(0).DepositCount)
	require.Equal(t, common.Hash{0x33}, owned.Eth1DataVotes().Get(0).BlockHash)
	require.Equal(t, common.Hash{0x44}, owned.GetLatestExecutionPayloadBid().BlockHash)
}

func TestGetOwnedStateAtBlockRootCopiesCurrentState(t *testing.T) {
	root := common.Hash{0x01}
	current := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, current.SetSlot(42))
	graph := &getFinalizedExecutionHashForkGraph{
		states:           map[common.Hash]*state.CachingBeaconState{root: current},
		currentStateRoot: root,
	}
	store := &ForkChoiceStore{forkGraph: graph}
	expectedRoot, err := current.HashSSZ()
	require.NoError(t, err)

	owned, err := store.GetOwnedStateAtBlockRoot(root)
	require.NoError(t, err)
	require.NotSame(t, current, owned)
	actualRoot, err := owned.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)
}
