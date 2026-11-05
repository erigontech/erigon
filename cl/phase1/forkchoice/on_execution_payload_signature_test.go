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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

type replayingEnvelopeForkGraph struct {
	*persistingEnvelopeForkGraph
	currentState     bool
	currentStateOnce bool
	currentStateHook func(int32)
	currentStateRead atomic.Int32
	hasEnvelopeValue func(int32) bool
	hasEnvelopeReads atomic.Int32
	replayHook       func(int32)
	replayCalls      atomic.Int32
	returnNilState   bool
	dumpCalls        atomic.Int32
}

func (g *replayingEnvelopeForkGraph) HasEnvelope(root common.Hash) bool {
	call := g.hasEnvelopeReads.Add(1)
	if g.hasEnvelopeValue != nil {
		return g.hasEnvelopeValue(call)
	}
	return g.persistingEnvelopeForkGraph.HasEnvelope(root)
}

func (g *replayingEnvelopeForkGraph) CurrentStateLocked(common.Hash) *state2.CachingBeaconState {
	call := g.currentStateRead.Add(1)
	if g.currentStateHook != nil {
		g.currentStateHook(call)
	}
	if g.currentStateOnce {
		if call == 1 {
			return g.state
		}
		return nil
	}
	if g.currentState {
		return g.state
	}
	return nil
}

func (g *replayingEnvelopeForkGraph) ReplayState(common.Hash) (*state2.CachingBeaconState, error) {
	call := g.replayCalls.Add(1)
	if g.replayHook != nil {
		g.replayHook(call)
	}
	if g.returnNilState {
		return nil, nil
	}
	return g.state.Copy()
}

func (g *replayingEnvelopeForkGraph) DumpEnvelopeOnDisk(root common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	g.dumpCalls.Add(1)
	return g.persistingEnvelopeForkGraph.DumpEnvelopeOnDisk(root, envelope)
}

type multiRootReplayForkGraph struct {
	*replayingEnvelopeForkGraph
	envelopes sync.Map
	blocks    map[common.Hash]*cltypes.SignedBeaconBlock
	states    map[common.Hash]*state2.CachingBeaconState
}

func (g *multiRootReplayForkGraph) HasEnvelope(root common.Hash) bool {
	_, ok := g.envelopes.Load(root)
	return ok
}

func (g *multiRootReplayForkGraph) DumpEnvelopeOnDisk(root common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	g.dumpCalls.Add(1)
	g.envelopes.Store(root, envelope)
	return nil
}

func (g *multiRootReplayForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block, ok := g.blocks[root]
	return block, ok
}

func (g *multiRootReplayForkGraph) CurrentStateLocked(common.Hash) *state2.CachingBeaconState {
	call := g.currentStateRead.Add(1)
	if g.currentStateHook != nil {
		g.currentStateHook(call)
	}
	return nil
}

func (g *multiRootReplayForkGraph) ReplayState(root common.Hash) (*state2.CachingBeaconState, error) {
	call := g.replayCalls.Add(1)
	if g.replayHook != nil {
		g.replayHook(call)
	}
	return g.states[root].Copy()
}

func TestOnExecutionPayloadRejectsBadSignatureWithoutForkChoiceWriteLock(t *testing.T) {
	for _, tc := range []struct {
		name         string
		currentState bool
		replayCalls  int32
	}{
		{name: "replayed state", replayCalls: 1},
		{name: "current state", currentState: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
			store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
			graph := &replayingEnvelopeForkGraph{
				persistingEnvelopeForkGraph: baseGraph,
				currentState:                tc.currentState,
			}
			store.forkGraph = graph

			store.mu.RLock()
			locked := true
			t.Cleanup(func() {
				if locked {
					store.mu.RUnlock()
				}
			})
			done := make(chan error, 1)
			go func() { done <- store.OnExecutionPayload(t.Context(), envelope, false, true) }()

			var err error
			select {
			case err = <-done:
			case <-time.After(lockScopeTimeout):
				t.Fatal("OnExecutionPayload waited for the fork-choice write lock before rejecting the signature")
			}
			store.mu.RUnlock()
			locked = false

			require.ErrorIs(t, err, ErrInvalidExecutionPayloadEnvelope)
			require.ErrorContains(t, err, "invalid builder signature")
			require.Equal(t, tc.replayCalls, graph.replayCalls.Load())
			require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
		})
	}
}

func TestOnExecutionPayloadReleasesLocksAfterBuilderSignaturePanic(t *testing.T) {
	for _, currentState := range []bool{false, true} {
		t.Run("currentState="+strconv.FormatBool(currentState), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
			store.forkGraph = &replayingEnvelopeForkGraph{persistingEnvelopeForkGraph: baseGraph, currentState: currentState}
			original := blsVerify
			blsVerify = func([]byte, []byte, []byte) (bool, error) { panic("builder signature verification") }
			t.Cleanup(func() { blsVerify = original })

			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_ = store.OnExecutionPayload(t.Context(), envelope, false, true)
			}()

			require.NotNil(t, recovered)
			require.True(t, store.mu.TryLock())
			store.mu.Unlock()
			if currentState {
				return
			}
			validation := store.envelopeValidationSemaphore()
			select {
			case validation <- struct{}{}:
				<-validation
			default:
				t.Fatal("execution payload validation capacity was not released after panic")
			}
		})
	}
}

func TestOnExecutionPayloadReplaysStateAtRequiredLockScope(t *testing.T) {
	for _, tc := range []struct {
		name             string
		currentStateOnce bool
		replayOffLock    bool
	}{
		{name: "non-current root", replayOffLock: true},
		{name: "root stops being current", currentStateOnce: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
			store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
			replayOffLock := false
			graph := &replayingEnvelopeForkGraph{
				persistingEnvelopeForkGraph: baseGraph,
				currentStateOnce:            tc.currentStateOnce,
				replayHook: func(int32) {
					replayOffLock = store.mu.TryLock()
					if replayOffLock {
						store.mu.Unlock()
					}
				},
			}
			store.forkGraph = graph
			calls := stubBLSVerify(t)

			err := store.OnExecutionPayload(t.Context(), envelope, false, true)

			require.NoError(t, err)
			require.Equal(t, tc.replayOffLock, replayOffLock)
			require.Equal(t, int32(1), graph.replayCalls.Load())
			require.Equal(t, int32(1), graph.dumpCalls.Load())
			require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
			require.Equal(t, 1, *calls)
		})
	}
}

func TestOnExecutionPayloadSkipsReplayWhenEnvelopeAppliedWhileWaitingForValidation(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	replayAdmissionReached := make(chan struct{})
	graph := &replayingEnvelopeForkGraph{
		persistingEnvelopeForkGraph: baseGraph,
		currentStateHook: func(int32) {
			close(replayAdmissionReached)
		},
	}
	store.forkGraph = graph
	validation := store.envelopeValidationSemaphore()
	validation <- struct{}{}
	validationHeld := true
	t.Cleanup(func() {
		if validationHeld {
			<-validation
		}
	})

	done := make(chan error, 1)
	go func() { done <- store.OnExecutionPayload(t.Context(), envelope, false, true) }()
	awaitSignal(t, replayAdmissionReached, "the replay admission point")
	require.NoError(t, baseGraph.DumpEnvelopeOnDisk(envelope.Message.BeaconBlockRoot, envelope))
	<-validation
	validationHeld = false

	require.ErrorIs(t, <-done, ErrIgnore)
	require.Zero(t, graph.replayCalls.Load())
}

func TestOnExecutionPayloadDoesNotReplayUnavailableStateUnderForkChoiceLock(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	store.pendingEnvelopes = pending
	graph := &replayingEnvelopeForkGraph{
		persistingEnvelopeForkGraph: baseGraph,
		returnNilState:              true,
	}
	store.forkGraph = graph

	store.mu.RLock()
	locked := true
	t.Cleanup(func() {
		if locked {
			store.mu.RUnlock()
		}
	})
	done := make(chan error, 1)
	go func() { done <- store.OnExecutionPayload(t.Context(), envelope, false, true) }()

	select {
	case err = <-done:
	case <-time.After(lockScopeTimeout):
		store.mu.RUnlock()
		locked = false
		err = <-done
		t.Fatalf("OnExecutionPayload waited for the fork-choice write lock after an unavailable replay: %v", err)
	}
	store.mu.RUnlock()
	locked = false

	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorContains(t, err, "state unavailable")
	require.Equal(t, int32(1), graph.replayCalls.Load())
	require.True(t, pending.Contains(envelope.Message.BeaconBlockRoot))
}

func TestApplyEnvelopeDoesNotQueueUnavailableRetry(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	store.pendingEnvelopes = pending
	graph := &replayingEnvelopeForkGraph{
		persistingEnvelopeForkGraph: baseGraph,
		returnNilState:              true,
	}
	store.forkGraph = graph

	_, err = store.applyEnvelope(t.Context(), envelope, false, true, true, retryQueuedEnvelope, time.Now())

	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeStateUnavailable)
	require.False(t, pending.Contains(envelope.Message.BeaconBlockRoot))
}

func TestOnExecutionPayloadDoesNotQueueUnavailableStateAfterConcurrentApply(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	store.pendingEnvelopes = pending
	graph := &replayingEnvelopeForkGraph{
		persistingEnvelopeForkGraph: baseGraph,
		returnNilState:              true,
	}
	graph.hasEnvelopeValue = func(int32) bool {
		return graph.replayCalls.Load() > 0
	}
	store.forkGraph = graph

	err = store.OnExecutionPayload(t.Context(), envelope, false, true)

	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeStateUnavailable)
	require.False(t, pending.Contains(envelope.Message.BeaconBlockRoot))
}

func TestOnExecutionPayloadDoesNotReplayUnverifiedEnvelopeUnderForkChoiceLock(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
	store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	store.pendingEnvelopes = pending
	graph := &replayingEnvelopeForkGraph{
		persistingEnvelopeForkGraph: baseGraph,
		hasEnvelopeValue: func(call int32) bool {
			switch call {
			case 1:
				return false
			case 2:
				return true
			default:
				return false
			}
		},
	}
	store.forkGraph = graph

	err = store.OnExecutionPayload(t.Context(), envelope, false, true)

	require.ErrorIs(t, err, ErrIgnore)
	require.NotErrorIs(t, err, ErrInvalidExecutionPayloadEnvelope)
	require.ErrorContains(t, err, "state unavailable")
	require.Zero(t, graph.replayCalls.Load())
}

func TestOnExecutionPayloadRetainsReplayAdmissionUntilForkChoiceWriteLock(t *testing.T) {
	cfg, firstState, firstBlock, firstEnvelope := validAdmissionCancellationFixture(t)
	_, secondState, secondBlock, secondEnvelope := validAdmissionCancellationFixtureAtSlot(t, 65)
	store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, firstState, firstBlock)
	firstReplayStarted := make(chan struct{})
	secondStateChecked := make(chan struct{})
	secondReplayStarted := make(chan struct{})
	releaseFirstReplay := make(chan struct{})
	graph := &multiRootReplayForkGraph{
		replayingEnvelopeForkGraph: &replayingEnvelopeForkGraph{
			persistingEnvelopeForkGraph: baseGraph,
			currentStateHook: func(call int32) {
				if call == 2 {
					close(secondStateChecked)
				}
			},
			replayHook: func(call int32) {
				switch call {
				case 1:
					close(firstReplayStarted)
					<-releaseFirstReplay
				case 2:
					close(secondReplayStarted)
				}
			},
		},
		blocks: map[common.Hash]*cltypes.SignedBeaconBlock{
			firstEnvelope.Message.BeaconBlockRoot:  firstBlock,
			secondEnvelope.Message.BeaconBlockRoot: secondBlock,
		},
		states: map[common.Hash]*state2.CachingBeaconState{
			firstEnvelope.Message.BeaconBlockRoot:  firstState,
			secondEnvelope.Message.BeaconBlockRoot: secondState,
		},
	}
	store.forkGraph = graph
	stubBLSVerify(t)

	firstDone := make(chan error, 1)
	go func() { firstDone <- store.OnExecutionPayload(t.Context(), firstEnvelope, false, true) }()
	awaitSignal(t, firstReplayStarted, "the first state replay")
	secondDone := make(chan error, 1)
	go func() { secondDone <- store.OnExecutionPayload(t.Context(), secondEnvelope, false, true) }()
	awaitSignal(t, secondStateChecked, "the second envelope state check")

	store.mu.Lock()
	close(releaseFirstReplay)
	secondReplayedEarly := false
	select {
	case <-secondReplayStarted:
		secondReplayedEarly = true
	case <-time.After(500 * time.Millisecond):
	}
	store.mu.Unlock()

	require.NoError(t, <-firstDone)
	awaitSignal(t, secondReplayStarted, "the second state replay after the first acquired the write lock")
	require.NoError(t, <-secondDone)
	require.False(t, secondReplayedEarly, "the second state replay started while the first replayed state waited for the write lock")
}

func TestOnExecutionPayloadAppliesOnceWhenConcurrentApplyWinsDuringReplay(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, baseGraph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	replayStarted := make(chan struct{})
	releaseReplay := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseReplay:
		default:
			close(releaseReplay)
		}
	})
	graph := &replayingEnvelopeForkGraph{
		persistingEnvelopeForkGraph: baseGraph,
		replayHook: func(call int32) {
			if call == 1 {
				close(replayStarted)
				<-releaseReplay
			}
		},
	}
	store.forkGraph = graph

	firstDone := make(chan error, 1)
	go func() { firstDone <- store.OnExecutionPayload(t.Context(), envelope, false, true) }()
	awaitSignal(t, replayStarted, "the first state replay")

	secondDone := make(chan error, 1)
	go func() { secondDone <- store.OnExecutionPayload(t.Context(), envelope, false, false) }()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(lockScopeTimeout):
		t.Fatal("the concurrent apply could not acquire the fork-choice lock during replay")
	}
	close(releaseReplay)

	require.ErrorIs(t, <-firstDone, ErrIgnore)
	require.Equal(t, int32(1), graph.dumpCalls.Load())
}

// The envelope uses another key, so success with one stub call proves only fork-choice verification ran.
func TestOnExecutionPayloadValidatedPathVerifiesBuilderSignatureOnce(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)

	calls := stubBLSVerify(t)

	err := store.OnExecutionPayload(t.Context(), envelope, false, true)

	require.Equal(t, 1, *calls)
	require.NoError(t, err)
}

func TestValidateExecutionPayloadEnvelopeForConsensusVerifiesBuilderSignatureOnce(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().
		NewPayload(gomock.Any(), envelope.Message.Payload, &block.Block.ParentRoot, []common.Hash{}, []hexutil.Bytes{}).
		Return(execution_client.PayloadStatusValidated, nil)
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	store.engine = engine

	calls := stubBLSVerify(t)

	err := store.ValidateExecutionPayloadEnvelopeForConsensus(t.Context(), envelope)

	require.Equal(t, 1, *calls)
	require.NoError(t, err)
}

func TestValidateExecutionPayloadEnvelopeForConsensusRejectsInvalidBuilderSignature(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	store.engine = engine

	err := store.ValidateExecutionPayloadEnvelopeForConsensus(t.Context(), envelope)

	require.ErrorContains(t, err, "invalid builder signature: invalid signature")
}

func TestOnExecutionPayloadRejectsInvalidBuilderSignature(t *testing.T) {
	for _, validatePayload := range []bool{false, true} {
		t.Run("validatePayload="+strconv.FormatBool(validatePayload), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
			store, graph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)

			err := store.OnExecutionPayload(t.Context(), envelope, false, validatePayload)

			require.ErrorIs(t, err, ErrInvalidExecutionPayloadEnvelope)
			if validatePayload {
				require.ErrorContains(t, err, "invalid builder signature: invalid signature")
			} else {
				require.ErrorContains(t, err, "invalid envelope signature")
			}
			require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
		})
	}
}

func TestOnExecutionPayloadKeepsBlockStateRoot(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	blockState.SetPreviousStateRoot(block.Block.StateRoot)
	before, err := blockState.HashSSZ()
	require.NoError(t, err)
	beforePreviousRoot := blockState.PeekPreviousStateRoot()

	require.NoError(t, store.OnExecutionPayload(t.Context(), envelope, false, true))

	after, err := blockState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, beforePreviousRoot, blockState.PeekPreviousStateRoot())
}

func TestOnExecutionPayloadSelfBuildUsesProposerKey(t *testing.T) {
	for _, validatePayload := range []bool{false, true} {
		for _, signer := range []struct {
			name    string
			key     string
			wantErr bool
		}{
			{name: "proposer", key: admissionSigningKey},
			{name: "builder", key: otherSigningKey, wantErr: true},
		} {
			t.Run("validatePayload="+strconv.FormatBool(validatePayload)+"/"+signer.name, func(t *testing.T) {
				cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
				configureSelfBuildEnvelope(t, cfg, blockState, block, envelope, signer.key)
				store, graph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)

				err := store.OnExecutionPayload(t.Context(), envelope, false, validatePayload)

				if signer.wantErr {
					require.ErrorIs(t, err, ErrInvalidExecutionPayloadEnvelope)
					require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
					return
				}
				require.NoError(t, err)
				require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
			})
		}
	}
}

func newEnvelopeApplicationTestStore(
	t testing.TB,
	cfg *clparams.BeaconChainConfig,
	blockState *state2.CachingBeaconState,
	block *cltypes.SignedBeaconBlock,
) (*ForkChoiceStore, *persistingEnvelopeForkGraph) {
	t.Helper()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{
		state: blockState,
		block: block,
	}}
	store := newPayloadVoteTestStore(t, common.Hash(root), false, false)
	store.beaconCfg = cfg
	store.forkGraph = graph
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	gasLimits, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	store.executionPayloadGasLimit = gasLimits
	return store, graph
}

func stubBLSVerify(t testing.TB) *int {
	t.Helper()
	calls := new(int)
	original := blsVerify
	blsVerify = func([]byte, []byte, []byte) (bool, error) {
		(*calls)++
		return true, nil
	}
	t.Cleanup(func() { blsVerify = original })
	return calls
}

func configureSelfBuildEnvelope(
	t testing.TB,
	cfg *clparams.BeaconChainConfig,
	blockState *state2.CachingBeaconState,
	block *cltypes.SignedBeaconBlock,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
	signingKey string,
) {
	t.Helper()
	builderKey, err := bls.NewPrivateKeyFromIKM([]byte(otherSigningKey))
	require.NoError(t, err)
	blockState.GetBuilders().Get(0).Pubkey = common.Bytes48(bls.CompressPublicKey(builderKey.PublicKey()))

	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.BuilderIndex = clparams.BuilderIndexSelfBuild
	blockState.SetLatestExecutionPayloadBid(bid)
	envelope.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	rebindAdmissionBlockRoot(t, blockState, block, envelope)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, signingKey)
}
