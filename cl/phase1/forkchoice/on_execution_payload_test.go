// Copyright 2024 The Erigon Authors
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
	"fmt"
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
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

type failingUpdateDB struct {
	kv.RwDB
	fail  bool
	calls int
}

type blockingUpdateDB struct {
	kv.RwDB
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

type panickingUpdateDB struct {
	kv.RwDB
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (db *panickingUpdateDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	if db.calls.Add(1) == 1 {
		close(db.started)
		<-db.release
		panic("injected update panic")
	}
	return db.RwDB.Update(ctx, f)
}

type observedContext struct {
	context.Context
	doneObserved chan struct{}
	once         sync.Once
}

func (ctx *observedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.doneObserved) })
	return ctx.Context.Done()
}

func (db *blockingUpdateDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	if db.calls.Add(1) == 1 {
		close(db.started)
		select {
		case <-db.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return db.RwDB.Update(ctx, f)
}

func (db *failingUpdateDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	db.calls++
	if db.fail {
		return errors.New("injected update failure")
	}
	return db.RwDB.Update(ctx, f)
}

type pendingRetryForkGraph struct {
	fork_graph.ForkGraph
	completed         common.Hash
	completedEnvelope *cltypes.SignedExecutionPayloadEnvelope
}

type transientEnvelopeReadForkGraph struct {
	pendingRetryForkGraph
	fail atomic.Bool
}

type replacingPendingForkGraph struct {
	fork_graph.ForkGraph
	replace func()
}

type missingBlockForkGraph struct {
	pendingRetryForkGraph
	state *state2.CachingBeaconState
}

func (g replacingPendingForkGraph) GetState(common.Hash, bool) (*state2.CachingBeaconState, error) {
	g.replace()
	return nil, nil
}

func (g replacingPendingForkGraph) HasEnvelope(common.Hash) bool { return false }

func (g missingBlockForkGraph) GetState(common.Hash, bool) (*state2.CachingBeaconState, error) {
	return g.state, nil
}

func (g missingBlockForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return nil, false
}

func (g *transientEnvelopeReadForkGraph) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if g.fail.Load() {
		return nil, errors.New("injected envelope read failure")
	}
	return g.pendingRetryForkGraph.ReadEnvelopeFromDisk(root)
}

func TestApplyLocalSelfBuildEnvelopeRejectsNilPayloadAtIngress(t *testing.T) {
	f := &ForkChoiceStore{}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{}}

	require.ErrorContains(t, f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope), "nil payload")
}

func (g pendingRetryForkGraph) HasEnvelope(root common.Hash) bool { return root == g.completed }
func (g pendingRetryForkGraph) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if root != g.completed {
		return nil, nil
	}
	return g.completedEnvelope, nil
}
func (g pendingRetryForkGraph) GetState(common.Hash, bool) (*state2.CachingBeaconState, error) {
	return nil, nil
}

func TestApplyPendingEnvelopeDoesNotIndexConcurrentWinner(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: blockRoot}}
	pending.Add(blockRoot, envelope)
	f := &ForkChoiceStore{
		forkGraph:        payloadVoteForkGraph{hasEnvelope: true},
		pendingEnvelopes: pending,
	}

	appliedEnvelope := f.applyPendingEnvelope(context.Background(), blockRoot, envelope, false, false)
	require.Nil(t, appliedEnvelope)
	require.False(t, pending.Contains(blockRoot))
}

func TestApplyPendingEnvelopeDropsHardFailure(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{}
	pending.Add(blockRoot, envelope)
	f := &ForkChoiceStore{
		forkGraph:                      payloadVoteForkGraph{},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
	}

	require.Nil(t, f.applyPendingEnvelope(context.Background(), blockRoot, envelope, false, false))
	require.False(t, pending.Contains(blockRoot))
}

func TestPendingEnvelopeErrorClassification(t *testing.T) {
	f := &ForkChoiceStore{}
	require.True(t, f.retryPendingEnvelopeError(errors.New("temporary disk failure"), nil))
	require.True(t, f.retryPendingEnvelopeError(ErrEIP7594ColumnDataNotAvailable, nil))
	require.False(t, f.retryPendingEnvelopeError(fmt.Errorf("%w: bad signature", errInvalidExecutionPayloadEnvelope), nil))
}

func TestRetryPendingExecutionPayloadEnvelopesCleansCompletedWork(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	otherRoot := common.HexToHash("0x5678")
	pending.Add(blockRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: blockRoot}})
	pending.Add(otherRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: otherRoot}})
	f := &ForkChoiceStore{
		forkGraph:                      payloadVoteForkGraph{hasEnvelope: true},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
	}

	f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
	require.Equal(t, 1, pending.Len())
}

func TestRetryPendingExecutionPayloadEnvelopesSharesBudgetAcrossOrigins(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](2)
	require.NoError(t, err)
	localRoot := common.HexToHash("0x1234")
	gossipRoot := common.HexToHash("0x5678")
	local.Add(localRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: localRoot}})
	pending.Add(gossipRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: gossipRoot}})
	f := &ForkChoiceStore{
		forkGraph:                      payloadVoteForkGraph{hasEnvelope: true},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
	}

	f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 2)
	require.Zero(t, local.Len())
	require.Zero(t, pending.Len())
}

func TestRetryPendingExecutionPayloadEnvelopesRotatesFailures(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](3)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	roots := []common.Hash{common.HexToHash("0x1"), common.HexToHash("0x2"), common.HexToHash("0x3")}
	for _, root := range roots {
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
		envelope.Message.BeaconBlockRoot = root
		pending.Add(root, envelope)
	}
	completedEnvelope, ok := pending.Peek(roots[2])
	require.True(t, ok)
	f := &ForkChoiceStore{
		forkGraph:                      pendingRetryForkGraph{completed: roots[2], completedEnvelope: completedEnvelope},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
	}

	f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 2)
	require.True(t, pending.Contains(roots[2]))
	f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 2)
	require.False(t, pending.Contains(roots[2]))
}

func TestRetryPendingExecutionPayloadEnvelopesDropsMissingBlockOlderThanFinality(t *testing.T) {
	for _, localOrigin := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", localOrigin), func(t *testing.T) {
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](3)
			require.NoError(t, err)
			local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](3)
			require.NoError(t, err)
			staleRoot := common.HexToHash("0x1234")
			boundaryRoot := common.HexToHash("0x3456")
			recentRoot := common.HexToHash("0x5678")
			stale := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
			stale.Message.BeaconBlockRoot = staleRoot
			stale.Message.Payload.SlotNumber = 63
			boundary := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
			boundary.Message.BeaconBlockRoot = boundaryRoot
			boundary.Message.Payload.SlotNumber = 64
			recent := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
			recent.Message.BeaconBlockRoot = recentRoot
			recent.Message.Payload.SlotNumber = 94
			origin := pending
			if localOrigin {
				origin = local
			}
			origin.Add(staleRoot, stale)
			origin.Add(boundaryRoot, boundary)
			origin.Add(recentRoot, recent)
			f := &ForkChoiceStore{
				beaconCfg:                      &clparams.MainnetBeaconConfig,
				forkGraph:                      pendingRetryForkGraph{},
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: local,
			}
			f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 2})

			f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
			require.False(t, origin.Contains(staleRoot))
			require.True(t, origin.Contains(boundaryRoot))
			require.True(t, origin.Contains(recentRoot))
			f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 2)
			require.True(t, origin.Contains(boundaryRoot))
			require.True(t, origin.Contains(recentRoot))

			f.forkGraph = pendingRetryForkGraph{completed: recentRoot, completedEnvelope: recent}
			f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 2)
			require.True(t, origin.Contains(boundaryRoot))
			require.False(t, origin.Contains(recentRoot))
		})
	}
}

func TestRetryPendingExecutionPayloadEnvelopesDropsFarFutureSlot(t *testing.T) {
	for _, localOrigin := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", localOrigin), func(t *testing.T) {
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			blockRoot := common.HexToHash("0x1234")
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
			envelope.Message.BeaconBlockRoot = blockRoot
			clock := eth_clock.NewEthereumClock(0, common.Hash{}, &clparams.MainnetBeaconConfig)
			envelope.Message.Payload.SlotNumber = clock.GetCurrentSlot() + (uint64(1) << 62)
			origin := pending
			if localOrigin {
				origin = local
			}
			origin.Add(blockRoot, envelope)
			f := &ForkChoiceStore{
				beaconCfg:                      &clparams.MainnetBeaconConfig,
				ethClock:                       clock,
				forkGraph:                      pendingRetryForkGraph{},
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: local,
			}
			f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 2})

			f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
			require.False(t, origin.Contains(blockRoot))
		})
	}
}

func TestRetryPendingExecutionPayloadEnvelopesKeepsNextSlotWithinClockDisparity(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	envelope.Message.Payload.SlotNumber = 101
	pending.Add(blockRoot, envelope)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(uint64(100))
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(101)).Return(true)
	f := &ForkChoiceStore{
		beaconCfg:                      &clparams.MainnetBeaconConfig,
		ethClock:                       clock,
		forkGraph:                      pendingRetryForkGraph{},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 2})

	f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
	require.True(t, pending.Contains(blockRoot))
}

func TestMissingBlockExecutionPayloadEnvelopeQueuesAtIngress(t *testing.T) {
	for _, missingState := range []bool{false, true} {
		for _, localOrigin := range []bool{false, true} {
			t.Run(fmt.Sprintf("missing-state=%t/local=%t", missingState, localOrigin), func(t *testing.T) {
				pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
				require.NoError(t, err)
				local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
				require.NoError(t, err)
				blockRoot := common.HexToHash("0x1234")
				envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
				envelope.Message.BeaconBlockRoot = blockRoot
				var graph fork_graph.ForkGraph = missingBlockForkGraph{state: state2.New(&clparams.MainnetBeaconConfig)}
				if missingState {
					graph = pendingRetryForkGraph{}
				}
				f := &ForkChoiceStore{
					forkGraph:                      graph,
					pendingEnvelopes:               pending,
					pendingLocalSelfBuildEnvelopes: local,
				}

				if localOrigin {
					require.ErrorIs(t, f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope), ErrIgnore)
					require.True(t, local.Contains(blockRoot))
				} else {
					require.ErrorIs(t, f.OnExecutionPayload(context.Background(), envelope, false, true), ErrIgnore)
					require.True(t, pending.Contains(blockRoot))
				}

				f.forkGraph = pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope}
				f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
				require.False(t, local.Contains(blockRoot))
				require.False(t, pending.Contains(blockRoot))
			})
		}
	}
}

func TestRetryPendingExecutionPayloadEnvelopesKeepsConcurrentReplacement(t *testing.T) {
	for _, localOrigin := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", localOrigin), func(t *testing.T) {
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			blockRoot := common.HexToHash("0x1234")
			stale := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
			stale.Message.BeaconBlockRoot = blockRoot
			stale.Message.Payload.SlotNumber = 63
			replacement := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
			replacement.Message.BeaconBlockRoot = blockRoot
			replacement.Message.Payload.SlotNumber = 64
			origin := pending
			if localOrigin {
				origin = local
			}
			origin.Add(blockRoot, stale)
			f := &ForkChoiceStore{
				beaconCfg:                      &clparams.MainnetBeaconConfig,
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: local,
			}
			f.forkGraph = replacingPendingForkGraph{replace: func() { origin.Add(blockRoot, replacement) }}
			f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 2})

			f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
			queued, ok := origin.Peek(blockRoot)
			require.True(t, ok)
			require.Same(t, replacement, queued)
		})
	}
}

func TestRetryPendingExecutionPayloadEnvelopesHandlesMalformedEnvelope(t *testing.T) {
	for name, envelope := range map[string]*cltypes.SignedExecutionPayloadEnvelope{
		"nil message": {},
		"nil payload": {Message: &cltypes.ExecutionPayloadEnvelope{}},
	} {
		t.Run(name, func(t *testing.T) {
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			blockRoot := common.HexToHash("0x1234")
			if envelope.Message != nil {
				envelope.Message.BeaconBlockRoot = blockRoot
			}
			pending.Add(blockRoot, envelope)
			f := &ForkChoiceStore{
				beaconCfg:                      &clparams.MainnetBeaconConfig,
				forkGraph:                      pendingRetryForkGraph{},
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: local,
			}
			f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 2})

			require.NotPanics(t, func() {
				f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
			})
			require.False(t, pending.Contains(blockRoot))
		})
	}
}

func TestPendingEnvelopeIndexWriteRetriesThroughOriginQueue(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	envelope.Message.Payload.BlockNumber = 42
	envelope.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	pending.Add(blockRoot, envelope)
	db := &failingUpdateDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB), fail: true}
	f := &ForkChoiceStore{
		forkGraph:                      pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	require.True(t, pending.Contains(blockRoot))
	require.Equal(t, 1, db.calls)
	db.fail = false
	f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
	require.False(t, pending.Contains(blockRoot))
	require.Equal(t, 2, db.calls)
}

func TestIndexRepairFailureQueuesPersistedEnvelope(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = blockRoot
	persisted.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	redelivered := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	redelivered.Message.BeaconBlockRoot = blockRoot
	redelivered.Message.Payload.BlockHash = common.HexToHash("0xbbbb")
	f := &ForkChoiceStore{
		forkGraph:        pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted},
		pendingEnvelopes: pending,
		db:               &failingUpdateDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB), fail: true},
	}

	require.Error(t, f.OnExecutionPayload(context.Background(), redelivered, false, false))
	queued, ok := pending.Get(blockRoot)
	require.True(t, ok)
	require.Same(t, persisted, queued)
}

func TestIndexRepairReadFailureQueuesRootRepair(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = blockRoot
	persisted.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	redelivered := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	redelivered.Message.BeaconBlockRoot = blockRoot
	redelivered.Message.Payload.BlockHash = common.HexToHash("0xbbbb")
	graph := &transientEnvelopeReadForkGraph{pendingRetryForkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted}}
	graph.fail.Store(true)
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	f := &ForkChoiceStore{
		forkGraph:                      graph,
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	require.Error(t, f.OnExecutionPayload(context.Background(), redelivered, false, false))
	queued, ok := pending.Get(blockRoot)
	require.True(t, ok)
	require.Nil(t, queued)
	graph.fail.Store(false)
	f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
	require.False(t, pending.Contains(blockRoot))
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestOnExecutionPayloadRedeliveryRepairsMissingIndices(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	envelope.Message.Payload.BlockNumber = 42
	envelope.Message.Payload.BlockHash = executionHash
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	f := &ForkChoiceStore{
		forkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope},
		db:        db,
	}

	require.NoError(t, f.OnExecutionPayload(context.Background(), envelope, false, true))
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, uint64(42), *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, executionHash, blockHash)
		return nil
	}))
}

func TestOnExecutionPayloadRedeliverySkipsExistingIndices(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	envelope.Message.Payload.BlockNumber = 42
	envelope.Message.Payload.BlockHash = executionHash
	rwdb := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, rwdb.Update(context.Background(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, envelope.Message)
	}))
	db := &failingUpdateDB{RwDB: rwdb}
	f := &ForkChoiceStore{
		forkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope},
		db:        db,
	}

	require.NoError(t, f.OnExecutionPayload(context.Background(), envelope, false, true))
	require.Zero(t, db.calls)
}

func TestExecutionPayloadIndexWritesCollapseByRoot(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	db := &blockingUpdateDB{
		RwDB:    memdb.NewTestDB(t, dbcfg.ChainDB),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	f := &ForkChoiceStore{
		forkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope},
		db:        db,
	}

	results := make(chan error, 1)
	go func() {
		_, err := f.ensureExecutionPayloadEnvelopeIndices(context.Background(), blockRoot, envelope, true)
		results <- err
	}()
	<-db.started
	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.ensureExecutionPayloadEnvelopeIndices(waiterCtx, blockRoot, envelope, false)
	require.ErrorIs(t, err, context.Canceled)
	close(db.release)
	require.NoError(t, <-results)
	require.Equal(t, int32(1), db.calls.Load())
}

func TestExecutionPayloadIndexWriteCanceledLeaderDoesNotPoisonWaiter(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	db := &blockingUpdateDB{
		RwDB:    memdb.NewTestDB(t, dbcfg.ChainDB),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	f := &ForkChoiceStore{forkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope}, db: db}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := f.ensureExecutionPayloadEnvelopeIndices(leaderCtx, blockRoot, envelope, true)
		leaderDone <- err
	}()
	<-db.started
	waiterCtx := &observedContext{Context: context.Background(), doneObserved: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() {
		_, err := f.ensureExecutionPayloadEnvelopeIndices(waiterCtx, blockRoot, envelope, false)
		waiterDone <- err
	}()
	<-waiterCtx.doneObserved
	cancelLeader()
	require.ErrorIs(t, <-leaderDone, context.Canceled)
	require.NoError(t, <-waiterDone)
	require.Equal(t, int32(2), db.calls.Load())
}

func TestExecutionPayloadIndexWritePanicDoesNotReportSuccessToWaiter(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	db := &panickingUpdateDB{RwDB: memdb.NewTestDB(t, dbcfg.ChainDB), started: make(chan struct{}), release: make(chan struct{})}
	f := &ForkChoiceStore{forkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope}, db: db}
	leaderDone := make(chan any, 1)
	go func() {
		defer func() { leaderDone <- recover() }()
		_, _ = f.ensureExecutionPayloadEnvelopeIndices(context.Background(), blockRoot, envelope, true)
	}()
	<-db.started
	waiterCtx := &observedContext{Context: context.Background(), doneObserved: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() {
		_, err := f.ensureExecutionPayloadEnvelopeIndices(waiterCtx, blockRoot, envelope, false)
		waiterDone <- err
	}()
	<-waiterCtx.doneObserved
	close(db.release)
	require.Equal(t, "injected update panic", <-leaderDone)
	require.NoError(t, <-waiterDone)
	require.Equal(t, int32(2), db.calls.Load())
}

// TestValidateEnvelopeAgainstBlock_NoBid tests that validation fails when block has no bid
func TestValidateEnvelopeAgainstBlock_NoBid(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.SlotNumber = 100 // Must match block.Slot to pass slot_number check
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload:      payload,
		},
	}

	// Block without bid (SignedExecutionPayloadBid is nil by default)
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = nil // Explicitly set to nil

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block missing signed_execution_payload_bid")
}

// TestValidateEnvelopeAgainstBlock_SlotNumberMismatch tests that validation fails when
// block.slot != envelope.payload.slot_number (EIP-7843 / GLOAS p2p-interface REJECT rule).
func TestValidateEnvelopeAgainstBlock_SlotNumberMismatch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	blockHash := common.HexToHash("0x1234")
	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.BlockHash = blockHash
	payload.SlotNumber = 200 // Different from block slot

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload:      payload,
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          blockHash,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100, // Different from payload.SlotNumber
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "block slot 100 != envelope.payload.slot_number 200")
}

// TestValidateEnvelopeAgainstBlock_BuilderIndexMismatch tests that validation fails when builder indices don't match
func TestValidateEnvelopeAgainstBlock_BuilderIndexMismatch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	blockHash := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload: &cltypes.Eth1Block{
				BlockHash:  blockHash,
				SlotNumber: 100, // Match block.Slot to pass slot_number check
			},
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       2, // Different builder
			BlockHash:          blockHash,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "envelope builder_index 1 != bid builder_index 2")
}

// TestValidateEnvelopeAgainstBlock_NilPayload tests that validation fails when envelope has no payload
func TestValidateEnvelopeAgainstBlock_NilPayload(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload:      nil, // No payload
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          common.HexToHash("0x1234"),
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "envelope missing payload")
}

// TestValidateEnvelopeAgainstBlock_BlockHashMismatch tests that validation fails when block hashes don't match
func TestValidateEnvelopeAgainstBlock_BlockHashMismatch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BuilderIndex: 1,
			Payload: &cltypes.Eth1Block{
				BlockHash:  common.HexToHash("0x1111"), // Different hash
				SlotNumber: 100,                        // Match block.Slot
			},
		},
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          common.HexToHash("0x2222"), // Different hash
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.validateEnvelopeAgainstBlock(envelope, block, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload block_hash")
	require.Contains(t, err.Error(), "!= bid block_hash")
}

// TestCheckDataAvailability_NoBid tests that checkDataAvailability returns nil when there's no bid
func TestCheckDataAvailability_NoBid(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = nil

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.checkDataAvailability(context.TODO(), block, common.Hash{})
	require.NoError(t, err)
}

// TestCheckDataAvailability_NoBlobs tests that checkDataAvailability returns nil when there are no blobs
func TestCheckDataAvailability_NoBlobs(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{beaconCfg: cfg}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       1,
			BlockHash:          common.HexToHash("0x1234"),
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48), // Empty
		},
	}

	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	err := f.checkDataAvailability(context.TODO(), block, common.Hash{})
	require.NoError(t, err)
}

// TestValidatePayloadWithEL_NoEngine tests that validatePayloadWithEL returns nil when there's no engine
func TestValidatePayloadWithEL_NoEngine(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &ForkChoiceStore{
		beaconCfg: cfg,
		engine:    nil, // No engine
	}

	envelope := &cltypes.ExecutionPayloadEnvelope{
		Payload: cltypes.NewEth1Block(clparams.GloasVersion, cfg),
	}

	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
			Body: body,
		},
	}

	_, err := f.validatePayloadWithEL(context.TODO(), envelope, block, common.Hash{})
	require.NoError(t, err)
}

func TestValidatePayloadWithELDoesNotRelockForkChoiceMu(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for _, tt := range []struct {
		name       string
		status     execution_client.PayloadStatus
		wantErr    bool
		wantVerify bool
	}{
		{
			name:       "validated",
			status:     execution_client.PayloadStatusValidated,
			wantVerify: true,
		},
		{
			name:    "invalidated",
			status:  execution_client.PayloadStatusInvalidated,
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			engine := execution_client.NewMockExecutionEngine(ctrl)
			engine.EXPECT().
				NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(tt.status, nil)

			verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](16)
			require.NoError(t, err)
			executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
			require.NoError(t, err)
			payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
			require.NoError(t, err)
			executionPayloadGasLimit, err := lru.New[common.Hash, uint64](16)
			require.NoError(t, err)

			blockRoot := common.HexToHash("0x1234")
			executionBlockHash := common.HexToHash("0xabcd")
			invalidatedHeader := common.Hash{}
			f := &ForkChoiceStore{
				beaconCfg:                cfg,
				engine:                   engine,
				forkGraph:                payloadVoteForkGraph{invalidatedHeader: &invalidatedHeader},
				verifiedExecutionPayload: verifiedExecutionPayload,
				executionPayloadStatus:   executionPayloadStatus,
				payloadStatusByRoot:      payloadStatusByRoot,
				executionPayloadGasLimit: executionPayloadGasLimit,
			}
			envelope := &cltypes.ExecutionPayloadEnvelope{
				Payload: &cltypes.Eth1Block{BlockHash: executionBlockHash},
			}
			body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
			body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
				Message: &cltypes.ExecutionPayloadBid{
					BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
				},
			}
			block := &cltypes.SignedBeaconBlock{
				Block: &cltypes.BeaconBlock{
					Body: body,
				},
			}

			done := make(chan error, 1)
			go func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				status, validationErr := f.validatePayloadWithEL(context.Background(), envelope, block, blockRoot)
				done <- f.applyPayloadValidationResultLocked(status, validationErr, envelope, block, blockRoot)
			}()

			select {
			case err := <-done:
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(time.Second):
				t.Fatal("validatePayloadWithEL blocked while forkchoice mutex was already held")
			}
			require.Equal(t, tt.wantVerify, f.IsPayloadVerified(blockRoot))
			if tt.status == execution_client.PayloadStatusInvalidated {
				require.Equal(t, blockRoot, invalidatedHeader)
			}
		})
	}
}

func TestValidatePayloadWithELReleasesForkChoiceMuDuringNewPayload(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engineStarted := make(chan struct{})
	releaseEngine := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			close(engineStarted)
			<-releaseEngine
			return execution_client.PayloadStatusValidated, nil
		})

	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](16)
	require.NoError(t, err)
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](16)
	require.NoError(t, err)

	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                payloadVoteForkGraph{},
		verifiedExecutionPayload: verifiedExecutionPayload,
		executionPayloadStatus:   executionPayloadStatus,
		payloadStatusByRoot:      payloadStatusByRoot,
		executionPayloadGasLimit: executionPayloadGasLimit,
	}
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
	}}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Body: body}}
	envelope := cltypes.NewExecutionPayloadEnvelope(cfg)
	envelope.Payload.BlockHash = common.HexToHash("0xabcd")

	done := make(chan error, 1)
	go func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		status, validationErr := f.validatePayloadWithEL(context.Background(), envelope, block, common.HexToHash("0x1234"))
		done <- f.applyPayloadValidationResultLocked(status, validationErr, envelope, block, common.HexToHash("0x1234"))
	}()
	<-engineStarted

	lockAcquired := make(chan struct{})
	go func() {
		f.mu.Lock()
		close(lockAcquired)
		f.mu.Unlock()
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		close(releaseEngine)
		t.Fatal("forkchoice mutex stayed locked during NewPayload")
	}
	close(releaseEngine)
	require.NoError(t, <-done)
}

func TestValidatePayloadWithELAdmissionCancellationIsNotELBehind(t *testing.T) {
	f := &ForkChoiceStore{
		engine:                     execution_client.NewMockExecutionEngine(gomock.NewController(t)),
		payloadValidationAdmission: make(chan struct{}, 1),
	}
	f.payloadValidationOnce.Do(func() {})
	f.payloadValidationAdmission <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f.mu.Lock()
	status, err := f.newPayloadWhileYieldingForkChoiceLock(ctx, common.Hash{}, nil, nil, nil, nil)
	f.mu.Unlock()

	require.EqualValues(t, execution_client.PayloadStatusNone, status)
	require.ErrorIs(t, err, errPayloadValidationAdmission)
	require.ErrorIs(t, err, context.Canceled)
}
