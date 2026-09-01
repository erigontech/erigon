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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/optimistic"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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

type blockRefreshForkGraph struct {
	fork_graph.ForkGraph
	block *cltypes.SignedBeaconBlock
}

type dataAvailabilityForkGraph struct {
	fork_graph.ForkGraph
	state    *state2.CachingBeaconState
	stateErr *error
	block    *cltypes.SignedBeaconBlock
}

type admissionYieldForkGraph struct {
	dataAvailabilityForkGraph
	stateRead   chan struct{}
	once        sync.Once
	hasEnvelope atomic.Bool
}

type admissionPersistingForkGraph struct {
	persistingEnvelopeForkGraph
	stateRead chan struct{}
	once      sync.Once
}

type commitmentYieldForkGraph struct {
	dataAvailabilityForkGraph
	blockReads atomic.Int32
}

type commitmentStateRefreshForkGraph struct {
	dataAvailabilityForkGraph
	stateReads atomic.Int32
}

type commitmentFinalityRefreshForkGraph struct {
	dataAvailabilityForkGraph
	blockReads atomic.Int32
	onRefresh  func()
}

type commitmentFallbackForkGraph struct {
	persistingEnvelopeForkGraph
	blockReads atomic.Int32
}

type dumpFailingForkGraph struct {
	dataAvailabilityForkGraph
	err error
}

type failOnceEnvelopeForkGraph struct {
	persistingEnvelopeForkGraph
	err    error
	failed atomic.Bool
}

type persistingEnvelopeForkGraph struct {
	dataAvailabilityForkGraph
	mu       sync.RWMutex
	envelope *cltypes.SignedExecutionPayloadEnvelope
	invalid  atomic.Bool
}

type interleavingIndexRepairForkGraph struct {
	fork_graph.ForkGraph
	envelope *cltypes.SignedExecutionPayloadEnvelope
	started  chan struct{}
	release  chan struct{}
	reads    atomic.Int32
}

func (g *interleavingIndexRepairForkGraph) HasEnvelope(common.Hash) bool { return true }

func (g *interleavingIndexRepairForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	envelope := g.envelope
	if g.reads.Add(1) == 1 {
		close(g.started)
		<-g.release
	}
	return envelope, nil
}

func (g dataAvailabilityForkGraph) HasEnvelope(common.Hash) bool {
	return false
}

func (g dataAvailabilityForkGraph) GetState(common.Hash, bool) (*state2.CachingBeaconState, error) {
	if g.stateErr != nil && *g.stateErr != nil {
		return nil, *g.stateErr
	}
	return g.state, nil
}

func (g dataAvailabilityForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return g.block, g.block != nil
}

func (g *admissionYieldForkGraph) GetState(root common.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	g.once.Do(func() { close(g.stateRead) })
	return g.dataAvailabilityForkGraph.GetState(root, alwaysCopy)
}

func (g *admissionPersistingForkGraph) GetState(root common.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	g.once.Do(func() { close(g.stateRead) })
	return g.persistingEnvelopeForkGraph.GetState(root, alwaysCopy)
}

func (g *commitmentYieldForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if g.blockReads.Add(1) > 1 {
		return nil, false
	}
	return g.dataAvailabilityForkGraph.GetBlock(root)
}

func (g *commitmentStateRefreshForkGraph) GetState(root common.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	if g.stateReads.Add(1) > 1 {
		return nil, nil
	}
	return g.dataAvailabilityForkGraph.GetState(root, alwaysCopy)
}

func (g *commitmentFinalityRefreshForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if g.blockReads.Add(1) == 2 {
		g.onRefresh()
	}
	return g.dataAvailabilityForkGraph.GetBlock(root)
}

func (g *commitmentFallbackForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if g.blockReads.Add(1) == 1 {
		return nil, false
	}
	return g.dataAvailabilityForkGraph.GetBlock(root)
}

func (g *admissionYieldForkGraph) HasEnvelope(common.Hash) bool {
	return g.hasEnvelope.Load()
}

func (g dumpFailingForkGraph) DumpEnvelopeOnDisk(common.Hash, *cltypes.SignedExecutionPayloadEnvelope) error {
	return g.err
}

func (g *failOnceEnvelopeForkGraph) DumpEnvelopeOnDisk(root common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if g.failed.CompareAndSwap(false, true) {
		return g.err
	}
	return g.persistingEnvelopeForkGraph.DumpEnvelopeOnDisk(root, envelope)
}

func (g *persistingEnvelopeForkGraph) HasEnvelope(common.Hash) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.envelope != nil
}

func (g *persistingEnvelopeForkGraph) DumpEnvelopeOnDisk(_ common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.envelope = envelope
	return nil
}

func (g *persistingEnvelopeForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.envelope, nil
}

func (g *persistingEnvelopeForkGraph) MarkHeaderAsInvalid(common.Hash) {
	g.invalid.Store(true)
}

func (g blockRefreshForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return g.block, g.block != nil
}

func (blockRefreshForkGraph) GetState(common.Hash, bool) (*state2.CachingBeaconState, error) {
	panic("refresh must not replay state")
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

type transientViewDB struct {
	kv.RwDB
	fail atomic.Bool
}

func (db *transientViewDB) View(ctx context.Context, f func(kv.Tx) error) error {
	if db.fail.Load() {
		return errors.New("injected view failure")
	}
	return db.RwDB.View(ctx, f)
}

type transientEnvelopeReadForkGraph struct {
	pendingRetryForkGraph
	fail atomic.Bool
}

type countingEnvelopeReadForkGraph struct {
	pendingRetryForkGraph
	reads atomic.Int32
}

type admissionEnvelopeReadForkGraph struct {
	fork_graph.ForkGraph
	hasEnvelope atomic.Bool
	envelope    *cltypes.SignedExecutionPayloadEnvelope
	readEntered chan struct{}
	releaseRead chan struct{}
	readErr     error
	clearOnRead bool
}

func (g *admissionEnvelopeReadForkGraph) HasEnvelope(common.Hash) bool {
	return g.hasEnvelope.Load()
}

func (g *admissionEnvelopeReadForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if g.readEntered != nil {
		close(g.readEntered)
	}
	if g.releaseRead != nil {
		<-g.releaseRead
	}
	if g.clearOnRead {
		g.hasEnvelope.Store(false)
	}
	return g.envelope, g.readErr
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

func (g *countingEnvelopeReadForkGraph) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	g.reads.Add(1)
	return g.pendingRetryForkGraph.ReadEnvelopeFromDisk(root)
}

func TestEnvelopeGossipClaimStopsAfterCancellationDuringPersistedRead(t *testing.T) {
	graph := &admissionEnvelopeReadForkGraph{
		readEntered: make(chan struct{}),
		releaseRead: make(chan struct{}),
		readErr:     errors.New("injected envelope read failure"),
	}
	graph.hasEnvelope.Store(true)
	store := &ForkChoiceStore{forkGraph: graph}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := store.ClaimExecutionPayloadEnvelopeForGossip(ctx, common.HexToHash("0x1234"), 42)
		result <- err
	}()
	<-graph.readEntered
	cancel()
	close(graph.releaseRead)
	require.ErrorIs(t, <-result, context.Canceled)

	graph.hasEnvelope.Store(false)
	token, err := store.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), common.HexToHash("0x1234"), 42)
	require.NoError(t, err)
	store.FinishExecutionPayloadEnvelopeForGossip(token, false)
}

func TestEnvelopeGossipClaimTreatsPersistedReadFailureAsBusy(t *testing.T) {
	graph := &admissionEnvelopeReadForkGraph{readErr: errors.New("injected envelope read failure")}
	graph.hasEnvelope.Store(true)
	store := &ForkChoiceStore{forkGraph: graph}

	_, err := store.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), common.HexToHash("0x1234"), 42)
	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAdmissionBusy)

	graph.hasEnvelope.Store(false)
	token, err := store.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), common.HexToHash("0x1234"), 42)
	require.NoError(t, err)
	store.FinishExecutionPayloadEnvelopeForGossip(token, false)
}

func TestEnvelopeGossipClaimDescribesIncompletePersistedEnvelope(t *testing.T) {
	graph := &admissionEnvelopeReadForkGraph{}
	graph.hasEnvelope.Store(true)
	store := &ForkChoiceStore{forkGraph: graph}

	_, err := store.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), common.HexToHash("0x1234"), 42)
	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAdmissionBusy)
	require.ErrorContains(t, err, "persisted execution payload envelope is incomplete")
}

func TestEnvelopeGossipClaimRepairsPersistedEnvelopeClearedByRead(t *testing.T) {
	graph := &admissionEnvelopeReadForkGraph{
		readErr:     errors.New("corrupt envelope"),
		clearOnRead: true,
	}
	graph.hasEnvelope.Store(true)
	store := &ForkChoiceStore{forkGraph: graph}

	token, err := store.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), common.HexToHash("0x1234"), 42)
	require.NoError(t, err)
	store.FinishExecutionPayloadEnvelopeForGossip(token, false)
}

func TestApplyLocalSelfBuildEnvelopeRejectsNilPayloadAtIngress(t *testing.T) {
	f := &ForkChoiceStore{}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{}}

	require.ErrorContains(t, f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope), "nil payload")
}

func validIngressEnvelope(cfg *clparams.BeaconChainConfig, blockRoot common.Hash) *cltypes.SignedExecutionPayloadEnvelope {
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = blockRoot
	envelope.Message.Payload.Extra = solid.NewExtraData()
	envelope.Message.Payload.Transactions = &solid.TransactionsSSZ{}
	envelope.Message.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	return envelope
}

func (g pendingRetryForkGraph) HasEnvelope(root common.Hash) bool { return root == g.completed }
func (g pendingRetryForkGraph) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return nil, false
}
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

func TestPendingExecutionPayloadRejectsInvalidCommitmentsBeforePersistence(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	envelope.Message.Payload.GasUsed++
	resignAdmissionEnvelope(t, cfg, blockState, envelope)
	root := envelope.Message.BeaconBlockRoot
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	pending.Add(root, envelope)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	f := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, pendingEnvelopes: pending}

	require.Nil(t, f.applyPendingEnvelope(t.Context(), root, envelope, false, false))
	require.False(t, pending.Contains(root))
	require.False(t, graph.HasEnvelope(root))
}

func TestPendingEnvelopeErrorClassification(t *testing.T) {
	f := &ForkChoiceStore{beaconCfg: &clparams.MainnetBeaconConfig}
	f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 2})
	recent := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	recent.Message.Payload.SlotNumber = 64
	stale := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	stale.Message.Payload.SlotNumber = 63
	require.True(t, f.retryPendingEnvelopeError(errors.New("temporary disk failure"), nil))
	require.True(t, f.retryPendingEnvelopeError(errors.New("temporary disk failure"), stale))
	require.True(t, f.retryPendingEnvelopeError(fmt.Errorf("%w: %w", errPayloadValidationAdmission, context.Canceled), recent))
	require.False(t, f.retryPendingEnvelopeError(fmt.Errorf("%w: %w", errPayloadValidationAdmission, context.Canceled), stale))
	require.True(t, f.retryPendingEnvelopeError(ErrEIP7594ColumnDataNotAvailable, recent))
	require.False(t, f.retryPendingEnvelopeError(ErrEIP7594ColumnDataNotAvailable, stale))
	require.False(t, f.retryPendingEnvelopeError(fmt.Errorf("%w: bad signature", errInvalidExecutionPayloadEnvelope), nil))
}

func TestOnExecutionPayloadRetainsEnvelopeWhenColumnDataIsUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
	commitments.Append(new(cltypes.KZGCommitment))
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlobKzgCommitments = *commitments
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = blockRoot
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	peerDas := das_mock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, blockRoot).Return(false, nil)
	syncedData := synced_data.NewSyncedDataManager(cfg, true)
	var stateErr error
	f := &ForkChoiceStore{
		forkGraph: dataAvailabilityForkGraph{
			state:    blockState,
			stateErr: &stateErr,
			block:    block,
		},
		beaconCfg:                      cfg,
		peerDas:                        peerDas,
		syncedDataManager:              syncedData,
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
	}

	err = f.OnExecutionPayload(context.Background(), envelope, true, false)
	require.ErrorIs(t, err, ErrEIP7594ColumnDataNotAvailable)
	retained, ok := pending.Peek(blockRoot)
	require.True(t, ok)
	require.Same(t, envelope, retained)

	stateErr = errors.New("temporary state read failure")
	replacement := validIngressEnvelope(cfg, blockRoot)
	replacement.Message.Payload.SlotNumber = block.Block.Slot
	require.Error(t, f.OnExecutionPayload(context.Background(), replacement, false, false))
	retained, ok = pending.Peek(blockRoot)
	require.True(t, ok)
	require.Same(t, envelope, retained)
}

func TestRetryPendingExecutionPayloadEnvelopesDropsStaleStorageFailure(t *testing.T) {
	for _, localOrigin := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", localOrigin), func(t *testing.T) {
			blockRoot := common.HexToHash("0x1234")
			payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
			payload.SlotNumber = 63
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
				BeaconBlockRoot: blockRoot,
				Payload:         payload,
			}}
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
			require.NoError(t, err)
			local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
			require.NoError(t, err)
			origin := pending
			if localOrigin {
				origin = local
			}
			origin.Add(blockRoot, envelope)
			stateErr := errors.New("persistent state read failure")
			f := &ForkChoiceStore{
				forkGraph:                      dataAvailabilityForkGraph{stateErr: &stateErr},
				beaconCfg:                      &clparams.MainnetBeaconConfig,
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: local,
			}
			f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 2})

			f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
			require.False(t, origin.Contains(blockRoot))
		})
	}
}

func TestRetryPendingExecutionPayloadEnvelopesDropsMissingIndexRepairEnvelope(t *testing.T) {
	for _, localOrigin := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", localOrigin), func(t *testing.T) {
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			blockRoot := common.HexToHash("0x1234")
			origin := pending
			if localOrigin {
				origin = local
			}
			origin.Add(blockRoot, nil)
			f := &ForkChoiceStore{
				forkGraph:                      pendingRetryForkGraph{},
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: local,
			}

			f.RetryPendingExecutionPayloadEnvelopes(context.Background(), 1)
			require.False(t, origin.Contains(blockRoot))
		})
	}
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
				envelope := validIngressEnvelope(&clparams.MainnetBeaconConfig, blockRoot)
				var graph fork_graph.ForkGraph = missingBlockForkGraph{state: state2.New(&clparams.MainnetBeaconConfig)}
				if missingState {
					graph = pendingRetryForkGraph{}
				}
				f := &ForkChoiceStore{
					forkGraph:                      graph,
					beaconCfg:                      &clparams.MainnetBeaconConfig,
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
	db := &failingUpdateDB{RwDB: mdbxtest.NewTestDB(t, dbcfg.ChainDB), fail: true}
	f := &ForkChoiceStore{
		forkGraph:                      pendingRetryForkGraph{completed: blockRoot, completedEnvelope: envelope},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	require.False(t, pending.Contains(blockRoot))
	require.Len(t, f.envelopeIndexRepairs.repairs(), 1)
	require.Equal(t, 1, db.calls)
	db.fail = false
	f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)
	require.False(t, pending.Contains(blockRoot))
	require.Equal(t, 2, db.calls)
}

func TestIndexRepairSurvivesUnknownRootQueueAdmissions(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = blockRoot
	persisted.Message.Payload.BlockNumber = 42
	persisted.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	pending.Add(blockRoot, nil)
	rwdb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db := &failingUpdateDB{RwDB: rwdb, fail: true}
	f := &ForkChoiceStore{
		forkGraph:                      pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	require.False(t, pending.Contains(blockRoot))
	require.Len(t, f.envelopeIndexRepairs.repairs(), 1)
	for i := range queueCacheSize {
		root := common.BigToHash(new(big.Int).SetUint64(uint64(i + 1)))
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
		envelope.Message.BeaconBlockRoot = root
		pending.Add(root, envelope)
	}
	require.False(t, pending.Contains(blockRoot))

	db.fail = false
	f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)

	require.NoError(t, rwdb.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.NotNil(t, blockNumber)
		require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestIndexRepairAdmissionBackpressuresBeforePersistence(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	injected := errors.New("envelope must not reach persistence")
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	f := &ForkChoiceStore{
		beaconCfg: cfg,
		forkGraph: dumpFailingForkGraph{
			dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
			err:                       injected,
		},
		pendingEnvelopes: pending,
		db:               mdbxtest.NewTestDB(t, dbcfg.ChainDB),
	}
	for i := range queueCacheSize {
		root := common.BigToHash(new(big.Int).SetUint64(uint64(i + 1)))
		_, ok := f.envelopeIndexRepairs.claim(root)
		require.True(t, ok)
	}

	err = f.OnExecutionPayload(context.Background(), envelope, false, false)

	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopePersistenceFailed)
	require.NotErrorIs(t, err, injected)
}

func TestIndexRepairGenerationDoesNotClearReplacement(t *testing.T) {
	var repairs envelopeIndexRepairTracker
	root := common.HexToHash("0x1234")
	first, ok := repairs.reserve(root)
	require.True(t, ok)
	repairs.persisted(first, 1, common.HexToHash("0x01"))
	repairs.complete(first)
	replacement, ok := repairs.claim(root)
	require.True(t, ok)
	require.NotEqual(t, first.generation, replacement.generation)

	repairs.complete(first)
	require.Equal(t, []envelopeIndexRepairToken{replacement}, repairs.repairs())
	repairs.complete(replacement)
	require.Empty(t, repairs.repairs())
}

func TestStaleIndexRepairValuesDoNotMutateReplacement(t *testing.T) {
	var repairs envelopeIndexRepairTracker
	root := common.HexToHash("0x1234")
	first, ok := repairs.claim(root)
	require.True(t, ok)
	repairs.complete(first)
	replacement, ok := repairs.claim(root)
	require.True(t, ok)

	stale := repairs.setValues(first, 99, common.HexToHash("0x99"))

	require.Zero(t, stale.generation)
	require.Equal(t, []envelopeIndexRepairToken{replacement}, repairs.repairs())
}

func TestStaleIndexRepairCannotOverwriteConcurrentSuccessfulRepair(t *testing.T) {
	root := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.BlockNumber = 42
	envelope.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	graph := &interleavingIndexRepairForkGraph{
		envelope: envelope,
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	f := &ForkChoiceStore{forkGraph: graph, db: db}
	_, ok := f.envelopeIndexRepairs.claim(root)
	require.True(t, ok)

	retryDone := make(chan struct{})
	go func() {
		f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)
		close(retryDone)
	}()
	<-graph.started
	require.NoError(t, f.StoreAnchorEnvelope(root, envelope))
	close(graph.release)
	<-retryDone

	require.Empty(t, f.envelopeIndexRepairs.repairs())
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
		require.NoError(t, err)
		require.NotNil(t, blockNumber)
		require.Equal(t, envelope.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, envelope.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestUntrackedIndexCheckUsesRequestedNonzeroRoot(t *testing.T) {
	zeroEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	zeroEnvelope.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	db := &failingUpdateDB{RwDB: mdbxtest.NewTestDB(t, dbcfg.ChainDB)}
	f := &ForkChoiceStore{
		forkGraph: pendingRetryForkGraph{completedEnvelope: zeroEnvelope},
		db:        db,
	}
	requestedRoot := common.HexToHash("0x1234")
	requested := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	requested.Message.BeaconBlockRoot = requestedRoot

	_, err := f.ensureClaimedEnvelopeIndexRepair(context.Background(), requestedRoot, envelopeIndexRepairToken{}, false, requested, false)

	require.NoError(t, err)
	require.Zero(t, db.calls)
}

func TestIndexRepairFailureQueuesPersistedEnvelope(t *testing.T) {
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = blockRoot
	persisted.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	pending.Add(blockRoot, nil)
	f := &ForkChoiceStore{
		forkGraph:                      pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             &failingUpdateDB{RwDB: mdbxtest.NewTestDB(t, dbcfg.ChainDB), fail: true},
	}

	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	repairs := f.envelopeIndexRepairs.repairs()
	require.Len(t, repairs, 1)
	require.Equal(t, blockRoot, repairs[0].root)
}

func TestIndexRepairRewritesWellShapedWrongIndices(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = blockRoot
	persisted.Message.Payload.BlockNumber = 42
	persisted.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	pending.Add(blockRoot, persisted)
	rwdb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, rwdb.Update(context.Background(), func(tx kv.RwTx) error {
		wrong := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
		wrong.Payload.BlockNumber = 7
		wrong.Payload.BlockHash = common.HexToHash("0xbbbb")
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, wrong)
	}))
	db := &failingUpdateDB{RwDB: rwdb, fail: true}
	f := &ForkChoiceStore{
		forkGraph:                      pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	require.Len(t, f.envelopeIndexRepairs.repairs(), 1)
	db.fail = false
	f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)

	require.Empty(t, f.envelopeIndexRepairs.repairs())
	require.NoError(t, rwdb.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestIndexRepairSurvivesEnvelopePruning(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	persisted.Message.BeaconBlockRoot = blockRoot
	persisted.Message.Payload.BlockNumber = 42
	persisted.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	pending.Add(blockRoot, persisted)
	rwdb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db := &failingUpdateDB{RwDB: rwdb, fail: true}
	f := &ForkChoiceStore{
		forkGraph:                      pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted},
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	require.Len(t, f.envelopeIndexRepairs.repairs(), 1)
	f.forkGraph = pendingRetryForkGraph{}
	db.fail = false
	f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)

	require.Empty(t, f.envelopeIndexRepairs.repairs())
	require.NoError(t, rwdb.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestIndexRepairSurvivesIndexWritePanic(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	db := &panickingUpdateDB{
		RwDB:    mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	f := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, db: db, eth2Roots: eth2Roots}
	panicResult := make(chan any, 1)
	go func() {
		defer func() { panicResult <- recover() }()
		_ = f.OnExecutionPayload(context.Background(), envelope, false, false)
	}()
	<-db.started
	close(db.release)

	require.Equal(t, "injected update panic", <-panicResult)
	repairs := f.envelopeIndexRepairs.repairs()
	require.Len(t, repairs, 1)
	require.True(t, repairs[0].valuesKnown)
	f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)

	require.Empty(t, f.envelopeIndexRepairs.repairs())
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, envelope.Message.BeaconBlockRoot)
		require.NoError(t, err)
		require.Equal(t, envelope.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, envelope.Message.BeaconBlockRoot)
		require.NoError(t, err)
		require.Equal(t, envelope.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestStoreAnchorEnvelopeCoordinatesCanonicalIndexWrite(t *testing.T) {
	root := common.HexToHash("0x1234")
	first := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	first.Message.BeaconBlockRoot = root
	first.Message.Payload.BlockNumber = 42
	first.Message.Payload.BlockHash = common.HexToHash("0xaaaa")
	second := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	second.Message.BeaconBlockRoot = root
	second.Message.Payload.BlockNumber = 99
	second.Message.Payload.BlockHash = common.HexToHash("0xbbbb")
	graph := &persistingEnvelopeForkGraph{}
	db := &blockingUpdateDB{
		RwDB:    mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	f := &ForkChoiceStore{forkGraph: graph, db: db, eth2Roots: eth2Roots}
	results := make(chan error, 2)
	go func() { results <- f.StoreAnchorEnvelope(root, first) }()
	<-db.started
	go func() { results <- f.StoreAnchorEnvelope(root, second) }()
	close(db.release)

	require.NoError(t, <-results)
	require.NoError(t, <-results)
	persisted, err := graph.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Same(t, first, persisted)
	require.Equal(t, int32(1), db.calls.Load())
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, root)
		require.NoError(t, err)
		require.Equal(t, first.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		require.NoError(t, err)
		require.Equal(t, first.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestIndexRepairDropsUnknownValuesAfterEnvelopeDisappears(t *testing.T) {
	root := common.HexToHash("0x1234")
	f := &ForkChoiceStore{forkGraph: pendingRetryForkGraph{}}
	_, ok := f.envelopeIndexRepairs.claim(root)
	require.True(t, ok)

	f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)

	require.Empty(t, f.envelopeIndexRepairs.repairs())
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
	pending.Add(blockRoot, nil)
	graph := &transientEnvelopeReadForkGraph{pendingRetryForkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted}}
	graph.fail.Store(true)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	f := &ForkChoiceStore{
		forkGraph:                      graph,
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	queued, ok := pending.Get(blockRoot)
	require.True(t, ok)
	require.Nil(t, queued)
	graph.fail.Store(false)
	f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
	require.False(t, pending.Contains(blockRoot))
	require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestOnExecutionPayloadRedeliveryRepairsMissingIndices(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	blockRoot := common.HexToHash("0x1234")
	persisted := validIngressEnvelope(cfg, blockRoot)
	persisted.Message.Payload.BlockNumber = 42
	persisted.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	redelivered := validIngressEnvelope(cfg, blockRoot)
	redelivered.Message.Payload.BlockNumber = 99
	redelivered.Message.Payload.BlockHash = common.HexToHash("0xffff")
	rwdb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db := &failingUpdateDB{RwDB: rwdb}
	graph := &countingEnvelopeReadForkGraph{pendingRetryForkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted}}
	f := &ForkChoiceStore{forkGraph: graph, beaconCfg: cfg, db: db}

	require.NoError(t, f.OnExecutionPayload(context.Background(), redelivered, false, true))
	require.Equal(t, int32(1), graph.reads.Load())
	require.Equal(t, 1, db.calls)
	require.NoError(t, rwdb.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestOnExecutionPayloadRedeliveryRepairsZeroHashIndices(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	blockRoot := common.HexToHash("0x1234")
	persisted := validIngressEnvelope(cfg, blockRoot)
	persisted.Message.Payload.BlockNumber = 42
	persisted.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	redelivered := validIngressEnvelope(cfg, blockRoot)
	redelivered.Message.Payload.BlockNumber = 99
	redelivered.Message.Payload.BlockHash = common.HexToHash("0xffff")
	rwdb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, rwdb.Update(context.Background(), func(tx kv.RwTx) error {
		if err := tx.Put(kv.BlockRootToBlockNumber, blockRoot[:], make([]byte, 4)); err != nil {
			return err
		}
		return tx.Put(kv.BlockRootToBlockHash, blockRoot[:], make([]byte, 32))
	}))
	db := &failingUpdateDB{RwDB: rwdb}
	graph := &countingEnvelopeReadForkGraph{pendingRetryForkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted}}
	f := &ForkChoiceStore{forkGraph: graph, beaconCfg: cfg, db: db}

	require.NoError(t, f.OnExecutionPayload(context.Background(), redelivered, false, true))
	require.Equal(t, int32(1), graph.reads.Load())
	require.Equal(t, 1, db.calls)
	require.NoError(t, rwdb.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestOnExecutionPayloadIndexPrecheckFailureQueuesRootRepair(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
	require.NoError(t, err)
	blockRoot := common.HexToHash("0x1234")
	persisted := validIngressEnvelope(cfg, blockRoot)
	persisted.Message.Payload.BlockNumber = 42
	persisted.Message.Payload.BlockHash = common.HexToHash("0xabcd")
	redelivered := validIngressEnvelope(cfg, blockRoot)
	redelivered.Message.Payload.BlockNumber = 99
	redelivered.Message.Payload.BlockHash = common.HexToHash("0xffff")
	rwdb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db := &transientViewDB{RwDB: rwdb}
	db.fail.Store(true)
	graph := &countingEnvelopeReadForkGraph{pendingRetryForkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted}}
	f := &ForkChoiceStore{
		forkGraph:                      graph,
		beaconCfg:                      cfg,
		pendingEnvelopes:               pending,
		pendingLocalSelfBuildEnvelopes: local,
		db:                             db,
	}

	require.ErrorIs(t, f.OnExecutionPayload(context.Background(), redelivered, false, true), ErrExecutionPayloadEnvelopeIndicesPending)
	repairs := f.envelopeIndexRepairs.repairs()
	require.Len(t, repairs, 1)
	require.Equal(t, blockRoot, repairs[0].root)
	require.Equal(t, int32(1), graph.reads.Load())

	db.fail.Store(false)
	f.RetryPendingExecutionPayloadEnvelopeIndices(context.Background(), 1)
	require.False(t, pending.Contains(blockRoot))
	require.Empty(t, f.envelopeIndexRepairs.repairs())
	require.Equal(t, int32(1), graph.reads.Load())
	require.NoError(t, rwdb.View(context.Background(), func(tx kv.Tx) error {
		blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
		blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		require.NoError(t, err)
		require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
		return nil
	}))
}

func TestOnExecutionPayloadRedeliverySkipsExistingIndices(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	blockRoot := common.HexToHash("0x1234")
	executionHash := common.HexToHash("0xabcd")
	persisted := validIngressEnvelope(cfg, blockRoot)
	persisted.Message.Payload.BlockNumber = 42
	persisted.Message.Payload.BlockHash = executionHash
	redelivered := validIngressEnvelope(cfg, blockRoot)
	redelivered.Message.Payload.BlockNumber = 99
	redelivered.Message.Payload.BlockHash = common.HexToHash("0xffff")
	rwdb := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, rwdb.Update(context.Background(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, blockRoot, persisted.Message)
	}))
	db := &failingUpdateDB{RwDB: rwdb}
	graph := &countingEnvelopeReadForkGraph{pendingRetryForkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted}}
	f := &ForkChoiceStore{
		forkGraph: graph,
		beaconCfg: cfg,
		db:        db,
	}

	require.NoError(t, f.OnExecutionPayload(context.Background(), redelivered, false, true))
	require.Equal(t, int32(1), graph.reads.Load())
	require.Zero(t, db.calls)
}

func TestPendingEnvelopeRepairsMalformedIndices(t *testing.T) {
	for _, test := range []struct {
		name        string
		blockNumber []byte
		blockHash   []byte
	}{
		{name: "short block number", blockNumber: []byte{1}, blockHash: make([]byte, 32)},
		{name: "oversized block number", blockNumber: make([]byte, 5), blockHash: make([]byte, 32)},
		{name: "short block hash", blockNumber: make([]byte, 4), blockHash: []byte{1}},
		{name: "oversized block hash", blockNumber: make([]byte, 4), blockHash: make([]byte, 33)},
		{name: "well-sized wrong values", blockNumber: []byte{0, 0, 0, 7}, blockHash: bytes.Repeat([]byte{7}, 32)},
	} {
		t.Run(test.name, func(t *testing.T) {
			blockRoot := common.HexToHash("0x1234")
			persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
			persisted.Message.BeaconBlockRoot = blockRoot
			persisted.Message.Payload.BlockNumber = 42
			persisted.Message.Payload.BlockHash = common.HexToHash("0xabcd")
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			local, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](1)
			require.NoError(t, err)
			pending.Add(blockRoot, nil)
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
				if err := tx.Put(kv.BlockRootToBlockNumber, blockRoot[:], test.blockNumber); err != nil {
					return err
				}
				return tx.Put(kv.BlockRootToBlockHash, blockRoot[:], test.blockHash)
			}))
			graph := &countingEnvelopeReadForkGraph{pendingRetryForkGraph: pendingRetryForkGraph{completed: blockRoot, completedEnvelope: persisted}}
			f := &ForkChoiceStore{
				forkGraph:                      graph,
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: local,
				db:                             db,
			}

			require.NotPanics(t, func() {
				f.processPendingEnvelopeAfterBlock(context.Background(), blockRoot, false)
			})
			require.Equal(t, int32(1), graph.reads.Load())
			require.NoError(t, db.View(context.Background(), func(tx kv.Tx) error {
				blockNumberBytes, err := tx.GetOne(kv.BlockRootToBlockNumber, blockRoot[:])
				require.NoError(t, err)
				require.Len(t, blockNumberBytes, 4)
				blockHashBytes, err := tx.GetOne(kv.BlockRootToBlockHash, blockRoot[:])
				require.NoError(t, err)
				require.Len(t, blockHashBytes, 32)
				blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
				require.NoError(t, err)
				require.Equal(t, persisted.Message.Payload.BlockNumber, *blockNumber)
				blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
				require.NoError(t, err)
				require.Equal(t, persisted.Message.Payload.BlockHash, blockHash)
				return nil
			}))
		})
	}
}

func TestExecutionPayloadIndexWritesCollapseByRoot(t *testing.T) {
	blockRoot := common.HexToHash("0x1234")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot
	db := &blockingUpdateDB{
		RwDB:    mdbxtest.NewTestDB(t, dbcfg.ChainDB),
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
		RwDB:    mdbxtest.NewTestDB(t, dbcfg.ChainDB),
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
	db := &panickingUpdateDB{RwDB: mdbxtest.NewTestDB(t, dbcfg.ChainDB), started: make(chan struct{}), release: make(chan struct{})}
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

func TestOnExecutionPayloadRejectsNilWithdrawalBeforeForkchoice(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	envelope := cltypes.NewExecutionPayloadEnvelope(cfg)
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	envelope.Payload.Withdrawals.Append(nil)
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	f := &ForkChoiceStore{beaconCfg: cfg}

	require.NotPanics(t, func() {
		err := f.OnExecutionPayload(context.Background(), &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}, false, true)
		require.ErrorContains(t, err, "nil withdrawal at index 0")
	})
}

func TestExecutionPayloadIngressRejectsUnpersistableEnvelopeBeforeForkchoice(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{make([]byte, clparams.MaxChunkSize)})
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	signedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}

	for _, test := range []struct {
		name string
		call func(*ForkChoiceStore) error
	}{
		{name: "remote", call: func(f *ForkChoiceStore) error {
			return f.OnExecutionPayload(context.Background(), signedEnvelope, false, true)
		}},
		{name: "local", call: func(f *ForkChoiceStore) error {
			return f.ApplyLocalSelfBuildEnvelope(context.Background(), signedEnvelope)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := &ForkChoiceStore{beaconCfg: &cfg}
			require.NotPanics(t, func() {
				require.ErrorContains(t, test.call(f), "exceeds max")
			})
		})
	}
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

func TestRefreshEnvelopeBlockDoesNotReplayState(t *testing.T) {
	want := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{}}
	f := &ForkChoiceStore{forkGraph: blockRefreshForkGraph{block: want}}

	var got *cltypes.SignedBeaconBlock
	require.NotPanics(t, func() {
		var err error
		got, err = f.refreshEnvelopeBlockLocked(common.HexToHash("0x1234"))
		require.NoError(t, err)
	})
	require.Same(t, want, got)
}

func TestNewPayloadWithAdmissionSerializesCallers(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	started := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			started <- struct{}{}
			<-release
			return execution_client.PayloadStatusValidated, nil
		})

	f := &ForkChoiceStore{engine: engine}
	call := func(done chan<- error) {
		_, err := f.NewPayloadWithAdmission(context.Background(), nil, nil, nil, nil)
		done <- err
	}
	done := make(chan error, 2)
	go call(done)
	go call(done)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first NewPayload call did not start")
	}
	select {
	case <-started:
		t.Fatal("second NewPayload call bypassed shared admission")
	case <-time.After(20 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second NewPayload call did not start after admission release")
	}
	release <- struct{}{}
	require.NoError(t, <-done)
	require.NoError(t, <-done)
}

func TestPersistedEnvelopeDoesNotSynthesizeValidatedELResult(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
	resignAdmissionEnvelope(t, cfg, blockState, envelope)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(execution_client.PayloadStatusNotValidated, nil)
	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](1)
	require.NoError(t, err)
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	graph := &admissionPersistingForkGraph{
		persistingEnvelopeForkGraph: persistingEnvelopeForkGraph{
			dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
		},
		stateRead: make(chan struct{}),
	}
	f := &ForkChoiceStore{
		beaconCfg:                  cfg,
		engine:                     engine,
		forkGraph:                  graph,
		verifiedExecutionPayload:   verifiedExecutionPayload,
		executionPayloadStatus:     executionPayloadStatus,
		executionPayloadGasLimit:   executionPayloadGasLimit,
		eth2Roots:                  eth2Roots,
		payloadValidationAdmission: make(chan struct{}, 1),
	}
	f.payloadValidationOnce.Do(func() {})
	f.payloadValidationAdmission <- struct{}{}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	done := make(chan error, 1)
	go func() {
		done <- f.OnExecutionPayload(context.Background(), envelope, false, true)
	}()
	<-graph.stateRead
	require.NoError(t, f.OnExecutionPayload(context.Background(), envelope, false, false))
	<-f.payloadValidationAdmission

	require.NoError(t, <-done)
	require.False(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
	if status, ok := executionPayloadStatus.Get(envelope.Message.Payload.BlockHash); ok {
		require.NotEqualValues(t, execution_client.PayloadStatusValidated, status)
	}
}

func TestValidatePayloadWithELDoesNotWaitForUnrelatedForkChoiceWriter(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engineStarted := make(chan struct{})
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			close(engineStarted)
			return execution_client.PayloadStatusValidated, nil
		})

	f := &ForkChoiceStore{
		beaconCfg:                  cfg,
		engine:                     engine,
		forkGraph:                  payloadVoteForkGraph{},
		payloadValidationAdmission: make(chan struct{}, 1),
	}
	f.payloadValidationOnce.Do(func() {})
	f.payloadValidationAdmission <- struct{}{}
	body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
	}}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Body: body}}
	envelope := cltypes.NewExecutionPayloadEnvelope(cfg)

	done := make(chan struct{})
	validationLocked := make(chan struct{})
	startValidation := make(chan struct{})
	go func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		close(validationLocked)
		<-startValidation
		_, _ = f.validatePayloadWithEL(context.Background(), envelope, block, common.HexToHash("0x1234"))
		close(done)
	}()
	<-validationLocked
	close(startValidation)

	writerAcquired := make(chan struct{})
	releaseWriter := make(chan struct{})
	go func() {
		f.mu.Lock()
		close(writerAcquired)
		<-releaseWriter
		f.mu.Unlock()
	}()
	select {
	case <-writerAcquired:
	case <-time.After(time.Second):
		t.Fatal("unrelated writer did not acquire forkchoice lock")
	}
	<-f.payloadValidationAdmission
	select {
	case <-engineStarted:
	case <-time.After(50 * time.Millisecond):
		close(releaseWriter)
		<-done
		t.Fatal("admitted NewPayload waited for an unrelated forkchoice writer")
	}
	close(releaseWriter)
	<-done
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
	status, err := f.newPayloadWhileYieldingForkChoiceLock(ctx, nil, nil, nil, nil)
	f.mu.Unlock()

	require.EqualValues(t, execution_client.PayloadStatusNone, status)
	require.ErrorIs(t, err, errPayloadValidationAdmission)
	require.ErrorIs(t, err, context.Canceled)
}

func TestExecutionPayloadAdmissionCancellationTransfersPendingOwnership(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", local), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
			require.NoError(t, err)
			pendingLocal, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
			require.NoError(t, err)
			f := &ForkChoiceStore{
				beaconCfg:                      cfg,
				engine:                         execution_client.NewMockExecutionEngine(gomock.NewController(t)),
				forkGraph:                      dataAvailabilityForkGraph{state: blockState, block: block},
				pendingEnvelopes:               pending,
				pendingLocalSelfBuildEnvelopes: pendingLocal,
				payloadValidationAdmission:     make(chan struct{}, 1),
			}
			f.payloadValidationOnce.Do(func() {})
			f.payloadValidationAdmission <- struct{}{}
			f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 9})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			if local {
				err = f.ApplyLocalSelfBuildEnvelope(ctx, envelope)
			} else {
				err = f.OnExecutionPayload(ctx, envelope, false, true)
			}
			require.ErrorIs(t, err, errPayloadValidationAdmission)
			require.ErrorIs(t, err, context.Canceled)
			origin := pending
			if local {
				origin = pendingLocal
			}
			queued, ok := origin.Peek(envelope.Message.BeaconBlockRoot)
			require.True(t, ok)
			require.Same(t, envelope, queued)

			f.RetryPendingExecutionPayloadEnvelopes(ctx, 1)
			require.False(t, origin.Contains(envelope.Message.BeaconBlockRoot))
		})
	}
}

func TestLocalSelfBuildPersistenceFailureIsRetryable(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
	require.NoError(t, err)
	injected := errors.New("injected envelope persistence failure")
	f := &ForkChoiceStore{
		beaconCfg: cfg,
		forkGraph: dumpFailingForkGraph{
			dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
			err:                       injected,
		},
		pendingLocalSelfBuildEnvelopes: pending,
	}

	err = f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope)

	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopePersistenceFailed)
	require.ErrorIs(t, err, injected)
	queued, ok := pending.Peek(envelope.Message.BeaconBlockRoot)
	require.True(t, ok)
	require.Same(t, envelope, queued)
}

func TestExecutionPayloadIngressRejectsPayloadThatDoesNotDeriveClaimedBlockHash(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*ForkChoiceStore, *cltypes.SignedExecutionPayloadEnvelope) error
	}{
		{
			name: "gossip validation",
			call: func(f *ForkChoiceStore, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
				return f.ValidateExecutionPayloadEnvelopeForGossip(envelope)
			},
		},
		{
			name: "engine-less sync persistence",
			call: func(f *ForkChoiceStore, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
				return f.OnExecutionPayload(context.Background(), envelope, false, false)
			},
		},
		{
			name: "engine-less network persistence",
			call: func(f *ForkChoiceStore, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
				return f.OnExecutionPayload(context.Background(), envelope, false, true)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			envelope.Message.Payload.GasUsed++
			resignAdmissionEnvelope(t, cfg, blockState, envelope)
			graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
			eth2Roots, err := lru.New[common.Hash, common.Hash](1)
			require.NoError(t, err)
			f := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, eth2Roots: eth2Roots}
			f.finalizedCheckpoint.Store(solid.Checkpoint{})

			err = test.call(f, envelope)

			require.ErrorContains(t, err, "mismatching hash")
			require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
		})
	}
}

func TestExecutionPayloadIngressDoesNotPersistDerivedHashRejectedByEngine(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
	envelope.Message.Payload.GasUsed++
	resignAdmissionEnvelope(t, cfg, blockState, envelope)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, payload *cltypes.Eth1Block, parentRoot *common.Hash, _ []common.Hash, requests []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			bal, err := execution_client.DecodeAndValidateBlockAccessList(payload)
			if err != nil {
				return execution_client.PayloadStatusInvalidated, err
			}
			requestsHash := cltypes.ComputeExecutionRequestHash(requests)
			if _, err := payload.RlpHeader(parentRoot, requestsHash, bal); err != nil {
				return execution_client.PayloadStatusInvalidated, err
			}
			return execution_client.PayloadStatusValidated, nil
		})

	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                graph,
		executionPayloadStatus:   executionPayloadStatus,
		executionPayloadGasLimit: executionPayloadGasLimit,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	err = f.OnExecutionPayload(context.Background(), envelope, false, true)

	require.ErrorIs(t, err, errInvalidExecutionPayloadEnvelope)
	require.True(t, graph.invalid.Load())
	require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
}

func TestExecutionPayloadIngressDoesNotPersistUncheckedHashWhenEngineUnavailable(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
	envelope.Message.Payload.GasUsed++
	resignAdmissionEnvelope(t, cfg, blockState, envelope)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	injected := errors.New("injected engine transport failure")
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(execution_client.PayloadStatusNone, injected)

	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	optimisticStore := optimistic.NewOptimisticStore()
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                graph,
		optimisticStore:          optimisticStore,
		executionPayloadStatus:   executionPayloadStatus,
		executionPayloadGasLimit: executionPayloadGasLimit,
		eth2Roots:                eth2Roots,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	err = f.OnExecutionPayload(context.Background(), envelope, false, true)

	require.ErrorContains(t, err, "mismatching hash")
	require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
	require.False(t, optimisticStore.IsOptimistic(envelope.Message.BeaconBlockRoot))
}

func TestExecutionPayloadIngressPersistsLocallyVerifiedHashWhenEngineUnavailable(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
	resignAdmissionEnvelope(t, cfg, blockState, envelope)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	injected := errors.New("injected engine transport failure")
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(execution_client.PayloadStatusNone, injected)

	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                graph,
		optimisticStore:          optimistic.NewOptimisticStore(),
		executionPayloadStatus:   executionPayloadStatus,
		executionPayloadGasLimit: executionPayloadGasLimit,
		eth2Roots:                eth2Roots,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	err = f.OnExecutionPayload(context.Background(), envelope, false, true)

	require.NoError(t, err)
	require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
	require.Len(t, f.pendingELPayloads, 1)
}

func TestLocalSelfBuildValidatesPayloadHashWhenEngineUnavailable(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(fmt.Sprintf("valid=%t", valid), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
			if !valid {
				envelope.Message.Payload.GasUsed++
			}

			ctrl := gomock.NewController(t)
			engine := execution_client.NewMockExecutionEngine(ctrl)
			injected := errors.New("injected engine transport failure")
			engine.EXPECT().
				NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(execution_client.PayloadStatusNone, injected)

			executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
			require.NoError(t, err)
			eth2Roots, err := lru.New[common.Hash, common.Hash](1)
			require.NoError(t, err)
			graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
			f := &ForkChoiceStore{
				beaconCfg:                cfg,
				engine:                   engine,
				forkGraph:                graph,
				optimisticStore:          optimistic.NewOptimisticStore(),
				executionPayloadStatus:   executionPayloadStatus,
				executionPayloadGasLimit: executionPayloadGasLimit,
				eth2Roots:                eth2Roots,
			}
			f.finalizedCheckpoint.Store(solid.Checkpoint{})

			err = f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope)

			if valid {
				require.NoError(t, err)
				require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
				require.Len(t, f.pendingELPayloads, 1)
			} else {
				require.ErrorContains(t, err, "mismatching hash")
				require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
				require.Empty(t, f.pendingELPayloads)
			}
		})
	}
}

func TestLocalSelfBuildValidatesPayloadHashWithoutEngine(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(fmt.Sprintf("valid=%t", valid), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
			if !valid {
				envelope.Message.Payload.GasUsed++
			}
			eth2Roots, err := lru.New[common.Hash, common.Hash](1)
			require.NoError(t, err)
			graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
			f := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, eth2Roots: eth2Roots}
			f.finalizedCheckpoint.Store(solid.Checkpoint{})

			err = f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope)

			if valid {
				require.NoError(t, err)
				require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
			} else {
				require.ErrorContains(t, err, "mismatching hash")
				require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
			}
		})
	}
}

func TestExecutionPayloadIngressDoesNotOverwriteConcurrentInvalidation(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
	resignAdmissionEnvelope(t, cfg, blockState, envelope)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engineStarted := make(chan struct{})
	releaseEngine := make(chan struct{})
	injected := errors.New("injected engine transport failure")
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			close(engineStarted)
			<-releaseEngine
			return execution_client.PayloadStatusNone, injected
		})

	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	optimisticStore := optimistic.NewOptimisticStore()
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                graph,
		optimisticStore:          optimisticStore,
		executionPayloadStatus:   executionPayloadStatus,
		payloadStatusByRoot:      payloadStatusByRoot,
		executionPayloadGasLimit: executionPayloadGasLimit,
		eth2Roots:                eth2Roots,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	done := make(chan error, 1)
	go func() {
		done <- f.OnExecutionPayload(context.Background(), envelope, false, true)
	}()
	<-engineStarted
	f.MarkPayloadInvalid(envelope.Message.BeaconBlockRoot, envelope.Message.Payload.BlockHash)
	close(releaseEngine)

	require.ErrorIs(t, <-done, errInvalidExecutionPayloadEnvelope)
	status, ok := executionPayloadStatus.Get(envelope.Message.Payload.BlockHash)
	require.True(t, ok)
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
	require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
	require.False(t, optimisticStore.IsOptimistic(envelope.Message.BeaconBlockRoot))
	require.Empty(t, f.pendingELPayloads)
}

func TestExecutionPayloadIngressAppliesTerminalResultBeforeDuplicateShortCircuit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status execution_client.PayloadStatus
	}{
		{name: "invalidated", status: execution_client.PayloadStatusInvalidated},
		{name: "validated", status: execution_client.PayloadStatusValidated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
			resignAdmissionEnvelope(t, cfg, blockState, envelope)

			graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
			ctrl := gomock.NewController(t)
			engine := execution_client.NewMockExecutionEngine(ctrl)
			engineStarted := make(chan struct{})
			releaseEngine := make(chan struct{})
			engine.EXPECT().
				NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
					close(engineStarted)
					<-releaseEngine
					return tc.status, nil
				})

			verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](1)
			require.NoError(t, err)
			executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
			require.NoError(t, err)
			eth2Roots, err := lru.New[common.Hash, common.Hash](1)
			require.NoError(t, err)
			f := &ForkChoiceStore{
				beaconCfg:                cfg,
				engine:                   engine,
				forkGraph:                graph,
				verifiedExecutionPayload: verifiedExecutionPayload,
				executionPayloadStatus:   executionPayloadStatus,
				payloadStatusByRoot:      payloadStatusByRoot,
				executionPayloadGasLimit: executionPayloadGasLimit,
				eth2Roots:                eth2Roots,
			}
			f.finalizedCheckpoint.Store(solid.Checkpoint{})

			done := make(chan error, 1)
			go func() {
				done <- f.OnExecutionPayload(context.Background(), envelope, false, true)
			}()
			<-engineStarted
			require.NoError(t, f.OnExecutionPayload(context.Background(), envelope, false, false))
			close(releaseEngine)
			err = <-done

			if tc.status == execution_client.PayloadStatusInvalidated {
				require.ErrorIs(t, err, errInvalidExecutionPayloadEnvelope)
				require.True(t, graph.invalid.Load())
				require.False(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
			} else {
				require.NoError(t, err)
				require.True(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
			}
			cachedStatus, ok := executionPayloadStatus.Get(envelope.Message.Payload.BlockHash)
			require.True(t, ok)
			require.EqualValues(t, tc.status, cachedStatus)
		})
	}
}

func TestCachedValidatedStatusSurvivesPersistenceFailureRetry(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, retryStatus := range []execution_client.PayloadStatus{
			execution_client.PayloadStatusNone,
			execution_client.PayloadStatusNotValidated,
		} {
			t.Run(fmt.Sprintf("local=%t/status=%d", local, retryStatus), func(t *testing.T) {
				cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
				require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
				resignAdmissionEnvelope(t, cfg, blockState, envelope)

				ctrl := gomock.NewController(t)
				engine := execution_client.NewMockExecutionEngine(ctrl)
				calls := atomic.Int32{}
				engine.EXPECT().
					NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Times(2).
					DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
						if calls.Add(1) == 1 {
							return execution_client.PayloadStatusValidated, nil
						}
						return retryStatus, nil
					})
				verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](1)
				require.NoError(t, err)
				executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
				require.NoError(t, err)
				payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
				require.NoError(t, err)
				executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
				require.NoError(t, err)
				eth2Roots, err := lru.New[common.Hash, common.Hash](1)
				require.NoError(t, err)
				pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
				require.NoError(t, err)
				pendingLocal, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
				require.NoError(t, err)
				injected := errors.New("injected first persistence failure")
				graph := &failOnceEnvelopeForkGraph{
					persistingEnvelopeForkGraph: persistingEnvelopeForkGraph{
						dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
					},
					err: injected,
				}
				f := &ForkChoiceStore{
					beaconCfg:                      cfg,
					engine:                         engine,
					forkGraph:                      graph,
					optimisticStore:                optimistic.NewOptimisticStore(),
					verifiedExecutionPayload:       verifiedExecutionPayload,
					executionPayloadStatus:         executionPayloadStatus,
					payloadStatusByRoot:            payloadStatusByRoot,
					executionPayloadGasLimit:       executionPayloadGasLimit,
					eth2Roots:                      eth2Roots,
					pendingEnvelopes:               pending,
					pendingLocalSelfBuildEnvelopes: pendingLocal,
				}
				f.finalizedCheckpoint.Store(solid.Checkpoint{})
				apply := func() error {
					if local {
						return f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope)
					}
					return f.OnExecutionPayload(context.Background(), envelope, false, true)
				}

				err = apply()
				require.ErrorIs(t, err, ErrExecutionPayloadEnvelopePersistenceFailed)
				require.ErrorIs(t, err, injected)
				require.True(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
				require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))

				require.NoError(t, apply())
				require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
				require.True(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
				status, ok := executionPayloadStatus.Get(envelope.Message.Payload.BlockHash)
				require.True(t, ok)
				require.EqualValues(t, execution_client.PayloadStatusValidated, status)
				require.Empty(t, f.pendingELPayloads)
			})
		}
	}
}

func TestCachedTerminalHashProjectsToSiblingRoot(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cachedStatus   execution_client.PayloadStatus
		incomingStatus execution_client.PayloadStatus
		wantStatus     execution_client.PayloadStatus
		wantInvalid    bool
	}{
		{
			name:           "validated_dominates_none",
			cachedStatus:   execution_client.PayloadStatusValidated,
			incomingStatus: execution_client.PayloadStatusNone,
			wantStatus:     execution_client.PayloadStatusValidated,
		},
		{
			name:           "invalidated_dominates_none",
			cachedStatus:   execution_client.PayloadStatusInvalidated,
			incomingStatus: execution_client.PayloadStatusNone,
			wantStatus:     execution_client.PayloadStatusInvalidated,
			wantInvalid:    true,
		},
		{
			name:           "invalidated_dominates_not_validated",
			cachedStatus:   execution_client.PayloadStatusInvalidated,
			incomingStatus: execution_client.PayloadStatusNotValidated,
			wantStatus:     execution_client.PayloadStatusInvalidated,
			wantInvalid:    true,
		},
		{
			name:           "incoming_invalidation_dominates_cached_validation",
			cachedStatus:   execution_client.PayloadStatusValidated,
			incomingStatus: execution_client.PayloadStatusInvalidated,
			wantStatus:     execution_client.PayloadStatusInvalidated,
			wantInvalid:    true,
		},
		{
			name:           "cached_invalidation_dominates_incoming_validation",
			cachedStatus:   execution_client.PayloadStatusInvalidated,
			incomingStatus: execution_client.PayloadStatusValidated,
			wantStatus:     execution_client.PayloadStatusInvalidated,
			wantInvalid:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			executionHash := envelope.Message.Payload.BlockHash
			root := envelope.Message.BeaconBlockRoot
			verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](2)
			require.NoError(t, err)
			executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](2)
			require.NoError(t, err)
			payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](2)
			require.NoError(t, err)
			executionPayloadGasLimit, err := lru.New[common.Hash, uint64](2)
			require.NoError(t, err)
			executionPayloadStatus.Add(executionHash, tc.cachedStatus)
			graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
			f := &ForkChoiceStore{
				beaconCfg:                cfg,
				forkGraph:                graph,
				optimisticStore:          optimistic.NewOptimisticStore(),
				verifiedExecutionPayload: verifiedExecutionPayload,
				executionPayloadStatus:   executionPayloadStatus,
				payloadStatusByRoot:      payloadStatusByRoot,
				executionPayloadGasLimit: executionPayloadGasLimit,
			}

			err = f.applyPayloadValidationResultLocked(tc.incomingStatus, nil, envelope.Message, block, root)

			if tc.wantInvalid {
				require.ErrorIs(t, err, errInvalidExecutionPayloadEnvelope)
				require.True(t, graph.invalid.Load())
				require.False(t, f.IsPayloadVerified(root))
			} else {
				require.NoError(t, err)
				require.True(t, f.IsPayloadVerified(root))
				require.False(t, graph.invalid.Load())
			}
			status, ok := executionPayloadStatus.Get(executionHash)
			require.True(t, ok)
			require.EqualValues(t, tc.wantStatus, status)
			rootStatus, ok := payloadStatusByRoot.Get(root)
			require.True(t, ok)
			require.EqualValues(t, tc.wantStatus, rootStatus)
			require.False(t, f.optimisticStore.IsOptimistic(root))
		})
	}
}

func TestSiblingInvalidationRevokesVerifiedExecutionHash(t *testing.T) {
	rootA := common.HexToHash("0x01")
	rootB := common.HexToHash("0x02")
	executionHash := common.HexToHash("0x03")
	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](2)
	require.NoError(t, err)
	verifiedExecutionPayloadHashes, err := lru.New[common.Hash, common.Hash](2)
	require.NoError(t, err)
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](2)
	require.NoError(t, err)
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](2)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{}
	f := &ForkChoiceStore{
		forkGraph:                      graph,
		verifiedExecutionPayload:       verifiedExecutionPayload,
		verifiedExecutionPayloadHashes: verifiedExecutionPayloadHashes,
		executionPayloadStatus:         executionPayloadStatus,
		payloadStatusByRoot:            payloadStatusByRoot,
	}

	f.MarkPayloadVerified(rootA, executionHash)
	require.True(t, f.IsPayloadVerified(rootA))
	f.MarkPayloadInvalid(rootB, executionHash)

	require.False(t, f.IsPayloadVerified(rootA))
	require.False(t, verifiedExecutionPayload.Contains(rootA))
	status, ok := f.GetRecentExecutionPayloadStatusByRoot(rootA)
	require.True(t, ok)
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
	executionPayloadStatus.Add(common.HexToHash("0x04"), execution_client.PayloadStatusValidated)
	executionPayloadStatus.Add(common.HexToHash("0x05"), execution_client.PayloadStatusValidated)
	require.False(t, f.IsPayloadVerified(rootA))
	rootC := common.HexToHash("0x06")
	f.MarkPayloadVerified(rootC, executionHash)
	require.False(t, f.IsPayloadVerified(rootC))
	status, ok = f.GetRecentExecutionPayloadStatus(executionHash)
	require.True(t, ok)
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
}

func TestLocalSelfBuildPreservesTerminalInvalidationAcrossYield(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status execution_client.PayloadStatus
	}{
		{name: "cached invalidation dominates stale accepted result", status: execution_client.PayloadStatusNotValidated},
		{name: "invalid result dominates concurrent persistence", status: execution_client.PayloadStatusInvalidated},
		{name: "validated result survives concurrent persistence", status: execution_client.PayloadStatusValidated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
			ctrl := gomock.NewController(t)
			engine := execution_client.NewMockExecutionEngine(ctrl)
			engineStarted := make(chan struct{})
			releaseEngine := make(chan struct{})
			engine.EXPECT().
				NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
					close(engineStarted)
					<-releaseEngine
					return tc.status, nil
				})

			verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](1)
			require.NoError(t, err)
			executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
			require.NoError(t, err)
			eth2Roots, err := lru.New[common.Hash, common.Hash](1)
			require.NoError(t, err)
			f := &ForkChoiceStore{
				beaconCfg:                cfg,
				engine:                   engine,
				forkGraph:                graph,
				optimisticStore:          optimistic.NewOptimisticStore(),
				verifiedExecutionPayload: verifiedExecutionPayload,
				executionPayloadStatus:   executionPayloadStatus,
				payloadStatusByRoot:      payloadStatusByRoot,
				executionPayloadGasLimit: executionPayloadGasLimit,
				eth2Roots:                eth2Roots,
			}
			f.finalizedCheckpoint.Store(solid.Checkpoint{})

			done := make(chan error, 1)
			go func() {
				done <- f.ApplyLocalSelfBuildEnvelope(context.Background(), envelope)
			}()
			<-engineStarted
			if tc.status == execution_client.PayloadStatusNotValidated {
				f.MarkPayloadInvalid(envelope.Message.BeaconBlockRoot, envelope.Message.Payload.BlockHash)
			} else {
				require.NoError(t, f.OnExecutionPayload(context.Background(), envelope, false, false))
			}
			close(releaseEngine)

			err = <-done
			if tc.status == execution_client.PayloadStatusValidated {
				require.NoError(t, err)
				require.True(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
			} else {
				require.ErrorIs(t, err, errInvalidExecutionPayloadEnvelope)
				require.True(t, graph.invalid.Load())
				require.False(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
			}
			status, ok := executionPayloadStatus.Get(envelope.Message.Payload.BlockHash)
			require.True(t, ok)
			expectedStatus := tc.status
			if tc.status == execution_client.PayloadStatusNotValidated {
				expectedStatus = execution_client.PayloadStatusInvalidated
			}
			require.EqualValues(t, expectedStatus, status)
		})
	}
}

func TestExecutionPayloadIngressDoesNotMarkVerifiedWhenEngineReturnsValidationError(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	injected := errors.New("injected validation error")
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(execution_client.PayloadStatusValidated, injected)

	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](1)
	require.NoError(t, err)
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                graph,
		verifiedExecutionPayload: verifiedExecutionPayload,
		executionPayloadStatus:   executionPayloadStatus,
		executionPayloadGasLimit: executionPayloadGasLimit,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	err = f.OnExecutionPayload(context.Background(), envelope, false, true)

	require.ErrorIs(t, err, injected)
	require.False(t, f.IsPayloadVerified(envelope.Message.BeaconBlockRoot))
	_, ok := executionPayloadStatus.Get(envelope.Message.Payload.BlockHash)
	require.False(t, ok)
	require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
}

func TestExecutionPayloadIngressPersistsEngineAcceptedPayload(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{0xc0}))
	resignAdmissionEnvelope(t, cfg, blockState, envelope)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().
		NewPayload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, payload *cltypes.Eth1Block, parentRoot *common.Hash, _ []common.Hash, requests []hexutil.Bytes) (execution_client.PayloadStatus, error) {
			bal, err := execution_client.DecodeAndValidateBlockAccessList(payload)
			if err != nil {
				return execution_client.PayloadStatusInvalidated, err
			}
			requestsHash := cltypes.ComputeExecutionRequestHash(requests)
			if _, err := payload.RlpHeader(parentRoot, requestsHash, bal); err != nil {
				return execution_client.PayloadStatusInvalidated, err
			}
			return execution_client.PayloadStatusNotValidated, nil
		})

	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	executionPayloadGasLimit, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block}}
	optimisticStore := optimistic.NewOptimisticStore()
	f := &ForkChoiceStore{
		beaconCfg:                cfg,
		engine:                   engine,
		forkGraph:                graph,
		optimisticStore:          optimisticStore,
		executionPayloadStatus:   executionPayloadStatus,
		executionPayloadGasLimit: executionPayloadGasLimit,
		eth2Roots:                eth2Roots,
	}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	err = f.OnExecutionPayload(context.Background(), envelope, false, true)

	require.NoError(t, err)
	require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
	status, ok := executionPayloadStatus.Get(envelope.Message.Payload.BlockHash)
	require.True(t, ok)
	require.EqualValues(t, execution_client.PayloadStatusNotValidated, status)
	require.Empty(t, f.pendingELPayloads)
}

func TestGossipCommitmentValidationRefreshesBlockAfterYield(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	graph := &commitmentYieldForkGraph{
		dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
	}
	f := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	require.ErrorIs(t, f.ValidateExecutionPayloadEnvelopeForGossip(envelope), ErrIgnore)
	require.Equal(t, int32(2), graph.blockReads.Load())
}

func TestGossipCommitmentValidationRefreshesStateAfterYield(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	graph := &commitmentStateRefreshForkGraph{
		dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
	}
	f := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph}
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	err := f.ValidateExecutionPayloadEnvelopeForGossip(envelope)

	require.ErrorContains(t, err, "unavailable after commitment validation")
	require.Equal(t, int32(2), graph.stateReads.Load())
}

func TestGossipCommitmentValidationRefreshesFinalityAfterYield(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	f := &ForkChoiceStore{beaconCfg: cfg}
	graph := &commitmentFinalityRefreshForkGraph{
		dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
		onRefresh: func() {
			f.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: envelope.Message.Payload.SlotNumber/cfg.SlotsPerEpoch + 1})
		},
	}
	f.forkGraph = graph
	f.finalizedCheckpoint.Store(solid.Checkpoint{})

	err := f.ValidateExecutionPayloadEnvelopeForGossip(envelope)

	require.ErrorContains(t, err, "before finalized slot")
	require.Equal(t, int32(2), graph.blockReads.Load())
}

func TestWithForkChoiceLockYieldedAllowsConcurrentLockUser(t *testing.T) {
	f := &ForkChoiceStore{}
	injected := errors.New("injected commitment failure")
	f.mu.Lock()
	err := f.withForkChoiceLockYielded(func() error {
		require.True(t, f.mu.TryLock())
		f.mu.Unlock()
		return injected
	})
	f.mu.Unlock()
	require.ErrorIs(t, err, injected)
}

func TestPayloadHashFallbackReconcilesInvalidationWhileLockYielded(t *testing.T) {
	blockRoot := common.HexToHash("0x01")
	executionBlockHash := common.HexToHash("0x02")
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{}
	f := &ForkChoiceStore{
		forkGraph:              graph,
		executionPayloadStatus: executionPayloadStatus,
		payloadStatusByRoot:    payloadStatusByRoot,
	}

	f.mu.Lock()
	err = f.validatePayloadHashFallbackLocked(blockRoot, executionBlockHash, func() error {
		require.True(t, f.mu.TryLock())
		f.markPayloadInvalidLocked(blockRoot, executionBlockHash)
		f.mu.Unlock()
		return nil
	})
	f.mu.Unlock()

	require.ErrorIs(t, err, errInvalidExecutionPayloadEnvelope)
	require.True(t, graph.invalid.Load())
	status, ok := executionPayloadStatus.Get(executionBlockHash)
	require.True(t, ok)
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
}

func TestExecutionPayloadCommitmentFallbackRefreshesAfterYield(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	graph := &commitmentFallbackForkGraph{
		persistingEnvelopeForkGraph: persistingEnvelopeForkGraph{
			dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
		},
	}
	eth2Roots, err := lru.New[common.Hash, common.Hash](1)
	require.NoError(t, err)
	f := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, eth2Roots: eth2Roots}
	require.NoError(t, f.OnExecutionPayload(t.Context(), envelope, false, false))
	require.Equal(t, int32(3), graph.blockReads.Load())
	require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
}

func TestExecutionPayloadAdmissionCancellationReconcilesConcurrentOwner(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		for _, local := range []bool{false, true} {
			t.Run(fmt.Sprintf("persisted=%t/local=%t", persisted, local), func(t *testing.T) {
				cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
				pending, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
				require.NoError(t, err)
				pendingLocal, err := lru.New[common.Hash, *cltypes.SignedExecutionPayloadEnvelope](queueCacheSize)
				require.NoError(t, err)
				graph := &admissionYieldForkGraph{
					dataAvailabilityForkGraph: dataAvailabilityForkGraph{state: blockState, block: block},
					stateRead:                 make(chan struct{}),
				}
				f := &ForkChoiceStore{
					beaconCfg:                      cfg,
					engine:                         execution_client.NewMockExecutionEngine(gomock.NewController(t)),
					forkGraph:                      graph,
					pendingEnvelopes:               pending,
					pendingLocalSelfBuildEnvelopes: pendingLocal,
					payloadValidationAdmission:     make(chan struct{}, 1),
				}
				f.payloadValidationOnce.Do(func() {})
				f.payloadValidationAdmission <- struct{}{}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					if local {
						done <- f.ApplyLocalSelfBuildEnvelope(ctx, envelope)
					} else {
						done <- f.OnExecutionPayload(ctx, envelope, false, true)
					}
				}()
				<-graph.stateRead

				f.mu.Lock()
				replacement := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
					BeaconBlockRoot: envelope.Message.BeaconBlockRoot,
					Payload:         envelope.Message.Payload,
				}}
				origin := pending
				if local {
					origin = pendingLocal
				}
				origin.Add(envelope.Message.BeaconBlockRoot, replacement)
				graph.hasEnvelope.Store(persisted)
				cancel()
				f.mu.Unlock()

				err = <-done
				require.ErrorIs(t, err, errPayloadValidationAdmission)
				queued, ok := origin.Peek(envelope.Message.BeaconBlockRoot)
				require.True(t, ok)
				if persisted {
					require.Same(t, replacement, queued)
				} else {
					require.Same(t, envelope, queued)
				}
			})
		}
	}
}

func validAdmissionCancellationFixture(t *testing.T) (*clparams.BeaconChainConfig, *state2.CachingBeaconState, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.GloasForkEpoch = 0
	cfg.GloasForkVersion = 0x80000038
	cfg.InitializeForkSchedule()

	blockState := state2.New(&cfg)
	blockState.SetVersion(clparams.GloasVersion)
	require.NoError(t, blockState.SetSlot(64))
	blockState.SetGenesisValidatorsRoot(common.HexToHash("0x01"))
	blockState.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
	})
	parentRoot := common.HexToHash("0x11")
	blockState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{ProposerIndex: 0, ParentRoot: parentRoot})
	privateKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
	require.NoError(t, err)
	publicKey := common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey()))
	require.NoError(t, blockState.AddValidator(solid.NewValidatorFromParameters(publicKey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance))
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	builders.Append(&cltypes.Builder{Pubkey: publicKey})
	blockState.SetBuilders(builders)

	requests := cltypes.NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	withdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.BlockHash = common.HexToHash("0x22")
	payload.SlotNumber = blockState.Slot()
	payload.Time = state2.ComputeTimestampAtSlot(blockState, blockState.Slot())
	payload.Withdrawals = withdrawals
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Extra = solid.NewExtraData()
	payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(&cfg, requests))
	blockHash, err := payload.ComputeBlockHash(&parentRoot, requestsHash, nil)
	require.NoError(t, err)
	payload.BlockHash = blockHash
	bid := &cltypes.ExecutionPayloadBid{
		BlockHash:             payload.BlockHash,
		BuilderIndex:          0,
		Slot:                  payload.SlotNumber,
		BlobKzgCommitments:    *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		ExecutionRequestsRoot: requestsRoot,
	}
	blockState.SetLatestExecutionPayloadBid(bid)
	blockState.SetLatestBlockHash(payload.ParentHash)
	blockState.SetPayloadExpectedWithdrawals(withdrawals)

	stateRoot := common.HexToHash("0x33")
	body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: bid}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{
		Slot:       blockState.Slot(),
		ParentRoot: parentRoot,
		StateRoot:  stateRoot,
		Body:       body,
	}}
	bodyRoot, err := body.HashSSZ()
	require.NoError(t, err)
	blockState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          block.Block.Slot,
		ProposerIndex: block.Block.ProposerIndex,
		ParentRoot:    block.Block.ParentRoot,
		BodyRoot:      bodyRoot,
	})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload:               payload,
		ExecutionRequests:     requests,
		BuilderIndex:          bid.BuilderIndex,
		BeaconBlockRoot:       blockRoot,
		ParentBeaconBlockRoot: parentRoot,
	}}
	domain, err := blockState.GetDomain(cfg.DomainBeaconBuilder, state2.GetEpochAtSlot(&cfg, blockState.Slot()))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(envelope.Message, domain)
	require.NoError(t, err)
	copy(envelope.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())
	return &cfg, blockState, block, envelope
}

func resignAdmissionEnvelope(
	t *testing.T,
	cfg *clparams.BeaconChainConfig,
	blockState *state2.CachingBeaconState,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
) {
	t.Helper()
	privateKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
	require.NoError(t, err)
	domain, err := blockState.GetDomain(cfg.DomainBeaconBuilder, state2.GetEpochAtSlot(cfg, blockState.Slot()))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(envelope.Message, domain)
	require.NoError(t, err)
	copy(envelope.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())
}
