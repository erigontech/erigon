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

package services

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	clutils "github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

type attesterSlashingErrorStore struct {
	forkchoice.ForkChoiceStorage
	err error
}

func (s attesterSlashingErrorStore) OnAttesterSlashing(*cltypes.AttesterSlashing, bool) error {
	return s.err
}

func newResolverTriggerBlock(t *testing.T, cfg *clparams.BeaconChainConfig, state *state2.CachingBeaconState, keys []*bls.PrivateKey, proposer uint64) (*cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	parentRoot := common.HexToHash("0x1234")
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = cfg.SlotsPerEpoch
	block.Block.ParentRoot = parentRoot
	block.Block.ProposerIndex = proposer
	block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockRoot = parentRoot
	block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = common.HexToHash("0xabcd")
	epoch := block.Block.Slot / cfg.SlotsPerEpoch
	var domain []byte
	var err error
	if cfg.GetCurrentStateVersion(epoch) > state.Version() {
		forkVersion := clutils.Uint32ToBytes4(cfg.GetForkVersionByVersion(clparams.GloasVersion))
		domain, err = fork.ComputeDomain(cfg.DomainBeaconProposer[:], forkVersion, state.GenesisValidatorsRoot())
	} else {
		domain, err = state.GetDomain(cfg.DomainBeaconProposer, epoch)
	}
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	require.NoError(t, err)
	copy(block.Signature[:], keys[proposer].Sign(signingRoot[:]).Bytes())
	valid, err := eth2.VerifyBlockSignature(state, block)
	require.NoError(t, err)
	require.True(t, valid)
	return block, parentRoot
}

func newResolverTriggerState(t *testing.T, cfg *clparams.BeaconChainConfig) (*state2.CachingBeaconState, []*bls.PrivateKey) {
	t.Helper()
	st := state2.New(cfg)
	st.SetSlot(cfg.SlotsPerEpoch)
	keys := make([]*bls.PrivateKey, 2)
	for i := range keys {
		key, err := bls.NewPrivateKeyFromIKM(append(make([]byte, 31), byte(i+1)))
		require.NoError(t, err)
		keys[i] = key
		pubkey := common.Bytes48(bls.CompressPublicKey(key.PublicKey()))
		st.AddValidator(solid.NewValidatorFromParameters(pubkey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance)
	}
	return st, keys
}

func TestBlockServiceAuthenticatedFullChildWithUnseenParentStatusTriggersResolver(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	st, keys := newResolverTriggerState(t, &cfg)
	expected, err := st.GetBeaconProposerIndex()
	require.NoError(t, err)
	block, parentRoot := newResolverTriggerBlock(t, &cfg, st, keys, expected)

	ctrl := gomock.NewController(t)
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	synced := synced_data.NewSyncedDataManager(&cfg, true)
	synced.OnHeadState(st)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[parentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	fcu.StateAtBlockRootVal[parentRoot] = st
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	parent.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash
	fcu.Blocks[parentRoot] = parent
	fcu.OnBlockErr = forkchoice.ErrParentEnvelopePending
	requester := &envelopeRequesterStub{started: make(chan struct{}, 1), release: make(chan struct{})}
	payloads := NewExecutionPayloadService(t.Context(), fcu, &cfg, beaconevents.NewEventEmitter(), requester)
	payloads.resolver.deadline = 20 * time.Millisecond
	payloads.resolver.retry = time.Hour
	service := NewBlockService(t.Context(), db, fcu, synced, clock, &cfg, nil, payloads).(*blockService)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	select {
	case <-requester.started:
	case <-time.After(time.Second):
		t.Fatal("resolver request was not started")
	}
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	job, ok := service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.True(t, ok)
	require.True(t, job.(*blockJob).resolveParentEnvelope)
	require.Eventually(t, func() bool { return requester.calls.Load() >= 2 }, time.Second, time.Millisecond)
	close(requester.release)
}

func TestBlockServiceWrongScheduledProposerTriggersNoResolver(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	st, keys := newResolverTriggerState(t, &cfg)
	expected, err := st.GetBeaconProposerIndex()
	require.NoError(t, err)
	wrong := (expected + 1) % uint64(len(keys))
	block, parentRoot := newResolverTriggerBlock(t, &cfg, st, keys, wrong)

	ctrl := gomock.NewController(t)
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	synced := synced_data.NewSyncedDataManager(&cfg, true)
	synced.OnHeadState(st)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[parentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	fcu.StateAtBlockRootVal[parentRoot] = st
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	parent.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash
	fcu.Blocks[parentRoot] = parent
	requester := &envelopeRequesterStub{}
	payloads := NewExecutionPayloadService(t.Context(), fcu, &cfg, beaconevents.NewEventEmitter(), requester)
	service := NewBlockService(t.Context(), db, fcu, synced, clock, &cfg, nil, payloads)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrInvalidSignature)
	require.Zero(t, requester.calls.Load())
}

func TestBlockServiceChildBeforeParentUpgradesScheduledResolverEligibility(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = 0, 0, 0, 0
	st, keys := newResolverTriggerState(t, &cfg)
	expected, err := st.GetBeaconProposerIndex()
	require.NoError(t, err)
	block, parentRoot := newResolverTriggerBlock(t, &cfg, st, keys, expected)

	ctrl := gomock.NewController(t)
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	synced := synced_data.NewSyncedDataManager(&cfg, true)
	synced.OnHeadState(st)
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.OnBlockErr = forkchoice.ErrParentEnvelopePending
	requester := &envelopeRequesterStub{}
	payloads := NewExecutionPayloadService(t.Context(), fcu, &cfg, beaconevents.NewEventEmitter(), requester)
	payloads.resolver.deadline, payloads.resolver.retry = 20*time.Millisecond, time.Hour
	service := NewBlockService(t.Context(), db, fcu, synced, eth_clock.NewMockEthereumClock(ctrl), &cfg, nil, payloads).(*blockService)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	job, ok := service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.True(t, ok)
	require.False(t, job.(*blockJob).resolveParentEnvelope)

	fcu.Headers[parentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	fcu.StateAtBlockRootVal[parentRoot] = st
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	parent.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash
	fcu.Blocks[parentRoot] = parent
	fcu.ExecutionPayloadStatusMap[block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash] = execution_client.PayloadStatusValidated

	require.Eventually(t, func() bool { return requester.calls.Load() >= 2 }, time.Second, time.Millisecond)
	job, ok = service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.True(t, ok)
	require.True(t, job.(*blockJob).resolveParentEnvelope)
}

func TestBlockServiceChildBeforeParentWrongProposerNeverTriggersResolver(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = 0, 0, 0, 0
	st, keys := newResolverTriggerState(t, &cfg)
	expected, err := st.GetBeaconProposerIndex()
	require.NoError(t, err)
	wrong := (expected + 1) % uint64(len(keys))
	block, parentRoot := newResolverTriggerBlock(t, &cfg, st, keys, wrong)

	ctrl := gomock.NewController(t)
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	synced := synced_data.NewSyncedDataManager(&cfg, true)
	synced.OnHeadState(st)
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.OnBlockErr = forkchoice.ErrParentEnvelopePending
	requester := &envelopeRequesterStub{}
	payloads := NewExecutionPayloadService(t.Context(), fcu, &cfg, beaconevents.NewEventEmitter(), requester)
	service := NewBlockService(t.Context(), db, fcu, synced, eth_clock.NewMockEthereumClock(ctrl), &cfg, nil, payloads).(*blockService)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	fcu.Headers[parentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	fcu.StateAtBlockRootVal[parentRoot] = st
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	parent.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash
	fcu.Blocks[parentRoot] = parent
	fcu.ExecutionPayloadStatusMap[block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash] = execution_client.PayloadStatusValidated

	require.Eventually(t, func() bool {
		_, exists := service.blocksScheduledForLaterExecution.Load(blockRoot)
		return !exists
	}, time.Second, time.Millisecond)
	require.Zero(t, requester.calls.Load())
}

func TestBlockServiceExpiredObservationDoesNotDeleteFreshReplacement(t *testing.T) {
	service := &blockService{}
	root := [32]byte{1}
	oldJob := &blockJob{creationTime: time.Now().Add(-blockJobExpiry - time.Second)}
	freshJob := &blockJob{creationTime: time.Now()}
	service.blocksScheduledForLaterExecution.Store(root, oldJob)
	observed, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	service.blocksScheduledForLaterExecution.Store(root, freshJob)

	service.deleteScheduledBlockJob(root, observed.(*blockJob))

	current, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, freshJob, current)
}

func TestBlockServiceExpiredTrustedChildIsRetainedWhileEnvelopePending(t *testing.T) {
	root := common.HexToHash("0x1234")
	payloads := &executionPayloadService{pendingRootCounts: map[common.Hash]int{root: 1}}
	now := time.Now()
	service := &blockService{envelopeResolver: payloads}
	job := &blockJob{
		block:                 &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{ParentRoot: root}},
		creationTime:          now.Add(-blockJobExpiry - time.Second),
		resolveParentEnvelope: true,
	}
	require.False(t, service.scheduledBlockJobExpired(job))
}

func TestBlockServiceExpiredTrustedChildProcessesAfterEnvelopeSuccess(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	payloads := &executionPayloadService{pendingRootCounts: map[common.Hash]int{}}
	now := time.Now()
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Envelopes[root] = newTestSignedEnvelope(0, root, 0)
	service := &blockService{
		beaconCfg:        cfg,
		db:               memdb.NewTestDB(t, dbcfg.ChainDB),
		forkchoiceStore:  fcu,
		envelopeResolver: payloads,
	}
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	block.Block.ParentRoot = root
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	job := &blockJob{block: block, creationTime: now.Add(-blockJobExpiry - time.Second), resolveParentEnvelope: true}
	service.blocksScheduledForLaterExecution.Store(blockRoot, job)

	service.processScheduledBlockJobs(t.Context())

	_, ok := service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.False(t, ok)
	require.Equal(t, int32(1), fcu.OnBlockCalls.Load())
}

func TestBlockServiceExpiredTrustedChildDeletesAfterPendingTerminates(t *testing.T) {
	root := common.HexToHash("0x1234")
	payloads := &executionPayloadService{pendingRootCounts: map[common.Hash]int{}}
	now := time.Now()
	service := &blockService{envelopeResolver: payloads}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{ParentRoot: root}}
	blockRoot := [32]byte{1}
	job := &blockJob{block: block, creationTime: now.Add(-blockJobExpiry - time.Second), resolveParentEnvelope: true}
	service.blocksScheduledForLaterExecution.Store(blockRoot, job)

	service.processScheduledBlockJobs(t.Context())

	_, ok := service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.False(t, ok)
}

func TestBlockServiceExpiredEnvelopeGraceTerminalErrorAttemptsOnce(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	root := common.HexToHash("0x1234")
	payloads := &executionPayloadService{pendingRootCounts: map[common.Hash]int{}}
	now := time.Now()
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Envelopes[root] = newTestSignedEnvelope(0, root, 0)
	fcu.OnBlockErr = errors.New("terminal block error")
	service := &blockService{
		beaconCfg:        cfg,
		db:               memdb.NewTestDB(t, dbcfg.ChainDB),
		forkchoiceStore:  fcu,
		envelopeResolver: payloads,
	}
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	block.Block.ParentRoot = root
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	job := &blockJob{block: block, creationTime: now.Add(-blockJobExpiry - time.Second), resolveParentEnvelope: true}
	service.blocksScheduledForLaterExecution.Store(blockRoot, job)

	service.processScheduledBlockJobs(t.Context())
	service.processScheduledBlockJobs(t.Context())

	require.Equal(t, int32(1), fcu.OnBlockCalls.Load())
	_, ok := service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.False(t, ok)
}

func TestEnvelopeResolutionTriggerRejectsValidSignatureFromWrongProposer(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.FuluForkEpoch = cfg.FarFutureEpoch
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	parentState := state2.New(&cfg)
	parentState.SetSlot(0)
	keys := make([]*bls.PrivateKey, 2)
	for i := range keys {
		key, err := bls.NewPrivateKeyFromIKM(append(make([]byte, 31), byte(i+1)))
		require.NoError(t, err)
		keys[i] = key
		pubkey := common.Bytes48(bls.CompressPublicKey(key.PublicKey()))
		parentState.AddValidator(solid.NewValidatorFromParameters(pubkey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance)
	}
	expected, err := parentState.GetBeaconProposerIndex()
	require.NoError(t, err)
	wrong := (expected + 1) % uint64(len(keys))
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = 0
	block.Block.ProposerIndex = wrong
	domain, err := parentState.GetDomain(cfg.DomainBeaconProposer, 0)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	require.NoError(t, err)
	copy(block.Signature[:], keys[wrong].Sign(signingRoot[:]).Bytes())
	valid, err := eth2.VerifyBlockSignature(parentState, block)
	require.NoError(t, err)
	require.True(t, valid)

	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.StateAtBlockRootVal[block.Block.ParentRoot] = parentState
	service := &blockService{forkchoiceStore: fcu}
	require.ErrorIs(t, service.authenticateEnvelopeResolutionTrigger(block), ErrInvalidSignature)
}

func setupBlockService(t *testing.T, ctrl *gomock.Controller) (BlockService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock, *mock_services.ForkChoiceStorageMock) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	blockService := NewBlockService(t.Context(), db, forkchoiceMock, syncedDataManager, ethClock, cfg, nil, nil)
	return blockService, syncedDataManager, ethClock, forkchoiceMock
}

func TestBlockServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, _ := tests.GetBellatrixRandom()

	blockService, _, _, _ := setupBlockService(t, ctrl)
	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceIgnoreSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, _ := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceLowerThanFinalizedCheckpoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	blocks[0].Block.Slot = 0

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceUnseenParentRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceYoungerThanParent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	blocks[1].Block.Slot--

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[1]))
}

func TestBlockServiceInvalidCommitmentsPerBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)
	// Append lots of commitments
	for range 100 {
		blocks[1].Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}
	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[1]))
}

func TestBlockServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)

	require.NoError(t, blockService.ProcessMessage(context.Background(), nil, blocks[1]))
}

func TestImportBlockOperationsAttesterSlashingLogging(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantLogged bool
	}{
		{name: "ignored", err: forkchoice.ErrIgnore},
		{name: "rejected", err: errors.New("invalid attester slashing"), wantLogged: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := log.Root()
			previousHandler := logger.GetHandler()
			logger.SetHandler(log.StreamHandler(&output, log.LogfmtFormat()))
			t.Cleanup(func() { logger.SetHandler(previousHandler) })

			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Body.AttesterSlashings.Append(&cltypes.AttesterSlashing{})
			service := blockService{forkchoiceStore: attesterSlashingErrorStore{err: tc.err}}

			service.importBlockOperations(block)

			require.Equal(t, tc.wantLogged, bytes.Contains(output.Bytes(), []byte("bad attester slashing received")))
		})
	}
}

// ==================== GLOAS (EIP-7732/ePBS) Tests ====================
//
// NOTE: GLOAS-specific ProcessMessage tests are currently not included because:
// 1. GLOAS-specific validation (bid checks, parent payload checks) happens AFTER
//    signature verification in the ProcessMessage flow
// 2. Signature verification requires properly signed blocks with matching validator keys
// 3. We don't have GLOAS test data with valid signatures available yet
//
// The GLOAS validation code is tested indirectly through:
// - Pre-GLOAS tests that verify the overall ProcessMessage flow
// - The validation code being structurally similar to pre-GLOAS validation
//
// Once GLOAS test vectors with proper signatures are available, these tests can be added:
// - TestBlockServiceGloasMismatchedParentBlockRoot
// - TestBlockServiceGloasParentPayloadNotSeen
// - TestBlockServiceGloasParentPayloadInvalid
// - TestBlockServiceGloasSuccess
//
// For now, the GLOAS validation code path is verified by code review and integration tests.
