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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestBlockServiceGossipDecodeRejectsNonCanonicalParentExecutionRequests(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	const requestOffsetsSize = 20
	requestsStart := len(encoded) - requestOffsetsSize
	for offset := requestsStart; offset < len(encoded); offset += 4 {
		binary.LittleEndian.PutUint32(encoded[offset:], requestOffsetsSize+1)
	}
	encoded = append(encoded, 0)

	ordinary := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	require.NoError(t, ordinary.DecodeSSZ(encoded, int(clparams.GloasVersion)))

	service := &blockService{beaconCfg: cfg}
	_, err = service.DecodeGossipMessage("", encoded, clparams.GloasVersion)
	require.Error(t, err)
}

func TestBlockServiceGossipDecodeRejectsNonCanonicalExecutionPayloadBid(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	commitment := new(cltypes.KZGCommitment)
	commitment[0] = 1
	block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Append(commitment)

	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedBid, err := block.Block.Body.SignedExecutionPayloadBid.EncodeSSZ(nil)
	require.NoError(t, err)
	bidStart := bytes.Index(encoded, encodedBid)
	require.NotEqual(t, -1, bidStart)

	const (
		signedBidFixedSize = 100
		bidOffsetPosition  = 188
		bidFixedSize       = 224
		commitmentSize     = 48
	)
	binary.LittleEndian.PutUint32(encoded[bidStart+signedBidFixedSize+bidOffsetPosition:], bidFixedSize+commitmentSize)

	ordinary := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	require.NoError(t, ordinary.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Zero(t, ordinary.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Len())

	service := &blockService{beaconCfg: cfg}
	_, err = service.DecodeGossipMessage("", encoded, clparams.GloasVersion)
	require.Error(t, err)
}

type attesterSlashingErrorStore struct {
	forkchoice.ForkChoiceStorage
	err error
}

type onBlockErrorStore struct {
	forkchoice.ForkChoiceStorage
	err     error
	calls   atomic.Int32
	headers sync.Map
}

type doneObservedContext struct {
	context.Context
	doneObserved chan struct{}
	once         sync.Once
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneObserved) })
	return c.Context.Done()
}

func (s attesterSlashingErrorStore) OnAttesterSlashing(*cltypes.AttesterSlashing, bool) error {
	return s.err
}

func (s *onBlockErrorStore) OnBlock(_ context.Context, block *cltypes.SignedBeaconBlock, _, _, _ bool) error {
	s.calls.Add(1)
	if s.err != nil {
		return s.err
	}
	root, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	s.headers.Store(common.Hash(root), block.SignedBeaconBlockHeader().Header)
	return nil
}

func (s *onBlockErrorStore) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if header, ok := s.headers.Load(root); ok {
		return header.(*cltypes.BeaconBlockHeader), true
	}
	return s.ForkChoiceStorage.GetHeader(root)
}

func setupBlockService(t *testing.T, ctrl *gomock.Controller) (BlockService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock, *mock_services.ForkChoiceStorageMock) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg := &clparams.MainnetBeaconConfig
	testCfg := *cfg
	testCfg.AltairForkEpoch = 0
	testCfg.BellatrixForkEpoch = 0
	testCfg.CapellaForkEpoch = testCfg.FarFutureEpoch
	syncedDataManager := synced_data.NewSyncedDataManager(&testCfg, true)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	blockService := newBlockService(db, forkchoiceMock, syncedDataManager, ethClock, &testCfg, nil)
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
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

// TestBlockServiceDoesNotIgnoreLateBlockAsFuture proves the future-slot check compares against the
// wall-clock slot, not the head: a block for a slot that has already passed, arriving while the
// head is still behind it, is not from the future.
func TestBlockServiceDoesNotIgnoreLateBlockAsFuture(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, _ := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	block := blocks[0]
	block.Block.Slot = post.Slot() + 1
	ethClock.EXPECT().GetCurrentSlot().Return(post.Slot() + 2).AnyTimes()

	err := blockService.ProcessMessage(context.Background(), nil, block)

	require.ErrorIs(t, err, ErrInvalidSignature)
}

func scheduledBlockCount(t *testing.T, service BlockService) int {
	t.Helper()
	count := 0
	service.(*blockService).blocksScheduledForLaterExecution.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// A late block whose proposer is past the head state's registry can only be checked against its
// parent's state. While the parent is unknown it must be ignored, and not queued unverified.
func TestBlockServiceIgnoresUnknownProposerWhileParentIsUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, _ := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	block := blocks[0]
	block.Block.Slot = post.Slot() + 1
	block.Block.ProposerIndex = uint64(post.ValidatorLength())
	ethClock.EXPECT().GetCurrentSlot().Return(post.Slot() + 2).AnyTimes()

	err := blockService.ProcessMessage(context.Background(), nil, block)

	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, scheduledBlockCount(t, blockService))
}

func TestBlockServiceRejectsUnknownProposerWhenParentIsKnown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, forkchoiceMock := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	block := blocks[0]
	block.Block.Slot = post.Slot() + 1
	block.Block.ProposerIndex = uint64(post.ValidatorLength())
	forkchoiceMock.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{block.Block.ParentRoot: {Slot: post.Slot()}}
	ethClock.EXPECT().GetCurrentSlot().Return(post.Slot() + 2).AnyTimes()

	err := blockService.ProcessMessage(context.Background(), nil, block)

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
}

// Only the unknown proposer is excused while the parent is unknown: a malformed signature is
// still rejected.
func TestBlockServiceRejectsMalformedSignatureWhileParentIsUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, _ := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	block := blocks[0]
	block.Block.Slot = post.Slot() + 1
	for i := range block.Signature {
		block.Signature[i] = 0xff
	}
	ethClock.EXPECT().GetCurrentSlot().Return(post.Slot() + 2).AnyTimes()

	err := blockService.ProcessMessage(context.Background(), nil, block)

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestBlockServiceLowerThanFinalizedCheckpoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	service, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	blocks[0].Block.Slot = 0

	require.ErrorIs(t, service.ProcessMessage(context.Background(), nil, blocks[0]), ErrIgnore)
	service.(*blockService).blocksScheduledForLaterExecution.Range(func(_, _ any) bool {
		t.Error("a block ignored before signature verification must not be queued")
		return true
	})
}

func TestBlockServiceUnseenParentRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

// A newPayload call the execution layer never answered says nothing about the
// block, so the sender must not be rejected (and banned) for it.
func TestBlockServiceIgnoresLocalExecutionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))

	svc, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}
	fcu.OnBlockErr = fmt.Errorf("%w: execution client is down", forkchoice.ErrNewPayloadNoStatus)

	err = svc.ProcessMessage(context.Background(), nil, blocks[1])
	require.ErrorIs(t, err, ErrIgnore)

	blockRoot, err := blocks[1].Block.HashSSZ()
	require.NoError(t, err)
	impl := svc.(*blockService)
	queuedValue, scheduled := impl.blocksScheduledForLaterExecution.Load(blockRoot)
	require.True(t, scheduled, "the block must be queued for a retry")

	// A local execution failure leaves the block queued in memory. Its indices
	// must stay absent until validation succeeds, including across retries.
	db := svc.(*blockService).db
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		require.NoError(t, err)
		require.Nil(t, slot)
		return nil
	}))

	job := queuedValue.(*blockJob)
	job.mu.Lock()
	attempt := job.attempt
	job.mu.Unlock()
	impl.processScheduledBlock(t.Context(), blockRoot, job, time.Now())
	select {
	case <-attempt.done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduled block retry")
	}
	require.ErrorIs(t, attempt.err, forkchoice.ErrNewPayloadNoStatus)
	require.Equal(t, blockRetryInitialDelay, job.retryDelay)
	attempt = job.attempt
	impl.processScheduledBlock(t.Context(), blockRoot, job, job.retryAfter.Add(-time.Nanosecond))
	require.Same(t, attempt, job.attempt, "gossip retries must respect the EL backoff")

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		require.NoError(t, err)
		require.Nil(t, slot)
		return nil
	}))
}

func TestBlockServiceIgnoresLocalOnBlockFailureOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		ignore bool
	}{
		{name: "local failure", err: forkchoice.ErrMissingSegment, ignore: true},
		{name: "invalid block", err: fmt.Errorf("%w: invalid block", forkchoice.ErrBlockInvalid)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			blocks, pre, post := tests.GetBellatrixRandom()
			parentState, err := pre.Copy()
			require.NoError(t, err)
			require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))

			svc, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
			require.NoError(t, syncedData.OnHeadState(post))
			ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
			ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
			fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
			fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
			fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
			finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
			fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}
			fcu.OnBlockErr = tc.err

			err = svc.ProcessMessage(context.Background(), nil, blocks[1])
			require.ErrorIs(t, err, tc.err)
			if tc.ignore {
				require.ErrorIs(t, err, ErrIgnore)
			} else {
				require.NotErrorIs(t, err, ErrIgnore)
				require.NotContains(t, err.Error(), "ignore")
			}
		})
	}
}

func TestBlockServicePreservesStateRootForUnacceptedBlock(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "rejected", err: forkchoice.ErrBlockInvalid},
		{name: "waiting for data", err: forkchoice.ErrEIP7594ColumnDataNotAvailable},
		{name: "ignored"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			store := mock_services.NewForkChoiceStorageMock(t)
			store.OnBlockErr = tc.err
			service := &blockService{db: db, forkchoiceStore: store}
			canonicalRoot := common.Hash{1}
			stateRoot := common.Hash{2}
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return beacon_indicies.WriteStateRoot(tx, canonicalRoot, stateRoot)
			}))
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.StateRoot = stateRoot
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)

			_, err = service.processAndStoreBlock(t.Context(), block, false)
			require.ErrorIs(t, err, tc.err)

			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				root, err := beacon_indicies.ReadBlockRootByStateRoot(tx, stateRoot)
				require.NoError(t, err)
				require.Equal(t, canonicalRoot, root)
				slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
				require.NoError(t, err)
				require.Nil(t, slot)
				body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, blockRoot))
				require.NoError(t, err)
				require.Empty(t, body)
				return nil
			}))
		})
	}
}

func TestBlockServiceYoungerThanParent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
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
	require.NoError(t, syncedData.OnHeadState(post))
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

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))

	service, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)

	require.NoError(t, service.ProcessMessage(context.Background(), nil, blocks[1]))
	key := proposerIndexAndSlot{
		proposerIndex: blocks[1].Block.ProposerIndex,
		slot:          blocks[1].Block.Slot,
	}
	seen, ok := service.(*blockService).seenBlocksCache.Get(key)
	require.True(t, ok)
	signedRoot, err := blocks[1].HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(signedRoot), seen.signedRoot)
}

func TestBlockServiceGossipRejectsBlockOutsideFinalizedChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()
	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: common.Hash{0xff}}

	err := blockService.ValidateGossip(t.Context(), blocks[1])
	require.ErrorContains(t, err, "finalized checkpoint is not an ancestor")
}

func TestBlockServiceGossipUsesCheckpointSyncAnchorForFinalizedAncestor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))
	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
	fcu.AnchorSlotVal = blocks[0].Block.Slot
	fcu.Ancestors[fcu.AnchorSlotVal] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}

	require.NoError(t, blockService.ValidateGossip(t.Context(), blocks[1]))
}

func TestBlockServiceGossipUsesForkChoiceFinalizedCheckpointAtGenesis(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))
	headState, err := post.Copy()
	require.NoError(t, err)
	headState.SetFinalizedCheckpoint(solid.Checkpoint{})

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(headState))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	anchorRoot := blocks[1].Block.ParentRoot
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Root: anchorRoot}
	fcu.Headers[anchorRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.StateAtBlockRootVal[anchorRoot] = parentState
	fcu.AnchorRootVal = anchorRoot
	fcu.AnchorSlotVal = blocks[0].Block.Slot
	fcu.Ancestors[fcu.AnchorSlotVal] = forkchoice.ForkChoiceNode{Root: anchorRoot}

	require.NoError(t, blockService.ValidateGossip(t.Context(), blocks[1]))
}

func TestBlockServiceGossipRejectsUnexpectedProposer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))
	targetEpoch := blocks[1].Block.Slot / parentState.BeaconConfig().SlotsPerEpoch
	mixPosition := (targetEpoch + parentState.BeaconConfig().EpochsPerHistoricalVector - parentState.BeaconConfig().MinSeedLookahead - 1) % parentState.BeaconConfig().EpochsPerHistoricalVector
	foundUnexpectedProposer := false
	for nonce := 1; nonce <= 255; nonce++ {
		require.NoError(t, parentState.SetRandaoMixAt(int(mixPosition), common.Hash{byte(nonce)}))
		expected, proposerErr := parentState.GetBeaconProposerIndexForSlot(blocks[1].Block.Slot)
		require.NoError(t, proposerErr)
		if expected != blocks[1].Block.ProposerIndex {
			foundUnexpectedProposer = true
			break
		}
	}
	require.True(t, foundUnexpectedProposer)

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.Blocks[blocks[1].Block.ParentRoot] = blocks[0]
	fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
	var stateReads atomic.Int32
	var stateCopyMu sync.Mutex
	fcu.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
		if root != blocks[1].Block.ParentRoot {
			return nil, fmt.Errorf("unexpected parent state request")
		}
		stateReads.Add(1)
		stateCopyMu.Lock()
		defer stateCopyMu.Unlock()
		return parentState.Copy()
	}
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}
	encodedBlock, err := blocks[1].EncodeSSZ(nil)
	require.NoError(t, err)
	messages := make([]*cltypes.SignedBeaconBlock, 4)
	for i := range messages {
		messages[i] = cltypes.NewSignedBeaconBlock(parentState.BeaconConfig(), blocks[1].Version())
		require.NoError(t, messages[i].DecodeSSZ(encodedBlock, int(blocks[1].Version())))
	}

	errs := make(chan error, 4)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, message := range messages {
		wg.Go(func() {
			<-start
			errs <- blockService.ValidateGossip(t.Context(), message)
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.ErrorContains(t, err, "does not match expected proposer")
	}
	require.EqualValues(t, 1, stateReads.Load())
}

func TestBlockServiceGossipUsesPostUpgradeExecutionHeadAtGloasBoundary(t *testing.T) {
	blocks, pre, _ := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))

	cfg := *parentState.BeaconConfig()
	activationEpoch := state.Epoch(parentState) + 1
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = activationEpoch
	cfg.InitializeForkSchedule()
	require.Equal(t, clparams.GloasVersion, cfg.GetCurrentStateVersion(activationEpoch))
	encodedState, err := parentState.EncodeSSZ(nil)
	require.NoError(t, err)
	parentState = state.New(&cfg)
	require.NoError(t, parentState.DecodeSSZ(encodedState, int(clparams.BellatrixVersion)))
	require.NoError(t, parentState.UpgradeToCapella())
	require.NoError(t, parentState.UpgradeToDeneb())
	require.NoError(t, parentState.UpgradeToElectra())
	require.NoError(t, parentState.UpgradeToFulu())
	activationSlot := activationEpoch * cfg.SlotsPerEpoch
	require.NoError(t, parentState.SetSlot(activationSlot-1))
	require.Equal(t, common.Hash{}, parentState.GetLatestBlockHash())
	wantParentHash := parentState.LatestExecutionPayloadHeader().BlockHash
	require.NotEqual(t, common.Hash{}, wantParentHash)

	postUpgrade, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(postUpgrade, activationSlot))
	expectedProposer, err := postUpgrade.GetBeaconProposerIndexForSlot(activationSlot)
	require.NoError(t, err)
	require.Equal(t, common.Hash{}, parentState.GetLatestBlockHash())
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	validator, err := parentState.ValidatorForValidatorIndex(int(expectedProposer))
	require.NoError(t, err)
	var pubkey [48]byte
	copy(pubkey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	validator.SetPublicKey(pubkey)
	parentState.SetValidatorAtIndex(int(expectedProposer), validator)

	parentRoot, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	require.Nil(t, blocks[0].Block.Body.GetSignedExecutionPayloadBid())
	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = activationSlot
	child.Block.ProposerIndex = expectedProposer
	child.Block.ParentRoot = parentRoot
	childBid := child.Block.Body.GetSignedExecutionPayloadBid().Message
	childBid.ParentBlockRoot = parentRoot
	childBid.ParentBlockHash = wantParentHash
	forkVersion := utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion))
	domain, err := fork.ComputeDomain(cfg.DomainBeaconProposer[:], forkVersion, parentState.GenesisValidatorsRoot())
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(child.Block, domain)
	require.NoError(t, err)
	copy(child.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())

	ctrl := gomock.NewController(t)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	syncedDataManager := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedDataManager.OnHeadState(parentState))
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	ethClock.EXPECT().GetCurrentSlot().Return(activationSlot).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Root: parentRoot}
	fcu.Headers[parentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.Blocks[parentRoot] = blocks[0]
	fcu.StateAtBlockRootVal[parentRoot] = parentState
	fcu.Ancestors[0] = forkchoice.ForkChoiceNode{Root: parentRoot}
	service := NewBlockService(t.Context(), db, fcu, nil, syncedDataManager, ethClock, &cfg, nil)

	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceGossipSharesParentStateFailure(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	parentState := fcu.StateAtBlockRootVal[parentRoot]
	var stateReads atomic.Int32
	validationEntered := make(chan struct{})
	finishValidation := make(chan struct{})
	var enterOnce sync.Once
	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		stateReads.Add(1)
		enterOnce.Do(func() { close(validationEntered) })
		<-finishValidation
		return nil, errors.New("state storage unavailable")
	}
	encodedBlock, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	messages := make([]*cltypes.SignedBeaconBlock, 4)
	for i := range messages {
		messages[i] = cltypes.NewSignedBeaconBlock(service.(*blockService).beaconCfg, child.Version())
		require.NoError(t, messages[i].DecodeSSZ(encodedBlock, int(child.Version())))
	}

	errs := make(chan error, len(messages))
	var wg sync.WaitGroup
	wg.Go(func() { errs <- service.ValidateGossip(t.Context(), messages[0]) })
	<-validationEntered
	for _, message := range messages[1:] {
		waiterCtx := &doneObservedContext{Context: t.Context(), doneObserved: make(chan struct{})}
		wg.Go(func() {
			errs <- service.ValidateGossip(waiterCtx, message)
		})
		<-waiterCtx.doneObserved
	}
	close(finishValidation)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.ErrorContains(t, err, "state storage unavailable")
	}
	require.EqualValues(t, 1, stateReads.Load())
	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		stateReads.Add(1)
		return parentState.Copy()
	}
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	require.EqualValues(t, 2, stateReads.Load())
}

func TestBlockServiceParentStateReplayPanicWakesWaitersAndAllowsRetry(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	bs := service.(*blockService)
	bs.validationSlots = make(chan struct{}, 1)
	parentState := fcu.StateAtBlockRootVal[parentRoot]
	validationEntered := make(chan struct{})
	triggerPanic := make(chan struct{})
	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		close(validationEntered)
		<-triggerPanic
		panic("state replay panic")
	}

	panicValue := make(chan any, 1)
	go func() {
		defer func() { panicValue <- recover() }()
		_, _ = bs.blockValidationContext(t.Context(), parentRoot, child.Block.Slot)
	}()
	<-validationEntered
	waiterCtx := &doneObservedContext{Context: t.Context(), doneObserved: make(chan struct{})}
	waiterErr := make(chan error, 1)
	go func() {
		_, err := bs.blockValidationContext(waiterCtx, parentRoot, child.Block.Slot)
		waiterErr <- err
	}()
	<-waiterCtx.doneObserved
	close(triggerPanic)
	require.Equal(t, "state replay panic", <-panicValue)
	require.ErrorContains(t, <-waiterErr, "parent state validation panicked")
	bs.validationMu.Lock()
	require.NotContains(t, bs.validationCalls, blockValidationContextKey{parentRoot: parentRoot, slot: child.Block.Slot})
	bs.validationMu.Unlock()

	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		return parentState.Copy()
	}
	_, err := bs.blockValidationContext(t.Context(), parentRoot, child.Block.Slot)
	require.NoError(t, err)
}

func TestBlockServiceBoundsConcurrentParentStateReplays(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	started := make(chan struct{}, maxConcurrentBlockValidationContexts+1)
	release := make(chan struct{})
	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		started <- struct{}{}
		<-release
		return fcu.StateAtBlockRootVal[parentRoot].Copy()
	}

	errs := make(chan error, maxConcurrentBlockValidationContexts)
	var wg sync.WaitGroup
	for offset := range uint64(maxConcurrentBlockValidationContexts) {
		wg.Go(func() {
			_, err := service.(*blockService).blockValidationContext(t.Context(), parentRoot, child.Block.Slot+offset)
			errs <- err
		})
	}
	for range maxConcurrentBlockValidationContexts {
		<-started
	}
	queuedCtx, cancelQueued := context.WithCancel(t.Context())
	queuedErr := make(chan error, 1)
	go func() {
		_, err := service.(*blockService).blockValidationContext(queuedCtx, parentRoot, child.Block.Slot+maxConcurrentBlockValidationContexts)
		queuedErr <- err
	}()
	select {
	case <-started:
		require.Fail(t, "queued replay started before a validation slot was available")
	case <-time.After(50 * time.Millisecond):
	}
	cancelQueued()
	require.ErrorIs(t, <-queuedErr, ErrIgnore)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func newBellatrixValidationContextFixture(t *testing.T) (*blockService, *mock_services.ForkChoiceStorageMock, *cltypes.SignedBeaconBlock, *state.CachingBeaconState) {
	ctrl := gomock.NewController(t)
	blocks, pre, _ := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))
	service, _, _, fcu := setupBlockService(t, ctrl)
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	return service.(*blockService), fcu, blocks[1], parentState
}

func TestBlockValidationContextReadsSameEpochParentStateWithoutCopy(t *testing.T) {
	service, fcu, child, parentState := newBellatrixValidationContextFixture(t)
	require.Equal(t, state.Epoch(parentState), child.Block.Slot/parentState.BeaconConfig().SlotsPerEpoch)
	fcu.ViewStateAtBlockRootFn = func(root common.Hash, fn func(*state.CachingBeaconState) error) error {
		if root != child.Block.ParentRoot {
			return fmt.Errorf("unexpected parent state request: root=%x", root)
		}
		return fn(parentState)
	}
	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		return nil, errors.New("same-epoch validation copied parent state")
	}
	parentSlot := parentState.Slot()

	validationContext, err := service.blockValidationContext(t.Context(), child.Block.ParentRoot, child.Block.Slot)
	require.NoError(t, err)
	require.Equal(t, child.Block.ProposerIndex, validationContext.expectedProposer)
	require.Equal(t, parentSlot, parentState.Slot())
}

func TestBlockValidationContextAdvancesCopiedParentStateAcrossEpoch(t *testing.T) {
	service, fcu, child, parentState := newBellatrixValidationContextFixture(t)
	slotsPerEpoch := parentState.BeaconConfig().SlotsPerEpoch
	nextEpochSlot := (state.Epoch(parentState) + 1) * slotsPerEpoch
	expected, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(expected, nextEpochSlot))
	expectedProposer, err := expected.GetBeaconProposerIndexForSlot(nextEpochSlot)
	require.NoError(t, err)
	var stateFetches atomic.Int32
	fcu.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
		if root != child.Block.ParentRoot {
			return nil, fmt.Errorf("unexpected parent state request: root=%x", root)
		}
		stateFetches.Add(1)
		return parentState.Copy()
	}
	parentSlot := parentState.Slot()

	validationContext, err := service.blockValidationContext(t.Context(), child.Block.ParentRoot, nextEpochSlot)
	require.NoError(t, err)
	require.Equal(t, expectedProposer, validationContext.expectedProposer)
	require.Equal(t, int32(1), stateFetches.Load(), "a non-head parent state is rebuilt on every fetch")
	require.Equal(t, parentSlot, parentState.Slot())
}

func TestBlockValidationContextReadsParentFromPreviousEpochWithoutCopy(t *testing.T) {
	tests := []struct {
		name    string
		version clparams.StateVersion
		gloas   bool
	}{
		{name: "Fulu", version: clparams.FuluVersion},
		{name: "Gloas", version: clparams.GloasVersion, gloas: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, fcu, parentRoot, parentState := newBoundaryValidationContextFixture(t, tt.version)
			if tt.gloas {
				parentState.SetLatestBlockHash(common.Hash{0x11})
				parentState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{BlockHash: common.Hash{0x22}})
			}
			var stateFetches atomic.Int32
			fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
				stateFetches.Add(1)
				return nil, errors.New("previous-epoch validation copied parent state")
			}
			var views atomic.Int32
			fcu.ViewStateAtBlockRootFn = func(root common.Hash, fn func(*state.CachingBeaconState) error) error {
				require.Equal(t, parentRoot, root)
				views.Add(1)
				return fn(parentState)
			}
			parentSlot := parentState.Slot()
			parentRootBefore, err := parentState.HashSSZ()
			require.NoError(t, err)
			firstSlot := (state.Epoch(parentState) + 1) * parentState.BeaconConfig().SlotsPerEpoch

			for _, slot := range []uint64{firstSlot, firstSlot + 1, firstSlot + 5} {
				want := boundaryValidationContextOracle(t, parentState, slot)
				got, err := service.computeBlockValidationContext(parentRoot, slot)
				require.NoError(t, err)
				require.Equal(t, want, got)
				if tt.gloas {
					require.Equal(t, common.Hash{0x11}, got.latestBlockHash)
					require.Equal(t, common.Hash{0x22}, got.latestExecutionPayloadBidBlockHash)
					require.True(t, got.hasLatestExecutionPayloadBid)
				}
			}

			require.Equal(t, int32(3), views.Load())
			require.Zero(t, stateFetches.Load())
			require.Equal(t, parentSlot, parentState.Slot())
			parentRootAfter, err := parentState.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, parentRootBefore, parentRootAfter)
		})
	}
}

func TestBlockValidationContextCopiesFuluParentFromTwoEpochsBack(t *testing.T) {
	service, fcu, parentRoot, parentState := newBoundaryValidationContextFixture(t, clparams.FuluVersion)
	var views atomic.Int32
	fcu.ViewStateAtBlockRootFn = func(common.Hash, func(*state.CachingBeaconState) error) error {
		views.Add(1)
		return errors.New("two-epoch parent state was viewed")
	}
	var stateFetches atomic.Int32
	fcu.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
		require.Equal(t, parentRoot, root)
		stateFetches.Add(1)
		return parentState.Copy()
	}
	slot := (state.Epoch(parentState) + 2) * parentState.BeaconConfig().SlotsPerEpoch
	want := boundaryValidationContextOracle(t, parentState, slot)

	got, err := service.computeBlockValidationContext(parentRoot, slot)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(t, views.Load())
	require.Equal(t, int32(1), stateFetches.Load())
}

func TestBlockValidationContextCopiesParentAtFuluActivation(t *testing.T) {
	parentState, cfg := newUpgradedValidationState(t, clparams.ElectraVersion, func(cfg *clparams.BeaconChainConfig, sourceEpoch uint64) {
		cfg.AltairForkEpoch = 0
		cfg.BellatrixForkEpoch = 0
		cfg.CapellaForkEpoch = 0
		cfg.DenebForkEpoch = 0
		cfg.ElectraForkEpoch = 0
		cfg.FuluForkEpoch = sourceEpoch + 1
		cfg.GloasForkEpoch = cfg.FarFutureEpoch
	})
	activationSlot := cfg.FuluForkEpoch * cfg.SlotsPerEpoch
	require.NoError(t, parentState.SetSlot(activationSlot-1))
	require.Equal(t, clparams.ElectraVersion, parentState.Version())

	parentRoot := common.Hash{0x43}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[parentRoot] = &cltypes.BeaconBlockHeader{Slot: parentState.Slot()}
	var views atomic.Int32
	fcu.ViewStateAtBlockRootFn = func(common.Hash, func(*state.CachingBeaconState) error) error {
		views.Add(1)
		return errors.New("Fulu activation parent state was viewed")
	}
	var stateFetches atomic.Int32
	fcu.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
		require.Equal(t, parentRoot, root)
		stateFetches.Add(1)
		return parentState.Copy()
	}
	service := &blockService{forkchoiceStore: fcu, beaconCfg: cfg}
	want := boundaryValidationContextOracle(t, parentState, activationSlot)

	got, err := service.computeBlockValidationContext(parentRoot, activationSlot)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(t, views.Load())
	require.Equal(t, int32(1), stateFetches.Load())
}

func TestBlockValidationContextFallsBackWhenViewedParentCannotServeLookahead(t *testing.T) {
	tests := []struct {
		name    string
		version clparams.StateVersion
		change  func(*state.CachingBeaconState) error
	}{
		{
			name:    "state version differs from scheduled version",
			version: clparams.GloasVersion,
			change: func(s *state.CachingBeaconState) error {
				s.SetVersion(clparams.FuluVersion)
				return nil
			},
		},
		{
			name:    "slot is outside viewed state lookahead",
			version: clparams.FuluVersion,
			change: func(s *state.CachingBeaconState) error {
				return s.SetSlot(s.Slot() - s.BeaconConfig().SlotsPerEpoch)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, fcu, parentRoot, parentState := newBoundaryValidationContextFixture(t, tt.version)
			viewedState, err := parentState.Copy()
			require.NoError(t, err)
			require.NoError(t, tt.change(viewedState))
			var views atomic.Int32
			fcu.ViewStateAtBlockRootFn = func(root common.Hash, fn func(*state.CachingBeaconState) error) error {
				require.Equal(t, parentRoot, root)
				views.Add(1)
				return fn(viewedState)
			}
			var stateFetches atomic.Int32
			fcu.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
				require.Equal(t, parentRoot, root)
				stateFetches.Add(1)
				return parentState.Copy()
			}
			slot := (state.Epoch(parentState) + 1) * parentState.BeaconConfig().SlotsPerEpoch
			want := boundaryValidationContextOracle(t, parentState, slot)

			got, err := service.computeBlockValidationContext(parentRoot, slot)
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.Equal(t, int32(1), views.Load())
			require.Equal(t, int32(1), stateFetches.Load())
		})
	}
}

func newBoundaryValidationContextFixture(
	t *testing.T,
	version clparams.StateVersion,
) (*blockService, *mock_services.ForkChoiceStorageMock, common.Hash, *state.CachingBeaconState) {
	t.Helper()
	parentState, cfg := newUpgradedValidationState(t, version, func(cfg *clparams.BeaconChainConfig, _ uint64) {
		cfg.AltairForkEpoch = 0
		cfg.BellatrixForkEpoch = 0
		cfg.CapellaForkEpoch = 0
		cfg.DenebForkEpoch = 0
		cfg.ElectraForkEpoch = 0
		cfg.FuluForkEpoch = 0
		cfg.GloasForkEpoch = cfg.FarFutureEpoch
		if version >= clparams.GloasVersion {
			cfg.GloasForkEpoch = 0
		}
	})
	parentSlot := (state.Epoch(parentState)+1)*cfg.SlotsPerEpoch - 1
	require.NoError(t, parentState.SetSlot(parentSlot))

	parentRoot := common.Hash{0x42}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[parentRoot] = &cltypes.BeaconBlockHeader{Slot: parentSlot}
	service := &blockService{forkchoiceStore: fcu, beaconCfg: cfg}
	return service, fcu, parentRoot, parentState
}

func newUpgradedValidationState(
	t *testing.T,
	version clparams.StateVersion,
	configure func(*clparams.BeaconChainConfig, uint64),
) (*state.CachingBeaconState, *clparams.BeaconChainConfig) {
	t.Helper()
	blocks, pre, _ := tests.GetBellatrixRandom()
	source, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(source, blocks[0], nil, false))

	cfg := *source.BeaconConfig()
	configure(&cfg, state.Epoch(source))
	cfg.InitializeForkSchedule()
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	upgraded := state.New(&cfg)
	require.NoError(t, upgraded.DecodeSSZ(encoded, int(clparams.BellatrixVersion)))
	require.NoError(t, upgraded.UpgradeToCapella())
	require.NoError(t, upgraded.UpgradeToDeneb())
	require.NoError(t, upgraded.UpgradeToElectra())
	if version >= clparams.FuluVersion {
		require.NoError(t, upgraded.UpgradeToFulu())
	}
	if version >= clparams.GloasVersion {
		require.NoError(t, upgraded.UpgradeToGloas())
	}
	return upgraded, &cfg
}

func boundaryValidationContextOracle(t *testing.T, parentState *state.CachingBeaconState, slot uint64) *blockValidationContext {
	t.Helper()
	bid := parentState.GetLatestExecutionPayloadBid()
	advanced, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advanced, slot))
	expectedProposer, err := advanced.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	want := &blockValidationContext{
		expectedProposer: expectedProposer,
		latestBlockHash:  advanced.GetLatestBlockHash(),
	}
	if bid != nil {
		want.latestExecutionPayloadBidBlockHash = bid.BlockHash
		want.hasLatestExecutionPayloadBid = true
	}
	return want
}

func TestBlockServiceGossipWaitsForFullParentPayloadVerification(t *testing.T) {
	service, child, fcu, parentRoot, parentBlockHash := newGloasGossipValidationFixture(t, nil)
	fcu.ExecutionPayloadStatusMap[parentBlockHash] = execution_client.PayloadStatusValidated

	err := service.ValidateGossip(t.Context(), child)
	require.ErrorContains(t, err, "parent payload is not verified")
	require.NotContains(t, fcu.PayloadStatusByRootMap, parentRoot)
}

func TestBlockServiceGossipAcceptsVerifiedFullParentPayload(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceGossipAcceptsOptimisticFullParentPayload(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusNotValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceGossipFirstValidReservationIsAtomic(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { errs <- service.ValidateGossip(t.Context(), child) })
	}
	wg.Wait()
	close(errs)
	accepted := 0
	ignored := 0
	for err := range errs {
		if err == nil {
			accepted++
		} else if errors.Is(err, ErrIgnore) {
			ignored++
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, ignored)
}

func TestBlockServiceGossipReservationCanBeReleased(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.ReleaseGossipReservation(child)
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceQueuesClockBoundaryBlockForRetry(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	processing := &onBlockErrorStore{
		ForkChoiceStorage: fcu,
		err:               forkchoice.ErrBlockTooEarly,
	}
	service.(*blockService).forkchoiceStore = processing

	require.NoError(t, service.ProcessMessage(t.Context(), nil, child))
	root, err := child.Block.HashSSZ()
	require.NoError(t, err)
	impl := service.(*blockService)
	queuedValue, queued := impl.blocksScheduledForLaterExecution.Load(root)
	require.True(t, queued)
	job := queuedValue.(*blockJob)

	impl.processScheduledBlock(t.Context(), root, job, time.Now())
	_, queued = impl.blocksScheduledForLaterExecution.Load(root)
	require.True(t, queued)
	require.Equal(t, int32(2), processing.calls.Load())

	processing.err = nil
	impl.processScheduledBlock(t.Context(), root, job, time.Now())
	_, queued = impl.blocksScheduledForLaterExecution.Load(root)
	require.False(t, queued)
	require.Equal(t, int32(3), processing.calls.Load())
	require.NoError(t, impl.db.View(t.Context(), func(tx kv.Tx) error {
		storedRoot, err := beacon_indicies.ReadBlockRootByStateRoot(tx, child.Block.StateRoot)
		require.NoError(t, err)
		require.Equal(t, common.Hash(root), storedRoot)
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(child.Block.Slot, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
}

func TestBlockServiceCommittedReservationAllowsExactRESTReplayOnly(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.CommitGossipReservation(child)
	require.ErrorIs(t, service.ValidateGossip(t.Context(), child), ErrIgnore)
	service.ReleaseGossipReservation(child)

	child.Block.StateRoot[0] ^= 1
	err := service.ValidateGossip(t.Context(), child)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorContains(t, err, "already seen")
	child.Block.StateRoot[0] ^= 1

	require.NoError(t, service.ValidateGossip(t.Context(), child))
	require.ErrorIs(t, service.ValidateGossip(t.Context(), child), ErrIgnore)
	child.Signature[0] ^= 1
	require.ErrorIs(t, service.ValidateGossip(t.Context(), child), ErrIgnore)
}

func TestBlockServiceExactRESTReplayClaimIsAtomic(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.CommitGossipReservation(child)
	service.ReleaseGossipReservation(child)

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { errs <- service.ValidateGossip(t.Context(), child) })
	}
	wg.Wait()
	close(errs)
	accepted := 0
	ignored := 0
	for err := range errs {
		if err == nil {
			accepted++
		} else if errors.Is(err, ErrIgnore) {
			ignored++
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, ignored)
}

func TestBlockServiceFailedExactRESTReplayRestoresClaim(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.CommitGossipReservation(child)
	service.ReleaseGossipReservation(child)
	require.NoError(t, service.ValidateGossip(t.Context(), child))

	service.ReleaseGossipReservation(child)
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceValidateGossipRejectsMissingBodyBeforeHashing(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Body = nil
	require.ErrorContains(t, service.ValidateGossip(t.Context(), block), "missing beacon block")
}

func TestBlockServiceP2PDuplicateIsIgnoredBeforeHashing(t *testing.T) {
	service, child, _, _, _ := newGloasGossipValidationFixture(t, nil)
	child.Block.Body.Eth1Data = nil
	key := blockGossipKey(child)
	service.(*blockService).seenBlocksCache.Add(key, seenBlock{})

	err := service.ProcessMessage(t.Context(), nil, child)

	require.ErrorIs(t, err, ErrIgnore)
	require.Nil(t, child.Block.Body.Eth1Data)
}

func TestScheduledBlockRepairsDatabaseWhenHeaderAlreadyExists(t *testing.T) {
	underlying := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db := &blockDBError{RwDB: underlying, failUpdateAt: 1, updateErr: errors.New("database unavailable")}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	fcu.Headers[root] = block.SignedBeaconBlockHeader().Header.Copy()
	service := &blockService{db: db, forkchoiceStore: fcu}

	service.ScheduleBlockForLaterProcessing(block)
	jobValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	job := jobValue.(*blockJob)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime)
	require.ErrorIs(t, job.lastAttempt.err, db.updateErr)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	service.processScheduledBlock(t.Context(), root, job, job.retryAfter)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)

	require.NoError(t, underlying.View(t.Context(), func(tx kv.Tx) error {
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
}

func TestScheduledBlockKeepsPayloadPreparationBehindRetries(t *testing.T) {
	store := mock_services.NewForkChoiceStorageMock(t)
	service := &blockService{forkchoiceStore: store}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Slot = 10
	attempts := 0
	job, _ := service.scheduleBlockForLaterProcessing(block, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return forkchoice.ErrNewPayloadNoStatus
		}
		return nil
	}, true, false)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	_, finish, ok := store.TryBeginPayloadPreparation(t.Context(), 10, 11)
	if ok {
		finish()
	}
	require.False(t, ok)
	service.processScheduledBlock(t.Context(), root, job, time.Now().Add(time.Second))
	_, finish, ok = store.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)
	finish()
}

func TestClaimingScheduleUpgradesUnclaimedBlockJob(t *testing.T) {
	store := mock_services.NewForkChoiceStorageMock(t)
	service := &blockService{forkchoiceStore: store}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Slot = 10
	job, _ := service.scheduleBlockForLaterProcessing(block, nil, false, false)
	service.ScheduleBlockForLaterProcessing(block)

	_, _, ok := store.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.False(t, ok)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service.removeScheduledBlock(root, job)
	_, finish, ok := store.TryBeginPayloadPreparation(t.Context(), 10, 11)
	require.True(t, ok)
	finish()
}

func TestPublishedBlockJobRetainsFullStoreUntilSuccess(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	attempts := 0
	storedSidecars := false
	imported := false
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("sidecar storage unavailable")
		}
		storedSidecars = true
		imported = true
		return db.Update(t.Context(), func(tx kv.RwTx) error {
			return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, false)
		})
	})
	jobValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	job := jobValue.(*blockJob)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.False(t, storedSidecars)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)
	require.True(t, job.terminal)
	require.NoError(t, handle.Wait(t.Context()))
	require.NoError(t, handle.Wait(t.Context()))
	require.True(t, storedSidecars)
	require.True(t, imported)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
}

func TestPublishedBlockJobUpgradesBlockOnlyRecovery(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	service.ScheduleBlockForLaterProcessing(block)
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	jobValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.NotNil(t, jobValue.(*blockJob).store)
}

func TestPublishedBlockJobUpgradeWithEqualCreationTime(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service := &blockService{}
	existing := newBlockJob(block, nil)
	candidate := newBlockJob(block, func(context.Context) error { return nil })
	candidate.creationTime = existing.creationTime
	service.blocksScheduledForLaterExecution.Store(root, existing)

	reused, generation := service.reuseScheduledBlockJob(root, existing, candidate, candidate.store)

	require.Same(t, existing, reused)
	require.Equal(t, uint64(1), generation)
	require.NotNil(t, existing.store)
}

func TestPublishedBlockJobIsNotDowngradedByBlockOnlyRecovery(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	calls := 0
	failure := forkchoice.ErrNewPayloadNoStatus
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		calls++
		return failure
	})
	fullValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	job := fullValue.(*blockJob)
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.Equal(t, blockRetryInitialDelay, job.retryDelay)
	retryAfter := job.retryAfter
	service.ScheduleBlockForLaterProcessing(block)
	currentValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, fullValue, currentValue)
	require.Equal(t, retryAfter, job.retryAfter)
	require.Equal(t, blockRetryInitialDelay, job.retryDelay)
	require.NotNil(t, job.store)
	failure = nil
	service.processScheduledBlock(t.Context(), root, job, job.retryAfter)
	require.Equal(t, 2, calls, "a block-only duplicate must preserve the publication callback")
	require.NoError(t, handle.Wait(t.Context()))
}

func TestOlderPublishedBlockJobDoesNotReplaceNewerFullStore(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	older := newBlockJob(block, func(context.Context) error {
		return errors.New("older store should not replace newer store")
	})
	newer := newBlockJob(block, func(context.Context) error { return forkchoice.ErrNewPayloadNoStatus })
	older.creationTime = newer.creationTime
	service := &blockService{}
	service.blocksScheduledForLaterExecution.Store(root, newer)
	service.processScheduledBlock(t.Context(), root, newer, time.Now())
	require.Equal(t, blockRetryInitialDelay, newer.retryDelay)
	retryAfter := newer.retryAfter

	reused, generation := service.reuseScheduledBlockJob(root, newer, older, older.store)

	require.Same(t, newer, reused)
	require.Equal(t, newer.storeGeneration, generation)
	currentValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, newer, currentValue)
	require.Equal(t, retryAfter, newer.retryAfter)
	require.Equal(t, blockRetryInitialDelay, newer.retryDelay)
}

func TestPublishedBlockUpgradeSurvivesStaleBlockOnlyWorker(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[root] = block.SignedBeaconBlockHeader().Header.Copy()
	service := &blockService{db: db, forkchoiceStore: fcu}
	service.ScheduleBlockForLaterProcessing(block)
	staleValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	staleJob := staleValue.(*blockJob)
	fullStoreCalls := 0
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		fullStoreCalls++
		return nil
	})
	service.processScheduledBlock(t.Context(), root, staleJob, staleJob.creationTime)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)
	require.Equal(t, 1, fullStoreCalls)
}

func TestPublishedBlockRefreshSurvivesStaleFullStoreWorker(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		return errors.New("stale store should not run")
	})
	staleValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	stableJob := staleValue.(*blockJob)
	freshStoreCalls := 0
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		freshStoreCalls++
		return nil
	})
	freshValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, stableJob, freshValue)
	service.processScheduledBlock(t.Context(), root, freshValue.(*blockJob), time.Now())
	require.Equal(t, 1, freshStoreCalls)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)
}

func TestBlockServicePendingGossipReservationHandsOffToP2P(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(t.Context(), child, nil, true)
	}()
	select {
	case err := <-errCh:
		t.Fatalf("P2P validation returned before REST reservation resolved: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	service.ReleaseGossipReservation(child)
	require.NoError(t, <-errCh)
}

func TestBlockServiceCommittedGossipReservationRejectsWaitingP2P(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(t.Context(), child, nil, true)
	}()
	select {
	case err := <-errCh:
		t.Fatalf("P2P validation returned before REST reservation resolved: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	service.CommitGossipReservation(child)
	require.ErrorIs(t, <-errCh, ErrIgnore)
}

func TestBlockServiceRevalidatesP2PAfterReservationRelease(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	type result struct {
		err       error
		scheduled bool
	}
	resultCh := make(chan result, 1)
	go func() {
		scheduled := false
		err := service.(*blockService).validateFirstGossip(t.Context(), child, func() { scheduled = true }, true)
		resultCh <- result{err: err, scheduled: scheduled}
	}()
	select {
	case got := <-resultCh:
		t.Fatalf("P2P validation returned before REST reservation resolved: %v", got.err)
	case <-time.After(20 * time.Millisecond):
	}
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusInvalidated
	service.ReleaseGossipReservation(child)
	got := <-resultCh
	require.ErrorIs(t, got.err, ErrIgnore)
	require.True(t, got.scheduled)
}

func TestBlockServiceUnrelatedReservationDoesNotRevalidateP2P(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	parentState := fcu.StateAtBlockRootVal[parentRoot]
	validationEntered := make(chan struct{})
	finishValidation := make(chan struct{})
	validationCalls := 0
	fcu.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
		require.Equal(t, parentRoot, root)
		validationCalls++
		if validationCalls == 1 {
			close(validationEntered)
			<-finishValidation
		}
		return parentState.Copy()
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(t.Context(), child, nil, true)
	}()
	<-validationEntered
	otherKey := proposerIndexAndSlot{proposerIndex: child.Block.ProposerIndex + 1, slot: child.Block.Slot}
	require.NoError(t, service.(*blockService).reserveGossipKey(otherKey, common.Hash{1}))
	service.(*blockService).releaseGossipKey(otherKey, common.Hash{1})
	close(finishValidation)
	require.NoError(t, <-errCh)
	require.Equal(t, 1, validationCalls)
}

func TestBlockServiceCanceledHandoffDoesNotClaimSeen(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	parentState := fcu.StateAtBlockRootVal[parentRoot]
	revalidationEntered := make(chan struct{})
	finishRevalidation := make(chan struct{})
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	bs := service.(*blockService)
	validationKey := blockValidationContextKey{parentRoot: parentRoot, slot: child.Block.Slot}
	bs.validationMu.Lock()
	bs.validationCache.Remove(validationKey)
	bs.validationMu.Unlock()
	fcu.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
		close(revalidationEntered)
		<-finishRevalidation
		return parentState.Copy()
	}
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(ctx, child, nil, true)
	}()
	service.ReleaseGossipReservation(child)
	<-revalidationEntered
	cancel()
	close(finishRevalidation)
	require.ErrorIs(t, <-errCh, ErrIgnore)
	key := blockGossipKey(child)
	bs.seenBlocksMu.Lock()
	require.False(t, bs.seenBlocksCache.Contains(key))
	require.NotContains(t, bs.reservations, key)
	bs.seenBlocksMu.Unlock()
	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		return parentState.Copy()
	}
	require.NoError(t, bs.validateFirstGossip(t.Context(), child, nil, true))
}

func TestBlockServiceGossipIgnoresInvalidatedFullParentPayload(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusInvalidated
	scheduled := false
	err := service.(*blockService).validateFirstGossip(t.Context(), child, func() { scheduled = true }, false)
	require.ErrorIs(t, err, ErrIgnore)
	require.True(t, scheduled)
}

func TestBlockServiceGossipIgnoresUnavailableParentState(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	fcu.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		return nil, errors.New("state storage unavailable")
	}
	scheduled := false

	err := service.(*blockService).validateFirstGossip(t.Context(), child, func() { scheduled = true }, false)
	require.ErrorIs(t, err, ErrIgnore)
	require.True(t, scheduled)
}

func TestUnknownParentBlockDoesNotHoldPayloadPreparation(t *testing.T) {
	service, child, store, _, _ := newGloasGossipValidationFixture(t, nil)
	validatorKey, err := bls.GenerateKey()
	require.NoError(t, err)
	validator := solid.NewValidator()
	var publicKey [48]byte
	copy(publicKey[:], bls.CompressPublicKey(validatorKey.PublicKey()))
	validator.SetPublicKey(publicKey)
	validator.SetActivationEpoch(0)
	validator.SetExitEpoch(service.(*blockService).beaconCfg.FarFutureEpoch)
	validator.SetEffectiveBalance(service.(*blockService).beaconCfg.MaxEffectiveBalance)
	parentState := store.StateAtBlockRootVal[child.Block.ParentRoot]
	require.NoError(t, parentState.AddValidator(validator, service.(*blockService).beaconCfg.MaxEffectiveBalance))
	require.NoError(t, service.(*blockService).syncedData.OnHeadState(parentState))

	child.Block.ProposerIndex = 1
	child.Block.ParentRoot = common.Hash{0xff}
	domain, err := parentState.GetDomain(service.(*blockService).beaconCfg.DomainBeaconProposer, child.Block.Slot/service.(*blockService).beaconCfg.SlotsPerEpoch)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(child.Block, domain)
	require.NoError(t, err)
	copy(child.Signature[:], validatorKey.Sign(signingRoot[:]).Bytes())
	root, err := child.Block.HashSSZ()
	require.NoError(t, err)

	err = service.ProcessMessage(t.Context(), nil, child)
	require.ErrorIs(t, err, ErrIgnore)
	queued, scheduled := service.(*blockService).blocksScheduledForLaterExecution.Load(root)
	require.True(t, scheduled)
	ctx, finish, ok := store.TryBeginPayloadPreparation(t.Context(), child.Block.Slot, child.Block.Slot+1)
	require.True(t, ok)
	defer finish()

	job := queued.(*blockJob)
	synctest.Test(t, func(t *testing.T) {
		bs := service.(*blockService)
		store.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
			t.Error("unknown-parent retry fetched parent state")
			return nil, nil
		}
		retryCtx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		for _, full := range []bool{false, true} {
			bs.validationSlots = make(chan struct{}, 1)
			if full {
				bs.validationSlots <- struct{}{}
			}

			bs.processScheduledBlock(retryCtx, root, job, time.Now())

			require.NoError(t, retryCtx.Err(), "unknown-parent retry waited for validation capacity")
			require.NoError(t, context.Cause(ctx))
			require.ErrorIs(t, job.lastAttempt.err, ErrIgnore)
			_, scheduled = bs.blocksScheduledForLaterExecution.Load(root)
			require.True(t, scheduled)
		}
	})
}

func TestScheduledBlockWaitsForProposerStateBeforeCancelingPayloadPreparation(t *testing.T) {
	service, child, store, root, job := parkUnknownParentBlockForRetry(t, true)
	stateErr := errors.New("parent state unavailable")
	store.ViewStateAtBlockRootFn = func(common.Hash, func(*state.CachingBeaconState) error) error {
		return stateErr
	}
	ctx, finish, ok := store.TryBeginPayloadPreparation(t.Context(), child.Block.Slot, child.Block.Slot+1)
	require.True(t, ok)
	defer finish()

	service.processScheduledBlock(t.Context(), root, job, time.Now())

	require.NoError(t, context.Cause(ctx))
	require.ErrorIs(t, job.lastAttempt.err, stateErr)
	require.Nil(t, job.finishPayloadImport)
	_, scheduled := service.blocksScheduledForLaterExecution.Load([32]byte(root))
	require.True(t, scheduled)

	store.ViewStateAtBlockRootFn = nil
	service.processScheduledBlock(t.Context(), root, job, time.Now())

	require.ErrorIs(t, context.Cause(ctx), forkchoice.ErrPayloadImportInProgress)
	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrEIP7594ColumnDataNotAvailable)
	require.NotNil(t, job.finishPayloadImport)
}

func TestAuthenticatedScheduledBlockTakesPayloadImportClaim(t *testing.T) {
	service, child, store, root, job := parkUnknownParentBlockForRetry(t, true)
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrEIP7594ColumnDataNotAvailable)

	_, finish, ok := store.TryBeginPayloadPreparation(t.Context(), child.Block.Slot, child.Block.Slot+1)
	if ok {
		finish()
	}
	require.False(t, ok)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime.Add(blockJobExpiry+time.Second))
	_, finish, ok = store.TryBeginPayloadPreparation(t.Context(), child.Block.Slot, child.Block.Slot+1)
	require.True(t, ok)
	finish()
}

func TestWrongProposerScheduledBlockDoesNotCancelPayloadPreparation(t *testing.T) {
	service, child, store, root, job := parkUnknownParentBlockForRetry(t, false)
	store.OnBlockErr = forkchoice.ErrParentEnvelopePending
	ctx, finish, ok := store.TryBeginPayloadPreparation(t.Context(), child.Block.Slot, child.Block.Slot+1)
	require.True(t, ok)
	defer finish()

	service.processScheduledBlock(t.Context(), root, job, time.Now())

	require.NoError(t, context.Cause(ctx))
	require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrBlockInvalid)
	require.Nil(t, job.finishPayloadImport)
	_, scheduled := service.blocksScheduledForLaterExecution.Load([32]byte(root))
	require.False(t, scheduled)
}

func TestScheduledBlockRejectsInvalidParentSlotBeforeStateAccess(t *testing.T) {
	tests := []struct {
		name      string
		blockSlot uint64
	}{
		{name: "parent in later epoch", blockSlot: 5},
		{name: "parent at same slot", blockSlot: clparams.MainnetBeaconConfig.SlotsPerEpoch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serviceInterface, child, store, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
			service := serviceInterface.(*blockService)
			child.Block.Slot = test.blockSlot
			root, err := child.Block.HashSSZ()
			require.NoError(t, err)
			job, _ := service.scheduleBlockForLaterProcessing(child, nil, false, false)
			store.OnBlockErr = forkchoice.ErrParentEnvelopePending

			stateReads := 0
			store.GetStateAtBlockRootFn = func(root common.Hash) (*state.CachingBeaconState, error) {
				require.Equal(t, parentRoot, root)
				stateReads++
				return store.StateAtBlockRootVal[root].Copy()
			}

			for range 2 {
				service.processScheduledBlock(t.Context(), root, job, time.Now())

				require.Zero(t, stateReads)
				require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrBlockInvalid)
				require.ErrorIs(t, job.lastAttempt.err, ErrBlockYoungerThanParent)
				require.Nil(t, job.finishPayloadImport)
				_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
				require.False(t, scheduled)
			}
		})
	}
}

func TestUnrelatedScheduledBlockErrorDoesNotRetainImportClaim(t *testing.T) {
	service, child, store, root, job := parkUnknownParentBlockForRetry(t, true)
	store.OnBlockErr = errors.New("unrelated retry error")
	validationCalls := 0
	store.ViewStateAtBlockRootFn = func(root common.Hash, fn func(*state.CachingBeaconState) error) error {
		validationCalls++
		return fn(store.StateAtBlockRootVal[root])
	}

	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.EqualError(t, job.lastAttempt.err, "unrelated retry error")
	require.Equal(t, 1, validationCalls)
	require.Nil(t, job.finishPayloadImport)
	_, finish, ok := store.TryBeginPayloadPreparation(t.Context(), child.Block.Slot, child.Block.Slot+1)
	require.True(t, ok)
	finish()
}

func TestShouldRetainBlockImportClaim(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "EIP-4844 data unavailable", err: forkchoice.ErrEIP4844DataNotAvailable, want: true},
		{name: "EIP-7594 column data unavailable", err: forkchoice.ErrEIP7594ColumnDataNotAvailable, want: true},
		{name: "parent envelope pending", err: forkchoice.ErrParentEnvelopePending, want: true},
		{name: "block too early", err: forkchoice.ErrBlockTooEarly, want: true},
		{name: "new payload has no status", err: forkchoice.ErrNewPayloadNoStatus, want: true},
		{name: "wrapped storage error", err: fmt.Errorf("wrapped: %w", errBlockStorage), want: true},
		{name: "unrelated error", err: errors.New("unrelated error")},
		{name: "invalid block", err: forkchoice.ErrBlockInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, shouldRetainBlockImportClaim(test.err))
		})
	}
}

func parkUnknownParentBlockForRetry(t *testing.T, matchingProposer bool) (*blockService, *cltypes.SignedBeaconBlock, *mock_services.ForkChoiceStorageMock, common.Hash, *blockJob) {
	t.Helper()
	serviceInterface, child, store, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	service := serviceInterface.(*blockService)
	parent := store.Blocks[parentRoot]
	parentHeader := store.Headers[parentRoot]
	if !matchingProposer {
		validatorKey, err := bls.GenerateKey()
		require.NoError(t, err)
		validator := solid.NewValidator()
		var publicKey [48]byte
		copy(publicKey[:], bls.CompressPublicKey(validatorKey.PublicKey()))
		validator.SetPublicKey(publicKey)
		validator.SetActivationEpoch(0)
		validator.SetExitEpoch(service.beaconCfg.FarFutureEpoch)
		validator.SetEffectiveBalance(service.beaconCfg.MaxEffectiveBalance)
		parentState := store.StateAtBlockRootVal[parentRoot]
		require.NoError(t, parentState.AddValidator(validator, service.beaconCfg.MaxEffectiveBalance))
		require.NoError(t, service.syncedData.OnHeadState(parentState))
		child.Block.ProposerIndex = 1
		domain, err := parentState.GetDomain(service.beaconCfg.DomainBeaconProposer, child.Block.Slot/service.beaconCfg.SlotsPerEpoch)
		require.NoError(t, err)
		signingRoot, err := fork.ComputeSigningRoot(child.Block, domain)
		require.NoError(t, err)
		copy(child.Signature[:], validatorKey.Sign(signingRoot[:]).Bytes())
	}
	delete(store.Blocks, parentRoot)
	delete(store.Headers, parentRoot)
	root, err := child.Block.HashSSZ()
	require.NoError(t, err)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, child), ErrIgnore)
	jobValue, loaded := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, loaded)
	job := jobValue.(*blockJob)
	require.Nil(t, job.finishPayloadImport)

	store.Blocks[parentRoot] = parent
	store.Headers[parentRoot] = parentHeader
	store.OnBlockErr = forkchoice.ErrEIP7594ColumnDataNotAvailable
	return service, child, store, root, job
}

func TestBlockServiceGossipRejectsWrongEmptyParentExecutionHead(t *testing.T) {
	service, child, _, _, _ := newGloasGossipValidationFixture(t, func(common.Hash, common.Hash) common.Hash {
		return common.Hash{0x99}
	})
	require.ErrorContains(t, service.ValidateGossip(t.Context(), child), "does not build on the parent's execution head")
}

func TestBlockServiceGossipAcceptsEmptyParentExecutionHead(t *testing.T) {
	service, child, _, _, _ := newGloasGossipValidationFixture(t, func(parentExecutionHead, _ common.Hash) common.Hash {
		return parentExecutionHead
	})
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceProcessMessageIgnoresForkSchemaMismatchBeforeStorage(t *testing.T) {
	service, child, fcu, _, _ := newGloasGossipValidationFixture(t, nil)
	impl := service.(*blockService)
	cfg := *impl.beaconCfg
	cfg.GloasForkEpoch = child.Block.Slot/cfg.SlotsPerEpoch + 1
	impl.beaconCfg = &cfg
	require.False(t, cfg.ForkSchemaMatchesSlot(child.Block.Slot, child.Version()))

	assertBlockServiceProcessMessageIgnoredBeforeStorage(t, service, child, fcu, "fork schema mismatch reached fork choice")
}

func TestBlockServiceProcessMessageIgnoresExactForkVersionMismatchBeforeStorage(t *testing.T) {
	service, child, fcu, _, _ := newGloasGossipValidationFixture(t, nil)
	impl := service.(*blockService)
	cfg := *impl.beaconCfg
	cfg.GloasForkEpoch = child.Block.Slot/cfg.SlotsPerEpoch + 1
	impl.beaconCfg = &cfg
	child.Block.Body.Version = clparams.ElectraVersion
	require.Equal(t, clparams.FuluVersion, cfg.GetCurrentStateVersion(child.Block.Slot/cfg.SlotsPerEpoch))
	require.True(t, cfg.ForkSchemaMatchesSlot(child.Block.Slot, child.Version()))

	assertBlockServiceProcessMessageIgnoredBeforeStorage(t, service, child, fcu, "fork version mismatch reached fork choice")
}

func assertBlockServiceProcessMessageIgnoredBeforeStorage(
	t *testing.T,
	service BlockService,
	child *cltypes.SignedBeaconBlock,
	fcu *mock_services.ForkChoiceStorageMock,
	forkChoiceErr string,
) {
	t.Helper()
	impl := service.(*blockService)
	forkChoice := &onBlockErrorStore{
		ForkChoiceStorage: fcu,
		err:               errors.New(forkChoiceErr),
	}
	impl.forkchoiceStore = forkChoice
	blockRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)

	err = service.ProcessMessage(t.Context(), nil, child)
	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, forkChoice.calls.Load())
	require.NoError(t, impl.db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		require.NoError(t, err)
		require.Nil(t, slot)
		return nil
	}))
}

func TestBlockServiceGossipAcceptsChildOfHeaderOnlyCheckpointAnchor(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, func(parentExecutionHead, _ common.Hash) common.Hash {
		return parentExecutionHead
	})
	parent := fcu.Blocks[parentRoot]
	fcu.StateAtBlockRootVal[parentRoot].SetLatestExecutionPayloadBid(parent.Block.Body.GetSignedExecutionPayloadBid().Message)
	delete(fcu.Blocks, parentRoot)
	fcu.AnchorRootVal = parentRoot
	fcu.AnchorSlotVal = parent.Block.Slot
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: parent.Block.Slot / clparams.MainnetBeaconConfig.SlotsPerEpoch, Root: parentRoot}
	fcu.Ancestors[parent.Block.Slot] = forkchoice.ForkChoiceNode{Root: parentRoot}

	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func newGloasGossipValidationFixture(t *testing.T, childParentHash func(parentExecutionHead, parentBlockHash common.Hash) common.Hash) (BlockService, *cltypes.SignedBeaconBlock, *mock_services.ForkChoiceStorageMock, common.Hash, common.Hash) {
	t.Helper()
	ctrl := gomock.NewController(t)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	parentSlot := cfg.SlotsPerEpoch
	childSlot := parentSlot + 1

	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	validator := solid.NewValidator()
	var pubkey [48]byte
	copy(pubkey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	validator.SetPublicKey(pubkey)
	validator.SetActivationEpoch(0)
	validator.SetExitEpoch(cfg.FarFutureEpoch)
	validator.SetEffectiveBalance(cfg.MaxEffectiveBalance)
	parentState := state.New(&cfg)
	parentState.SetVersion(clparams.GloasVersion)
	require.NoError(t, parentState.SetSlot(parentSlot))
	require.NoError(t, parentState.AddValidator(validator, cfg.MaxEffectiveBalance))
	parentState.SetProposerLookahead(solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch)))
	parentExecutionHead := common.Hash{0x11}
	parentState.SetLatestBlockHash(parentExecutionHead)

	parentBlockHash := common.Hash{0x22}
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	parent.Block.Slot = parentSlot
	parent.Block.ProposerIndex = 0
	parent.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash: parentExecutionHead,
		BlockHash:       parentBlockHash,
	}}
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)

	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = childSlot
	child.Block.ProposerIndex = 0
	child.Block.ParentRoot = parentRoot
	selectedParentHash := parentBlockHash
	if childParentHash != nil {
		selectedParentHash = childParentHash(parentExecutionHead, parentBlockHash)
	}
	child.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash: selectedParentHash,
		ParentBlockRoot: parentRoot,
	}}
	domain, err := parentState.GetDomain(cfg.DomainBeaconProposer, childSlot/cfg.SlotsPerEpoch)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(child.Block, domain)
	require.NoError(t, err)
	copy(child.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	syncedDataManager := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedDataManager.OnHeadState(parentState))
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	ethClock.EXPECT().GetCurrentSlot().Return(childSlot).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[parentRoot] = parent.SignedBeaconBlockHeader().Header.Copy()
	fcu.Blocks[parentRoot] = parent
	fcu.StateAtBlockRootVal[parentRoot] = parentState
	service := newBlockService(db, fcu, syncedDataManager, ethClock, &cfg, nil)
	return service, child, fcu, parentRoot, parentBlockHash
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
			output := captureServiceLogs(t)

			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Body.AttesterSlashings.Append(&cltypes.AttesterSlashing{})
			service := blockService{forkchoiceStore: attesterSlashingErrorStore{err: tc.err}}

			service.importBlockOperations(block)

			require.Equal(t, tc.wantLogged, bytes.Contains(output.Bytes(), []byte("bad attester slashing received")))
		})
	}
}

type attestationCountingStore struct {
	forkchoice.ForkChoiceStorage
	attestations int
}

func (s *attestationCountingStore) OnAttestation(*solid.Attestation, bool, bool) error {
	s.attestations++
	return nil
}

// OnBlock already applies body attestations, so the gossip path must not apply them again.
func TestImportBlockOperationsLeavesAttestationsToOnBlock(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Body.Attestations.Append(&solid.Attestation{})
	store := &attestationCountingStore{}
	service := blockService{forkchoiceStore: store}

	service.importBlockOperations(block)

	require.Zero(t, store.attestations)
}

func TestValidateGloasBlockBodyLimitsRejectsOversizedOperationAndRequests(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxProposerSlashings = 1
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	body.ProposerSlashings.Append(&cltypes.ProposerSlashing{})
	body.ProposerSlashings.Append(&cltypes.ProposerSlashing{})
	require.Error(t, validateGloasBlockBodyLimits(&cfg, body))

	body = cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	body.ParentExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	body.ParentExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	require.Error(t, validateGloasBlockBodyLimits(&cfg, body))
}

func TestValidateGloasBlockBodyLimitsRejectsDeposit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	require.NoError(t, validateGloasBlockBodyLimits(&cfg, body))
	body.Deposits.Append(&cltypes.Deposit{})
	require.ErrorContains(t, validateGloasBlockBodyLimits(&cfg, body), "deposits")
}

func TestBlockServiceDecodeGossipMessageStrict(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	service := &blockService{beaconCfg: &cfg}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	_, err = service.DecodeGossipMessage("peer", encoded, clparams.GloasVersion)
	require.NoError(t, err)

	outerGap := append([]byte(nil), encoded[:100]...)
	outerGap = append(outerGap, make([]byte, 4)...)
	outerGap = append(outerGap, encoded[100:]...)
	binary.LittleEndian.PutUint32(outerGap, 104)
	_, err = service.DecodeGossipMessage("peer", outerGap, clparams.GloasVersion)
	require.Error(t, err)

	const blockStart = 100
	const blockFixedSize = 84
	nestedGap := append([]byte(nil), encoded[:blockStart+blockFixedSize]...)
	nestedGap = append(nestedGap, make([]byte, 4)...)
	nestedGap = append(nestedGap, encoded[blockStart+blockFixedSize:]...)
	binary.LittleEndian.PutUint32(nestedGap[blockStart+80:], blockFixedSize+4)
	_, err = service.DecodeGossipMessage("peer", nestedGap, clparams.GloasVersion)
	require.Error(t, err)
}

func TestBlockServiceDecodeGossipMessageStrictPreGloasCompatibility(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	service := &blockService{beaconCfg: &cfg}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	_, err = service.DecodeGossipMessage("peer", encoded, clparams.DenebVersion)
	require.NoError(t, err)
}

func TestPublishedBlockJobUpgradeKeepsWaiterOnRequiredStoreGeneration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		blockOnly bool
	}{
		{name: "success"},
		{name: "EL failure", err: forkchoice.ErrNewPayloadNoStatus},
		{name: "block-only storage failure", err: errors.New("database unavailable"), blockOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			firstStarted := make(chan struct{})
			firstRelease := make(chan struct{})
			releaseFirst := sync.OnceFunc(func() { close(firstRelease) })
			waitForRelease := func() {
				close(firstStarted)
				<-firstRelease
			}
			if tc.blockOnly {
				service.db = &blockDBError{viewErr: tc.err, beforeView: waitForRelease}
				service.forkchoiceStore = mock_services.NewForkChoiceStorageMock(t)
				service.ScheduleBlockForLaterProcessing(block)
			} else {
				service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
					waitForRelease()
					return tc.err
				})
			}
			job := serviceJob(t, service, root)
			firstDone := make(chan struct{})
			go func() {
				service.processScheduledBlock(t.Context(), root, job, time.Now())
				close(firstDone)
			}()
			t.Cleanup(func() {
				releaseFirst()
				<-firstDone
			})
			<-firstStarted
			secondCalls := 0
			handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
				secondCalls++
				return nil
			})
			waitDone := make(chan error, 1)
			go func() { waitDone <- handle.Wait(t.Context()) }()
			releaseFirst()
			<-firstDone
			require.ErrorIs(t, job.lastAttempt.err, tc.err)
			if tc.blockOnly {
				require.ErrorIs(t, job.lastAttempt.err, errBlockStorage)
			}
			_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
			require.True(t, scheduled)
			require.True(t, job.retryAfter.IsZero(), "a stale attempt must not delay the new store generation")
			require.Zero(t, job.retryDelay)
			select {
			case err := <-waitDone:
				t.Fatalf("waiter completed for superseded store generation: %v", err)
			default:
			}
			service.processScheduledBlock(t.Context(), root, job, time.Now())
			require.NoError(t, <-waitDone)
			require.Equal(t, 1, secondCalls)
		})
	}
}

func TestPublishedBlockJobTransientFailureKeepsWaiterUntilRetrySucceeds(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	transient := errors.New("database unavailable")
	calls := 0
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		calls++
		if calls == 1 {
			return transient
		}
		return nil
	})
	waitDone := make(chan error, 1)
	waitStarted := make(chan struct{})
	go func() {
		close(waitStarted)
		waitDone <- handle.Wait(t.Context())
	}()
	<-waitStarted
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	select {
	case err := <-waitDone:
		t.Fatalf("waiter completed for a retryable failure: %v", err)
	default:
	}
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	require.NoError(t, <-waitDone)
	require.Equal(t, 2, calls)
}

func TestPublishedBlockJobWaitConsumesAttemptCompletedWhileWaiting(t *testing.T) {
	transient := errors.New("database unavailable")
	job := newBlockJob(cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version), func(context.Context) error {
		return transient
	})
	handle := &publishedBlockJobHandle{job: job, generation: job.storeGeneration}
	attempt := job.attempt
	attempt.err = transient
	attempt.generation = job.storeGeneration
	close(attempt.done)
	job.mu.Lock()
	job.lastAttempt = attempt
	job.attempt = &blockJobAttempt{done: make(chan struct{})}
	job.mu.Unlock()
	waitCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, handle.Wait(waitCtx), context.Canceled)
}

func TestPublishedBlockJobWaitersObserveCancellationIndependently(t *testing.T) {
	job := newBlockJob(cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version), nil)
	handle := &publishedBlockJobHandle{job: job}
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	firstStarted := make(chan struct{})
	go func() {
		close(firstStarted)
		firstDone <- handle.Wait(firstCtx)
	}()
	<-firstStarted

	secondCtx, cancelSecond := context.WithCancel(t.Context())
	cancelSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- handle.Wait(secondCtx) }()
	select {
	case err := <-secondDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("canceled waiter blocked behind another waiter")
	}
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
}

func TestPublishedBlockJobRequestCancellationDoesNotCancelIntegration(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(ctx context.Context) error {
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	})
	job := serviceJob(t, service, root)
	processDone := make(chan struct{})
	go func() {
		service.processScheduledBlock(context.Background(), root, job, time.Now())
		close(processDone)
	}()
	<-started
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, handle.Wait(waitCtx), context.Canceled)
	close(release)
	<-processDone
	require.NoError(t, handle.Wait(t.Context()))
}

func TestPublishedBlockJobPermanentFailureIsTerminal(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	calls := 0
	permanent := fmt.Errorf("%w: execution payload is invalid", forkchoice.ErrBlockInvalid)
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		calls++
		return permanent
	})
	job := serviceJob(t, service, root)
	service.processScheduledBlock(context.Background(), root, job, time.Now())
	_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, scheduled)
	require.ErrorIs(t, handle.Wait(t.Context()), forkchoice.ErrBlockInvalid)
	require.ErrorIs(t, handle.Wait(t.Context()), forkchoice.ErrBlockInvalid)
	service.processScheduledBlock(context.Background(), root, job, time.Now())
	require.Equal(t, 1, calls)
}

func TestPublishedBlockJobHashFailureWaitIsReplayable(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	hashErr := errors.New("hash failure")
	service := &blockService{}
	job := newFailedBlockJob(block, func(context.Context) error { return nil }, hashErr)
	handle := &publishedBlockJobHandle{job: job, generation: job.storeGeneration}
	require.EqualError(t, handle.Wait(t.Context()), hashErr.Error())
	require.EqualError(t, handle.Wait(t.Context()), hashErr.Error())
	count := 0
	service.blocksScheduledForLaterExecution.Range(func(_, _ any) bool {
		count++
		return true
	})
	require.Zero(t, count)
}

func TestPublishedBlockJobDetachedTerminalCannotReplaceCurrentJob(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	detached := newBlockJob(block, func(context.Context) error { return nil })
	detached.terminal = true
	current := newBlockJob(block, func(context.Context) error { return nil })
	service.blocksScheduledForLaterExecution.Store(root, current)
	candidate := newBlockJob(block, func(context.Context) error { return nil })

	reused, _ := service.reuseScheduledBlockJob(root, detached, candidate, candidate.store)

	require.Nil(t, reused)
	stored, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, current, stored)
}

func TestPublishedBlockJobExpiryRescheduleKeepsFreshStore(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	expiredHandle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	job := serviceJob(t, service, root)
	job.creationTime = time.Now().Add(-blockJobExpiry - time.Second)
	service.processScheduledBlock(context.Background(), root, job, time.Now())
	require.ErrorIs(t, expiredHandle.Wait(t.Context()), ErrPublishedBlockJobExpired)

	freshStoreCalls := 0
	freshHandle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		freshStoreCalls++
		return nil
	})
	freshJob := serviceJob(t, service, root)
	require.NotSame(t, job, freshJob)
	service.processScheduledBlock(context.Background(), root, freshJob, time.Now())
	require.NoError(t, freshHandle.Wait(t.Context()))
	require.Equal(t, 1, freshStoreCalls)
}

func TestPublishedBlockJobRefreshSurvivesEarlierExpirySample(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	firstHandle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	job := serviceJob(t, service, root)
	job.creationTime = time.Now().Add(-blockJobExpiry - time.Second)
	expiryNow := time.Now()
	freshStoreCalls := 0
	freshHandle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		freshStoreCalls++
		return nil
	})

	service.processScheduledBlock(context.Background(), root, job, expiryNow)

	require.NoError(t, firstHandle.Wait(t.Context()))
	require.NoError(t, freshHandle.Wait(t.Context()))
	require.Equal(t, 1, freshStoreCalls)
	_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, scheduled)
}

func TestPublishedBlockJobShutdownClosesQueuedWaitersAndRejectsNewSchedules(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := clparams.MainnetBeaconConfig
	service := NewBlockService(ctx, nil, mock_services.NewForkChoiceStorageMock(t), nil, nil, nil, &cfg, nil).(*blockService)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	secondBlock := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	secondBlock.Block.Slot = 1
	handles := []PublishedBlockJob{
		service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil }),
		service.SchedulePublishedBlockForLaterProcessing(secondBlock, func(context.Context) error { return nil }),
	}
	cancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	for _, handle := range handles {
		require.ErrorIs(t, handle.Wait(waitCtx), ErrPublishedBlockJobStopped)
	}

	lateHandle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	require.ErrorIs(t, lateHandle.Wait(waitCtx), ErrPublishedBlockJobStopped)
}

func TestPublishedBlockJobShutdownOwnsRunningAttemptAndIgnoresLateResult(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	storeStarted := make(chan struct{})
	storeRelease := make(chan struct{})
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		close(storeStarted)
		<-storeRelease
		return nil
	})
	job := serviceJob(t, service, root)
	processDone := make(chan struct{})
	go func() {
		service.processScheduledBlock(context.Background(), root, job, time.Now())
		close(processDone)
	}()
	<-storeStarted
	service.stopPublishedBlockJobs()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	require.ErrorIs(t, handle.Wait(waitCtx), ErrPublishedBlockJobStopped)
	close(storeRelease)
	<-processDone
	require.ErrorIs(t, handle.Wait(waitCtx), ErrPublishedBlockJobStopped)
	_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, scheduled)
}

func TestPublishedBlockJobShutdownPreservesCompletedAttempt(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	require.NoError(t, handle.Wait(t.Context()))

	service.stopPublishedBlockJobs()

	require.NoError(t, handle.Wait(t.Context()))
}

func TestPublishedBlockJobConcurrentSchedulesReturnWaitableHandles(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	const schedules = 32
	start := make(chan struct{})
	handles := make(chan PublishedBlockJob, schedules)
	var wg sync.WaitGroup
	for range schedules {
		wg.Go(func() {
			<-start
			handles <- service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
		})
	}
	close(start)
	wg.Wait()
	close(handles)
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	for handle := range handles {
		require.NoError(t, handle.Wait(t.Context()))
	}
}

func serviceJob(t *testing.T, service *blockService, root [32]byte) *blockJob {
	t.Helper()
	job, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	return job.(*blockJob)
}
