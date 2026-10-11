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
	"context"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	syncpoolmock "github.com/erigontech/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func newSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller, ethClock eth_clock.EthereumClock) (SyncCommitteeMessagesService, *synced_data.SyncedDataManager) {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	syncContributionPool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	batchSignatureVerifier := NewBatchSignatureVerifier(t.Context(), nil)
	go batchSignatureVerifier.Start()
	s := NewSyncCommitteeMessagesService(cfg, ethClock, syncedDataManager, syncContributionPool, batchSignatureVerifier, true)
	syncContributionPool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s, syncedDataManager
}

func setupSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller) (SyncCommitteeMessagesService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock) {
	t.Helper()
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	s, syncedDataManager := newSyncCommitteesServiceTest(t, ctrl, ethClock)
	return s, syncedDataManager, ethClock
}

func getObjectsForSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller) (*state.CachingBeaconState, *SyncCommitteeMessageForGossip) {
	_, _, state := tests.GetBellatrixRandom()
	br, _ := state.BlockRoot()
	msg := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            state.Slot(),
			BeaconBlockRoot: br,
			ValidatorIndex:  0,
		},
		ImmediateVerification: true,
	}
	return state, msg
}

// TestSyncCommitteesIgnoresForgedSlotThatAliasesToNow uses the real clock: a slot 2^62 ahead
// of the current one maps to the same start time under 64-bit arithmetic, but must still be ignored,
// and before any signature work, since the message signature does not cover the slot.
func TestSyncCommitteesIgnoresForgedSlotThatAliasesToNow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFuncs := &mockFuncs{ctrl: ctrl}
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures // no expectation: any call fails the test

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	cfg := &clparams.MainnetBeaconConfig
	midSlotGenesis := uint64(time.Now().Unix()) - state.Slot()*cfg.SecondsPerSlot - cfg.SecondsPerSlot/2
	ethClock := eth_clock.NewEthereumClock(midSlotGenesis, common.Hash{}, cfg)
	s, syncedDataManager := newSyncCommitteesServiceTest(t, ctrl, ethClock)
	require.NoError(t, syncedDataManager.OnHeadState(state))
	require.Equal(t, state.Slot(), ethClock.GetCurrentSlot())

	msg.SyncCommitteeMessage.Slot = state.Slot() + 1<<62
	require.ErrorIs(t, s.ProcessMessage(context.Background(), new(uint64), msg), ErrIgnore)
}

func TestSyncCommitteesServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, _, _ := setupSyncCommitteesServiceTest(t, ctrl)
	require.Error(t, s.ProcessMessage(context.TODO(), nil, nil))
}

func TestSyncCommitteesBadTiming(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)

	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(false).AnyTimes()
	require.Error(t, s.ProcessMessage(context.Background(), nil, msg))
}

func TestSyncCommitteesBadSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	sn := uint64(1000)

	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()
	require.Error(t, s.ProcessMessage(context.Background(), &sn, msg))
}

func TestSyncCommitteesSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFuncs := &mockFuncs{ctrl: ctrl}
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	ctrl.RecordCall(mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()
	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg))
	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg)) // Silent ignore: returns nil if done twice
}

// TestSyncCommitteesIgnoresReplacementWithDifferentContent proves a second
// message for the same (subnet, slot, validator_index) - the seen-key - is
// ignored without ever verifying its signature when its content differs
// from the first, already-verified message. Before this, a cache hit
// returned nil unconditionally, so different (and here, never verified)
// bytes were treated as an already-validated message and would reach
// PublishBackground/gossip forwarding the same way a genuinely valid
// message would.
func TestSyncCommitteesIgnoresReplacementWithDifferentContent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFuncs := &mockFuncs{ctrl: ctrl}
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	// Exactly one verification call is ever expected: gomock fails the test
	// if the replacement below triggers a second one.
	ctrl.RecordCall(mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()
	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg))

	replacement := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            msg.SyncCommitteeMessage.Slot,
			BeaconBlockRoot: common.Hash{0xff},
			ValidatorIndex:  msg.SyncCommitteeMessage.ValidatorIndex,
			Signature:       common.Bytes96{}, // zero signature: would fail verification if ever checked
		},
		ImmediateVerification: true,
	}
	err := s.ProcessMessage(context.Background(), new(uint64), replacement)
	require.ErrorIs(t, err, ErrIgnore)

	// The rejected replacement must not have disturbed the original entry:
	// a genuine retry of the first message is still a silent, verification-free duplicate.
	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg))
}

// TestSyncCommitteesRetryAfterFailedPublishStillSucceeds proves a caller
// whose earlier PublishBackground admission failed (e.g. a full queue) can
// retry the identical message and still get a nil - not ErrIgnore - so it
// gets another chance to publish. MarkPublished is the only thing that
// should ever turn a matching duplicate into ErrIgnore.
func TestSyncCommitteesRetryAfterFailedPublishStillSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFuncs := &mockFuncs{ctrl: ctrl}
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	ctrl.RecordCall(mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()

	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg))
	// No MarkPublished call: the caller's publish attempt is assumed to have failed.
	require.NoError(t, s.ProcessMessage(context.Background(), new(uint64), msg))
}

// TestSyncCommitteesIgnoresRetryAfterSuccessfulPublish proves that once a
// caller reports the message published, a later retry of the identical
// content is ErrIgnore, not another nil that would spend a second admission
// attempt on it.
func TestSyncCommitteesIgnoresRetryAfterSuccessfulPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFuncs := &mockFuncs{ctrl: ctrl}
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures

	state, msg := getObjectsForSyncCommitteesServiceTest(t, ctrl)
	ctrl.RecordCall(mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()

	subnet := new(uint64)
	require.NoError(t, s.ProcessMessage(context.Background(), subnet, msg))
	s.(*syncCommitteeMessagesService).MarkPublished(*subnet, msg.SyncCommitteeMessage.Slot, msg.SyncCommitteeMessage.ValidatorIndex,
		msg.SyncCommitteeMessage.BeaconBlockRoot, msg.SyncCommitteeMessage.Signature)

	err := s.ProcessMessage(context.Background(), subnet, msg)
	require.ErrorIs(t, err, ErrIgnore)
}
