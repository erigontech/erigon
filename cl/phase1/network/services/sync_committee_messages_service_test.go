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
	"sync"
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

func getObjectsForSyncCommitteesServiceTest() (*state.CachingBeaconState, *SyncCommitteeMessageForGossip) {
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

	state, msg := getObjectsForSyncCommitteesServiceTest()
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

	state, msg := getObjectsForSyncCommitteesServiceTest()

	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(false).AnyTimes()
	require.Error(t, s.ProcessMessage(context.Background(), nil, msg))
}

func TestSyncCommitteesBadSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state, msg := getObjectsForSyncCommitteesServiceTest()
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

	state, msg := getObjectsForSyncCommitteesServiceTest()
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

	state, msg := getObjectsForSyncCommitteesServiceTest()
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

	state, msg := getObjectsForSyncCommitteesServiceTest()
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

	state, msg := getObjectsForSyncCommitteesServiceTest()
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

// Members assigned to slot+1 sign for slot, so at the last slot of a period the next committee signs,
// whatever slot the head is at.
func TestSyncCommitteesChecksTheCommitteeOfTheMessageSlot(t *testing.T) {
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(_, _, _ [][]byte) (bool, error) { return true, nil }
	fixture, _ := getObjectsForSyncCommitteesServiceTest()
	period := fixture.BeaconConfig().SlotsPerEpoch * fixture.BeaconConfig().EpochsPerSyncCommitteePeriod

	tests := []struct {
		name         string
		headBehind   uint64 // slots before the first slot of the next period: 1 puts the head at the last slot
		messageSlot  uint64 // slots before the first slot of the next period
		inNext       bool
		wantRejected bool
		wantIgnored  bool
		unknown      bool // the validator index is not in the registry
	}{
		{"previous slot with the head at the last slot", 1, 2, false, false, false, false},
		{"last slot with the head one slot behind", 2, 1, true, false, false, false},
		{"last slot signed by the current committee", 2, 1, false, true, false, false},
		{"committee not in the head state", 1 + 2*period, 1, true, false, true, false},
		{"unknown validator with the committee not in the head state", 1 + 2*period, 1, true, true, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			head, msg := getObjectsForSyncCommitteesServiceTest()
			cfg := head.BeaconConfig()
			// Three periods after the fixture, so a head two periods behind the message stays above zero.
			nextPeriod := cfg.RoundSlotToSyncCommitteePeriod(head.Slot()) + 3*cfg.SlotsPerEpoch*cfg.EpochsPerSyncCommitteePeriod
			require.Greater(t, nextPeriod, tt.headBehind)
			require.NoError(t, head.SetSlot(nextPeriod-tt.headBehind))

			publicKey := common.Bytes48{0xaa}
			validator, err := head.ValidatorForValidatorIndex(0)
			require.NoError(t, err)
			validator.SetPublicKey(publicKey)
			head.SetValidatorAtIndex(0, validator)
			current, next := head.CurrentSyncCommittee().Copy(), head.NextSyncCommittee().Copy()
			member, other := current, next
			if tt.inNext {
				member, other = next, current
			}
			pubkeys := member.GetCommittee()
			pubkeys[0] = publicKey
			member.SetCommittee(pubkeys)
			require.NotContains(t, other.GetCommittee(), publicKey)
			require.NoError(t, head.SetCurrentSyncCommittee(current))
			require.NoError(t, head.SetNextSyncCommittee(next))

			msg.SyncCommitteeMessage.Slot = nextPeriod - tt.messageSlot
			msg.SyncCommitteeMessage.ValidatorIndex = 0
			if tt.unknown {
				msg.SyncCommitteeMessage.ValidatorIndex = uint64(head.ValidatorLength())
			}
			msg.ImmediateVerification = true

			s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
			require.NoError(t, synced.OnHeadState(head))
			ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(msg.SyncCommitteeMessage.Slot).Return(true).AnyTimes()
			ethClock.EXPECT().GetCurrentSlot().Return(msg.SyncCommitteeMessage.Slot).AnyTimes()

			err = s.ProcessMessage(context.Background(), new(uint64), msg)
			if tt.wantIgnored {
				require.ErrorIs(t, err, ErrIgnore)
				return
			}
			if tt.wantRejected {
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrIgnore)
				return
			}
			require.NoError(t, err)
		})
	}
}

// A gossip message is accepted, and so forwarded, only once its signature is verified.
func TestSyncCommitteesGossipRejectsInvalidSignatureBeforeAccepting(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockFuncs := &mockFuncs{ctrl: ctrl}
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = mockFuncs.BlsVerifyMultipleSignatures
	ctrl.RecordCall(mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	state, msg := getObjectsForSyncCommitteesServiceTest()
	msg.ImmediateVerification = false
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	require.ErrorIs(t, s.ProcessMessage(t.Context(), new(uint64), msg), ErrInvalidBlsSignature)
}

// Waiting for the signature verdict holds no service or head-state lock, so a head-state publication is not blocked.
func TestSyncCommitteesGossipWaitsForVerificationWithoutHoldingLocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	entered, release := make(chan struct{}), make(chan struct{})
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		close(entered)
		<-release
		return true, nil
	}
	state, msg := getObjectsForSyncCommitteesServiceTest()
	msg.ImmediateVerification = false
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	done := make(chan error, 1)
	go func() { done <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	<-entered
	published := make(chan error, 1)
	go func() { published <- synced.OnHeadState(state) }()
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("head-state publication waited for a sync committee signature verification")
	}
	close(release)
	require.NoError(t, <-done)
}

// Of two different valid messages for one validator and slot verified at the same time, only the first is accepted.
func TestSyncCommitteesAcceptsOneOfConcurrentlyVerifiedMessagesForTheSameValidator(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	var entered sync.WaitGroup
	entered.Add(2)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		entered.Done()
		entered.Wait()
		return true, nil
	}
	state, msg := getObjectsForSyncCommitteesServiceTest()
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	other := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            msg.SyncCommitteeMessage.Slot,
			BeaconBlockRoot: common.Hash{0xff},
			ValidatorIndex:  msg.SyncCommitteeMessage.ValidatorIndex,
		},
		ImmediateVerification: true,
	}

	results := make(chan error, 2)
	for _, m := range []*SyncCommitteeMessageForGossip{msg, other} {
		go func() { results <- s.ProcessMessage(t.Context(), new(uint64), m) }()
	}
	var accepted, ignored int
	for range 2 {
		if err := <-results; err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, ErrIgnore)
			ignored++
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, ignored)
}
