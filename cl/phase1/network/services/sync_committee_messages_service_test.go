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
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
	syncpoolmock "github.com/erigontech/erigon/cl/validator/sync_contribution_pool/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func newSyncCommitteesServiceTest(t *testing.T, ctrl *gomock.Controller, ethClock eth_clock.EthereumClock) (SyncCommitteeMessagesService, *synced_data.SyncedDataManager) {
	t.Helper()
	syncContributionPool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	syncContributionPool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return newSyncCommitteesServiceWithPool(t, ethClock, syncContributionPool)
}

func newSyncCommitteesServiceWithPool(t *testing.T, ethClock eth_clock.EthereumClock, syncContributionPool sync_contribution_pool.SyncContributionPool) (SyncCommitteeMessagesService, *synced_data.SyncedDataManager) {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	batchSignatureVerifier := NewBatchSignatureVerifier(t.Context(), nil)
	go batchSignatureVerifier.Start()
	return NewSyncCommitteeMessagesService(cfg, ethClock, syncedDataManager, syncContributionPool, batchSignatureVerifier, true), syncedDataManager
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
	releaseVerification := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseVerification)
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
	releaseVerification()
	require.NoError(t, <-done)
}

// Of two different valid messages for one validator and slot verified at the same time, only the first is accepted
// and added to the pool, whether both come through the API or one of them comes from gossip.
func TestSyncCommitteesAcceptsOneOfConcurrentlyVerifiedMessagesForTheSameValidator(t *testing.T) {
	for _, tc := range []struct {
		name   string
		gossip bool
	}{
		{name: "api", gossip: false},
		{name: "gossip and api", gossip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			saveSignatureGlobals(t)
			var entered sync.WaitGroup
			entered.Add(2)
			blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
				entered.Done()
				entered.Wait()
				return true, nil
			}
			var poolAdds atomic.Int32
			pool := syncpoolmock.NewMockSyncContributionPool(ctrl)
			pool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(*state.CachingBeaconState, uint64, *cltypes.SyncCommitteeMessage) error {
					poolAdds.Add(1)
					return nil
				}).AnyTimes()
			ethClock := eth_clock.NewMockEthereumClock(ctrl)
			s, synced := newSyncCommitteesServiceWithPool(t, ethClock, pool)
			state, msg := getObjectsForSyncCommitteesServiceTest()
			msg.ImmediateVerification = !tc.gossip
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
			require.Equal(t, int32(1), poolAdds.Load())
		})
	}
}

// Adding a verified message to the pool does not hold up the gossip checks: a duplicate of it, which returns from
// checkMessage on the seen-map hit, is answered while the addition waits for the pool.
func TestSyncCommitteesPoolAddDoesNotBlockDuplicateCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) { return true, nil }
	entered, release := make(chan struct{}), make(chan struct{})
	releasePoolAdd := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releasePoolAdd)
	pool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	pool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(*state.CachingBeaconState, uint64, *cltypes.SyncCommitteeMessage) error {
			close(entered)
			<-release
			return nil
		}).Times(1)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	s, synced := newSyncCommitteesServiceWithPool(t, ethClock, pool)
	state, msg := getObjectsForSyncCommitteesServiceTest()
	require.NoError(t, synced.OnHeadState(state))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	done := make(chan error, 1)
	go func() { done <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("message not added to the pool: %v", err)
	}
	duplicate := make(chan error, 1)
	go func() { duplicate <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	select {
	case err := <-duplicate:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a sync committee message waited for the pool add of another one")
	}
	releasePoolAdd()
	require.NoError(t, <-done)
}

// One pool addition at a time holds a head-state view: an addition waiting for the pool must not hold a view, or a
// pending head-state publication, and every reader behind it, would wait for that addition too.
func TestSyncCommitteesPoolAddsHoldOneHeadStateViewAtATime(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	verified := make(chan struct{}, 2)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		verified <- struct{}{}
		return true, nil
	}
	entered := make(chan struct{}, 2)
	firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
	releaseFirst := sync.OnceFunc(func() { close(firstRelease) })
	releaseSecond := sync.OnceFunc(func() { close(secondRelease) })
	t.Cleanup(releaseFirst)
	t.Cleanup(releaseSecond)
	var adds atomic.Int32
	pool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	pool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(*state.CachingBeaconState, uint64, *cltypes.SyncCommitteeMessage) error {
			first := adds.Add(1) == 1
			entered <- struct{}{}
			if first {
				<-firstRelease
			} else {
				<-secondRelease
			}
			return nil
		}).Times(2)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	s, synced := newSyncCommitteesServiceWithPool(t, ethClock, pool)
	headState, msg := getObjectsForSyncCommitteesServiceTest()
	require.NoError(t, synced.OnHeadState(headState))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	other := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            msg.SyncCommitteeMessage.Slot,
			BeaconBlockRoot: msg.SyncCommitteeMessage.BeaconBlockRoot,
			ValidatorIndex:  otherValidatorOnSubnet(t, headState, msg.SyncCommitteeMessage.Slot, 0, msg.SyncCommitteeMessage.ValidatorIndex),
		},
		ImmediateVerification: true,
	}
	await := func(ch <-chan struct{}, done <-chan error, what string) {
		t.Helper()
		select {
		case <-ch:
		case err := <-done:
			t.Fatalf("%s: message processing ended early: %v", what, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: timed out", what)
		}
	}

	done := make(chan error, 2)
	go func() { done <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	await(verified, done, "first message verified")
	await(entered, done, "first addition in the pool")
	go func() { done <- s.ProcessMessage(t.Context(), new(uint64), other) }()
	await(verified, done, "second message verified")
	waitForBlockedGoroutine(t, "sync.Mutex.Lock", "(*syncCommitteeMessagesService).storeVerifiedMessage")

	published := make(chan error, 1)
	go func() { published <- synced.OnHeadState(headState) }()
	// A writer reported in this state has already turned away new readers and waits for the held view.
	waitForBlockedGoroutine(t, "sync.RWMutex.Lock", "(*SyncedDataManager).publishHeadStateLocked")
	releaseFirst()
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("head-state publication waited for a pool addition that was still waiting for the pool")
	}
	releaseSecond()
	require.NoError(t, <-done)
	require.NoError(t, <-done)
}

// waitForBlockedGoroutine waits until a goroutine that has frame in its stack is blocked in state, as runtime.Stack
// reports it.
func waitForBlockedGoroutine(t *testing.T, state, frame string) {
	t.Helper()
	buf := make([]byte, 1<<22)
	require.Eventually(t, func() bool {
		n := runtime.Stack(buf, true)
		for goroutine := range strings.SplitSeq(string(buf[:n]), "\n\n") {
			if strings.Contains(goroutine, "["+state) && strings.Contains(goroutine, frame) {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "no goroutine blocked in %s at %s", state, frame)
}

func otherValidatorOnSubnet(t *testing.T, s *state.CachingBeaconState, slot, subnet uint64, except ...uint64) uint64 {
	t.Helper()
	for index := range uint64(s.ValidatorLength()) {
		if slices.Contains(except, index) {
			continue
		}
		validatorSubnets, err := subnets.ComputeSubnetsForSyncCommittee(s, slot, index)
		require.NoError(t, err)
		if slices.Contains(validatorSubnets, subnet) {
			return index
		}
	}
	t.Fatalf("no other validator on sync subnet %d", subnet)
	return 0
}

// Entries go stale only when the head slot advances, so the seen map is cleaned once per head slot, and again after
// the head moves.
func TestSyncCommitteesCleansStaleMessagesOncePerHeadSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) { return true, nil }
	headState, msg := getObjectsForSyncCommitteesServiceTest()
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	require.NoError(t, synced.OnHeadState(headState))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	seen := &s.(*syncCommitteeMessagesService).seenSyncCommitteeMessages
	staleKey := func(validatorIndex uint64) seenSyncCommitteeMessage {
		return seenSyncCommitteeMessage{slot: headState.Slot() - 2, validatorIndex: validatorIndex}
	}
	seen.Store(staleKey(1), verifiedSyncCommitteeMessage{})

	require.NoError(t, s.ProcessMessage(t.Context(), new(uint64), msg))
	_, kept := seen.Load(staleKey(1))
	require.False(t, kept, "a stale entry outlived the first cleanup at this head slot")

	seen.Store(staleKey(2), verifiedSyncCommitteeMessage{})
	other := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            msg.SyncCommitteeMessage.Slot,
			BeaconBlockRoot: msg.SyncCommitteeMessage.BeaconBlockRoot,
			ValidatorIndex:  otherValidatorOnSubnet(t, headState, msg.SyncCommitteeMessage.Slot, 0, msg.SyncCommitteeMessage.ValidatorIndex),
		},
		ImmediateVerification: true,
	}
	require.NoError(t, s.ProcessMessage(t.Context(), new(uint64), other))
	_, kept = seen.Load(staleKey(2))
	require.True(t, kept, "the seen map was scanned again at the same head slot")

	nextHead, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, nextHead.SetSlot(headState.Slot()+2))
	require.NoError(t, synced.OnHeadState(nextHead))
	staleForNextHead := seenSyncCommitteeMessage{slot: nextHead.Slot() - 2, validatorIndex: 3}
	seen.Store(staleForNextHead, verifiedSyncCommitteeMessage{})
	third := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            nextHead.Slot(),
			BeaconBlockRoot: msg.SyncCommitteeMessage.BeaconBlockRoot,
			ValidatorIndex: otherValidatorOnSubnet(t, nextHead, nextHead.Slot(), 0,
				msg.SyncCommitteeMessage.ValidatorIndex, other.SyncCommitteeMessage.ValidatorIndex),
		},
		ImmediateVerification: true,
	}
	require.NoError(t, s.ProcessMessage(t.Context(), new(uint64), third))
	_, kept = seen.Load(staleForNextHead)
	require.False(t, kept, "a stale entry outlived the first cleanup after the head moved")
	_, kept = seen.Load(staleKey(2))
	require.False(t, kept, "an entry stale since the earlier head outlived the cleanup after the head moved")
}

// A message whose verification ends after the head has moved two slots past it is outside the slot window the gossip
// checks admitted it for. It is ignored, not recorded and not added to the pool: a recorded stale entry would outlive
// this head slot's cleanup, and a record removed by that cleanup would let another message from the validator in.
func TestSyncCommitteesIgnoresMessageVerifiedAfterTheHeadMovedPastIt(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	entered, release := make(chan struct{}), make(chan struct{})
	releaseVerification := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseVerification)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		close(entered)
		<-release
		return true, nil
	}
	var poolAdds atomic.Int32
	pool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	pool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(*state.CachingBeaconState, uint64, *cltypes.SyncCommitteeMessage) error {
			poolAdds.Add(1)
			return nil
		}).AnyTimes()
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	s, synced := newSyncCommitteesServiceWithPool(t, ethClock, pool)
	headState, msg := getObjectsForSyncCommitteesServiceTest()
	require.NoError(t, synced.OnHeadState(headState))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	done := make(chan error, 1)
	go func() { done <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("message processing ended before verification: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("verification did not start")
	}
	nextHead, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, nextHead.SetSlot(msg.SyncCommitteeMessage.Slot+2))
	require.NoError(t, synced.OnHeadState(nextHead))
	releaseVerification()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrIgnore)
	case <-time.After(5 * time.Second):
		t.Fatal("message processing did not finish")
	}
	require.Zero(t, poolAdds.Load())
	key := seenSyncCommitteeMessage{slot: msg.SyncCommitteeMessage.Slot, validatorIndex: msg.SyncCommitteeMessage.ValidatorIndex}
	_, recorded := s.(*syncCommitteeMessagesService).seenSyncCommitteeMessages.Load(key)
	require.False(t, recorded)
}

// Of two identical API submissions verified at the same time, the one that finishes after the first was published is
// ignored, as checkMessage ignores a published duplicate, so the message is not queued for publishing twice.
func TestSyncCommitteesIgnoresDuplicateVerifiedAfterTheFirstWasPublished(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	entered := make(chan chan struct{}, 2)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		release := make(chan struct{})
		entered <- release
		<-release
		return true, nil
	}
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	headState, msg := getObjectsForSyncCommitteesServiceTest()
	require.NoError(t, synced.OnHeadState(headState))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	duplicate := *msg

	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	releaseFirst := <-entered
	go func() { second <- s.ProcessMessage(t.Context(), new(uint64), &duplicate) }()
	releaseSecond := <-entered
	close(releaseFirst)
	require.NoError(t, <-first)
	m := msg.SyncCommitteeMessage
	s.MarkPublished(0, m.Slot, m.ValidatorIndex, m.BeaconBlockRoot, m.Signature)
	close(releaseSecond)

	require.ErrorIs(t, <-second, ErrIgnore)
}

// A message that goes stale while it waits for the pool, because the head moved two slots past it, is not added to the
// pool and not forwarded.
func TestSyncCommitteesMessageStaleAfterWaitingForThePoolIsIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) { return true, nil }
	entered, release := make(chan struct{}, 2), make(chan struct{})
	releasePool := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releasePool)
	var poolAdds atomic.Int32
	pool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	pool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(*state.CachingBeaconState, uint64, *cltypes.SyncCommitteeMessage) error {
			poolAdds.Add(1)
			entered <- struct{}{}
			<-release
			return nil
		}).AnyTimes()
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	s, synced := newSyncCommitteesServiceWithPool(t, ethClock, pool)
	headState, msg := getObjectsForSyncCommitteesServiceTest()
	require.NoError(t, synced.OnHeadState(headState))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	other := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            msg.SyncCommitteeMessage.Slot,
			BeaconBlockRoot: msg.SyncCommitteeMessage.BeaconBlockRoot,
			ValidatorIndex:  otherValidatorOnSubnet(t, headState, msg.SyncCommitteeMessage.Slot, 0, msg.SyncCommitteeMessage.ValidatorIndex),
		},
		ImmediateVerification: true,
	}

	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	<-entered
	go func() { second <- s.ProcessMessage(t.Context(), new(uint64), other) }()
	waitForBlockedGoroutine(t, "sync.Mutex.Lock", "(*syncCommitteeMessagesService).storeVerifiedMessage")
	nextHead, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, nextHead.SetSlot(msg.SyncCommitteeMessage.Slot+2))
	published := make(chan error, 1)
	go func() { published <- synced.OnHeadState(nextHead) }()
	waitForBlockedGoroutine(t, "sync.RWMutex.Lock", "(*SyncedDataManager).publishHeadStateLocked")
	releasePool()

	require.NoError(t, <-first)
	require.NoError(t, <-published)
	select {
	case err := <-second:
		require.ErrorIs(t, err, ErrIgnore)
	case <-time.After(5 * time.Second):
		t.Fatal("the second message did not finish")
	}
	require.Equal(t, int32(1), poolAdds.Load(), "a stale message was added to the pool")
}

// A slot whose records were evicted stays evicted when the head rolls back: otherwise a different message from a
// validator whose record was removed could claim the freed key and be accepted as well.
func TestSyncCommitteesEvictedSlotStaysEvictedAfterHeadRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) { return true, nil }
	s, synced, ethClock := setupSyncCommitteesServiceTest(t, ctrl)
	headState, msg := getObjectsForSyncCommitteesServiceTest()
	require.NoError(t, synced.OnHeadState(headState))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	slot := msg.SyncCommitteeMessage.Slot
	require.NoError(t, s.ProcessMessage(t.Context(), new(uint64), msg))

	headAt := func(headSlot uint64) {
		t.Helper()
		head, err := headState.Copy()
		require.NoError(t, err)
		require.NoError(t, head.SetSlot(headSlot))
		require.NoError(t, synced.OnHeadState(head))
	}
	// The head moves two slots ahead and another message runs the cleanup, which evicts the first record.
	headAt(slot + 2)
	evicting := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            slot + 2,
			BeaconBlockRoot: msg.SyncCommitteeMessage.BeaconBlockRoot,
			ValidatorIndex:  otherValidatorOnSubnet(t, headState, slot, 0, msg.SyncCommitteeMessage.ValidatorIndex),
		},
		ImmediateVerification: true,
	}
	require.NoError(t, s.ProcessMessage(t.Context(), new(uint64), evicting))
	key := seenSyncCommitteeMessage{slot: slot, validatorIndex: msg.SyncCommitteeMessage.ValidatorIndex}
	_, kept := s.(*syncCommitteeMessagesService).seenSyncCommitteeMessages.Load(key)
	require.False(t, kept, "the first record was not evicted")

	// The head rolls back one slot, where the evicted slot would count as fresh again, and a message from a third
	// validator runs the cleanup at that head.
	headAt(slot + 1)
	third := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            slot + 1,
			BeaconBlockRoot: msg.SyncCommitteeMessage.BeaconBlockRoot,
			ValidatorIndex: otherValidatorOnSubnet(t, headState, slot, 0,
				msg.SyncCommitteeMessage.ValidatorIndex, evicting.SyncCommitteeMessage.ValidatorIndex),
		},
		ImmediateVerification: true,
	}
	require.NoError(t, s.ProcessMessage(t.Context(), new(uint64), third))
	other := &SyncCommitteeMessageForGossip{
		SyncCommitteeMessage: &cltypes.SyncCommitteeMessage{
			Slot:            slot,
			BeaconBlockRoot: common.Hash{0xff},
			ValidatorIndex:  msg.SyncCommitteeMessage.ValidatorIndex,
		},
		ImmediateVerification: true,
	}
	require.ErrorIs(t, s.ProcessMessage(t.Context(), new(uint64), other), ErrIgnore)
	_, recorded := s.(*syncCommitteeMessagesService).seenSyncCommitteeMessages.Load(key)
	require.False(t, recorded, "a different message claimed an evicted slot after the head rolled back")
}

// While the first copy of a message waits in its pool add, an identical submission is answered from the seen map and
// published. The first copy then reports the message as already published instead of queuing it for publishing again.
func TestSyncCommitteesClaimantIgnoresMessagePublishedWhileItWaitedForThePool(t *testing.T) {
	ctrl := gomock.NewController(t)
	saveSignatureGlobals(t)
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) { return true, nil }
	entered, release := make(chan struct{}, 1), make(chan struct{})
	releasePool := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releasePool)
	pool := syncpoolmock.NewMockSyncContributionPool(ctrl)
	pool.EXPECT().AddSyncCommitteeMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(*state.CachingBeaconState, uint64, *cltypes.SyncCommitteeMessage) error {
			entered <- struct{}{}
			<-release
			return nil
		}).Times(1)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	s, synced := newSyncCommitteesServiceWithPool(t, ethClock, pool)
	headState, msg := getObjectsForSyncCommitteesServiceTest()
	require.NoError(t, synced.OnHeadState(headState))
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	claimant := make(chan error, 1)
	go func() { claimant <- s.ProcessMessage(t.Context(), new(uint64), msg) }()
	select {
	case <-entered:
	case err := <-claimant:
		t.Fatalf("the first copy finished before its pool add: %v", err)
	}
	duplicate := *msg
	require.NoError(t, s.ProcessMessage(t.Context(), new(uint64), &duplicate), "the identical submission is not answered from the seen map")
	m := msg.SyncCommitteeMessage
	s.MarkPublished(0, m.Slot, m.ValidatorIndex, m.BeaconBlockRoot, m.Signature)
	releasePool()

	select {
	case err := <-claimant:
		require.ErrorIs(t, err, ErrIgnore)
	case <-time.After(5 * time.Second):
		t.Fatal("the first copy did not finish")
	}
}
