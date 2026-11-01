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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

type payloadAttestationValidationContextResult struct {
	validationContext *payloadAttestationValidationContext
	err               error
}

type payloadAttestationObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func newPayloadAttestationObservedContext(parent context.Context) *payloadAttestationObservedContext {
	return &payloadAttestationObservedContext{Context: parent, observed: make(chan struct{})}
}

func (c *payloadAttestationObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func waitForPayloadAttestationWaiter(t *testing.T, ctx *payloadAttestationObservedContext) {
	t.Helper()
	select {
	case <-ctx.observed:
	case <-time.After(time.Second):
		t.Fatal("payload attestation validation call did not reach its wait point")
	}
}

func TestOnPayloadAttestationMessageRejectsNil(t *testing.T) {
	f := &ForkChoiceStore{}
	require.Error(t, f.OnPayloadAttestationMessage(context.Background(), nil))
	require.Error(t, f.OnPayloadAttestationMessage(context.Background(), &cltypes.PayloadAttestationMessage{}))
}

func TestOnPayloadAttestationMessageIgnoresUnavailableKnownBlockState(t *testing.T) {
	root := common.HexToHash("0x1234")
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)
	graph := &getFinalizedExecutionHashForkGraph{
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{root: {Slot: 100}},
		states:  map[common.Hash]*state.CachingBeaconState{},
	}
	f := &ForkChoiceStore{forkGraph: graph, payloadAttestationContexts: contexts}
	msg := &cltypes.PayloadAttestationMessage{Data: &cltypes.PayloadAttestationData{Slot: 100, BeaconBlockRoot: root}}

	err = f.OnPayloadAttestationMessage(context.Background(), msg)

	require.ErrorIs(t, err, ErrIgnore)
}

func TestCurrentSlotPayloadVotesInvalidateHeadAtSlotBoundary(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	f.headHash = gossipTestBlockRoot
	f.headPayloadStatus = cltypes.PayloadStatusFull

	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil))

	require.Equal(t, gossipTestBlockRoot, f.headHash, "current-slot PTC votes do not affect the current head tiebreaker")
	require.Equal(t, cltypes.PayloadStatusFull, f.headPayloadStatus)
	timeliness, dataAvailability, timelinessPresent, dataAvailabilityPresent := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.True(t, timelinessPresent)
	require.True(t, dataAvailabilityPresent)
	require.Equal(t, int8(1), timeliness[gossipTestPTCPosition])
	require.Equal(t, int8(-1), dataAvailability[gossipTestPTCPosition])

	f.OnTick((gossipTestSlot + 1) * f.beaconCfg.SecondsPerSlot * 1000)

	require.Equal(t, common.Hash{}, f.headHash, "the next slot must resolve its head using the recorded PTC votes")
	require.Equal(t, cltypes.PayloadStatusPending, f.headPayloadStatus)
	timeliness, dataAvailability, timelinessPresent, dataAvailabilityPresent = f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.True(t, timelinessPresent)
	require.True(t, dataAvailabilityPresent)
	require.Equal(t, int8(1), timeliness[gossipTestPTCPosition])
	require.Equal(t, int8(-1), dataAvailability[gossipTestPTCPosition])
}

func TestOnGossipPayloadAttestationMessageKeepsFirstValidForCurrentAndPreviousSlot(t *testing.T) {
	f, signCurrent := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), signCurrent(true), nil))

	f.timeMs.Store((gossipTestSlot + 2) * f.beaconCfg.SecondsPerSlot * 1000)
	signPrevious := addGossipPayloadAttestationSigner(t, f, common.Hash{0x01}, gossipTestSlot+1, gossipTestValidator, gossipTestPTCPosition+1, false)
	err := f.OnGossipPayloadAttestationMessage(context.Background(), signPrevious(true), nil)
	require.ErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
	require.NotContains(t, f.payloadAttestationSeen, payloadAttestationSeenKey{slot: gossipTestSlot, validatorIndex: gossipTestValidator})

	signOld := addGossipPayloadAttestationSigner(t, f, common.Hash{0x02}, gossipTestSlot, gossipTestValidator+1, gossipTestPTCPosition+2, false)
	err = f.OnGossipPayloadAttestationMessage(context.Background(), signOld(true), nil)
	require.ErrorIs(t, err, ErrIgnore)
	require.NotErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)

	signCurrent = addGossipPayloadAttestationSigner(t, f, common.Hash{0x03}, gossipTestSlot+2, gossipTestValidator, gossipTestPTCPosition+3, false)
	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), signCurrent(true), nil))
	require.Len(t, f.payloadAttestationSeen, 2)
	require.Contains(t, f.payloadAttestationSeen, payloadAttestationSeenKey{slot: gossipTestSlot + 1, validatorIndex: gossipTestValidator})
	require.Contains(t, f.payloadAttestationSeen, payloadAttestationSeenKey{slot: gossipTestSlot + 2, validatorIndex: gossipTestValidator})
}

func TestOnGossipPayloadAttestationMessageIgnoresSlotsBeforePreviousStoreSlot(t *testing.T) {
	f, _ := newGossipPayloadAttestationTestStore(t, gossipTestSlot+2)
	sign101 := addGossipPayloadAttestationSigner(t, f, common.Hash{0x01}, gossipTestSlot+1, 43, 8, false)
	sign100 := addGossipPayloadAttestationSigner(t, f, common.Hash{0x02}, gossipTestSlot, 44, 9, false)
	sign99 := addGossipPayloadAttestationSigner(t, f, common.Hash{0x03}, gossipTestSlot-1, 45, 10, false)

	for _, sign := range []func(bool) *cltypes.PayloadAttestationMessage{sign100, sign99} {
		err := f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil)
		require.ErrorIs(t, err, ErrIgnore)
		require.NotErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
	}
	require.Empty(t, f.payloadAttestationSeen)
	require.ErrorIs(t, f.OnGossipPayloadAttestationMessage(context.Background(), sign101(true), nil), ErrPayloadAttestationNotCurrentSlot)
	require.Len(t, f.payloadAttestationSeen, 1)
}

func TestPayloadAttestationValidationContextsCollapseConcurrentBuilds(t *testing.T) {
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)

	root := common.HexToHash("0x1234")
	expected := &payloadAttestationValidationContext{slot: 100}
	started := make(chan struct{})
	release := make(chan struct{})
	var builds atomic.Int32

	build := func() (*payloadAttestationValidationContext, error) {
		if builds.Add(1) == 1 {
			close(started)
		}
		<-release
		return expected, nil
	}

	results := make(chan payloadAttestationValidationContextResult, 16)
	go func() {
		validationContext, getErr := contexts.get(context.Background(), root, build)
		results <- payloadAttestationValidationContextResult{validationContext, getErr}
	}()
	<-started
	for range 15 {
		waiterCtx := newPayloadAttestationObservedContext(context.Background())
		go func() {
			validationContext, getErr := contexts.get(waiterCtx, root, build)
			results <- payloadAttestationValidationContextResult{validationContext, getErr}
		}()
		waitForPayloadAttestationWaiter(t, waiterCtx)
	}
	require.Equal(t, int32(1), builds.Load())
	close(release)
	for range 16 {
		result := <-results
		require.NoError(t, result.err)
		require.Same(t, expected, result.validationContext)
	}
	_, err = contexts.get(context.Background(), root, build)
	require.NoError(t, err)
	require.Equal(t, int32(1), builds.Load())
}

func TestPayloadAttestationValidationContextsCanceledWaiterDoesNotCancelBuild(t *testing.T) {
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)
	root := common.HexToHash("0x1234")
	started := make(chan struct{})
	release := make(chan struct{})
	expected := &payloadAttestationValidationContext{slot: 100}
	leader := make(chan payloadAttestationValidationContextResult, 1)
	go func() {
		value, getErr := contexts.get(context.Background(), root, func() (*payloadAttestationValidationContext, error) {
			close(started)
			<-release
			return expected, nil
		})
		leader <- payloadAttestationValidationContextResult{value, getErr}
	}()
	<-started
	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = contexts.get(waiterCtx, root, func() (*payloadAttestationValidationContext, error) { return nil, nil })
	require.ErrorIs(t, err, context.Canceled)
	close(release)
	result := <-leader
	require.NoError(t, result.err)
	require.Same(t, expected, result.validationContext)
}

func TestPayloadAttestationValidationContextsCanceledLeaderDoesNotPoisonWaiter(t *testing.T) {
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)
	contexts.buildAdmission <- struct{}{}
	root := common.HexToHash("0x1234")
	leaderParentCtx, cancelLeader := context.WithCancel(context.Background())
	leaderCtx := newPayloadAttestationObservedContext(leaderParentCtx)
	leaderResult := make(chan error, 1)
	go func() {
		_, getErr := contexts.get(leaderCtx, root, func() (*payloadAttestationValidationContext, error) {
			return nil, errors.New("leader build must not run")
		})
		leaderResult <- getErr
	}()
	waitForPayloadAttestationWaiter(t, leaderCtx)

	expected := &payloadAttestationValidationContext{slot: 100}
	waiterResult := make(chan payloadAttestationValidationContextResult, 1)
	waiterCtx := newPayloadAttestationObservedContext(context.Background())
	go func() {
		value, getErr := contexts.get(waiterCtx, root, func() (*payloadAttestationValidationContext, error) { return expected, nil })
		waiterResult <- payloadAttestationValidationContextResult{value, getErr}
	}()
	waitForPayloadAttestationWaiter(t, waiterCtx)
	cancelLeader()
	require.ErrorIs(t, <-leaderResult, context.Canceled)
	<-contexts.buildAdmission
	result := <-waiterResult
	require.NoError(t, result.err)
	require.Same(t, expected, result.validationContext)
}

func TestPayloadAttestationValidationContextsPanicCleansInflightBuild(t *testing.T) {
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)
	root := common.HexToHash("0x1234")
	func() {
		defer func() { require.Equal(t, "boom", recover()) }()
		_, _ = contexts.get(context.Background(), root, func() (*payloadAttestationValidationContext, error) { panic("boom") })
	}()
	expected := &payloadAttestationValidationContext{slot: 100}
	actual, err := contexts.get(context.Background(), root, func() (*payloadAttestationValidationContext, error) { return expected, nil })
	require.NoError(t, err)
	require.Same(t, expected, actual)
}

func TestPayloadAttestationValidationContextsDoNotCacheBuildErrors(t *testing.T) {
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)

	root := common.HexToHash("0x1234")
	var builds atomic.Int32
	_, err = contexts.get(context.Background(), root, func() (*payloadAttestationValidationContext, error) {
		builds.Add(1)
		return nil, errors.New("state unavailable")
	})
	require.ErrorContains(t, err, "state unavailable")

	expected := &payloadAttestationValidationContext{slot: 100}
	actual, err := contexts.get(context.Background(), root, func() (*payloadAttestationValidationContext, error) {
		builds.Add(1)
		return expected, nil
	})
	require.NoError(t, err)
	require.Same(t, expected, actual)
	require.Equal(t, int32(2), builds.Load())
}

func TestPayloadAttestationValidationContextsBoundDifferentRootBuilds(t *testing.T) {
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)

	var active atomic.Int32
	var maxActive atomic.Int32
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	results := make(chan error, 3)
	for i := range 3 {
		go func() {
			_, getErr := contexts.get(context.Background(), common.Hash{byte(i + 1)}, func() (*payloadAttestationValidationContext, error) {
				current := active.Add(1)
				defer active.Add(-1)
				for {
					maximum := maxActive.Load()
					if current <= maximum || maxActive.CompareAndSwap(maximum, current) {
						break
					}
				}
				started <- struct{}{}
				<-release
				return &payloadAttestationValidationContext{}, nil
			})
			results <- getErr
		}()
	}

	for range maxConcurrentValidationContextBuilds {
		select {
		case <-started:
		case <-time.After(time.Second):
			require.FailNow(t, "validation context build did not start")
		}
	}
	select {
	case <-started:
		require.FailNow(t, "too many validation contexts built concurrently")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	for range 3 {
		require.NoError(t, <-results)
	}
	require.Equal(t, int32(maxConcurrentValidationContextBuilds), maxActive.Load())
}

func TestPayloadAttestationValidationContextsBoundPendingRoots(t *testing.T) {
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)
	contexts.buildAdmission <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, maxPendingValidationContextBuilds+1)
	for i := range maxPendingValidationContextBuilds + 1 {
		go func() {
			_, getErr := contexts.get(ctx, common.Hash{byte(i), byte(i >> 8)}, func() (*payloadAttestationValidationContext, error) {
				return &payloadAttestationValidationContext{}, nil
			})
			results <- getErr
		}()
	}

	require.Eventually(t, func() bool {
		contexts.mu.Lock()
		defer contexts.mu.Unlock()
		return len(contexts.builds) == maxPendingValidationContextBuilds
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, <-results, ErrIgnore)
	cancel()
	<-contexts.buildAdmission
	for range maxPendingValidationContextBuilds {
		<-results
	}
}

func TestPayloadAttestationValidationContextPositions(t *testing.T) {
	validationContext := &payloadAttestationValidationContext{
		slot:      100,
		positions: map[uint64][]int{42: {1, 7}},
	}
	msg := &cltypes.PayloadAttestationMessage{
		ValidatorIndex: 42,
		Data:           &cltypes.PayloadAttestationData{Slot: 100},
	}

	positions, err := validationContext.ptcPositions(msg)
	require.NoError(t, err)
	require.Equal(t, []int{1, 7}, positions)

	msg.Data.Slot = 99
	_, err = validationContext.ptcPositions(msg)
	require.ErrorIs(t, err, ErrIgnore)
}

func TestPayloadAttestationValidationContextSignature(t *testing.T) {
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)

	data := &cltypes.PayloadAttestationData{
		BeaconBlockRoot:   common.HexToHash("0x1234"),
		Slot:              100,
		PayloadPresent:    true,
		BlobDataAvailable: true,
	}
	domain := common.HexToHash("0xabcd")
	signingRoot, err := fork.ComputeSigningRoot(data, domain[:])
	require.NoError(t, err)

	var publicKey common.Bytes48
	copy(publicKey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	msg := &cltypes.PayloadAttestationMessage{
		ValidatorIndex: 42,
		Data:           data,
	}
	copy(msg.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())
	validationContext := &payloadAttestationValidationContext{
		slot:       100,
		domain:     domain,
		positions:  map[uint64][]int{42: {1}},
		publicKeys: map[uint64]common.Bytes48{42: publicKey},
	}

	require.NoError(t, validationContext.validateSignature(context.Background(), msg, nil))
	msg.Data.PayloadPresent = false
	require.ErrorContains(t, validationContext.validateSignature(context.Background(), msg, nil), "invalid payload attestation signature")
}

const (
	gossipTestSlot        = uint64(100)
	gossipTestValidator   = uint64(42)
	gossipTestPTCPosition = 7
)

var gossipTestBlockRoot = common.HexToHash("0x1234")

func newGossipPayloadAttestationTestStore(t *testing.T, storeSlot uint64) (*ForkChoiceStore, func(payloadPresent bool) *cltypes.PayloadAttestationMessage) {
	t.Helper()
	contexts, err := newPayloadAttestationValidationContexts()
	require.NoError(t, err)
	f := &ForkChoiceStore{beaconCfg: &clparams.MainnetBeaconConfig, payloadAttestationContexts: contexts}
	f.timeMs.Store(storeSlot * f.beaconCfg.SecondsPerSlot * 1000)
	sign := addGossipPayloadAttestationSigner(t, f, gossipTestBlockRoot, gossipTestSlot, gossipTestValidator, gossipTestPTCPosition, false)
	return f, sign
}

func addGossipPayloadAttestationSigner(
	t *testing.T,
	f *ForkChoiceStore,
	root common.Hash,
	slot uint64,
	validatorIndex uint64,
	ptcPosition int,
	blobDataAvailable bool,
) func(payloadPresent bool) *cltypes.PayloadAttestationMessage {
	t.Helper()
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	var publicKey common.Bytes48
	copy(publicKey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	domain := common.HexToHash("0xabcd")
	f.payloadAttestationContexts.cache.Add(root, &payloadAttestationValidationContext{
		slot:       slot,
		domain:     domain,
		positions:  map[uint64][]int{validatorIndex: {ptcPosition}},
		publicKeys: map[uint64]common.Bytes48{validatorIndex: publicKey},
	})
	if _, ok := f.payloadTimelinessVote.Load(root); !ok {
		f.payloadTimelinessVote.Store(root, [clparams.PtcSize]int8{})
	}
	if _, ok := f.payloadDataAvailabilityVote.Load(root); !ok {
		f.payloadDataAvailabilityVote.Store(root, [clparams.PtcSize]int8{})
	}
	sign := func(payloadPresent bool) *cltypes.PayloadAttestationMessage {
		data := &cltypes.PayloadAttestationData{
			BeaconBlockRoot:   root,
			Slot:              slot,
			PayloadPresent:    payloadPresent,
			BlobDataAvailable: blobDataAvailable,
		}
		signingRoot, err := fork.ComputeSigningRoot(data, domain[:])
		require.NoError(t, err)
		msg := &cltypes.PayloadAttestationMessage{ValidatorIndex: validatorIndex, Data: data}
		copy(msg.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())
		return msg
	}
	return sign
}

func TestOnGossipPayloadAttestationMessageUsesInjectedSignatureVerifier(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	msg := sign(true)
	validationContext, ok := f.payloadAttestationContexts.cache.Get(gossipTestBlockRoot)
	require.True(t, ok)
	expectedRoot, err := fork.ComputeSigningRoot(msg.Data, validationContext.domain[:])
	require.NoError(t, err)
	expectedPublicKey := validationContext.publicKeys[msg.ValidatorIndex]
	called := false

	err = f.OnGossipPayloadAttestationMessage(context.Background(), msg, func(_ context.Context, signature, signingRoot, publicKey []byte) error {
		called = true
		require.Equal(t, msg.Signature[:], signature)
		require.Equal(t, expectedRoot[:], signingRoot)
		require.Equal(t, expectedPublicKey[:], publicKey)
		return nil
	})

	require.NoError(t, err)
	require.True(t, called)
	timeliness, _, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, int8(1), timeliness[gossipTestPTCPosition])
}

func TestOnGossipPayloadAttestationMessageInjectedVerifierErrorRejects(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	msg := sign(true)
	verificationErr := errors.New("batch verification failed")

	err := f.OnGossipPayloadAttestationMessage(context.Background(), msg, func(context.Context, []byte, []byte, []byte) error {
		return verificationErr
	})

	require.ErrorIs(t, err, verificationErr)
	require.NotErrorIs(t, err, ErrIgnore)
	require.NotContains(t, strings.ToLower(err.Error()), "ignore")
	timeliness, dataAvailability, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, [clparams.PtcSize]int8{}, timeliness)
	require.Equal(t, [clparams.PtcSize]int8{}, dataAvailability)
	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), msg, func(context.Context, []byte, []byte, []byte) error {
		return nil
	}))
}

func TestOnGossipPayloadAttestationMessageCountsOnlyFirstCurrentSlotMessage(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)

	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil))
	err := f.OnGossipPayloadAttestationMessage(context.Background(), sign(false), nil)

	require.ErrorIs(t, err, ErrIgnore)
	require.NotErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
	timeliness, _, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, int8(1), timeliness[gossipTestPTCPosition])
}

func TestOnGossipPayloadAttestationMessageDoesNotCountLateMessage(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot+1)

	err := f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil)

	require.ErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
	require.NotErrorIs(t, err, ErrIgnore)
	timeliness, dataAvailability, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, [clparams.PtcSize]int8{}, timeliness)
	require.Equal(t, [clparams.PtcSize]int8{}, dataAvailability)
}

func TestOnGossipPayloadAttestationMessageDoesNotCountVoteAppliedAfterSlotTick(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	f.mu.RLock()
	releaseHeadView := sync.OnceFunc(f.mu.RUnlock)
	t.Cleanup(releaseHeadView)
	result := make(chan error, 1)
	go func() { result <- f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil) }()
	require.Eventually(t, func() bool {
		if !f.mu.TryRLock() {
			return true
		}
		f.mu.RUnlock()
		return false
	}, 5*time.Second, time.Millisecond, "payload attestation did not reach the store lock")

	f.timeMs.Store((gossipTestSlot + 1) * f.beaconCfg.SecondsPerSlot * 1000)
	releaseHeadView()

	require.ErrorIs(t, <-result, ErrPayloadAttestationNotCurrentSlot)
	timeliness, _, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, [clparams.PtcSize]int8{}, timeliness)
}

func TestOnGossipPayloadAttestationMessageCurrentSlotConflictingMessagesHaveOneWinner(t *testing.T) {
	type result struct {
		err            error
		payloadPresent bool
	}

	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	messages := make([]*cltypes.PayloadAttestationMessage, 16)
	for i := range messages {
		messages[i] = sign(i%2 == 0)
	}
	start := make(chan struct{})
	results := make(chan result, len(messages))
	for _, msg := range messages {
		go func() {
			<-start
			results <- result{
				err:            f.OnGossipPayloadAttestationMessage(context.Background(), msg, nil),
				payloadPresent: msg.Data.PayloadPresent,
			}
		}()
	}

	close(start)
	winners := 0
	var winnerVote int8
	for range messages {
		result := <-results
		if result.err == nil {
			winners++
			winnerVote = boolToVote(result.payloadPresent)
			continue
		}
		require.ErrorIs(t, result.err, ErrIgnore)
		require.NotErrorIs(t, result.err, ErrPayloadAttestationNotCurrentSlot)
	}

	require.Equal(t, 1, winners)
	timeliness, dataAvailability, timelinessPresent, dataAvailabilityPresent := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.True(t, timelinessPresent)
	require.True(t, dataAvailabilityPresent)
	require.Equal(t, winnerVote, timeliness[gossipTestPTCPosition])
	require.Equal(t, int8(-1), dataAvailability[gossipTestPTCPosition])
}

func TestOnGossipPayloadAttestationMessageLateConflictingMessagesHaveOneWinner(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot+1)
	messages := make([]*cltypes.PayloadAttestationMessage, 16)
	for i := range messages {
		messages[i] = sign(i%2 == 0)
	}
	start := make(chan struct{})
	results := make(chan error, len(messages))
	for _, msg := range messages {
		go func() {
			<-start
			results <- f.OnGossipPayloadAttestationMessage(context.Background(), msg, nil)
		}()
	}

	close(start)
	winners := 0
	for range messages {
		err := <-results
		if errors.Is(err, ErrPayloadAttestationNotCurrentSlot) {
			winners++
			require.NotErrorIs(t, err, ErrIgnore)
			continue
		}
		require.ErrorIs(t, err, ErrIgnore)
	}

	require.Equal(t, 1, winners)
}

func TestOnGossipPayloadAttestationMessageIgnoresLateConflictAfterCurrentSlotVote(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil))
	f.timeMs.Store((gossipTestSlot + 1) * f.beaconCfg.SecondsPerSlot * 1000)

	err := f.OnGossipPayloadAttestationMessage(context.Background(), sign(false), nil)

	require.ErrorIs(t, err, ErrIgnore)
	require.NotErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
	timeliness, _, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, int8(1), timeliness[gossipTestPTCPosition])
}

func TestOnGossipPayloadAttestationMessageAcceptsLateMessageAfterNewerSlotVote(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot+1)
	newerSlotSign := addGossipPayloadAttestationSigner(
		t,
		f,
		common.HexToHash("0x5678"),
		gossipTestSlot+1,
		gossipTestValidator+1,
		gossipTestPTCPosition+1,
		false,
	)
	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), newerSlotSign(true), nil))

	err := f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil)

	require.ErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
}

func TestOnGossipPayloadAttestationMessageRejectsLateInvalidSignature(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot+1)
	msg := sign(true)
	msg.Data.PayloadPresent = false

	err := f.OnGossipPayloadAttestationMessage(context.Background(), msg, nil)

	require.ErrorContains(t, err, "invalid payload attestation signature")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestOnGossipPayloadAttestationMessageInvalidLateMessageDoesNotBlockValidOne(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot+1)
	forged := sign(true)
	forged.Data.PayloadPresent = false

	require.Error(t, f.OnGossipPayloadAttestationMessage(context.Background(), forged, nil))
	err := f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil)

	require.ErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
}

func TestOnGossipPayloadAttestationMessageIgnoresMessageAheadOfStore(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot-1)

	err := f.OnGossipPayloadAttestationMessage(context.Background(), sign(true), nil)

	require.ErrorIs(t, err, ErrIgnore)
	require.NotErrorIs(t, err, ErrPayloadAttestationNotCurrentSlot)
}

func TestOnGossipPayloadAttestationMessageAheadOfStoreCountsAfterTick(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot-1)
	msg := sign(true)
	require.ErrorIs(t, f.OnGossipPayloadAttestationMessage(context.Background(), msg, nil), ErrIgnore)
	f.timeMs.Store(gossipTestSlot * f.beaconCfg.SecondsPerSlot * 1000)

	require.NoError(t, f.OnGossipPayloadAttestationMessage(context.Background(), msg, nil))

	timeliness, _, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, int8(1), timeliness[gossipTestPTCPosition])
}

func TestPayloadAttestationSlotMismatchIsNoOpOnlyForSpecHandler(t *testing.T) {
	f, sign := newGossipPayloadAttestationTestStore(t, gossipTestSlot)
	msg := sign(true)
	msg.Data.Slot = gossipTestSlot - 1

	require.NoError(t, f.OnPayloadAttestationMessage(context.Background(), msg))
	require.ErrorIs(t, f.OnGossipPayloadAttestationMessage(context.Background(), msg, nil), ErrIgnore)
	timeliness, _, _, _ := f.PayloadAttestationVotes(gossipTestBlockRoot)
	require.Equal(t, [clparams.PtcSize]int8{}, timeliness)
}
