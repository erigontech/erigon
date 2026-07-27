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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
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
	require.Error(t, f.OnPayloadAttestationMessage(context.Background(), nil, false))
	require.Error(t, f.OnPayloadAttestationMessage(context.Background(), &cltypes.PayloadAttestationMessage{}, false))
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

	require.NoError(t, validationContext.validateSignature(msg))
	msg.Data.PayloadPresent = false
	require.ErrorContains(t, validationContext.validateSignature(msg), "invalid payload attestation signature")
}
