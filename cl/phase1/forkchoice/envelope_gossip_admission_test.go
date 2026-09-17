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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestExecutionPayloadEnvelopeAdmissionsRejectCanceledFreeClaim(t *testing.T) {
	var admissions ExecutionPayloadEnvelopeAdmissions
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := admissions.Claim(ctx, common.HexToHash("0x1234"), 42)
	require.ErrorIs(t, err, context.Canceled)

	token, err := admissions.Claim(t.Context(), common.HexToHash("0x1234"), 42)
	require.NoError(t, err)
	admissions.Finish(token, false)
}

func TestExecutionPayloadEnvelopeAdmissionsBoundsSameIdentityWaiters(t *testing.T) {
	var admissions ExecutionPayloadEnvelopeAdmissions
	root := common.HexToHash("0x1234")
	owner, err := admissions.Claim(t.Context(), root, 42)
	require.NoError(t, err)

	standbyResult := make(chan struct {
		token ExecutionPayloadEnvelopeAdmissionToken
		err   error
	}, 1)
	go func() {
		token, err := admissions.Claim(t.Context(), root, 42)
		standbyResult <- struct {
			token ExecutionPayloadEnvelopeAdmissionToken
			err   error
		}{token: token, err: err}
	}()
	require.Eventually(t, func() bool {
		admissions.mu.Lock()
		defer admissions.mu.Unlock()
		return admissions.inflight[executionPayloadEnvelopeIdentity{beaconBlockRoot: root, builderIndex: 42}].waiters == 1
	}, time.Second, 10*time.Millisecond)

	_, err = admissions.Claim(t.Context(), root, 42)
	require.ErrorContains(t, err, "already being published")

	admissions.Finish(owner, false)
	standby := <-standbyResult
	require.NoError(t, standby.err)
	admissions.Finish(standby.token, false)
}

func TestExecutionPayloadEnvelopeAdmissionsCanceledWaiterReleasesStandby(t *testing.T) {
	var admissions ExecutionPayloadEnvelopeAdmissions
	root := common.HexToHash("0x1234")
	owner, err := admissions.Claim(t.Context(), root, 42)
	require.NoError(t, err)

	waiterCtx, cancelWaiter := context.WithCancel(t.Context())
	waiterErr := make(chan error, 1)
	go func() {
		_, err := admissions.Claim(waiterCtx, root, 42)
		waiterErr <- err
	}()
	require.Eventually(t, func() bool {
		admissions.mu.Lock()
		defer admissions.mu.Unlock()
		return admissions.inflight[executionPayloadEnvelopeIdentity{beaconBlockRoot: root, builderIndex: 42}].waiters == 1
	}, time.Second, 10*time.Millisecond)
	cancelWaiter()
	require.ErrorIs(t, <-waiterErr, context.Canceled)

	replacementResult := make(chan struct {
		token ExecutionPayloadEnvelopeAdmissionToken
		err   error
	}, 1)
	go func() {
		token, err := admissions.Claim(t.Context(), root, 42)
		replacementResult <- struct {
			token ExecutionPayloadEnvelopeAdmissionToken
			err   error
		}{token: token, err: err}
	}()
	require.Eventually(t, func() bool {
		admissions.mu.Lock()
		defer admissions.mu.Unlock()
		return admissions.inflight[executionPayloadEnvelopeIdentity{beaconBlockRoot: root, builderIndex: 42}].waiters == 1
	}, time.Second, 10*time.Millisecond)
	admissions.Finish(owner, false)
	replacement := <-replacementResult
	require.NoError(t, replacement.err)
	admissions.Finish(replacement.token, false)
}

func TestExecutionPayloadEnvelopeAdmissionsBoundsDistinctOwners(t *testing.T) {
	var admissions ExecutionPayloadEnvelopeAdmissions
	tokens := make([]ExecutionPayloadEnvelopeAdmissionToken, 0, maxInflightExecutionPayloadEnvelopes)
	for i := range maxInflightExecutionPayloadEnvelopes {
		token, err := admissions.Claim(t.Context(), common.Hash{byte(i), byte(i >> 8)}, 42)
		require.NoError(t, err)
		tokens = append(tokens, token)
	}

	_, err := admissions.Claim(t.Context(), common.HexToHash("0xffff"), 42)
	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAdmissionBusy)

	for _, token := range tokens {
		admissions.Finish(token, false)
	}
}

func TestExecutionPayloadEnvelopeAdmissionsTryClaimNeverWaits(t *testing.T) {
	var admissions ExecutionPayloadEnvelopeAdmissions
	root := common.HexToHash("0x1234")
	owner, err := admissions.Claim(t.Context(), root, 42)
	require.NoError(t, err)

	_, err = admissions.TryClaim(root, 42)
	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAdmissionBusy)
	other, err := admissions.TryClaim(common.HexToHash("0x5678"), 42)
	require.NoError(t, err)
	admissions.Finish(other, false)

	admissions.Finish(owner, true)
	_, err = admissions.TryClaim(root, 42)
	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAlreadySeen)
}

func TestExecutionPayloadEnvelopeAdmissionsForgetSeenPreservesFIFO(t *testing.T) {
	var admissions ExecutionPayloadEnvelopeAdmissions
	root := func(i int) common.Hash { return common.Hash{byte(i), byte(i >> 8)} }
	for i := range maxSeenExecutionPayloadEnvelopes {
		token, err := admissions.TryClaim(common.Hash{byte(i), byte(i >> 8)}, uint64(i))
		require.NoError(t, err)
		admissions.Finish(token, true)
	}
	firstExtra := maxSeenExecutionPayloadEnvelopes
	extra, err := admissions.TryClaim(root(firstExtra), uint64(firstExtra))
	require.NoError(t, err)
	admissions.Finish(extra, true)
	target := maxSeenExecutionPayloadEnvelopes / 2
	admissions.ForgetSeen(root(target), uint64(target))
	for _, retained := range []int{1, target - 1, target + 1, firstExtra} {
		_, err = admissions.TryClaim(root(retained), uint64(retained))
		require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAlreadySeen)
	}
	targetToken, err := admissions.TryClaim(root(target), uint64(target))
	require.NoError(t, err)
	admissions.Finish(targetToken, true)
	secondExtra := firstExtra + 1
	extra, err = admissions.TryClaim(root(secondExtra), uint64(secondExtra))
	require.NoError(t, err)
	admissions.Finish(extra, true)

	oldest, err := admissions.TryClaim(root(1), 1)
	require.NoError(t, err)
	admissions.Finish(oldest, false)
	_, err = admissions.TryClaim(root(target), uint64(target))
	require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAlreadySeen)
	for _, retained := range []int{2, target - 1, target + 1, firstExtra, secondExtra} {
		_, err = admissions.TryClaim(root(retained), uint64(retained))
		require.ErrorIs(t, err, ErrExecutionPayloadEnvelopeAlreadySeen)
	}
}
