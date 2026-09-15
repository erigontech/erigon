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
