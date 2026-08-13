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

package network

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestBlobBackfillRequestBoundsInflightAttempts(t *testing.T) {
	ticks := make(chan time.Time)
	expires := make(chan time.Time, 1)
	started := make(chan context.Context, 3)
	client := blobRequesterFunc(func(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		started <- ctx
		<-ctx.Done()
		return nil, "peer", ctx.Err()
	})
	done := make(chan error, 1)
	go func() {
		_, err := requestBlobsForBackfillWithSchedule(t.Context(), client, emptyBlobRequest, func(context.Context, *PeerAndSidecars) (bool, bool, error) { return true, true, nil }, blobBackfillRequestSchedule{
			ticks:   ticks,
			expires: expires,
			now:     func() time.Time { return time.Unix(100, 0) },
		})
		done <- err
	}()

	first := receiveBlobTestValue(t, started)
	ticks <- time.Unix(101, 0)
	second := receiveBlobTestValue(t, started)
	ticks <- time.Unix(102, 0)
	select {
	case <-started:
		t.Fatal("started more than two concurrent blob requests")
	default:
	}
	expires <- time.Unix(103, 0)
	require.ErrorIs(t, receiveBlobTestValue(t, done), ErrTimeout)
	receiveBlobTestSignal(t, first.Done())
	receiveBlobTestSignal(t, second.Done())
}

func TestBlobBackfillRequestRejectsPartialCandidateAndAcceptsFullLaterCandidate(t *testing.T) {
	type scriptedRequest struct {
		ctx   context.Context
		reply chan blobRequestResult
	}
	ticks := make(chan time.Time)
	expires := make(chan time.Time)
	requests := make(chan scriptedRequest, 2)
	client := blobRequesterFunc(func(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		reply := make(chan blobRequestResult)
		requests <- scriptedRequest{ctx: ctx, reply: reply}
		select {
		case result := <-reply:
			return result.responses, result.peer, result.err
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	})
	done := make(chan struct {
		result *PeerAndSidecars
		err    error
	}, 1)
	persisted := atomic.Bool{}
	accept := func(_ context.Context, candidate *PeerAndSidecars) (bool, bool, error) {
		if len(candidate.Responses) != 2 {
			return false, false, errors.New("candidate is incomplete")
		}
		persisted.Store(true)
		return true, true, nil
	}
	go func() {
		result, err := requestBlobsForBackfillWithSchedule(t.Context(), client, emptyBlobRequest, accept, blobBackfillRequestSchedule{
			ticks:   ticks,
			expires: expires,
			now:     func() time.Time { return time.Unix(100, 0) },
		})
		done <- struct {
			result *PeerAndSidecars
			err    error
		}{result: result, err: err}
	}()

	first := receiveBlobTestValue(t, requests)
	ticks <- time.Unix(101, 0)
	second := receiveBlobTestValue(t, requests)
	first.reply <- blobRequestResult{peer: "partial", responses: []*cltypes.BlobSidecar{{}}}
	select {
	case result := <-done:
		t.Fatalf("partial candidate completed request: %+v", result)
	default:
	}
	require.False(t, persisted.Load())
	second.reply <- blobRequestResult{peer: "full", responses: []*cltypes.BlobSidecar{{}, {}}}
	result := receiveBlobTestValue(t, done)
	require.NoError(t, result.err)
	require.Equal(t, "full", result.result.Peer)
	require.Len(t, result.result.Responses, 2)
	require.True(t, persisted.Load())
}

func TestBlobBackfillRequestPacingBacksOffEveryFailure(t *testing.T) {
	now := time.Unix(100, 0)
	pacing := newBlobBackfillRequestPacing()

	pacing.failed(now)
	require.False(t, pacing.ready(now.Add(requestBlobRetryInterval-time.Nanosecond)))
	require.True(t, pacing.ready(now.Add(requestBlobRetryInterval)))

	for range 20 {
		pacing.failed(now)
	}
	require.Equal(t, requestBlobMaxBackoff, pacing.backoff)
}

func TestBlobBackfillReadyValidationWinsExpiration(t *testing.T) {
	ticks := make(chan time.Time)
	expires := make(chan time.Time, 1)
	validationReady := make(chan struct{})
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		return []*cltypes.BlobSidecar{{}}, "peer", nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := requestBlobsForBackfillWithSchedule(t.Context(), client, emptyBlobRequest, func(context.Context, *PeerAndSidecars) (bool, bool, error) {
			return true, true, nil
		}, blobBackfillRequestSchedule{
			ticks:           ticks,
			expires:         expires,
			now:             time.Now,
			validationReady: func() { close(validationReady) },
		})
		done <- err
	}()

	receiveBlobTestSignal(t, validationReady)
	expires <- time.Now()
	require.NoError(t, receiveBlobTestValue(t, done))
}

func TestBlobBackfillBlockedValidationCancelsWithoutBlockingSender(t *testing.T) {
	ticks := make(chan time.Time)
	expires := make(chan time.Time, 1)
	validationStarted := make(chan struct{})
	validationReady := make(chan struct{})
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		return []*cltypes.BlobSidecar{{}}, "peer", nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := requestBlobsForBackfillWithSchedule(t.Context(), client, emptyBlobRequest, func(ctx context.Context, _ *PeerAndSidecars) (bool, bool, error) {
			close(validationStarted)
			<-ctx.Done()
			return false, false, ctx.Err()
		}, blobBackfillRequestSchedule{
			ticks:           ticks,
			expires:         expires,
			now:             time.Now,
			validationReady: func() { close(validationReady) },
		})
		done <- err
	}()

	receiveBlobTestSignal(t, validationStarted)
	expires <- time.Now()
	require.ErrorIs(t, receiveBlobTestValue(t, done), ErrTimeout)
	receiveBlobTestSignal(t, validationReady)
}

func emptyBlobRequest() *solid.ListSSZ[*cltypes.BlobIdentifier] {
	return solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 2)
}

func receiveBlobTestValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test value")
		var zero T
		return zero
	}
}

func receiveBlobTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test signal")
	}
}

type blobRequesterFunc func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)

func (f blobRequesterFunc) SendBlobsSidecarByIdentifierReq(ctx context.Context, req *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return f(ctx, req)
}
