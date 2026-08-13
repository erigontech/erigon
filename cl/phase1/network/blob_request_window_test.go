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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestRequestBlobsFranticallyBoundsInflightRequests(t *testing.T) {
	restoreBlobRequestTiming(t, 5*time.Millisecond, 60*time.Millisecond)
	release := make(chan struct{})
	var active atomic.Int64
	var maximum atomic.Int64
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		current := active.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		<-release
		active.Add(-1)
		return nil, "peer", errors.New("unavailable")
	})

	_, err := RequestBlobsFrantically(t.Context(), client, solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 1))
	require.ErrorIs(t, err, ErrTimeout)
	require.Positive(t, maximum.Load())
	require.LessOrEqual(t, maximum.Load(), int64(2))
	close(release)
	require.Eventually(t, func() bool { return active.Load() == 0 }, time.Second, time.Millisecond)
}

func TestBlobRequestPacingBackoffAndReset(t *testing.T) {
	restoreBlobRequestTiming(t, 5*time.Millisecond, time.Second)
	now := time.Unix(100, 0)
	pacing := newBlobRequestPacing()

	pacing.complete(now, errors.New("unavailable"))
	require.Equal(t, 10*time.Millisecond, pacing.backoff)
	require.False(t, pacing.ready(now.Add(9*time.Millisecond)))
	require.True(t, pacing.ready(now.Add(10*time.Millisecond)))

	for range 20 {
		pacing.failed(now)
	}
	require.Equal(t, requestBlobMaxBackoff, pacing.backoff)

	pacing.complete(now, nil)
	require.Equal(t, requestBlobRetryInterval, pacing.backoff)
	require.True(t, pacing.ready(now))
}

func TestRequestBlobsFranticallyKeepsWaitingAfterPartialResponse(t *testing.T) {
	restoreBlobRequestTiming(t, 5*time.Millisecond, 100*time.Millisecond)
	block, sidecars := makeBlobBoundaryObjects(t, 100, 2)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	var calls atomic.Int64
	client := blobRequesterFunc(func(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		if calls.Add(1) == 1 {
			return sidecars[:1], "partial", nil
		}
		select {
		case <-ctx.Done():
			return nil, "complete", ctx.Err()
		case <-time.After(10 * time.Millisecond):
			return sidecars, "complete", nil
		}
	})
	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 2)
	req.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: 0})
	req.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: 1})

	response, err := requestBlobsFranticallyValidated(t.Context(), req, client.SendBlobsSidecarByIdentifierReq, nil, func(responses []*cltypes.BlobSidecar) error {
		if len(responses) != req.Len() {
			return errors.New("partial")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "complete", response.Peer)
	require.Len(t, response.Responses, 2)
}

func TestRequestBlobsFranticallyTimesOutBlockedValidation(t *testing.T) {
	restoreBlobRequestTiming(t, 5*time.Millisecond, 30*time.Millisecond)
	block, sidecars := makeBlobBoundaryObjects(t, 100, 1)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	requestCanceled := make(chan struct{})
	validationStarted := make(chan struct{})
	validationRelease := make(chan struct{})
	var canceledOnce sync.Once
	client := blobRequesterFunc(func(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		go func() {
			<-ctx.Done()
			canceledOnce.Do(func() { close(requestCanceled) })
		}()
		return sidecars, "peer", nil
	})

	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 1)
	req.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: 0})
	_, err = requestBlobsFranticallyValidated(t.Context(), req, client.SendBlobsSidecarByIdentifierReq, nil, func([]*cltypes.BlobSidecar) error {
		close(validationStarted)
		<-validationRelease
		return nil
	})
	require.ErrorIs(t, err, ErrTimeout)
	select {
	case <-validationStarted:
	default:
		t.Fatal("candidate validation did not start")
	}
	close(validationRelease)
	require.Eventually(t, func() bool {
		select {
		case <-requestCanceled:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestRequestBlobsFranticallyBacksOffRepeatedValidationFailures(t *testing.T) {
	restoreBlobRequestTiming(t, 5*time.Millisecond, 95*time.Millisecond)
	block, sidecars := makeBlobBoundaryObjects(t, 100, 1)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	var calls atomic.Int64
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		calls.Add(1)
		return sidecars, "invalid-peer", nil
	})
	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 1)
	req.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: 0})

	_, err = requestBlobsFranticallyValidated(t.Context(), req, client.SendBlobsSidecarByIdentifierReq, nil, func([]*cltypes.BlobSidecar) error {
		return errors.New("invalid candidate")
	})
	require.ErrorIs(t, err, ErrTimeout)
	require.GreaterOrEqual(t, calls.Load(), int64(3))
	require.LessOrEqual(t, calls.Load(), int64(5))
}

func TestRequestBlobsFranticallyStartsFirstRequestImmediately(t *testing.T) {
	restoreBlobRequestTiming(t, time.Second, 100*time.Millisecond)
	started := make(chan struct{})
	client := blobRequesterFunc(func(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		close(started)
		<-ctx.Done()
		return nil, "peer", ctx.Err()
	})
	done := make(chan error, 1)
	go func() {
		_, err := RequestBlobsFrantically(t.Context(), client, solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 1))
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("first blob request waited for the retry interval")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrTimeout)
	case <-time.After(time.Second):
		t.Fatal("blob request did not stop after expiration")
	}
}

func TestRequestBlobsFranticallyDoesNotStartAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var calls atomic.Int64
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		calls.Add(1)
		return nil, "peer", nil
	})

	_, err := RequestBlobsFrantically(ctx, client, solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 1))
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls.Load())
}

func restoreBlobRequestTiming(t *testing.T, retryInterval, expiration time.Duration) {
	t.Helper()
	previousRetryInterval := requestBlobRetryInterval
	previousExpiration := requestBlobBatchExpiration
	requestBlobRetryInterval = retryInterval
	requestBlobBatchExpiration = expiration
	t.Cleanup(func() {
		requestBlobRetryInterval = previousRetryInterval
		requestBlobBatchExpiration = previousExpiration
	})
}

type blobRequesterFunc func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)

func (blobRequesterFunc) Peers() (uint64, error) { return 1, nil }

func (f blobRequesterFunc) SendBlobsSidecarByIdentifierReq(ctx context.Context, req *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	return f(ctx, req)
}
