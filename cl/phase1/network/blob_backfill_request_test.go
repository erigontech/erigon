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

	"github.com/erigontech/erigon/cl/clparams"
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
	requestIndex := uint64(0)
	requestFactory := func() *solid.ListSSZ[*cltypes.BlobIdentifier] {
		request := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 2)
		request.Append(&cltypes.BlobIdentifier{Index: requestIndex})
		requestIndex++
		return request
	}
	accept := func(_ context.Context, candidate *PeerAndSidecars) (bool, bool, error) {
		expectedIndex := uint64(0)
		if candidate.Peer == "full" {
			expectedIndex = 1
		}
		if candidate.requested.Len() != 1 || candidate.requested.Get(0).Index != expectedIndex {
			return false, false, errors.New("candidate lost its launch-time request snapshot")
		}
		if len(candidate.Responses) != 2 {
			return false, false, errors.New("candidate is incomplete")
		}
		persisted.Store(true)
		return true, true, nil
	}
	go func() {
		result, err := requestBlobsForBackfillWithSchedule(t.Context(), client, requestFactory, accept, blobBackfillRequestSchedule{
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

func TestBlobBackfillRequestRetriesProgressingCandidateWithoutRefetch(t *testing.T) {
	ticks := make(chan time.Time)
	expires := make(chan time.Time)
	requests := make(chan struct{}, 2)
	validationReady := make(chan struct{}, 2)
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		requests <- struct{}{}
		return []*cltypes.BlobSidecar{{}}, "peer", nil
	})
	attempts := 0
	done := make(chan error, 1)
	go func() {
		_, err := requestBlobsForBackfillWithSchedule(t.Context(), client, emptyBlobRequest, func(context.Context, *PeerAndSidecars) (bool, bool, error) {
			attempts++
			if attempts == 1 {
				return true, false, errors.New("temporary persistence failure")
			}
			return true, true, nil
		}, blobBackfillRequestSchedule{
			ticks: ticks, expires: expires, now: func() time.Time { return time.Unix(100, 0) },
			validationReady: func() { validationReady <- struct{}{} },
		})
		done <- err
	}()

	receiveBlobTestSignal(t, requests)
	receiveBlobTestSignal(t, validationReady)
	ticks <- time.Unix(101, 0)
	require.NoError(t, receiveBlobTestValue(t, done))
	require.Equal(t, 2, attempts)
	select {
	case <-requests:
		t.Fatal("refetched a candidate that had already made validation progress")
	default:
	}
}

func TestBlobBackfillProgressWithPersistenceErrorBacksOff(t *testing.T) {
	base := time.Unix(100, 0)
	pacing := newBlobBackfillRequestPacing()
	pacing.failed(base)
	validationTime := base.Add(requestBlobRetryInterval)

	pacing.recordValidation(validationTime, true, errors.New("temporary persistence failure"))

	require.False(t, pacing.ready(validationTime.Add(requestBlobRetryInterval)))
	require.True(t, pacing.ready(validationTime.Add(2*requestBlobRetryInterval)))
}

func TestBlobBackfillSuccessfulProgressResetsFailureBackoff(t *testing.T) {
	base := time.Unix(100, 0)
	pacing := newBlobBackfillRequestPacing()
	pacing.failed(base)
	validationTime := base.Add(requestBlobRetryInterval)

	pacing.recordValidation(validationTime, true, nil)

	require.False(t, pacing.ready(validationTime.Add(requestBlobRetryInterval-time.Nanosecond)))
	require.True(t, pacing.ready(validationTime.Add(requestBlobRetryInterval)))
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

func TestBlobBackfillEmptyResponsesBackOffWithoutProgress(t *testing.T) {
	ticks := make(chan time.Time)
	expires := make(chan time.Time)
	requests := make(chan struct{}, 3)
	validationReady := make(chan struct{}, 3)
	done := make(chan error, 1)
	base := time.Unix(100, 0)
	var elapsed atomic.Int64
	var attempts atomic.Int64
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		elapsed.Store((attempts.Add(1) - 1) * int64(requestBlobRetryInterval))
		return nil, "peer", nil
	})
	go func() {
		_, err := requestBlobsForBackfillWithSchedule(t.Context(), client, func() *solid.ListSSZ[*cltypes.BlobIdentifier] {
			requests <- struct{}{}
			return emptyBlobRequest()
		}, func(context.Context, *PeerAndSidecars) (bool, bool, error) {
			return false, false, nil
		}, blobBackfillRequestSchedule{
			ticks:   ticks,
			expires: expires,
			now:     func() time.Time { return base.Add(time.Duration(elapsed.Load())) },
			validationReady: func() {
				validationReady <- struct{}{}
			},
		})
		done <- err
	}()

	receiveBlobTestSignal(t, requests)
	receiveBlobTestSignal(t, validationReady)
	ticks <- base.Add(requestBlobRetryInterval)
	receiveBlobTestSignal(t, requests)
	receiveBlobTestSignal(t, validationReady)
	ticks <- base.Add(2 * requestBlobRetryInterval)
	select {
	case <-requests:
		t.Fatal("empty response retried before exponential backoff elapsed")
	default:
	}
	ticks <- base.Add(3 * requestBlobRetryInterval)
	receiveBlobTestSignal(t, requests)
	expires <- base.Add(4 * requestBlobRetryInterval)
	require.ErrorIs(t, receiveBlobTestValue(t, done), ErrTimeout)
}

func TestBlobBackfillDenebEmptyResponsesBackOffWithoutProgress(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	request, err := BlobsIdentifiersFromBlocks([]*cltypes.SignedBeaconBlock{block}, &clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	batch, err := newDenebRecoveryBatch([]*cltypes.SignedBeaconBlock{block}, request)
	require.NoError(t, err)

	ticks := make(chan time.Time)
	expires := make(chan time.Time)
	requests := make(chan struct{}, 3)
	validationReady := make(chan struct{}, 3)
	done := make(chan error, 1)
	base := time.Unix(100, 0)
	var elapsed atomic.Int64
	var attempts atomic.Int64
	client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		elapsed.Store((attempts.Add(1) - 1) * int64(requestBlobRetryInterval))
		return nil, "peer", nil
	})
	go func() {
		_, err := requestBlobsForBackfillWithSchedule(t.Context(), client, func() *solid.ListSSZ[*cltypes.BlobIdentifier] {
			requests <- struct{}{}
			return batch.remaining()
		}, func(_ context.Context, candidate *PeerAndSidecars) (bool, bool, error) {
			progress, err := batch.validate(candidate.requested, candidate.Responses)
			return progress > 0, false, err
		}, blobBackfillRequestSchedule{
			ticks: ticks, expires: expires,
			now: func() time.Time { return base.Add(time.Duration(elapsed.Load())) },
			validationReady: func() {
				validationReady <- struct{}{}
			},
		})
		done <- err
	}()

	receiveBlobTestSignal(t, requests)
	receiveBlobTestSignal(t, validationReady)
	ticks <- base.Add(requestBlobRetryInterval)
	receiveBlobTestSignal(t, requests)
	receiveBlobTestSignal(t, validationReady)
	ticks <- base.Add(2 * requestBlobRetryInterval)
	select {
	case <-requests:
		t.Fatal("production Deneb empty response retried before exponential backoff elapsed")
	default:
	}
	ticks <- base.Add(3 * requestBlobRetryInterval)
	receiveBlobTestSignal(t, requests)
	expires <- base.Add(4 * requestBlobRetryInterval)
	require.ErrorIs(t, receiveBlobTestValue(t, done), ErrTimeout)
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

func TestBlobBackfillStopWaitsForValidationOwnership(t *testing.T) {
	tests := map[string]struct {
		stop func(context.CancelFunc, chan<- time.Time)
		want error
	}{
		"expiration": {
			stop: func(_ context.CancelFunc, expires chan<- time.Time) { expires <- time.Now() },
			want: ErrTimeout,
		},
		"caller cancellation": {
			stop: func(cancel context.CancelFunc, _ chan<- time.Time) { cancel() },
			want: context.Canceled,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			ticks := make(chan time.Time)
			expires := make(chan time.Time, 1)
			validationStarted := make(chan struct{})
			validationContext := make(chan context.Context, 1)
			releaseValidation := make(chan struct{})
			client := blobRequesterFunc(func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
				return []*cltypes.BlobSidecar{{}}, "peer", nil
			})
			done := make(chan error, 1)
			go func() {
				_, err := requestBlobsForBackfillWithSchedule(ctx, client, emptyBlobRequest, func(ctx context.Context, _ *PeerAndSidecars) (bool, bool, error) {
					validationContext <- ctx
					close(validationStarted)
					<-releaseValidation
					return true, false, nil
				}, blobBackfillRequestSchedule{
					ticks:   ticks,
					expires: expires,
					now:     time.Now,
				})
				done <- err
			}()

			receiveBlobTestSignal(t, validationStarted)
			validationCtx := receiveBlobTestValue(t, validationContext)
			test.stop(cancel, expires)
			receiveBlobTestSignal(t, validationCtx.Done())
			select {
			case err := <-done:
				t.Fatalf("request returned before validation released ownership: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			close(releaseValidation)
			require.ErrorIs(t, receiveBlobTestValue(t, done), test.want)
		})
	}
}

func TestBlobBackfillStopWaitsForRequestOwnership(t *testing.T) {
	tests := map[string]struct {
		stop func(context.CancelFunc, chan<- time.Time)
		want error
	}{
		"expiration": {
			stop: func(_ context.CancelFunc, expires chan<- time.Time) { expires <- time.Now() },
			want: ErrTimeout,
		},
		"caller cancellation": {
			stop: func(cancel context.CancelFunc, _ chan<- time.Time) { cancel() },
			want: context.Canceled,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			ticks := make(chan time.Time)
			expires := make(chan time.Time, 1)
			requestStarted := make(chan struct{})
			requestContext := make(chan context.Context, 1)
			releaseRequest := make(chan struct{})
			client := blobRequesterFunc(func(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
				requestContext <- ctx
				close(requestStarted)
				<-ctx.Done()
				<-releaseRequest
				return nil, "peer", ctx.Err()
			})
			done := make(chan error, 1)
			go func() {
				_, err := requestBlobsForBackfillWithSchedule(ctx, client, emptyBlobRequest, func(context.Context, *PeerAndSidecars) (bool, bool, error) {
					return true, true, nil
				}, blobBackfillRequestSchedule{
					ticks:   ticks,
					expires: expires,
					now:     time.Now,
				})
				done <- err
			}()

			receiveBlobTestSignal(t, requestStarted)
			requestCtx := receiveBlobTestValue(t, requestContext)
			test.stop(cancel, expires)
			receiveBlobTestSignal(t, requestCtx.Done())
			select {
			case err := <-done:
				t.Fatalf("request returned before request handler released ownership: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			close(releaseRequest)
			require.ErrorIs(t, receiveBlobTestValue(t, done), test.want)
		})
	}
}

func TestBlobBackfillSuccessWaitsForRequestOwnership(t *testing.T) {
	ticks := make(chan time.Time)
	expires := make(chan time.Time)
	firstStarted := make(chan struct{})
	firstReply := make(chan struct{})
	secondStarted := make(chan context.Context, 1)
	releaseSecond := make(chan struct{})
	var requestIndex atomic.Int64
	client := blobRequesterFunc(func(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
		switch requestIndex.Add(1) {
		case 1:
			close(firstStarted)
			<-firstReply
			return []*cltypes.BlobSidecar{{}}, "first", nil
		case 2:
			secondStarted <- ctx
			<-ctx.Done()
			<-releaseSecond
			return nil, "second", ctx.Err()
		default:
			return nil, "unexpected", errors.New("unexpected request")
		}
	})
	type requestOutcome struct {
		result *PeerAndSidecars
		err    error
	}
	done := make(chan requestOutcome, 1)
	go func() {
		result, err := requestBlobsForBackfillWithSchedule(t.Context(), client, emptyBlobRequest, func(context.Context, *PeerAndSidecars) (bool, bool, error) {
			return true, true, nil
		}, blobBackfillRequestSchedule{
			ticks:   ticks,
			expires: expires,
			now:     time.Now,
		})
		done <- requestOutcome{result: result, err: err}
	}()

	receiveBlobTestSignal(t, firstStarted)
	ticks <- time.Now()
	secondCtx := receiveBlobTestValue(t, secondStarted)
	close(firstReply)
	receiveBlobTestSignal(t, secondCtx.Done())
	select {
	case outcome := <-done:
		t.Fatalf("request returned before request handler released ownership: %+v", outcome)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseSecond)
	outcome := receiveBlobTestValue(t, done)
	require.NoError(t, outcome.err)
	require.Equal(t, "first", outcome.result.Peer)
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
