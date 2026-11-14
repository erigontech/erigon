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

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

func (f *envelopeContentsFixture) phases() []contentsEventKind {
	kinds := make([]contentsEventKind, len(f.events))
	for i, e := range f.events {
		kinds[i] = e.kind
	}
	return slices.Compact(kinds)
}

func (f *envelopeContentsFixture) requireContentsRequiredRecord(wantRecord bool) {
	f.t.Helper()
	record, ok := f.handler.executionPayloadEnvelopeRetries.Peek(executionPayloadEnvelopeGossipKey{
		BeaconBlockRoot: f.root, BuilderIndex: f.signedEnvelope.Message.BuilderIndex,
	})
	require.Equal(f.t, wantRecord, ok)
	if ok {
		envelopeRoot, err := f.signedEnvelope.HashSSZ()
		require.NoError(f.t, err)
		require.Equal(f.t, envelopeRoot, record.EnvelopeRoot)
		require.True(f.t, record.ContentsRequired)
	}
}

func (f *envelopeContentsFixture) requireNoArrival() {
	f.t.Helper()
	require.NotContains(f.t, f.forkchoice.ExecutionPayloadReceivedAt, f.root, "an envelope that was not broadcast must not count as received")
}

func (f *envelopeContentsFixture) requireClaimReleased() {
	f.t.Helper()
	token, err := f.forkchoice.EnvelopeGossipAdmissions.TryClaim(f.root, f.signedEnvelope.Message.BuilderIndex)
	require.NoError(f.t, err, "the admission claim must be released")
	f.forkchoice.EnvelopeGossipAdmissions.Finish(token, false)
}

// expirePendingLocal moves the clock past the block's slot, which prunes the contents that a request
// without blob data would attach.
func (f *envelopeContentsFixture) expirePendingLocal() {
	f.currentSlot = f.slot + 1
}

func (f *envelopeContentsFixture) postEnvelopeWithoutContents(envelope *cltypes.SignedExecutionPayloadEnvelope) *httptest.ResponseRecorder {
	f.t.Helper()
	body, err := json.Marshal(envelope)
	require.NoError(f.t, err)
	return f.post(f.t.Context(), body, "application/json", "false", "")
}

func (f *envelopeContentsFixture) postLegacy(ctx context.Context) *httptest.ResponseRecorder {
	f.t.Helper()
	body, err := json.Marshal(f.signedEnvelope)
	require.NoError(f.t, err)
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	f.handler.postEthV1BeaconExecutionPayloadEnvelopeLegacy(recorder, request)
	return recorder
}

func (f *envelopeContentsFixture) custodyColumns() []uint64 {
	f.t.Helper()
	custody, err := f.handler.peerDas.StateReader().GetMyCustodyColumns()
	require.NoError(f.t, err)
	return slices.Sorted(maps.Keys(custody))
}

// failFirstCustodyWrite makes the store stop before the first custody column, so that data
// availability is missing afterwards.
func (f *envelopeContentsFixture) failFirstCustodyWrite() {
	first := f.custodyColumns()[0]
	f.failWrite = func(index uint64) error {
		if index == first {
			return errors.New("injected custody write failure")
		}
		return nil
	}
}

func failPublishOf(column uint64) func(uint64) error {
	return func(index uint64) error {
		if index == column {
			return errors.New("injected publish failure")
		}
		return nil
	}
}

// storeCustodyColumns writes the custody columns straight to the store, without logging them, as column sync would.
func (f *envelopeContentsFixture) storeCustodyColumns() {
	f.t.Helper()
	encoded := f.expectedColumns(f.blobs...)
	store := f.handler.columnStorage.(*loggingColumnStorage).DataColumnStorage
	for _, index := range f.custodyColumns() {
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		require.NoError(f.t, sidecar.DecodeSSZ(encoded[index], int(clparams.GloasVersion)))
		require.NoError(f.t, store.WriteColumnSidecars(f.t.Context(), f.root, int64(index), sidecar))
	}
}

func (f *envelopeContentsFixture) wrapImport(wrap func(ctx context.Context, next func() error) error) {
	next := f.forkchoice.OnExecutionPayloadFn
	f.forkchoice.OnExecutionPayloadFn = func(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) error {
		return wrap(ctx, func() error { return next(ctx, envelope, checkBlobData, validatePayload) })
	}
}

func (f *envelopeContentsFixture) subscribeOperations() chan *beaconevents.EventStream {
	f.handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 16)
	subscription := f.handler.emitters.Operation().Subscribe(events)
	f.t.Cleanup(subscription.Unsubscribe)
	return events
}

func drainEventCounts(events chan *beaconevents.EventStream) map[beaconevents.EventTopic]int {
	counts := map[beaconevents.EventTopic]int{}
	for {
		select {
		case event := <-events:
			counts[event.Event]++
		default:
			return counts
		}
	}
}

// repostWatch fails the test on any work, from its creation on, that a re-POST answered from state must not do.
type repostWatch struct {
	f      *envelopeContentsFixture
	events int
}

// watchWork replaces the validation hooks and wraps the import, so create it after the hooks of the test are set.
func (f *envelopeContentsFixture) watchWork() *repostWatch {
	f.forkchoice.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		f.t.Error("the envelope must not be gossip-validated again")
		return nil
	}
	f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error {
		f.t.Error("the contents must not be validated again")
		return nil
	}
	f.wrapImport(func(_ context.Context, next func() error) error {
		f.t.Error("the envelope must not be imported again")
		return next()
	})
	return &repostWatch{f: f, events: len(f.events)}
}

func (w *repostWatch) requireIdle() {
	w.f.t.Helper()
	require.Equal(w.f.t, w.events, len(w.f.events), "nothing may be published, written or imported")
}

// markSeenThroughGossip closes the admission identity, as an envelope that arrived through gossip does.
func (f *envelopeContentsFixture) markSeenThroughGossip() {
	f.t.Helper()
	token, err := f.forkchoice.EnvelopeGossipAdmissions.TryClaim(f.root, f.signedEnvelope.Message.BuilderIndex)
	require.NoError(f.t, err)
	f.forkchoice.EnvelopeGossipAdmissions.Finish(token, true)
}

func (f *envelopeContentsFixture) requireIdentitySeen() {
	f.t.Helper()
	_, err := f.forkchoice.EnvelopeGossipAdmissions.TryClaim(f.root, f.signedEnvelope.Message.BuilderIndex)
	require.ErrorIs(f.t, err, forkchoice.ErrExecutionPayloadEnvelopeAlreadySeen)
}

// differentEnvelope returns an envelope for the same block and builder with another signature, so another root.
func (f *envelopeContentsFixture) differentEnvelope() *cltypes.SignedExecutionPayloadEnvelope {
	different := &cltypes.SignedExecutionPayloadEnvelope{Message: f.signedEnvelope.Message}
	different.Signature[0] ^= 1
	return different
}

func (f *envelopeContentsFixture) postWithBlobs(envelope *cltypes.SignedExecutionPayloadEnvelope) *httptest.ResponseRecorder {
	f.t.Helper()
	contents := f.contents(f.blobs...)
	contents.SignedExecutionPayloadEnvelope = envelope
	return f.postContents(f.t.Context(), contents, "")
}

func (f *envelopeContentsFixture) failImport(err error) {
	f.wrapImport(func(context.Context, func() error) error { return err })
}

func (f *envelopeContentsFixture) makePersistedReadFail() {
	f.forkchoice.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
		return nil, errors.New("persisted envelope unavailable")
	}
}

func (f *envelopeContentsFixture) makePersistedEnvelopeUnusable(tc unavailablePersistedEnvelopeCase) {
	f.forkchoice.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
		if tc.persisted == nil {
			return nil, tc.err
		}
		return tc.persisted(f.signedEnvelope), tc.err
	}
}

func TestPostEnvelopeContentsGossipModePublishesEnvelopeFirst(t *testing.T) {
	t.Parallel()
	withBlobs := []contentsEventKind{eventEnvelopePublish, eventColumnPublish, eventColumnWrite, eventImport}
	for _, tc := range []struct {
		name         string
		blobs        int
		pendingLocal bool
		want         []contentsEventKind
	}{
		{name: "stateless with blobs", blobs: 2, want: withBlobs},
		{name: "stateless without blobs", blobs: 0, want: []contentsEventKind{eventEnvelopePublish, eventImport}},
		{name: "pendingLocal", blobs: 2, pendingLocal: true, want: withBlobs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, tc.blobs)
			recorder := f.postAllBlobs(tc.pendingLocal, "")

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, tc.want, f.phases())
			require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
			if tc.blobs > 0 {
				f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
				f.requireAllColumns("written", f.succeededIndices(eventColumnWrite))
			}
		})
	}
}

func TestPostEnvelopeContentsGossipModePublishesEnvelopeUnderTheBlockEpochDigest(t *testing.T) {
	t.Parallel()
	const epoch = 3
	slotsPerEpoch := clparams.MainnetBeaconConfig.SlotsPerEpoch
	for _, tc := range []struct {
		name string
		slot uint64
	}{
		{"last slot before the boundary", epoch*slotsPerEpoch - 1},
		{"first slot of the epoch", epoch * slotsPerEpoch},
		{"slot after the boundary", epoch*slotsPerEpoch + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, tc.slot, 2)
			f.currentSlot = tc.slot + slotsPerEpoch
			blockEpoch := tc.slot / slotsPerEpoch

			recorder := f.postContents(t.Context(), f.contents(f.blobs...), "")

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			envelopes := f.eventsOf(eventEnvelopePublish)
			require.Len(t, envelopes, 1)
			wantDigest := forkDigestOfEpoch(blockEpoch)
			require.Equal(t, wantDigest, envelopes[0].forkDigest, "envelope")
			for _, e := range f.eventsOf(eventColumnPublish) {
				require.Equal(t, wantDigest, e.forkDigest, "column %d", e.index)
			}
			require.Equal(t, []uint64{blockEpoch}, f.digestEpochs, "the digest is computed once, for the block's epoch")
		})
	}
}

func TestPostEnvelopeContentsGossipModePublishesEnvelopeAfterClientCancel(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	slot := time.Duration(f.handler.beaconChainCfg.SecondsPerSlot) * time.Second
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	f.forkchoice.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		cancelRequest()
		return nil
	}
	var validations int
	var validationErr error
	var validationDeadline time.Time
	var validationHasDeadline bool
	f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope) error {
		validations++
		validationErr = ctx.Err()
		validationDeadline, validationHasDeadline = ctx.Deadline()
		return ctx.Err()
	}
	f.wrapImport(func(ctx context.Context, next func() error) error {
		if ctx.Err() != nil {
			return forkchoice.ErrPayloadValidationInterrupted
		}
		return next()
	})

	start := time.Now()
	recorder := f.postContents(requestCtx, f.contents(f.blobs...), "")
	end := time.Now()

	require.Equal(t, 1, validations)
	require.NoError(t, validationErr, "the contents validation must not see the client's cancellation")
	require.True(t, validationHasDeadline, "the contents validation must have a deadline")
	require.False(t, validationDeadline.Before(start.Add(slot)))
	require.False(t, validationDeadline.After(end.Add(slot)))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	envelopes := f.eventsOf(eventEnvelopePublish)
	require.Len(t, envelopes, 1)
	require.NoError(t, envelopes[0].err)
	require.False(t, envelopes[0].deadline.IsZero(), "the envelope was published without a deadline")
	require.False(t, envelopes[0].deadline.After(end.Add(slot)))
	f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
}

func requireSameDeadline(t *testing.T, want, got time.Time, what string) {
	t.Helper()
	require.False(t, got.IsZero(), "%s has no deadline", what)
	require.Truef(t, want.Equal(got), "%s has deadline %v, want the contents validation's %v", what, got, want)
}

func TestPostEnvelopeContentsGossipModeImportsAfterClientCancel(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		pendingLocal bool
		retry        bool
	}{
		{name: "stateless"},
		{name: "pendingLocal without blob data", pendingLocal: true},
		{name: "retry with GossipRequired", retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			if tc.retry {
				f.failEnvelope = errors.New("injected envelope publish failure")
				first := f.postAllBlobs(false, "?broadcast_validation=consensus")
				require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
				record, ok := f.handler.executionPayloadEnvelopeRetries.Peek(executionPayloadEnvelopeGossipKey{
					BeaconBlockRoot: f.root, BuilderIndex: f.signedEnvelope.Message.BuilderIndex,
				})
				require.True(t, ok)
				require.True(t, record.GossipRequired)
				require.True(t, f.forkchoice.HasEnvelope(f.root))
				f.failEnvelope = nil
				f.events = nil
			}
			requestCtx, cancelRequest := context.WithCancel(t.Context())
			defer cancelRequest()
			f.failWrite = func(uint64) error {
				cancelRequest()
				return nil
			}
			var validationDeadline time.Time
			f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope) error {
				validationDeadline, _ = ctx.Deadline()
				return nil
			}
			var imports int
			var importErr error
			var importDeadline time.Time
			f.wrapImport(func(ctx context.Context, next func() error) error {
				imports++
				importErr = ctx.Err()
				importDeadline, _ = ctx.Deadline()
				if ctx.Err() != nil {
					return forkchoice.ErrPayloadValidationInterrupted
				}
				return next()
			})

			recorder := f.postAllBlobsWithContext(requestCtx, tc.pendingLocal, "")

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, 1, imports)
			require.NoError(t, importErr, "the import must not see the client's cancellation")
			requireSameDeadline(t, validationDeadline, importDeadline, "the import")
			publishes := f.eventsOf(eventColumnPublish)
			require.NotEmpty(t, publishes)
			for _, e := range publishes {
				requireSameDeadline(t, validationDeadline, e.deadline, fmt.Sprintf("the publish of column %d", e.index))
			}
			writes := f.eventsOf(eventColumnWrite)
			require.NotEmpty(t, writes)
			for _, e := range writes {
				require.NoError(t, e.ctxErr, "the write of column %d must not see the client's cancellation", e.index)
				requireSameDeadline(t, validationDeadline, e.deadline, fmt.Sprintf("the write of column %d", e.index))
			}
			require.True(t, f.forkchoice.HasEnvelope(f.root))
			f.requireIdentitySeen()
		})
	}
}

func TestPostEnvelopeContentsGossipModeImportStopsAtTheOneSlotDeadline(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 0)
	f.handler.beaconChainCfg.SecondsPerSlot = 1
	var validationDeadline, importDeadline time.Time
	f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope) error {
		validationDeadline, _ = ctx.Deadline()
		return nil
	}
	imported := make(chan struct{})
	f.wrapImport(func(ctx context.Context, next func() error) error {
		importDeadline, _ = ctx.Deadline()
		close(imported)
		select {
		case <-ctx.Done():
			return forkchoice.ErrPayloadValidationInterrupted
		case <-time.After(3 * time.Second):
			return next()
		}
	})
	body, err := f.contents().EncodeSSZ(nil)
	require.NoError(t, err)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.post(t.Context(), body, "application/octet-stream", "true", "") }()

	select {
	case <-imported:
	case recorder := <-done:
		require.Failf(t, "the handler answered before the import", "status %d: %s", recorder.Code, recorder.Body.String())
	}
	var recorder *httptest.ResponseRecorder
	select {
	case recorder = <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the handler did not answer")
	}

	requireSameDeadline(t, validationDeadline, importDeadline, "the import")
	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	require.False(t, f.forkchoice.HasEnvelope(f.root))
	f.requireClaimReleased()
}

func TestPostEnvelopeContentsGossipModeRepostOfImportedEnvelopeAnswersOK(t *testing.T) {
	requireFirstOK := func(t *testing.T, _ *envelopeContentsFixture, post func() *httptest.ResponseRecorder) {
		t.Helper()
		first := post()
		require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	}
	for _, tc := range []struct {
		name         string
		pendingLocal bool
		first        func(t *testing.T, f *envelopeContentsFixture, post func() *httptest.ResponseRecorder)
	}{
		{name: "after a 200", first: requireFirstOK},
		{name: "pendingLocal after a 200", pendingLocal: true, first: requireFirstOK},
		{name: "after an interrupted import that the pending retry finished", first: func(t *testing.T, f *envelopeContentsFixture, post func() *httptest.ResponseRecorder) {
			f.failImport(forkchoice.ErrPayloadValidationInterrupted)
			first := post()
			require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
			f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
		}},
		{name: "the identity was closed through gossip", first: func(_ *testing.T, f *envelopeContentsFixture, _ func() *httptest.ResponseRecorder) {
			f.markSeenThroughGossip()
			f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			post := func() *httptest.ResponseRecorder { return f.postAllBlobs(tc.pendingLocal, "") }
			tc.first(t, f, post)
			watch := f.watchWork()

			recorder := post()

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			watch.requireIdle()
		})
	}

	t.Run("while the first request still runs", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requestCtx, cancelRequest := context.WithCancel(t.Context())
		defer cancelRequest()
		f.failWrite = func(uint64) error {
			cancelRequest()
			return nil
		}
		importing := make(chan struct{})
		release := make(chan struct{})
		closeImporting := sync.OnceFunc(func() { close(importing) })
		closeRelease := sync.OnceFunc(func() { close(release) })
		t.Cleanup(closeRelease)
		var importErr error
		f.wrapImport(func(ctx context.Context, _ func() error) error {
			closeImporting()
			<-release
			importErr = ctx.Err()
			f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
			return nil
		})
		first, second := make(chan *httptest.ResponseRecorder, 1), make(chan *httptest.ResponseRecorder, 1)
		await := func(what string, signal <-chan struct{}) {
			t.Helper()
			select {
			case <-signal:
			case recorder := <-first:
				require.Failf(t, "the first request answered before "+what, "status %d: %s", recorder.Code, recorder.Body.String())
			case recorder := <-second:
				require.Failf(t, "the second request answered before "+what, "status %d: %s", recorder.Code, recorder.Body.String())
			case <-time.After(10 * time.Second):
				require.FailNow(t, "timed out before "+what)
			}
		}
		answer := func(what string, answers <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
			t.Helper()
			select {
			case recorder := <-answers:
				return recorder
			case <-time.After(10 * time.Second):
				require.FailNow(t, "the "+what+" request did not answer")
				return nil
			}
		}
		body, err := f.contents(f.blobs...).EncodeSSZ(nil)
		require.NoError(t, err)
		go func() { first <- f.post(requestCtx, body, "application/octet-stream", "true", "") }()
		await("the import", importing)
		claiming := make(chan struct{})
		f.onClaim = sync.OnceFunc(func() { close(claiming) })
		go func() { second <- f.post(t.Context(), body, "application/octet-stream", "true", "") }()
		await("the claim of the second request", claiming)
		watch := f.watchWork()
		closeRelease()

		secondRecorder := answer("second", second)
		firstRecorder := answer("first", first)

		require.Equal(t, http.StatusOK, secondRecorder.Code, secondRecorder.Body.String())
		require.Equal(t, http.StatusOK, firstRecorder.Code, firstRecorder.Body.String())
		require.NoError(t, importErr, "the import must not see the client's cancellation")
		watch.requireIdle()
	})
}

func TestPostEnvelopeContentsGossipModeRepostOfUnimportedEnvelopeAnswersAccepted(t *testing.T) {
	t.Parallel()
	const rejected = "invalid execution payload"
	t.Run("the import was rejected", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.failImport(errors.New(rejected))
		first := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
		watch := f.watchWork()

		recorder := f.postAllBlobs(false, "")

		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
		watch.requireIdle()
		f.requireIdentitySeen()
	})

	t.Run("the persisted envelope is unreadable", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.failImport(forkchoice.ErrPayloadValidationInterrupted)
		first := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
		f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
		f.makePersistedReadFail()
		watch := f.watchWork()

		recorder := f.postAllBlobs(false, "")

		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
		watch.requireIdle()
		f.requireClaimReleased()
	})

	for _, tc := range []struct {
		name      string
		importErr error
		status    int
	}{
		{name: "after a 200", status: http.StatusOK},
		{name: "after an interrupted import that sync finished", importErr: forkchoice.ErrPayloadValidationInterrupted, status: http.StatusAccepted},
	} {
		t.Run("the persisted payload was then marked invalid, "+tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			if tc.importErr != nil {
				f.failImport(tc.importErr)
			}
			first := f.postAllBlobs(false, "")
			require.Equal(t, tc.status, first.Code, first.Body.String())
			f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
			f.forkchoice.PayloadStatusByRootMap[f.root] = execution_client.PayloadStatusInvalidated
			watch := f.watchWork()

			recorder := f.postAllBlobs(false, "")

			require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
			watch.requireIdle()
		})
	}

	for _, tc := range unavailablePersistedEnvelopeCases() {
		t.Run("seen, with a persisted envelope that is unusable: "+tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failImport(errors.New(rejected))
			first := f.postAllBlobs(false, "")
			require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
			f.makePersistedEnvelopeUnusable(tc)
			watch := f.watchWork()

			var recorder *httptest.ResponseRecorder
			require.NotPanics(t, func() { recorder = f.postAllBlobs(false, "") })

			require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
			watch.requireIdle()
		})
	}

	t.Run("another envelope of the same block and builder was published in between", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.forkchoice.OnExecutionPayloadFn = func(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
			if envelope.Signature == f.signedEnvelope.Signature {
				return forkchoice.ErrPayloadValidationInterrupted
			}
			return errors.New(rejected)
		}
		firstA := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusAccepted, firstA.Code, firstA.Body.String())
		f.requireClaimReleased()
		published := len(f.eventsOf(eventEnvelopePublish))
		b := f.postWithBlobs(f.differentEnvelope())
		require.Equal(t, http.StatusAccepted, b.Code, b.Body.String())
		envelopes := f.eventsOf(eventEnvelopePublish)
		require.Len(t, envelopes, published+1, "POST B must publish its envelope early")
		require.NoError(t, envelopes[published].err)
		f.requireIdentitySeen()
		watch := f.watchWork()

		secondA := f.postAllBlobs(false, "")

		require.Equal(t, http.StatusAccepted, secondA.Code, secondA.Body.String())
		watch.requireIdle()
	})
}

func TestPostEnvelopeContentsGossipModeEnvelopePublishFailureStopsBeforeColumns(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.failEnvelope = errors.New("injected envelope publish failure")
	body := f.contents(f.blobs...)

	first := f.postContents(t.Context(), body, "")

	require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
	require.Equal(t, []contentsEventKind{eventEnvelopePublish}, f.phases(), "nothing may follow a failed envelope publish")
	require.False(t, f.forkchoice.OnExecutionPayloadCalled)
	f.requireNoArrival()
	f.requireContentsRequiredRecord(false)

	f.failEnvelope = nil
	second := f.postContents(t.Context(), body, "")

	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.Len(t, f.eventsOf(eventEnvelopePublish), 2)
	f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
	f.requireAllColumns("written", f.succeededIndices(eventColumnWrite))
}

type eofRecorder struct {
	io.Reader
	done *time.Time
}

func (r eofRecorder) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if errors.Is(err, io.EOF) && r.done.IsZero() {
		*r.done = time.Now()
	}
	return n, err
}

func TestPostEnvelopeContentsGossipModeRecordsDecodeTimeAsArrival(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 1)
	var bodyDone, gossipEntry, arrival time.Time
	var arrivalRecorded, sampled bool
	f.forkchoice.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		gossipEntry = time.Now()
		return nil
	}
	f.failWrite = func(uint64) error {
		if !sampled {
			sampled = true
			arrival, arrivalRecorded = f.forkchoice.ExecutionPayloadReceivedAt[f.root]
		}
		return nil
	}
	body, err := f.contents(f.blobs...).EncodeSSZ(nil)
	require.NoError(t, err)

	recorder := f.postReader(t.Context(), eofRecorder{Reader: bytes.NewReader(body), done: &bodyDone}, "application/octet-stream", "true", "")

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.True(t, arrivalRecorded, "the arrival must be recorded before the columns are stored")
	require.False(t, arrival.Before(bodyDone), "the arrival must not precede the end of the body")
	require.False(t, arrival.After(gossipEntry), "the arrival must not follow gossip validation")
}

func (f *envelopeContentsFixture) preparationAllowed() bool {
	_, finish, ok := f.forkchoice.TryBeginPayloadPreparation(f.t.Context(), f.slot, f.slot+1)
	if ok {
		finish()
	}
	return ok
}

func TestPostEnvelopeContentsGossipModeHoldsPreparationUntilImport(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	writes, imports := 0, 0
	f.failWrite = func(uint64) error {
		writes++
		require.False(t, f.preparationAllowed(), "preparation must wait while the columns are stored")
		return nil
	}
	f.forkchoice.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		imports++
		require.False(t, f.preparationAllowed(), "preparation must wait until the envelope is imported")
		return nil
	}

	recorder := f.postAllBlobs(false, "")

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, int(f.handler.beaconChainCfg.NumberOfColumns), writes)
	require.Equal(t, 1, imports)
	require.True(t, f.preparationAllowed(), "preparation must resume when the handler returns")
}

func TestPostEnvelopeContentsGossipModeReleasesPreparationAfterOneSlot(t *testing.T) {
	f := newEnvelopeContentsFixture(t, 0, 1)
	f.handler.beaconChainCfg.SecondsPerSlot = 1
	writing := make(chan struct{})
	release := make(chan struct{})
	closeWriting := sync.OnceFunc(func() { close(writing) })
	closeRelease := sync.OnceFunc(func() { close(release) })
	t.Cleanup(closeRelease)
	f.failWrite = func(uint64) error {
		closeWriting()
		<-release
		return nil
	}
	body, err := f.contents(f.blobs...).EncodeSSZ(nil)
	require.NoError(t, err)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.post(t.Context(), body, "application/octet-stream", "true", "") }()

	select {
	case <-writing:
	case recorder := <-done:
		require.Failf(t, "the handler answered before the first column write", "status %d: %s", recorder.Code, recorder.Body.String())
	}
	require.False(t, f.preparationAllowed(), "preparation must wait while the columns are stored")
	require.Eventually(t, f.preparationAllowed, 2*time.Second, 10*time.Millisecond, "the hold must end one slot after gossip validation")
	closeRelease()

	recorder := <-done
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostEnvelopeContentsGossipModeAnswersAcceptedOncePublished(t *testing.T) {
	t.Parallel()
	t.Run("pendingLocal with a storage failure and missing data availability", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.failWrite = func(uint64) error { return errors.New("injected write failure") }

		recorder := f.postAllBlobs(true, "")

		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
		require.Equal(t, eventEnvelopePublish, f.events[0].kind)
		require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
		f.requireContentsRequiredRecord(true)
	})

	t.Run("a different envelope is persisted", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		events := f.subscribeOperations()
		other := *f.signedEnvelope
		other.Signature[0] = 1
		f.wrapImport(func(context.Context, func() error) error {
			f.forkchoice.Envelopes[f.root] = &other
			return forkchoice.ErrIgnore
		})
		body := f.contents(f.blobs...)

		first := f.postContents(t.Context(), body, "")

		require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
		require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
		counts := drainEventCounts(events)
		require.Equal(t, map[beaconevents.EventTopic]int{beaconevents.OpExecutionPayloadGossip: 1}, counts)

		second := f.postContents(t.Context(), body, "")

		require.Equal(t, http.StatusAccepted, second.Code, second.Body.String())
		require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
	})

	t.Run("the import fails", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.wrapImport(func(context.Context, func() error) error {
			return errors.New("invalid execution payload")
		})

		recorder := f.postAllBlobs(false, "")

		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
		require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
		f.requireContentsRequiredRecord(false)
		_, err := f.forkchoice.EnvelopeGossipAdmissions.TryClaim(f.root, f.signedEnvelope.Message.BuilderIndex)
		require.ErrorIs(t, err, forkchoice.ErrExecutionPayloadEnvelopeAlreadySeen, "a rejected import must close the identity")
	})

	t.Run("the block is gone and nothing is persisted", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		events := f.subscribeOperations()
		f.wrapImport(func(context.Context, func() error) error {
			delete(f.forkchoice.Blocks, f.root)
			return forkchoice.ErrIgnore
		})

		recorder := f.postAllBlobs(false, "")

		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
		require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
		require.Empty(t, drainEventCounts(events))
		f.requireClaimReleased()
	})
}

func TestPostEnvelopeRetryWithoutContentsKeepsContentsRequired(t *testing.T) {
	t.Parallel()
	const failedColumn = 5
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.failPublish = failPublishOf(failedColumn)
	body := f.contents(f.blobs...)

	first := f.postContents(t.Context(), body, "")

	require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
	imports := len(f.eventsOf(eventImport))
	f.expirePendingLocal()

	second := f.postEnvelopeWithoutContents(f.signedEnvelope)

	require.Equal(t, http.StatusBadRequest, second.Code, second.Body.String())
	require.Contains(t, second.Body.String(), "blob data is required")
	require.Len(t, f.eventsOf(eventImport), imports, "the envelope must not be processed")
	f.requireContentsRequiredRecord(true)

	f.failPublish = nil
	third := f.postContents(t.Context(), body, "")

	require.Equal(t, http.StatusOK, third.Code, third.Body.String())
	require.Contains(t, f.succeededIndices(eventColumnPublish), uint64(failedColumn))
	require.Len(t, f.eventsOf(eventEnvelopePublish), 1, "the retry must not publish the envelope again")
	f.requireContentsRequiredRecord(false)
}

func TestPostEnvelopeFreshClaimWithoutContentsKeepsContentsRequired(t *testing.T) {
	const failedColumn = 5
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.failPublish = failPublishOf(failedColumn)
	f.failFirstCustodyWrite()
	body := f.contents(f.blobs...)

	first := f.postContents(t.Context(), body, "")

	require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
	require.False(t, f.forkchoice.HasEnvelope(f.root), "the envelope must not be persisted without its custody columns")
	f.storeCustodyColumns()
	f.expirePendingLocal()
	imports := len(f.eventsOf(eventImport))

	second := f.postEnvelopeWithoutContents(f.signedEnvelope)

	require.Equal(t, http.StatusBadRequest, second.Code, second.Body.String())
	require.Contains(t, second.Body.String(), "blob data is required")
	require.Len(t, f.eventsOf(eventImport), imports, "the envelope must not be processed")
	f.requireContentsRequiredRecord(true)
	f.requireClaimReleased()

	f.failPublish, f.failWrite = nil, nil
	requestCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	third := f.postContents(requestCtx, body, "")

	require.Equal(t, http.StatusOK, third.Code, third.Body.String())
	require.Contains(t, f.succeededIndices(eventColumnPublish), uint64(failedColumn))
	f.requireContentsRequiredRecord(false)
}

func TestPostEnvelopeContentsRepairWithPendingImportClearsContentsRequired(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		importErr error
	}{
		{"the import is interrupted", forkchoice.ErrPayloadValidationInterrupted},
		{"the import is ignored", forkchoice.ErrIgnore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failPublish = failPublishOf(5)
			f.failFirstCustodyWrite()
			body := f.contents(f.blobs...)
			require.Equal(t, http.StatusInternalServerError, f.postContents(t.Context(), body, "").Code)
			f.requireContentsRequiredRecord(true)
			f.failPublish, f.failWrite = nil, nil
			importFn := f.forkchoice.OnExecutionPayloadFn
			f.forkchoice.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
				return tc.importErr
			}

			repair := f.postContents(t.Context(), body, "")

			require.Equal(t, http.StatusAccepted, repair.Code, repair.Body.String())
			require.Contains(t, f.succeededIndices(eventColumnPublish), uint64(5))

			f.forkchoice.OnExecutionPayloadFn = importFn
			f.expirePendingLocal()
			recorder := f.postEnvelopeWithoutContents(f.signedEnvelope)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		})
	}
}

func TestPostEnvelopeContentsGossipModeValidationDeadlineAnswersUnavailable(t *testing.T) {
	waitForDeadline := func(f *envelopeContentsFixture, result func(ctxErr error) error) context.Context {
		f.handler.beaconChainCfg.SecondsPerSlot = 1
		f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope) error {
			<-ctx.Done()
			return result(ctx.Err())
		}
		requestCtx, cancel := context.WithTimeout(f.t.Context(), 3*time.Second)
		f.t.Cleanup(cancel)
		return requestCtx
	}

	t.Run("the contents validation reaches the one-slot deadline", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		body := f.contents(f.blobs...)

		first := f.postContents(waitForDeadline(f, func(err error) error { return err }), body, "")

		require.Equal(t, http.StatusServiceUnavailable, first.Code, first.Body.String())
		f.requireNoEvents()
		f.requireClaimReleased()
		f.requireNoArrival()

		f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = nil
		second := f.postContents(t.Context(), body, "")

		require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	})

	t.Run("the contents validation succeeds after the one-slot deadline", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)

		recorder := f.postContents(waitForDeadline(f, func(error) error { return nil }), f.contents(f.blobs...), "")

		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		f.requireNoEvents()
		f.requireClaimReleased()
	})

	t.Run("a non-context error after the deadline stays a bad request", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)

		recorder := f.postContents(waitForDeadline(f, func(error) error { return errors.New("invalid signature") }), f.contents(f.blobs...), "")

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		f.requireNoEvents()
	})

	t.Run("a deadline error before the deadline stays a bad request", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error {
			return fmt.Errorf("validation: %w", context.DeadlineExceeded)
		}

		recorder := f.postAllBlobs(false, "")

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		f.requireNoEvents()
		f.requireNoArrival()
	})

	// The rows below are not on the early path, so their contents validation runs under the request context.
	interruptions := []struct {
		name      string
		interrupt func(*envelopeContentsFixture) context.Context
	}{
		{name: "the client leaves", interrupt: func(f *envelopeContentsFixture) context.Context {
			requestCtx, cancel := context.WithCancel(f.t.Context())
			f.t.Cleanup(cancel)
			f.forkchoice.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
				cancel()
				return nil
			}
			f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope) error {
				return ctx.Err()
			}
			return requestCtx
		}},
		{name: "the request deadline passes", interrupt: func(f *envelopeContentsFixture) context.Context {
			requestCtx, cancel := context.WithTimeout(f.t.Context(), 500*time.Millisecond)
			f.t.Cleanup(cancel)
			f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope) error {
				<-ctx.Done()
				return ctx.Err()
			}
			return requestCtx
		}},
	}

	for _, interruption := range interruptions {
		t.Run("consensus mode when "+interruption.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)

			recorder := f.postContents(interruption.interrupt(f), f.contents(f.blobs...), "?broadcast_validation=consensus")

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			f.requireNoEvents()
		})

		t.Run("legacy route when "+interruption.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)

			recorder := f.postLegacy(interruption.interrupt(f))

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			f.requireNoEvents()
		})

		t.Run("retry of a persisted envelope when "+interruption.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failPublish = failPublishOf(5)
			body := f.contents(f.blobs...)
			require.Equal(t, http.StatusInternalServerError, f.postContents(t.Context(), body, "").Code)
			events := len(f.events)

			recorder := f.postContents(interruption.interrupt(f), body, "")

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Len(t, f.events, events)
		})
	}

	t.Run("consensus mode when the client leaves before consensus validation", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requestCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f.forkchoice.ValidateExecutionPayloadEnvelopeFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error {
			cancel()
			return nil
		}

		recorder := f.postContents(requestCtx, f.contents(f.blobs...), "?broadcast_validation=consensus")

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		require.Equal(t, []contentsEventKind{eventConsensusValidation}, f.phases())
	})

	t.Run("consensus mode when the client leaves during consensus validation", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requestCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f.duringConsensusValidation = cancel

		recorder := f.postContents(requestCtx, f.contents(f.blobs...), "?broadcast_validation=consensus")

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		require.Equal(t, []contentsEventKind{eventConsensusValidation}, f.phases())
	})

	t.Run("consensus mode when the client leaves during column writes", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requestCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f.failWrite = func(uint64) error {
			cancel()
			return nil
		}
		imports := 0
		f.wrapImport(func(ctx context.Context, next func() error) error {
			imports++
			if ctx.Err() != nil {
				return forkchoice.ErrPayloadValidationInterrupted
			}
			return next()
		})

		recorder := f.postContents(requestCtx, f.contents(f.blobs...), "?broadcast_validation=consensus")

		f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
		require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
		require.Equal(t, 1, imports)
		require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
	})

	t.Run("consensus mode when the payload is invalidated and the client leaves during column writes", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requestCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		f.failWrite = func(uint64) error {
			f.payloadInvalidated = true
			cancel()
			return nil
		}
		imports := 0
		f.wrapImport(func(ctx context.Context, next func() error) error {
			imports++
			if ctx.Err() != nil {
				return forkchoice.ErrPayloadValidationInterrupted
			}
			return next()
		})

		recorder := f.postContents(requestCtx, f.contents(f.blobs...), "?broadcast_validation=consensus")

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		require.Equal(t, 1, imports)
		require.Empty(t, f.eventsOf(eventEnvelopePublish))
	})
}

func TestPostEnvelopeConsensusModeBroadcastFailureKeepsContentsRequired(t *testing.T) {
	t.Parallel()
	const failedColumn = 5
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.failPublish = failPublishOf(failedColumn)
	f.failEnvelope = errors.New("injected envelope publish failure")
	body := f.contents(f.blobs...)

	first := f.postContents(t.Context(), body, "?broadcast_validation=consensus")

	require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
	envelopes := f.eventsOf(eventEnvelopePublish)
	require.Len(t, envelopes, 1)
	require.Error(t, envelopes[0].err, "the envelope publish must have been attempted and failed")
	f.expirePendingLocal()
	imports := len(f.eventsOf(eventImport))

	second := f.postEnvelopeWithoutContents(f.signedEnvelope)

	require.Equal(t, http.StatusBadRequest, second.Code, second.Body.String())
	require.Contains(t, second.Body.String(), "blob data is required")
	require.Len(t, f.eventsOf(eventImport), imports, "the envelope must not be processed")

	f.failPublish, f.failEnvelope = nil, nil
	before := len(f.events)
	third := f.postContents(t.Context(), body, "")

	require.Equal(t, http.StatusOK, third.Code, third.Body.String())
	require.Equal(t, eventEnvelopePublish, f.events[before].kind, "the retry must publish the envelope first")
	require.Len(t, f.eventsOf(eventEnvelopePublish), 2)
	require.Contains(t, f.succeededIndices(eventColumnPublish), uint64(failedColumn))
	f.requireContentsRequiredRecord(false)
}

func TestPostEnvelopeConsensusModeRetryPublishesFailedEnvelope(t *testing.T) {
	t.Parallel()
	const failedColumn = 5
	for _, mode := range []string{"consensus", "consensus_and_equivocation"} {
		for _, tc := range []struct {
			name       string
			failColumn bool
		}{
			{name: "envelope publish fails"},
			{name: "envelope and column publish fail", failColumn: true},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := newEnvelopeContentsFixture(t, 0, 2)
				f.failEnvelope = errors.New("injected envelope publish failure")
				if tc.failColumn {
					f.failPublish = failPublishOf(failedColumn)
				}
				body := f.contents(f.blobs...)
				query := "?broadcast_validation=" + mode
				key := executionPayloadEnvelopeGossipKey{BeaconBlockRoot: f.root, BuilderIndex: f.signedEnvelope.Message.BuilderIndex}

				first := f.postContents(t.Context(), body, query)

				require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
				retry, ok := f.handler.executionPayloadEnvelopeRetries.Peek(key)
				require.True(t, ok)
				require.True(t, retry.GossipRequired)
				require.Equal(t, tc.failColumn, retry.ContentsRequired)

				f.failEnvelope, f.failPublish = nil, nil
				second := f.postContents(t.Context(), body, query)

				require.Equal(t, http.StatusOK, second.Code, second.Body.String())
				envelopes := f.eventsOf(eventEnvelopePublish)
				require.Len(t, envelopes, 2)
				require.NoError(t, envelopes[1].err)
				if tc.failColumn {
					require.Contains(t, f.succeededIndices(eventColumnPublish), uint64(failedColumn))
				}
				_, ok = f.handler.executionPayloadEnvelopeRetries.Peek(key)
				require.False(t, ok, "a published envelope must clear its retry record")
			})
		}

		t.Run(mode+"/the payload is invalidated during the retry's writes", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failEnvelope = errors.New("injected envelope publish failure")
			body := f.contents(f.blobs...)
			query := "?broadcast_validation=" + mode
			require.Equal(t, http.StatusInternalServerError, f.postContents(t.Context(), body, query).Code)
			f.failEnvelope = nil
			f.failWrite = func(uint64) error {
				f.payloadInvalidated = true
				return nil
			}

			retry := f.postContents(t.Context(), body, query)

			require.Equal(t, http.StatusBadRequest, retry.Code, retry.Body.String())
			require.Contains(t, retry.Body.String(), errExecutionPayloadInvalidated.Error())
			require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
		})
	}
}

func TestPostEnvelopeContentsConsensusModesPublishEnvelopeLast(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"consensus", "consensus_and_equivocation"} {
		for _, tc := range []struct {
			name          string
			blobCount     int
			bare          bool
			wantPhases    []contentsEventKind
			checkBlobData bool
		}{
			{
				name: "contents with blobs", blobCount: 2,
				wantPhases: []contentsEventKind{eventConsensusValidation, eventColumnPublish, eventColumnWrite, eventImport, eventEnvelopePublish},
			},
			{
				name:       "contents without blobs",
				wantPhases: []contentsEventKind{eventConsensusValidation, eventImport, eventEnvelopePublish},
			},
			{
				name: "bare envelope without blobs", bare: true, checkBlobData: true,
				wantPhases: []contentsEventKind{eventConsensusValidation, eventImport, eventEnvelopePublish},
			},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := newEnvelopeContentsFixture(t, 0, tc.blobCount)
				if tc.bare {
					f.expirePendingLocal()
				}

				recorder := f.postAllBlobs(tc.bare, "?broadcast_validation="+mode)

				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
				require.Equal(t, tc.wantPhases, f.phases())
				require.Equal(t, tc.checkBlobData, f.consensusCheckBlobData)
			})
		}
	}
}

func TestPostEnvelopeContentsConsensusFailurePublishesNothing(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"consensus", "consensus_and_equivocation"} {
		for _, pendingLocal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pendingLocal=%t", mode, pendingLocal), func(t *testing.T) {
				f := newEnvelopeContentsFixture(t, 0, 2)
				f.forkchoice.ValidateExecutionPayloadEnvelopeForConsensusErr = errors.New("consensus validation failed")

				recorder := f.postAllBlobs(pendingLocal, "?broadcast_validation="+mode)

				require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
				require.Equal(t, []contentsEventKind{eventConsensusValidation}, f.phases())
				requireNotCached(t, f.handler, f.blobs...)
				f.requireClaimReleased()
				f.requireContentsRequiredRecord(false)
			})
		}
	}
}

func TestPostEnvelopeContentsConsensusModesWriteFailureAnswersAccepted(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"consensus", "consensus_and_equivocation"} {
		query := "?broadcast_validation=" + mode
		t.Run(mode+"/DA missing", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failFirstCustodyWrite()
			body := f.contents(f.blobs...)

			first := f.postContents(t.Context(), body, query)

			require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
			require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
			require.Equal(t, eventEnvelopePublish, f.events[len(f.events)-1].kind)
			f.requireContentsRequiredRecord(true)
			f.requireClaimReleased()

			f.failWrite = nil
			second := f.postContents(t.Context(), body, query)

			require.Equal(t, http.StatusOK, second.Code, second.Body.String())
			f.requireContentsRequiredRecord(false)
		})

		t.Run(mode+"/the pending retry imports during the repair", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failFirstCustodyWrite()
			body := f.contents(f.blobs...)
			require.Equal(t, http.StatusAccepted, f.postContents(t.Context(), body, query).Code)
			f.requireContentsRequiredRecord(true)
			f.requireClaimReleased()
			f.failWrite = nil
			f.wrapImport(func(context.Context, func() error) error {
				f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
				return forkchoice.ErrIgnore
			})

			repair := f.postContents(t.Context(), body, query)

			require.Equal(t, http.StatusOK, repair.Code, repair.Body.String())
			f.requireContentsRequiredRecord(false)
		})

		t.Run(mode+"/the pending retry imports a payload invalidated during the repair writes", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failFirstCustodyWrite()
			body := f.contents(f.blobs...)
			require.Equal(t, http.StatusAccepted, f.postContents(t.Context(), body, query).Code)
			f.requireContentsRequiredRecord(true)
			f.requireClaimReleased()
			envelopes := len(f.eventsOf(eventEnvelopePublish))
			f.failWrite = func(uint64) error {
				f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
				f.payloadInvalidated = true
				return nil
			}

			repair := f.postContents(t.Context(), body, query)

			require.Equal(t, http.StatusBadRequest, repair.Code, repair.Body.String())
			require.Len(t, f.eventsOf(eventEnvelopePublish), envelopes)
			require.Equal(t, f.root, f.invalidatedBlockRoot)
			require.Equal(t, f.signedEnvelope.Message.Payload.BlockHash, f.invalidatedExecutionHash)
			f.requireClaimReleased()
		})

		t.Run(mode+"/a column publish fails too", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failPublish = failPublishOf(5)
			f.failFirstCustodyWrite()

			recorder := f.postContents(t.Context(), f.contents(f.blobs...), query)

			require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
			require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
			f.requireContentsRequiredRecord(true)
			f.requireClaimReleased()
		})

		for _, tc := range []struct {
			name             string
			persistDifferent bool
		}{
			{name: "the repair's import is ignored and a different envelope is persisted", persistDifferent: true},
			{name: "the repair's import is ignored and no envelope is persisted"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := newEnvelopeContentsFixture(t, 0, 2)
				f.failFirstCustodyWrite()
				body := f.contents(f.blobs...)
				require.Equal(t, http.StatusAccepted, f.postContents(t.Context(), body, query).Code)
				f.requireContentsRequiredRecord(true)
				f.requireClaimReleased()
				envelopes := len(f.eventsOf(eventEnvelopePublish))
				f.failWrite = nil
				var persisted *cltypes.SignedExecutionPayloadEnvelope
				if tc.persistDifferent {
					persisted = f.differentEnvelope()
				}
				f.wrapImport(func(context.Context, func() error) error {
					if persisted != nil {
						f.forkchoice.SetEnvelope(f.root, persisted)
					}
					return forkchoice.ErrIgnore
				})

				repair := f.postContents(t.Context(), body, query)

				require.Equal(t, http.StatusServiceUnavailable, repair.Code, repair.Body.String())
				require.Len(t, f.eventsOf(eventEnvelopePublish), envelopes)
				f.requireContentsRequiredRecord(true)
			})
		}

		t.Run(mode+"/the payload was invalidated during the writes", func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failFirstCustodyWrite()
			failWrite := f.failWrite
			f.failWrite = func(index uint64) error {
				err := failWrite(index)
				if err != nil {
					f.payloadInvalidated = true
				}
				return err
			}

			recorder := f.postContents(t.Context(), f.contents(f.blobs...), query)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Empty(t, f.eventsOf(eventEnvelopePublish))
			require.Len(t, f.eventsOf(eventImport), 1)
			require.Equal(t, f.root, f.invalidatedBlockRoot)
			require.Equal(t, f.signedEnvelope.Message.Payload.BlockHash, f.invalidatedExecutionHash)
			require.False(t, f.consensusCheckBlobData)
			f.requireClaimReleased()
			f.requireContentsRequiredRecord(false)
		})
	}
}

func TestPostEnvelopeContentsConsensusRepairUsesAdmissionContentsRequired(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.failFirstCustodyWrite()
	body := f.contents(f.blobs...)
	query := "?broadcast_validation=consensus"
	recorder := f.postContents(t.Context(), body, query)
	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	f.requireContentsRequiredRecord(true)
	f.requireClaimReleased()

	f.failWrite = func(uint64) error {
		f.handler.clearExecutionPayloadEnvelopeRetry(executionPayloadEnvelopeGossipKey{
			BeaconBlockRoot: f.root,
			BuilderIndex:    f.signedEnvelope.Message.BuilderIndex,
		})
		return nil
	}
	f.wrapImport(func(context.Context, func() error) error {
		f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
		return forkchoice.ErrIgnore
	})

	repair := f.postContents(t.Context(), body, query)

	require.Equal(t, http.StatusOK, repair.Code, repair.Body.String())
}

func TestPostEnvelopeContentsConsensusAndEquivocationRechecksBeforeColumns(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	block := f.forkchoice.Blocks[f.root].Block
	f.duringConsensusValidation = func() {
		f.forkchoice.Headers[common.Hash{0xff}] = &cltypes.BeaconBlockHeader{Slot: block.Slot, ProposerIndex: block.ProposerIndex}
	}

	recorder := f.postAllBlobs(false, "?broadcast_validation=consensus_and_equivocation")

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "equivocation")
	require.Equal(t, []contentsEventKind{eventConsensusValidation}, f.phases())
}

func TestPostEnvelopeContentsConsensusModesRecheckFinalityAfterTheWrites(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"consensus", "consensus_and_equivocation"} {
		t.Run(mode, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.failWrite = func(uint64) error {
				f.forkchoice.FinalizedCheckpointVal.Epoch = f.slot/f.handler.beaconChainCfg.SlotsPerEpoch + 1
				return nil
			}

			recorder := f.postAllBlobs(false, "?broadcast_validation="+mode)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), "before finalized slot")
			f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
			f.requireAllColumns("written", f.succeededIndices(eventColumnWrite))
			require.Empty(t, f.eventsOf(eventImport))
			require.Empty(t, f.eventsOf(eventEnvelopePublish))
			f.requireClaimReleased()
			f.requireContentsRequiredRecord(false)
		})
	}
}

func TestPostEnvelopeConsensusModeWithoutBlobDataReadsStoredColumns(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.expirePendingLocal()

	first := f.postAllBlobs(true, "?broadcast_validation=consensus")

	require.Equal(t, http.StatusBadRequest, first.Code, first.Body.String())
	require.True(t, f.consensusCheckBlobData)
	require.Equal(t, []contentsEventKind{eventConsensusValidation}, f.phases())

	f.storeCustodyColumns()
	second := f.postAllBlobs(true, "?broadcast_validation=consensus")

	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.True(t, f.consensusCheckBlobData)
}

func TestPostEnvelopeContentsConsensusModesImportRejectionAfterColumnsKeepsBadRequest(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"consensus", "consensus_and_equivocation"} {
		for _, tc := range []struct {
			name        string
			err         error
			invalidated bool
			status      int
			envelopes   int
		}{
			{name: "invalid execution payload", err: forkchoice.ErrInvalidExecutionPayloadEnvelope, status: http.StatusBadRequest},
			{name: "DA missing after successful writes", err: forkchoice.ErrEIP7594ColumnDataNotAvailable, status: http.StatusBadRequest},
			{
				name:      "persistence failure",
				err:       fmt.Errorf("disk unavailable: %w", forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed),
				status:    http.StatusInternalServerError,
				envelopes: 1,
			},
			{
				name:        "persistence failure for a payload invalidated during the writes",
				err:         fmt.Errorf("disk unavailable: %w", forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed),
				invalidated: true,
				status:      http.StatusBadRequest,
			},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := newEnvelopeContentsFixture(t, 0, 2)
				f.failImport(tc.err)
				if tc.invalidated {
					f.failWrite = func(uint64) error {
						f.payloadInvalidated = true
						return nil
					}
				}

				recorder := f.postAllBlobs(false, "?broadcast_validation="+mode)

				require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
				f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
				f.requireAllColumns("written", f.succeededIndices(eventColumnWrite))
				require.Len(t, f.eventsOf(eventEnvelopePublish), tc.envelopes)
				f.requireClaimReleased()
			})
		}
	}
}

func TestPostEnvelopeContentsConsensusAndEquivocationChecksBeforeTheEnvelope(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	block := f.forkchoice.Blocks[f.root].Block
	f.wrapImport(func(_ context.Context, next func() error) error {
		f.forkchoice.Headers[common.Hash{0xff}] = &cltypes.BeaconBlockHeader{Slot: block.Slot, ProposerIndex: block.ProposerIndex}
		return next()
	})

	recorder := f.postAllBlobs(false, "?broadcast_validation=consensus_and_equivocation")

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "equivocation")
	f.requireAllColumns("published", f.succeededIndices(eventColumnPublish))
	f.requireAllColumns("written", f.succeededIndices(eventColumnWrite))
	require.Len(t, f.eventsOf(eventImport), 1)
	require.Empty(t, f.eventsOf(eventEnvelopePublish))
}

func TestPostEnvelopeContentsLegacyRoutePublishesEnvelopeLast(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)

	recorder := f.postLegacy(t.Context())

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, []contentsEventKind{eventColumnPublish, eventColumnWrite, eventImport, eventEnvelopePublish}, f.phases())
}

func TestPostEnvelopeContentsRequiredDoesNotRefuseADifferentEnvelope(t *testing.T) {
	t.Parallel()
	f := newEnvelopeContentsFixture(t, 0, 2)
	f.failPublish = failPublishOf(5)
	f.failFirstCustodyWrite()
	require.Equal(t, http.StatusInternalServerError, f.postContents(t.Context(), f.contents(f.blobs...), "").Code)
	f.storeCustodyColumns()
	f.expirePendingLocal()
	imports := len(f.eventsOf(eventImport))
	other := *f.signedEnvelope
	other.Signature[0] = 1

	recorder := f.postEnvelopeWithoutContents(&other)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Len(t, f.eventsOf(eventImport), imports+1, "the other envelope must be processed")
}

func TestPostEnvelopeNonEarlyPathsImportUnderTheRequestContext(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                     string
		columns                  bool
		cancelInGossipValidation bool
		prepare                  func(f *envelopeContentsFixture)
		post                     func(f *envelopeContentsFixture, ctx context.Context) *httptest.ResponseRecorder
	}{
		{name: "consensus mode", columns: true, post: func(f *envelopeContentsFixture, ctx context.Context) *httptest.ResponseRecorder {
			return f.postAllBlobsWithContext(ctx, false, "?broadcast_validation=consensus")
		}},
		{name: "legacy route", columns: true, post: func(f *envelopeContentsFixture, ctx context.Context) *httptest.ResponseRecorder {
			return f.postLegacy(ctx)
		}},
		{
			name:                     "without contents",
			cancelInGossipValidation: true,
			prepare:                  (*envelopeContentsFixture).expirePendingLocal,
			post: func(f *envelopeContentsFixture, ctx context.Context) *httptest.ResponseRecorder {
				return f.postAllBlobsWithContext(ctx, true, "")
			},
		},
		{
			name:    "retry of a persisted envelope",
			columns: true,
			prepare: func(f *envelopeContentsFixture) {
				f.failPublish = failPublishOf(5)
				require.Equal(f.t, http.StatusInternalServerError, f.postAllBlobs(false, "").Code)
				f.requireContentsRequiredRecord(true)
				f.failPublish = nil
				f.events = nil
			},
			post: func(f *envelopeContentsFixture, ctx context.Context) *httptest.ResponseRecorder {
				return f.postAllBlobsWithContext(ctx, false, "")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			if tc.prepare != nil {
				tc.prepare(f)
			}
			requestCtx, cancelRequest := context.WithCancel(t.Context())
			defer cancelRequest()
			if tc.cancelInGossipValidation {
				f.forkchoice.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
					cancelRequest()
					return nil
				}
			} else {
				f.failWrite = func(uint64) error {
					cancelRequest()
					return nil
				}
			}
			var imports int
			var importErr error
			f.wrapImport(func(ctx context.Context, next func() error) error {
				imports++
				importErr = ctx.Err()
				if ctx.Err() != nil {
					return forkchoice.ErrPayloadValidationInterrupted
				}
				return next()
			})

			tc.post(f, requestCtx)

			require.Equal(t, 1, imports)
			require.ErrorIs(t, importErr, context.Canceled, "the import must run under the request context")
			if !tc.columns {
				return
			}
			publishes := f.eventsOf(eventColumnPublish)
			require.NotEmpty(t, publishes)
			for _, e := range publishes {
				require.False(t, e.deadline.IsZero(), "the publish of column %d must have its own deadline", e.index)
			}
			writes := f.eventsOf(eventColumnWrite)
			require.NotEmpty(t, writes)
			for _, e := range writes {
				require.ErrorIs(t, e.ctxErr, context.Canceled, "the write of column %d must run under the request context", e.index)
				require.True(t, e.deadline.IsZero(), "the write of column %d must not have a deadline", e.index)
			}
		})
	}
}

func TestPostEnvelopeContentsRepostNotAnsweredFromStateKeepsItsStatus(t *testing.T) {
	t.Parallel()
	requireFirstOK := func(t *testing.T, f *envelopeContentsFixture) {
		t.Helper()
		first := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	}
	requireRepost := func(t *testing.T, f *envelopeContentsFixture, want int, repost func() *httptest.ResponseRecorder) {
		t.Helper()
		events := len(f.events)
		recorder := repost()
		require.Equal(t, want, recorder.Code, recorder.Body.String())
		require.Len(t, f.events, events, "the re-POST must not publish, write or import")
	}

	t.Run("consensus mode after a 200", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requireFirstOK(t, f)
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder {
			return f.postAllBlobs(false, "?broadcast_validation=consensus")
		})
	})

	t.Run("legacy route after a 200", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requireFirstOK(t, f)
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder { return f.postLegacy(t.Context()) })
	})

	t.Run("without contents after a 200", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requireFirstOK(t, f)
		f.expirePendingLocal()
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder {
			return f.postEnvelopeWithoutContents(f.signedEnvelope)
		})
	})

	t.Run("a different envelope after a 200", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		requireFirstOK(t, f)
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder {
			return f.postWithBlobs(f.differentEnvelope())
		})
	})

	t.Run("a different envelope after a rejected import", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.failImport(errors.New("invalid execution payload"))
		first := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder {
			return f.postWithBlobs(f.differentEnvelope())
		})
	})

	t.Run("the early publish failed and the identity was closed through gossip", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.failEnvelope = errors.New("injected envelope publish failure")
		first := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
		f.markSeenThroughGossip()
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder { return f.postAllBlobs(false, "") })
	})

	t.Run("lookup required with an unreadable persisted envelope and no POST before", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
		f.makePersistedReadFail()
		requireRepost(t, f, http.StatusServiceUnavailable, func() *httptest.ResponseRecorder { return f.postAllBlobs(false, "") })
	})

	t.Run("lookup required with a different persisted envelope and a ContentsRequired record", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.failPublish = failPublishOf(5)
		other := f.differentEnvelope()
		f.wrapImport(func(context.Context, func() error) error {
			f.forkchoice.SetEnvelope(f.root, other)
			return forkchoice.ErrIgnore
		})
		first := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
		f.requireContentsRequiredRecord(true)
		requireRepost(t, f, http.StatusServiceUnavailable, func() *httptest.ResponseRecorder { return f.postAllBlobs(false, "") })
	})

	t.Run("an envelope imported by sync whose payload was then marked invalid, and no POST before", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
		f.forkchoice.PayloadStatusByRootMap[f.root] = execution_client.PayloadStatusInvalidated
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder { return f.postAllBlobs(false, "") })
	})

	t.Run("the anchor envelope", func(t *testing.T) {
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.forkchoice.AnchorRootVal = f.root
		f.forkchoice.SetEnvelope(f.root, f.signedEnvelope)
		f.markSeenThroughGossip()
		requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder { return f.postAllBlobs(false, "") })
	})

	for _, tc := range unavailablePersistedEnvelopeCases() {
		t.Run("seen, with a persisted envelope that is unusable: "+tc.name, func(t *testing.T) {
			f := newEnvelopeContentsFixture(t, 0, 2)
			f.markSeenThroughGossip()
			f.makePersistedEnvelopeUnusable(tc)
			requireRepost(t, f, http.StatusBadRequest, func() *httptest.ResponseRecorder {
				var recorder *httptest.ResponseRecorder
				require.NotPanics(t, func() { recorder = f.postAllBlobs(false, "") })
				return recorder
			})
		})
	}

	t.Run("seen, with a ContentsRequired record", func(t *testing.T) {
		const failedColumn = 5
		f := newEnvelopeContentsFixture(t, 0, 2)
		f.failPublish = failPublishOf(failedColumn)
		first := f.postAllBlobs(false, "")
		require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
		f.requireContentsRequiredRecord(true)
		f.markSeenThroughGossip()
		f.failPublish = nil

		second := f.postAllBlobs(false, "")

		require.Equal(t, http.StatusOK, second.Code, second.Body.String())
		require.Contains(t, f.succeededIndices(eventColumnPublish), uint64(failedColumn), "the retry must republish the column")
		require.Len(t, f.eventsOf(eventEnvelopePublish), 1)
		f.requireContentsRequiredRecord(false)
	})
}
