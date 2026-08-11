package services

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
)

type envelopeRequesterStub struct {
	mu        sync.Mutex
	responses [][]*cltypes.SignedExecutionPayloadEnvelope
	calls     atomic.Int32
	bans      atomic.Int32
	started   chan struct{}
	release   chan struct{}
}

func (s *envelopeRequesterStub) SendExecutionPayloadEnvelopesByRootReq(ctx context.Context, _ [][32]byte) ([]*cltypes.SignedExecutionPayloadEnvelope, string, error) {
	s.calls.Add(1)
	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, "peer", ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.responses) == 0 {
		return nil, "peer", nil
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	return response, "peer", nil
}

func (s *envelopeRequesterStub) BanPeer(string) { s.bans.Add(1) }

func TestEnvelopeResolverInvalidResponderThenValid(t *testing.T) {
	root := common.HexToHash("0x1234")
	wrong := newTestSignedEnvelope(100, common.HexToHash("0x5678"), 1)
	valid := newTestSignedEnvelope(100, root, 2)
	requester := &envelopeRequesterStub{responses: [][]*cltypes.SignedExecutionPayloadEnvelope{{wrong, valid}}}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	processed := make(chan struct{})
	fcu.OnExecutionPayloadFunc = func(candidate *cltypes.SignedExecutionPayloadEnvelope) error {
		fcu.Envelopes[root] = candidate
		close(processed)
		return nil
	}
	service := NewExecutionPayloadService(t.Context(), fcu, &clparams.MainnetBeaconConfig, beaconevents.NewEventEmitter(), requester)
	service.ResolveExecutionPayloadEnvelope(root)
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("resolver did not process the valid response")
	}
	require.True(t, fcu.HasEnvelope(root))
	require.Equal(t, int32(1), requester.bans.Load())
	require.Equal(t, int32(1), requester.calls.Load())
}

func TestEnvelopeResolverSameRootTriggersCoalesce(t *testing.T) {
	root := common.HexToHash("0x1234")
	requester := &envelopeRequesterStub{started: make(chan struct{}, 1), release: make(chan struct{})}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	service := NewExecutionPayloadService(t.Context(), fcu, &clparams.MainnetBeaconConfig, beaconevents.NewEventEmitter(), requester)
	for range 100 {
		service.ResolveExecutionPayloadEnvelope(root)
	}
	<-requester.started
	require.Equal(t, int32(1), requester.calls.Load())
	close(requester.release)
}

func TestEnvelopeResolverServiceCancellationStopsRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	requester := &envelopeRequesterStub{started: make(chan struct{}, 1), release: make(chan struct{})}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	service := NewExecutionPayloadService(ctx, fcu, &clparams.MainnetBeaconConfig, beaconevents.NewEventEmitter(), requester)
	service.ResolveExecutionPayloadEnvelope(common.HexToHash("0x1234"))
	<-requester.started
	cancel()
	require.Eventually(t, func() bool {
		service.resolver.mu.Lock()
		defer service.resolver.mu.Unlock()
		return len(service.resolver.jobs) == 0
	}, time.Second, time.Millisecond)
}

func TestEnvelopeResolverDeadlineStopsRetries(t *testing.T) {
	requester := &envelopeRequesterStub{}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	service := NewExecutionPayloadService(t.Context(), fcu, &clparams.MainnetBeaconConfig, beaconevents.NewEventEmitter(), requester)
	service.resolver.deadline = 20 * time.Millisecond
	service.resolver.retry = time.Millisecond
	service.ResolveExecutionPayloadEnvelope(common.HexToHash("0x1234"))
	require.Eventually(t, func() bool {
		service.resolver.mu.Lock()
		defer service.resolver.mu.Unlock()
		return requester.calls.Load() > 0 && len(service.resolver.jobs) == 0
	}, time.Second, time.Millisecond)
}

func TestEnvelopeResolverGossipAndFetchRace(t *testing.T) {
	root := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, root, 2)
	requester := &envelopeRequesterStub{responses: [][]*cltypes.SignedExecutionPayloadEnvelope{{envelope}}, release: make(chan struct{}), started: make(chan struct{}, 1)}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	var calls atomic.Int32
	fcu.OnExecutionPayloadFunc = func(candidate *cltypes.SignedExecutionPayloadEnvelope) error {
		calls.Add(1)
		fcu.Envelopes[root] = candidate
		return nil
	}
	service := NewExecutionPayloadService(t.Context(), fcu, &clparams.MainnetBeaconConfig, beaconevents.NewEventEmitter(), requester)
	service.ResolveExecutionPayloadEnvelope(root)
	<-requester.started
	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))
	close(requester.release)
	require.Eventually(t, func() bool {
		service.resolver.mu.Lock()
		defer service.resolver.mu.Unlock()
		return len(service.resolver.jobs) == 0
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, service.pendingCount.Load())
}

func TestExecutionPayloadServiceUnknownRootFloodRetainsNothing(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	for i := range maxEnvelopeResolverJobs * 2 {
		root := common.Hash{byte(i), byte(i >> 8)}
		require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, root, uint64(i))), ErrIgnore)
	}
	require.Zero(t, impl.pendingCount.Load())
}

func TestEnvelopeResolverDoesNotRefetchWhileAuthenticatedRetryOwnsRoot(t *testing.T) {
	root := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, root, 2)
	requester := &envelopeRequesterStub{responses: [][]*cltypes.SignedExecutionPayloadEnvelope{{envelope}}}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	var terminal atomic.Bool
	fcu.OnExecutionPayloadFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		if terminal.Load() {
			return errors.New("terminal invalid envelope")
		}
		return forkchoice.ErrEIP7594ColumnDataNotAvailable
	}
	service := NewExecutionPayloadService(t.Context(), fcu, &clparams.MainnetBeaconConfig, beaconevents.NewEventEmitter(), requester)
	service.resolver.deadline = time.Second
	service.resolver.retry = 5 * time.Millisecond
	service.ResolveExecutionPayloadEnvelope(root)
	require.Eventually(t, func() bool { return service.pendingCount.Load() == 1 }, time.Second, time.Millisecond)
	for range 100 {
		service.ResolveExecutionPayloadEnvelope(root)
	}
	select {
	case <-time.After(40 * time.Millisecond):
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	require.Equal(t, int32(1), requester.calls.Load())

	terminal.Store(true)
	key := pendingEnvelopeKeyForTest(t, envelope)
	value, ok := service.pendingEnvelopes.Load(key)
	require.True(t, ok)
	service.pendingMu.Lock()
	value.(*envelopeJob).nextAttempt = time.Time{}
	service.pendingMu.Unlock()
	service.processPendingEnvelopes(t.Context())
	require.Eventually(t, func() bool { return requester.calls.Load() >= 2 }, time.Second, time.Millisecond)
}
