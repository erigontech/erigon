package epbs

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type testEnvelopeProcessor struct {
	err              error
	errors           []error
	calls            *int
	persisted        *cltypes.SignedExecutionPayloadEnvelope
	readErr          error
	headRoot         common.Hash
	headHeader       *cltypes.BeaconBlockHeader
	headBid          *cltypes.ExecutionPayloadBid
	buildOnFull      bool
	compatibilityErr error
}

func (p testEnvelopeProcessor) OnExecutionPayload(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
	if p.calls != nil {
		*p.calls++
		if len(p.errors) >= *p.calls {
			return p.errors[*p.calls-1]
		}
	}
	return p.err
}

func (p testEnvelopeProcessor) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return p.persisted, p.readErr
}

func (p testEnvelopeProcessor) IsBidCompatibleWithHead(bid *cltypes.ExecutionPayloadBid) (bool, error) {
	if p.compatibilityErr != nil {
		return false, p.compatibilityErr
	}
	return forkchoice.BidCompatibleWithHead(bid, p.headRoot, p.headHeader, p.headBid, p.buildOnFull), nil
}

type testGossipPublisher struct {
	published int
	err       error
	errors    map[int]error
	topics    []string
}

type blockingBidPublisher struct {
	started chan struct{}
	release chan struct{}
}

type testColumnStorage struct {
	writes map[uint64]int
	errors map[uint64]error
}

type blockingColumnStorage struct {
	started chan struct{}
}

type cancelCleanupColumnStorage struct {
	started chan struct{}
	cleanup chan struct{}
	release chan struct{}
}

func (s *blockingColumnStorage) WriteColumnSidecars(ctx context.Context, _ common.Hash, _ int64, _ *cltypes.DataColumnSidecar) error {
	s.started <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

func (s *cancelCleanupColumnStorage) WriteColumnSidecars(ctx context.Context, _ common.Hash, _ int64, _ *cltypes.DataColumnSidecar) error {
	close(s.started)
	<-ctx.Done()
	close(s.cleanup)
	<-s.release
	return ctx.Err()
}

func (s *testColumnStorage) WriteColumnSidecars(_ context.Context, _ common.Hash, columnIndex int64, _ *cltypes.DataColumnSidecar) error {
	if columnIndex < 0 {
		return fmt.Errorf("negative column index")
	}
	index := uint64(columnIndex)
	s.writes[index]++
	return s.errors[index]
}

func (p *testGossipPublisher) Publish(_ context.Context, topic string, _ []byte) error {
	p.published++
	p.topics = append(p.topics, topic)
	if err := p.errors[p.published]; err != nil {
		return err
	}
	return p.err
}

func (p *blockingBidPublisher) Publish(context.Context, string, []byte) error {
	close(p.started)
	<-p.release
	return nil
}

func TestCaplinBidSubmitter_SubmitBidDoesNotStoreUnpublishedBid(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	gossipPublisher := &testGossipPublisher{err: fmt.Errorf("%w: publish failed", gossip.ErrNotPublished)}
	bid := &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		Slot:               100,
		ParentBlockHash:    common.HexToHash("0x1111"),
		ParentBlockRoot:    common.HexToHash("0x2222"),
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48),
	}}
	submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, testProcessorForBid(bid), nil)

	err := submitter.SubmitBid(context.Background(), bid)
	require.ErrorContains(t, err, "publish failed")
	require.ErrorIs(t, err, ErrBidNotPublished)
	_, stored := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: bid.Message.ParentBlockHash, ParentBlockRoot: bid.Message.ParentBlockRoot})
	require.False(t, stored)
}

func TestCaplinBidSubmitterSubmitBidRejectsParentAfterHeadChanges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		processor testEnvelopeProcessor
	}{
		{name: "beacon root changed", processor: testEnvelopeProcessor{
			headRoot:   common.HexToHash("0xaaaa"),
			headHeader: &cltypes.BeaconBlockHeader{ParentRoot: common.HexToHash("0xbbbb")},
			headBid:    &cltypes.ExecutionPayloadBid{ParentBlockHash: common.HexToHash("0x1111")},
		}},
		{name: "execution hash changed", processor: testEnvelopeProcessor{
			headRoot:   common.HexToHash("0xaaaa"),
			headHeader: &cltypes.BeaconBlockHeader{ParentRoot: common.HexToHash("0x2222")},
			headBid:    &cltypes.ExecutionPayloadBid{ParentBlockHash: common.HexToHash("0xbbbb")},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epbsPool := pool.NewEpbsPool()
			gossipPublisher := &testGossipPublisher{}
			submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, tc.processor, nil)
			bid := testSignedBid(100, 1, 1000)

			err := submitter.SubmitBid(t.Context(), bid)

			require.ErrorIs(t, err, ErrBidNotPublished)
			require.Zero(t, gossipPublisher.published)
			_, stored := epbsPool.HighestBids.Get(pool.HighestBidKey{
				Slot:            bid.Message.Slot,
				ParentBlockHash: bid.Message.ParentBlockHash,
				ParentBlockRoot: bid.Message.ParentBlockRoot,
			})
			require.False(t, stored)
		})
	}
}

func TestCaplinBidSubmitterSubmitBidAcceptsActiveFullAndEmptyParents(t *testing.T) {
	for _, tc := range []struct {
		name        string
		slot        uint64
		buildOnFull bool
	}{
		{name: "current slot full parent", slot: 100, buildOnFull: true},
		{name: "next slot empty parent", slot: 101, buildOnFull: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epbsPool := pool.NewEpbsPool()
			gossipPublisher := &testGossipPublisher{}
			bid := testSignedBid(tc.slot, 1, 1000)
			processor := testEnvelopeProcessor{
				headRoot:    bid.Message.ParentBlockRoot,
				headHeader:  &cltypes.BeaconBlockHeader{},
				headBid:     &cltypes.ExecutionPayloadBid{ParentBlockHash: bid.Message.ParentBlockHash, BlockHash: common.HexToHash("0x4444")},
				buildOnFull: tc.buildOnFull,
			}
			if tc.buildOnFull {
				bid.Message.ParentBlockHash = processor.headBid.BlockHash
			}
			submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, processor, nil)

			require.NoError(t, submitter.SubmitBid(t.Context(), bid))
			require.Equal(t, 1, gossipPublisher.published)
		})
	}
}

func TestCaplinBidSubmitterSubmitBidAcceptsParentOfLateHead(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	gossipPublisher := &testGossipPublisher{}
	bid := testSignedBid(100, 1, 1000)
	processor := testEnvelopeProcessor{
		headRoot:    common.HexToHash("0x9999"),
		headHeader:  &cltypes.BeaconBlockHeader{Slot: 99, ParentRoot: bid.Message.ParentBlockRoot},
		headBid:     &cltypes.ExecutionPayloadBid{ParentBlockHash: bid.Message.ParentBlockHash, BlockHash: common.HexToHash("0xaaaa")},
		buildOnFull: true,
	}
	submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, processor, nil)

	require.NoError(t, submitter.SubmitBid(t.Context(), bid))
	require.Equal(t, 1, gossipPublisher.published)
}

func TestCaplinBidSubmitterSubmitBidFailsClosedWithoutCompatibleHead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		processor executionPayloadProcessor
	}{
		{name: "forkchoice unavailable"},
		{name: "no compatible head snapshot", processor: testEnvelopeProcessor{}},
		{name: "head lookup failed", processor: testEnvelopeProcessor{compatibilityErr: errors.New("head unavailable")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			epbsPool := pool.NewEpbsPool()
			gossipPublisher := &testGossipPublisher{}
			submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, tc.processor, nil)

			err := submitter.SubmitBid(t.Context(), testSignedBid(100, 1, 1000))

			require.ErrorIs(t, err, ErrBidNotPublished)
			require.Zero(t, gossipPublisher.published)
		})
	}
}

func TestCaplinBidSubmitterSubmitBidDoesNotPublishBelowHighestSeenBid(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	gossipPublisher := &testGossipPublisher{}
	submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, testEnvelopeProcessor{}, nil)
	high := testSignedBid(100, 2, 2000)
	low := testSignedBid(100, 1, 1000)
	key := pool.HighestBidKey{Slot: 100, ParentBlockHash: high.Message.ParentBlockHash, ParentBlockRoot: high.Message.ParentBlockRoot}
	epbsPool.HighestBids.Add(key, high)

	err := submitter.SubmitBid(t.Context(), low)

	require.ErrorIs(t, err, ErrBidNotPublished)
	require.Zero(t, gossipPublisher.published)
	stored, found := epbsPool.HighestBids.Get(key)
	require.True(t, found)
	require.Same(t, high, stored)
}

func TestCaplinBidSubmitterSubmitBidReplacesLowerHighestSeenBid(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	gossipPublisher := &testGossipPublisher{}
	low := testSignedBid(100, 1, 1000)
	high := testSignedBid(100, 2, 2000)
	submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, testProcessorForBid(high), nil)

	require.NoError(t, submitter.SubmitBid(t.Context(), low))
	require.NoError(t, submitter.SubmitBid(t.Context(), high))

	require.Equal(t, 2, gossipPublisher.published)
	stored, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: high.Message.ParentBlockHash, ParentBlockRoot: high.Message.ParentBlockRoot})
	require.True(t, found)
	require.Same(t, high, stored)
}

func TestCaplinBidSubmitterSubmitBidDoesNotReplaceEqualHighestSeenBid(t *testing.T) {
	for _, builderIndex := range []uint64{1, 2} {
		t.Run(fmt.Sprintf("builder_%d", builderIndex), func(t *testing.T) {
			epbsPool := pool.NewEpbsPool()
			gossipPublisher := &testGossipPublisher{}
			submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, testEnvelopeProcessor{}, nil)
			first := testSignedBid(100, 1, 1000)
			equal := testSignedBid(100, builderIndex, 1000)
			key := pool.HighestBidKey{Slot: 100, ParentBlockHash: first.Message.ParentBlockHash, ParentBlockRoot: first.Message.ParentBlockRoot}
			epbsPool.HighestBids.Add(key, first)

			err := submitter.SubmitBid(t.Context(), equal)

			require.ErrorIs(t, err, ErrBidNotPublished)
			require.Zero(t, gossipPublisher.published)
			stored, found := epbsPool.HighestBids.Get(key)
			require.True(t, found)
			require.Same(t, first, stored)
		})
	}
}

func TestCaplinBidSubmitterSubmitBidAcceptsZeroValueInEmptyMarket(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	gossipPublisher := &testGossipPublisher{}
	bid := testSignedBid(100, 1, 0)
	submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, testProcessorForBid(bid), nil)

	require.NoError(t, submitter.SubmitBid(t.Context(), bid))

	require.Equal(t, 1, gossipPublisher.published)
	stored, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: bid.Message.ParentBlockHash, ParentBlockRoot: bid.Message.ParentBlockRoot})
	require.True(t, found)
	require.Same(t, bid, stored)
}

func TestCaplinBidSubmitterSubmitBidKeepsConcurrentHigherBid(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	publisher := &blockingBidPublisher{started: make(chan struct{}), release: make(chan struct{})}
	low := testSignedBid(100, 1, 1000)
	high := testSignedBid(100, 2, 2000)
	submitter := NewCaplinBidSubmitter(epbsPool, publisher, testProcessorForBid(low), nil)
	result := make(chan error, 1)

	go func() {
		result <- submitter.SubmitBid(t.Context(), low)
	}()
	<-publisher.started
	require.True(t, epbsPool.AddHighestBid(high))
	close(publisher.release)
	require.NoError(t, <-result)

	stored, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: high.Message.ParentBlockHash, ParentBlockRoot: high.Message.ParentBlockRoot})
	require.True(t, found)
	require.Same(t, high, stored)
}

func testSignedBid(slot, builderIndex, value uint64) *cltypes.SignedExecutionPayloadBid {
	return &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		Slot:               slot,
		BuilderIndex:       builderIndex,
		Value:              value,
		ParentBlockHash:    common.HexToHash("0x1111"),
		ParentBlockRoot:    common.HexToHash("0x2222"),
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48),
	}}
}

func testProcessorForBid(bid *cltypes.SignedExecutionPayloadBid) testEnvelopeProcessor {
	return testEnvelopeProcessor{
		headRoot:    bid.Message.ParentBlockRoot,
		headHeader:  &cltypes.BeaconBlockHeader{},
		headBid:     &cltypes.ExecutionPayloadBid{BlockHash: bid.Message.ParentBlockHash},
		buildOnFull: true,
	}
}

func TestCaplinBidSubmitter_BroadcastPayloadRejectsForkchoiceError(t *testing.T) {
	gossipPublisher := &testGossipPublisher{}
	submitter := NewCaplinBidSubmitter(nil, gossipPublisher, testEnvelopeProcessor{err: errors.New("invalid envelope")}, nil)
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")

	err := submitter.BroadcastPayload(context.Background(), envelope, nil)
	require.ErrorContains(t, err, "invalid envelope")
	require.Zero(t, gossipPublisher.published)
}

func TestCaplinBidSubmitterBroadcastPayloadRecoversAfterPersistedIndexFailure(t *testing.T) {
	publisher := &testGossipPublisher{}
	calls := 0
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	processor := testEnvelopeProcessor{
		errors:    []error{errors.New("index write failed"), forkchoice.ErrIgnore},
		calls:     &calls,
		persisted: envelope,
	}
	submitter := NewCaplinBidSubmitter(nil, publisher, processor, nil)

	require.ErrorContains(t, submitter.BroadcastPayload(t.Context(), envelope, nil), "index write failed")
	require.NoError(t, submitter.BroadcastPayload(t.Context(), envelope, nil))
	require.Equal(t, 2, calls)
	require.Equal(t, []string{gossip.TopicNameExecutionPayload}, publisher.topics)
}

func TestCaplinBidSubmitterBroadcastPayloadRecoversAfterRestart(t *testing.T) {
	publisher := &testGossipPublisher{}
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	submitter := NewCaplinBidSubmitter(nil, publisher, testEnvelopeProcessor{
		err:       forkchoice.ErrIgnore,
		persisted: envelope,
	}, nil)

	require.NoError(t, submitter.BroadcastPayload(t.Context(), envelope, nil))
	require.Equal(t, []string{gossip.TopicNameExecutionPayload}, publisher.topics)
}

func TestCaplinBidSubmitterBroadcastPayloadRejectsUnverifiedIgnore(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	different := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	different.Message.BeaconBlockRoot = envelope.Message.BeaconBlockRoot
	different.Message.Payload.BlockNumber = 1

	for _, tc := range []struct {
		name      string
		persisted *cltypes.SignedExecutionPayloadEnvelope
		readErr   error
	}{
		{name: "missing"},
		{name: "mismatch", persisted: different},
		{name: "read error", readErr: errors.New("disk unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &testGossipPublisher{}
			submitter := NewCaplinBidSubmitter(nil, publisher, testEnvelopeProcessor{
				err:       forkchoice.ErrIgnore,
				persisted: tc.persisted,
				readErr:   tc.readErr,
			}, nil)

			err := submitter.BroadcastPayload(t.Context(), envelope, nil)
			require.ErrorIs(t, err, forkchoice.ErrIgnore)
			require.Empty(t, publisher.topics)
		})
	}
}

func TestCaplinBidSubmitterBroadcastPayloadResumesAfterPublishedPrefix(t *testing.T) {
	publisher := &testGossipPublisher{errors: map[int]error{3: errors.New("column unavailable")}}
	storage := &testColumnStorage{writes: make(map[uint64]int), errors: make(map[uint64]error)}
	processorCalls := 0
	submitter := NewCaplinBidSubmitter(nil, publisher, testEnvelopeProcessor{calls: &processorCalls}, storage)
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	columns := []*cltypes.DataColumnSidecar{
		cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion),
		cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion),
	}
	columns[1].Index = 1

	require.ErrorContains(t, submitter.BroadcastPayload(t.Context(), envelope, columns), "column unavailable")
	delete(publisher.errors, 3)
	require.NoError(t, submitter.BroadcastPayload(t.Context(), envelope, columns))
	require.Equal(t, 1, processorCalls)
	require.Equal(t, 4, publisher.published)
	require.Equal(t, map[uint64]int{0: 1, 1: 1}, storage.writes)
}

func TestCaplinBidSubmitterRetriesColumnStorageBeforeGossip(t *testing.T) {
	publisher := &testGossipPublisher{}
	storage := &testColumnStorage{
		writes: make(map[uint64]int),
		errors: map[uint64]error{1: errors.New("storage unavailable")},
	}
	processorCalls := 0
	submitter := NewCaplinBidSubmitter(nil, publisher, testEnvelopeProcessor{calls: &processorCalls}, storage)
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	columns := []*cltypes.DataColumnSidecar{
		cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion),
		cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion),
	}
	columns[1].Index = 1

	require.ErrorContains(t, submitter.BroadcastPayload(t.Context(), envelope, columns), "storage unavailable")
	require.Zero(t, processorCalls)
	require.Zero(t, publisher.published)
	delete(storage.errors, 1)
	require.NoError(t, submitter.BroadcastPayload(t.Context(), envelope, columns))
	require.Equal(t, map[uint64]int{0: 1, 1: 2}, storage.writes)
	require.Equal(t, 1, processorCalls)
	require.Equal(t, 3, publisher.published)
}

func TestCaplinBidSubmitterColumnStorageCancellationDoesNotBlockCaller(t *testing.T) {
	storage := &blockingColumnStorage{started: make(chan struct{})}
	submitter := NewCaplinBidSubmitter(nil, &testGossipPublisher{}, testEnvelopeProcessor{}, storage)
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	column := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- submitter.BroadcastPayload(ctx, envelope, []*cltypes.DataColumnSidecar{column}) }()
	<-storage.started
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("column storage blocked reveal cancellation")
	}
}

func TestCaplinBidSubmitterCancellationWaitsForColumnStorageHandoff(t *testing.T) {
	storage := &cancelCleanupColumnStorage{
		started: make(chan struct{}), cleanup: make(chan struct{}), release: make(chan struct{}),
	}
	submitter := NewCaplinBidSubmitter(nil, &testGossipPublisher{}, testEnvelopeProcessor{}, storage)
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	column := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- submitter.BroadcastPayload(ctx, envelope, []*cltypes.DataColumnSidecar{column}) }()
	<-storage.started
	cancel()
	<-storage.cleanup
	select {
	case <-done:
		t.Fatal("column storage work outlived the canceled broadcast")
	default:
	}
	close(storage.release)
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestCaplinBidSubmitterBoundsBlockedColumnWrites(t *testing.T) {
	storage := &blockingColumnStorage{started: make(chan struct{}, maxConcurrentColumnWrites)}
	submitter := NewCaplinBidSubmitter(nil, &testGossipPublisher{}, testEnvelopeProcessor{}, storage)
	cfg := clparams.MainnetBeaconConfig
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, maxConcurrentColumnWrites+1)
	for i := range maxConcurrentColumnWrites + 1 {
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
		envelope.Message.BeaconBlockRoot = common.Hash{byte(i + 1)}
		column := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
		go func() { done <- submitter.BroadcastPayload(ctx, envelope, []*cltypes.DataColumnSidecar{column}) }()
	}
	for range maxConcurrentColumnWrites {
		<-storage.started
	}
	require.Len(t, storage.started, 0)
	cancel()
	for range maxConcurrentColumnWrites + 1 {
		require.ErrorIs(t, <-done, context.Canceled)
	}
}

func TestCaplinBidSubmitterDiscardPayloadBroadcastProgress(t *testing.T) {
	publisher := &testGossipPublisher{err: errors.New("unavailable")}
	submitter := NewCaplinBidSubmitter(nil, publisher, testEnvelopeProcessor{}, nil)
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")

	require.Error(t, submitter.BroadcastPayload(t.Context(), envelope, nil))
	require.Len(t, submitter.progress, 1)
	submitter.discardPayloadBroadcast(envelope.Message.BeaconBlockRoot)
	require.Empty(t, submitter.progress)
}
