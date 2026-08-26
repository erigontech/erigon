package epbs

import (
	"context"
	"errors"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type testEnvelopeProcessor struct {
	err   error
	calls *int
}

func (p testEnvelopeProcessor) OnExecutionPayload(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
	if p.calls != nil {
		*p.calls++
	}
	return p.err
}

type testGossipPublisher struct {
	published int
	err       error
	errors    map[int]error
	topics    []string
}

func (p *testGossipPublisher) Publish(_ context.Context, topic string, _ []byte) error {
	p.published++
	p.topics = append(p.topics, topic)
	if err := p.errors[p.published]; err != nil {
		return err
	}
	return p.err
}

func TestCaplinBidSubmitter_SubmitBidDoesNotStoreUnpublishedBid(t *testing.T) {
	epbsPool := pool.NewEpbsPool()
	gossipPublisher := &testGossipPublisher{err: errors.New("publish failed")}
	submitter := NewCaplinBidSubmitter(epbsPool, gossipPublisher, testEnvelopeProcessor{})
	bid := &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		Slot:               100,
		ParentBlockHash:    common.HexToHash("0x1111"),
		ParentBlockRoot:    common.HexToHash("0x2222"),
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48),
	}}

	err := submitter.SubmitBid(context.Background(), bid)
	require.ErrorContains(t, err, "publish failed")
	_, stored := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: bid.Message.ParentBlockHash, ParentBlockRoot: bid.Message.ParentBlockRoot})
	require.False(t, stored)
}

func TestCaplinBidSubmitter_BroadcastPayloadRejectsForkchoiceError(t *testing.T) {
	gossipPublisher := &testGossipPublisher{}
	submitter := NewCaplinBidSubmitter(nil, gossipPublisher, testEnvelopeProcessor{err: errors.New("invalid envelope")})
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")

	err := submitter.BroadcastPayload(context.Background(), envelope, nil)
	require.ErrorContains(t, err, "invalid envelope")
	require.Zero(t, gossipPublisher.published)
}

func TestCaplinBidSubmitterBroadcastPayloadResumesAfterPublishedPrefix(t *testing.T) {
	publisher := &testGossipPublisher{errors: map[int]error{3: errors.New("column unavailable")}}
	processorCalls := 0
	submitter := NewCaplinBidSubmitter(nil, publisher, testEnvelopeProcessor{calls: &processorCalls})
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
}

func TestCaplinBidSubmitterDiscardPayloadBroadcastProgress(t *testing.T) {
	publisher := &testGossipPublisher{err: errors.New("unavailable")}
	submitter := NewCaplinBidSubmitter(nil, publisher, testEnvelopeProcessor{})
	cfg := clparams.MainnetBeaconConfig
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")

	require.Error(t, submitter.BroadcastPayload(t.Context(), envelope, nil))
	require.Len(t, submitter.progress, 1)
	submitter.discardPayloadBroadcast(envelope.Message.BeaconBlockRoot)
	require.Empty(t, submitter.progress)
}
