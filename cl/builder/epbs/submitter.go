package epbs

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
)

// GossipPublisher is the subset of the gossip manager needed by the submitter.
type GossipPublisher interface {
	Publish(ctx context.Context, name string, data []byte) error
}

// BidSubmitter submits signed bids to the network and broadcasts payloads.
type BidSubmitter interface {
	// SubmitBid adds the bid to the local pool and publishes it on gossip.
	SubmitBid(ctx context.Context, bid *cltypes.SignedExecutionPayloadBid) error

	// BroadcastPayload publishes the signed envelope on gossip and processes
	// it through forkchoice so the local node transitions to FULL status.
	BroadcastPayload(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope) error
}

// CaplinBidSubmitter implements BidSubmitter using the Caplin gossip and forkchoice.
type CaplinBidSubmitter struct {
	epbsPool      *pool.EpbsPool
	gossipManager GossipPublisher
	forkchoice    forkchoice.ForkChoiceStorageWriter
}

// NewCaplinBidSubmitter creates a CaplinBidSubmitter.
func NewCaplinBidSubmitter(
	epbsPool *pool.EpbsPool,
	gossipManager GossipPublisher,
	fc forkchoice.ForkChoiceStorageWriter,
) *CaplinBidSubmitter {
	return &CaplinBidSubmitter{
		epbsPool:      epbsPool,
		gossipManager: gossipManager,
		forkchoice:    fc,
	}
}

// SubmitBid adds the bid to the local highest-bids pool and publishes it on
// the execution_payload_bid gossip topic.
func (s *CaplinBidSubmitter) SubmitBid(ctx context.Context, bid *cltypes.SignedExecutionPayloadBid) error {
	if bid == nil || bid.Message == nil {
		return fmt.Errorf("epbs/submitter: nil bid")
	}

	key := pool.HighestBidKey{
		Slot:            bid.Message.Slot,
		ParentBlockHash: bid.Message.ParentBlockHash,
		ParentBlockRoot: bid.Message.ParentBlockRoot,
	}
	s.epbsPool.HighestBids.Add(key, bid)

	encodedSSZ, err := bid.EncodeSSZ(nil)
	if err != nil {
		return fmt.Errorf("epbs/submitter: encode bid: %w", err)
	}

	if err := s.gossipManager.Publish(ctx, gossip.TopicNameExecutionPayloadBid, encodedSSZ); err != nil {
		return fmt.Errorf("epbs/submitter: publish bid: %w", err)
	}

	return nil
}

// BroadcastPayload publishes the signed execution payload envelope on the
// execution_payload gossip topic and processes it through forkchoice so the
// local node transitions the block from PENDING to FULL status.
// Pattern follows broadcastSelfBuildEnvelope in block_production.go.
func (s *CaplinBidSubmitter) BroadcastPayload(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if envelope == nil || envelope.Message == nil {
		return fmt.Errorf("epbs/submitter: nil envelope")
	}

	// Process through forkchoice first (local state transition).
	// checkBlobData=false, validatePayload=true (send to EL via NewPayload).
	// This may return an error if OnBlock hasn't finished yet — the forkchoice
	// store queues the envelope in pendingEnvelopes for later processing.
	if err := s.forkchoice.OnExecutionPayload(ctx, envelope, false, true); err != nil {
		// Non-fatal: envelope is queued for pending processing.
		_ = err
	}

	// Broadcast on gossip
	encodedSSZ, err := envelope.EncodeSSZ(nil)
	if err != nil {
		return fmt.Errorf("epbs/submitter: encode envelope: %w", err)
	}

	if err := s.gossipManager.Publish(ctx, gossip.TopicNameExecutionPayload, encodedSSZ); err != nil {
		return fmt.Errorf("epbs/submitter: publish envelope: %w", err)
	}

	return nil
}
