package epbs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
)

var ErrBidNotPublished = errors.New("bid was not published")

const maxConcurrentColumnWrites = 4

// GossipPublisher is the subset of the gossip manager needed by the submitter.
type GossipPublisher interface {
	Publish(ctx context.Context, name string, data []byte) error
}

type ColumnSidecarStorage interface {
	WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error
}

// BidSubmitter submits signed bids to the network and broadcasts payloads.
type BidSubmitter interface {
	// SubmitBid adds the bid to the local pool and publishes it on gossip.
	SubmitBid(ctx context.Context, bid *cltypes.SignedExecutionPayloadBid) error

	// BroadcastPayload publishes the signed envelope on gossip and processes
	// it through forkchoice so the local node transitions to FULL status.
	BroadcastPayload(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, columnSidecars []*cltypes.DataColumnSidecar) error
}

// CaplinBidSubmitter implements BidSubmitter using the Caplin gossip and forkchoice.
type CaplinBidSubmitter struct {
	epbsPool      *pool.EpbsPool
	gossipManager GossipPublisher
	forkchoice    executionPayloadProcessor
	columnStorage ColumnSidecarStorage
	progressMu    sync.Mutex
	progress      map[common.Hash]*payloadBroadcastProgress
	columnWrites  chan struct{}
}

type payloadBroadcastProgress struct {
	mu               sync.Mutex
	processed        bool
	envelope         bool
	storedColumns    map[uint64]bool
	publishedColumns map[uint64]bool
	completed        bool
}

type executionPayloadProcessor interface {
	OnExecutionPayload(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error
	ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error)
}

// NewCaplinBidSubmitter creates a CaplinBidSubmitter.
func NewCaplinBidSubmitter(
	epbsPool *pool.EpbsPool,
	gossipManager GossipPublisher,
	fc executionPayloadProcessor,
	columnStorage ColumnSidecarStorage,
) *CaplinBidSubmitter {
	return &CaplinBidSubmitter{
		epbsPool:      epbsPool,
		gossipManager: gossipManager,
		forkchoice:    fc,
		columnStorage: columnStorage,
		progress:      make(map[common.Hash]*payloadBroadcastProgress),
		columnWrites:  make(chan struct{}, maxConcurrentColumnWrites),
	}
}

// SubmitBid adds the bid to the local highest-bids pool and publishes it on
// the execution_payload_bid gossip topic.
func (s *CaplinBidSubmitter) SubmitBid(ctx context.Context, bid *cltypes.SignedExecutionPayloadBid) error {
	if bid == nil || bid.Message == nil {
		return fmt.Errorf("%w: nil bid", ErrBidNotPublished)
	}
	if !s.epbsPool.WouldIncreaseHighestBid(bid) {
		return fmt.Errorf("%w: bid value %d does not exceed highest seen", ErrBidNotPublished, bid.Message.Value)
	}

	encodedSSZ, err := bid.EncodeSSZ(nil)
	if err != nil {
		return fmt.Errorf("%w: encode bid: %w", ErrBidNotPublished, err)
	}

	if err := s.gossipManager.Publish(ctx, gossip.TopicNameExecutionPayloadBid, encodedSSZ); err != nil {
		if errors.Is(err, gossip.ErrNotPublished) {
			return fmt.Errorf("%w: publish bid: %w", ErrBidNotPublished, err)
		}
		return fmt.Errorf("epbs/submitter: publish bid: %w", err)
	}
	s.epbsPool.AddHighestBid(bid)

	return nil
}

// BroadcastPayload publishes the envelope and advances the local block to FULL.
func (s *CaplinBidSubmitter) BroadcastPayload(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, columnSidecars []*cltypes.DataColumnSidecar) error {
	if envelope == nil || envelope.Message == nil {
		return fmt.Errorf("epbs/submitter: nil envelope")
	}
	root := envelope.Message.BeaconBlockRoot
	s.progressMu.Lock()
	progress := s.progress[root]
	if progress == nil {
		progress = &payloadBroadcastProgress{
			storedColumns:    make(map[uint64]bool),
			publishedColumns: make(map[uint64]bool),
		}
		s.progress[root] = progress
	}
	s.progressMu.Unlock()
	progress.mu.Lock()
	defer progress.mu.Unlock()
	if progress.completed {
		return nil
	}

	for _, column := range columnSidecars {
		if column == nil || progress.storedColumns[column.Index] {
			continue
		}
		if s.columnStorage == nil {
			return fmt.Errorf("epbs/submitter: data column storage unavailable")
		}
		if column.Index > math.MaxInt64 {
			return fmt.Errorf("epbs/submitter: data column sidecar index %d exceeds storage range", column.Index)
		}
		if err := s.writeColumnSidecar(ctx, root, int64(column.Index), column); err != nil {
			return fmt.Errorf("epbs/submitter: store data column sidecar %d: %w", column.Index, err)
		}
		progress.storedColumns[column.Index] = true
	}

	if !progress.processed {
		if err := s.forkchoice.OnExecutionPayload(ctx, envelope, false, true); err != nil {
			if !errors.Is(err, forkchoice.ErrIgnore) || !s.persistedEnvelopeMatches(root, envelope) {
				return fmt.Errorf("epbs/submitter: process payload: %w", err)
			}
		}
		progress.processed = true
	}

	if !progress.envelope {
		encodedSSZ, err := envelope.EncodeSSZ(nil)
		if err != nil {
			return fmt.Errorf("epbs/submitter: encode envelope: %w", err)
		}
		if err := s.gossipManager.Publish(ctx, gossip.TopicNameExecutionPayload, encodedSSZ); err != nil {
			return fmt.Errorf("epbs/submitter: publish envelope: %w", err)
		}
		progress.envelope = true
	}

	for _, column := range columnSidecars {
		if column == nil || progress.publishedColumns[column.Index] {
			continue
		}
		columnSSZ, err := column.EncodeSSZ(nil)
		if err != nil {
			return fmt.Errorf("epbs/submitter: encode data column sidecar %d: %w", column.Index, err)
		}
		subnet := das.ComputeSubnetForDataColumnSidecar(column.Index)
		if err := s.gossipManager.Publish(ctx, gossip.TopicNameDataColumnSidecar(subnet), columnSSZ); err != nil {
			return fmt.Errorf("epbs/submitter: publish data column sidecar %d: %w", column.Index, err)
		}
		progress.publishedColumns[column.Index] = true
	}
	progress.completed = true
	s.progressMu.Lock()
	if s.progress[root] == progress {
		delete(s.progress, root)
	}
	s.progressMu.Unlock()

	return nil
}

func (s *CaplinBidSubmitter) persistedEnvelopeMatches(root common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) bool {
	persisted, err := s.forkchoice.ReadEnvelopeFromDisk(root)
	if err != nil || !validEnvelopeForIdentity(persisted) || !validEnvelopeForIdentity(envelope) {
		return false
	}
	persistedRoot, err := persisted.HashSSZ()
	if err != nil {
		return false
	}
	envelopeRoot, err := envelope.HashSSZ()
	return err == nil && persistedRoot == envelopeRoot
}

func validEnvelopeForIdentity(envelope *cltypes.SignedExecutionPayloadEnvelope) bool {
	return envelope != nil && envelope.Message != nil && envelope.Message.Payload != nil && envelope.Message.ExecutionRequests != nil
}

func (s *CaplinBidSubmitter) writeColumnSidecar(ctx context.Context, root common.Hash, index int64, column *cltypes.DataColumnSidecar) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.columnWrites <- struct{}{}:
	}
	defer func() { <-s.columnWrites }()
	return s.columnStorage.WriteColumnSidecars(ctx, root, index, column)
}

func (s *CaplinBidSubmitter) discardPayloadBroadcast(root common.Hash) {
	s.progressMu.Lock()
	delete(s.progress, root)
	s.progressMu.Unlock()
}
