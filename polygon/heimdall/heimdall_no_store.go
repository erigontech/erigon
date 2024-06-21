package heimdall

import (
	"context"

	"github.com/ledgerwatch/log/v3"
)

//go:generate mockgen -typed=true -destination=./heimdall_no_store_mock.go -package=heimdall . HeimdallNoStore
type HeimdallNoStore interface {
	LastCheckpointId(ctx context.Context) (CheckpointId, bool, error)
	LastMilestoneId(ctx context.Context) (MilestoneId, bool, error)
	LastSpanId(ctx context.Context) (SpanId, bool, error)
	FetchLatestSpan(ctx context.Context) (*Span, error)

	FetchCheckpoints(ctx context.Context, start CheckpointId, end CheckpointId) ([]*Checkpoint, error)
	FetchMilestones(ctx context.Context, start MilestoneId, end MilestoneId) ([]*Milestone, error)
	FetchSpans(ctx context.Context, start SpanId, end SpanId) ([]*Span, error)

	FetchCheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)
	FetchMilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)
	FetchSpansFromBlock(ctx context.Context, startBlock uint64) ([]*Span, error)

	OnCheckpointEvent(ctx context.Context, callback func(*Checkpoint)) error
	OnMilestoneEvent(ctx context.Context, callback func(*Milestone)) error
	OnSpanEvent(ctx context.Context, callback func(*Span)) error
}

type heimdallNoStore struct {
	Heimdall
}

type noopStore struct {
}

func (s noopStore) LastCheckpointId(ctx context.Context) (CheckpointId, bool, error) {
	return 0, false, nil
}
func (s noopStore) GetCheckpoint(ctx context.Context, checkpointId CheckpointId) (*Checkpoint, error) {
	return nil, nil
}
func (s noopStore) PutCheckpoint(ctx context.Context, checkpointId CheckpointId, checkpoint *Checkpoint) error {
	return nil
}
func (s noopStore) LastMilestoneId(ctx context.Context) (MilestoneId, bool, error) {
	return 0, false, nil
}
func (s noopStore) GetMilestone(ctx context.Context, milestoneId MilestoneId) (*Milestone, error) {
	return nil, nil
}
func (s noopStore) PutMilestone(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error {
	return nil
}
func (s noopStore) LastSpanId(ctx context.Context) (SpanId, bool, error) {
	return 0, false, nil
}
func (s noopStore) GetSpan(ctx context.Context, spanId SpanId) (*Span, error) {
	return nil, nil
}
func (s noopStore) PutSpan(ctx context.Context, span *Span) error {
	return nil
}

func NewHeimdallNoStore(client HeimdallClient, logger log.Logger) HeimdallNoStore {
	h := heimdallNoStore{
		NewHeimdall(client, logger),
	}
	return &h
}

func (h *heimdallNoStore) LastCheckpointId(ctx context.Context) (CheckpointId, bool, error) {
	return h.Heimdall.LastCheckpointId(ctx, noopStore{})
}

func (h *heimdallNoStore) LastMilestoneId(ctx context.Context) (MilestoneId, bool, error) {
	return h.Heimdall.LastMilestoneId(ctx, noopStore{})
}

func (h *heimdallNoStore) LastSpanId(ctx context.Context) (SpanId, bool, error) {
	return h.Heimdall.LastSpanId(ctx, noopStore{})
}

func (h *heimdallNoStore) FetchLatestSpan(ctx context.Context) (*Span, error) {
	return h.Heimdall.FetchLatestSpan(ctx, noopStore{})
}

func (h *heimdallNoStore) FetchCheckpoints(ctx context.Context, start CheckpointId, end CheckpointId) ([]*Checkpoint, error) {
	return h.Heimdall.FetchCheckpoints(ctx, noopStore{}, start, end)
}

func (h *heimdallNoStore) FetchMilestones(ctx context.Context, start MilestoneId, end MilestoneId) ([]*Milestone, error) {
	return h.Heimdall.FetchMilestones(ctx, noopStore{}, start, end)
}

func (h *heimdallNoStore) FetchSpans(ctx context.Context, start SpanId, end SpanId) ([]*Span, error) {
	return h.Heimdall.FetchSpans(ctx, noopStore{}, start, end)
}

func (h *heimdallNoStore) FetchCheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	return h.Heimdall.FetchCheckpointsFromBlock(ctx, noopStore{}, startBlock)
}

func (h *heimdallNoStore) FetchMilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	return h.Heimdall.FetchMilestonesFromBlock(ctx, noopStore{}, startBlock)
}

func (h *heimdallNoStore) FetchSpansFromBlock(ctx context.Context, startBlock uint64) ([]*Span, error) {
	return h.Heimdall.FetchSpansFromBlock(ctx, noopStore{}, startBlock)
}

func (h *heimdallNoStore) OnCheckpointEvent(ctx context.Context, callback func(*Checkpoint)) error {
	return h.Heimdall.OnCheckpointEvent(ctx, noopStore{}, callback)
}

func (h *heimdallNoStore) OnMilestoneEvent(ctx context.Context, callback func(*Milestone)) error {
	return h.Heimdall.OnMilestoneEvent(ctx, noopStore{}, callback)
}

func (h *heimdallNoStore) OnSpanEvent(ctx context.Context, callback func(*Span)) error {
	return h.Heimdall.OnSpanEvent(ctx, noopStore{}, callback)
}
