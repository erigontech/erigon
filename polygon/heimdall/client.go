package heimdall

import (
	"context"
	"time"
)

//go:generate mockgen -typed=true -destination=./client_mock.go -package=heimdall . Client
type Client interface {
	FetchStateSyncEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*EventRecordWithTime, error)
	FetchStateSyncEvent(ctx context.Context, id uint64) (*EventRecordWithTime, error)

	FetchLatestSpan(ctx context.Context) (*Span, error)
	FetchSpan(ctx context.Context, spanID uint64) (*Span, error)
	FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*Span, error)

	FetchCheckpoint(ctx context.Context, number int64) (*Checkpoint, error)
	FetchCheckpointCount(ctx context.Context) (int64, error)
	FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error)

	FetchMilestone(ctx context.Context, number int64) (*Milestone, error)
	FetchMilestoneCount(ctx context.Context) (int64, error)
	FetchFirstMilestoneNum(ctx context.Context) (int64, error)

	// FetchNoAckMilestone fetches a bool value whether milestone corresponding to the given id failed in the Heimdall
	FetchNoAckMilestone(ctx context.Context, milestoneID string) error

	// FetchLastNoAckMilestone fetches the latest failed milestone id
	FetchLastNoAckMilestone(ctx context.Context) (string, error)

	// FetchMilestoneID fetches a bool value whether milestone corresponding to the given id is in process in Heimdall
	FetchMilestoneID(ctx context.Context, milestoneID string) error

	Close()
}
