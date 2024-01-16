package heimdall

import (
	"context"

	"github.com/ledgerwatch/erigon/polygon/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/polygon/heimdall/milestone"
	"github.com/ledgerwatch/erigon/polygon/heimdall/span"

	"github.com/ledgerwatch/erigon/polygon/bor/clerk"
	"github.com/ledgerwatch/erigon/polygon/bor/finality/generics"
)

func MilestoneRewindPending() bool {
	return generics.BorMilestoneRewind.Load() != nil && *generics.BorMilestoneRewind.Load() != 0
}

//go:generate mockgen -destination=./mock/heimdall_client_mock.go -package=mock . IHeimdallClient
type IHeimdallClient interface {
	StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*clerk.EventRecordWithTime, error)
	Span(ctx context.Context, spanID uint64) (*span.HeimdallSpan, error)
	FetchCheckpoint(ctx context.Context, number int64) (*checkpoint.Checkpoint, error)
	FetchCheckpointCount(ctx context.Context) (int64, error)
	FetchMilestone(ctx context.Context, number int64) (*milestone.Milestone, error)
	FetchMilestoneCount(ctx context.Context) (int64, error)
	FetchNoAckMilestone(ctx context.Context, milestoneID string) error //Fetch the bool value whether milestone corresponding to the given id failed in the Heimdall
	FetchLastNoAckMilestone(ctx context.Context) (string, error)       //Fetch latest failed milestone id
	FetchMilestoneID(ctx context.Context, milestoneID string) error    //Fetch the bool value whether milestone corresponding to the given id is in process in Heimdall
	Close()
}
