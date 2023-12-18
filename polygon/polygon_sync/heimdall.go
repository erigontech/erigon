package polygon_sync

import (
	"context"
	"errors"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/log/v3"
	"time"
)

// Heimdall is a wrapper of Heimdall HTTP API
type Heimdall interface {
	FetchCheckpoints(ctx context.Context, start uint64) ([]*checkpoint.Checkpoint, error)
	FetchMilestones(ctx context.Context, start uint64) ([]*milestone.Milestone, error)
	FetchSpan(ctx context.Context) (*span.HeimdallSpan, error)
	OnMilestoneEvent(ctx context.Context, callback func(*milestone.Milestone)) error
}

type HeimdallImpl struct {
	client    heimdall.IHeimdallClient
	pollDelay time.Duration
	logger    log.Logger
}

func NewHeimdall(client heimdall.IHeimdallClient, logger log.Logger) Heimdall {
	impl := HeimdallImpl{
		client:    client,
		pollDelay: time.Second,
		logger:    logger,
	}
	return &impl
}

func checkpointNumContainingBlockNum(n uint64) int64 {
	// TODO: implement
	return 1
}

func milestoneNumContainingBlockNum(n uint64) int64 {
	// TODO: implement
	return 1
}

func (impl *HeimdallImpl) FetchCheckpoints(ctx context.Context, start uint64) ([]*checkpoint.Checkpoint, error) {
	startCheckpoint := checkpointNumContainingBlockNum(start)
	count, err := impl.client.FetchCheckpointCount(ctx)
	if err != nil {
		return nil, err
	}

	checkpoints := make([]*checkpoint.Checkpoint, 0, count-startCheckpoint)

	for i := startCheckpoint; i <= count; i++ {
		c, err := impl.client.FetchCheckpoint(ctx, i)
		if err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, c)
	}
	return checkpoints, nil
}

func (impl *HeimdallImpl) FetchMilestones(ctx context.Context, start uint64) ([]*milestone.Milestone, error) {
	startMilestone := milestoneNumContainingBlockNum(start)
	count, err := impl.client.FetchMilestoneCount(ctx)
	if err != nil {
		return nil, err
	}

	milestones := make([]*milestone.Milestone, 0, count-startMilestone)

	for i := startMilestone; i <= count; i++ {
		// TODO: how to pass i ?
		m, err := impl.client.FetchMilestone(ctx)
		if err != nil {
			return nil, err
		}
		milestones = append(milestones, m)
	}
	return milestones, nil
}

func (impl *HeimdallImpl) FetchSpan(ctx context.Context) (*span.HeimdallSpan, error) {
	// TODO: calc last spanID
	return impl.client.Span(ctx, 0)
}

func (impl *HeimdallImpl) OnMilestoneEvent(ctx context.Context, callback func(*milestone.Milestone)) error {
	currentCount, err := impl.client.FetchMilestoneCount(ctx)
	if err != nil {
		return err
	}

	go func() {
		for {
			count, err := impl.client.FetchMilestoneCount(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					impl.logger.Error("HeimdallImpl.OnMilestoneEvent FetchMilestoneCount error", "err", err)
				}
				break
			}

			if count <= currentCount {
				pollDelayTimer := time.NewTimer(impl.pollDelay)
				select {
				case <-ctx.Done():
					return
				case <-pollDelayTimer.C:
				}
			} else {
				currentCount = count
				m, err := impl.client.FetchMilestone(ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						impl.logger.Error("HeimdallImpl.OnMilestoneEvent FetchMilestone error", "err", err)
					}
					break
				}

				go callback(m)
			}
		}
	}()

	return nil
}
