package sync

import (
	"context"
	"errors"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

// Heimdall is a wrapper of Heimdall HTTP API
//
//go:generate mockgen -destination=./heimdall_mock.go -package=sync . Heimdall
type Heimdall interface {
	FetchCheckpoints(ctx context.Context, start uint64) (heimdall.HashAccumulators, error)
	FetchMilestones(ctx context.Context, start uint64) (heimdall.HashAccumulators, error)
	FetchSpan(ctx context.Context, start uint64) (*heimdall.HeimdallSpan, error)
	OnMilestoneEvent(ctx context.Context, callback func(*heimdall.Milestone)) error
}

// ErrIncompleteMilestoneRange happens when FetchMilestones is called with an old start block because old milestones are evicted
var ErrIncompleteMilestoneRange = errors.New("milestone range doesn't contain the start block")

type HeimdallImpl struct {
	client    heimdall.HeimdallClient
	pollDelay time.Duration
	logger    log.Logger
}

func NewHeimdall(client heimdall.HeimdallClient, logger log.Logger) Heimdall {
	impl := HeimdallImpl{
		client:    client,
		pollDelay: time.Second,
		logger:    logger,
	}
	return &impl
}

func (impl *HeimdallImpl) FetchCheckpoints(ctx context.Context, start uint64) (heimdall.HashAccumulators, error) {
	count, err := impl.client.FetchCheckpointCount(ctx)
	if err != nil {
		return nil, err
	}

	var checkpoints []heimdall.HashAccumulator

	for i := count; i >= 1; i-- {
		c, err := impl.client.FetchCheckpoint(ctx, i)
		if err != nil {
			return nil, err
		}

		cmpResult := c.CmpRange(start)
		// the start block is past the last checkpoint
		if cmpResult > 0 {
			return nil, nil
		}

		checkpoints = append(checkpoints, c)

		// the checkpoint contains the start block
		if cmpResult == 0 {
			break
		}
	}

	common.SliceReverse(checkpoints)
	return checkpoints, nil
}

func (impl *HeimdallImpl) FetchMilestones(ctx context.Context, start uint64) (heimdall.HashAccumulators, error) {
	count, err := impl.client.FetchMilestoneCount(ctx)
	if err != nil {
		return nil, err
	}

	var milestones heimdall.HashAccumulators

	for i := count; i >= 1; i-- {
		m, err := impl.client.FetchMilestone(ctx, i)
		if err != nil {
			if errors.Is(err, heimdall.ErrNotInMilestoneList) {
				common.SliceReverse(milestones)
				return milestones, ErrIncompleteMilestoneRange
			}
			return nil, err
		}

		cmpResult := m.CmpRange(start)
		// the start block is past the last milestone
		if cmpResult > 0 {
			return nil, nil
		}

		milestones = append(milestones, m)

		// the checkpoint contains the start block
		if cmpResult == 0 {
			break
		}
	}

	common.SliceReverse(milestones)
	return milestones, nil
}

func (impl *HeimdallImpl) FetchSpan(ctx context.Context, start uint64) (*heimdall.HeimdallSpan, error) {
	return impl.client.Span(ctx, bor.SpanIDAt(start))
}

func (impl *HeimdallImpl) OnMilestoneEvent(ctx context.Context, callback func(*heimdall.Milestone)) error {
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
				m, err := impl.client.FetchMilestone(ctx, count)
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
