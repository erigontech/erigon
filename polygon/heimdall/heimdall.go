package heimdall

import (
	"context"
	"errors"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
)

// Heimdall is a wrapper of Heimdall HTTP API
//
//go:generate mockgen -destination=./heimdall_mock.go -package=heimdall . Heimdall
type Heimdall interface {
	LastCheckpointId(ctx context.Context, store CheckpointStore) (CheckpointId, bool, error)
	LastMilestoneId(ctx context.Context, store MilestoneStore) (MilestoneId, bool, error)
	LastSpanId(ctx context.Context, store SpanStore) (SpanId, bool, error)
	FetchLatestSpan(ctx context.Context, store SpanStore) (*Span, error)

	FetchCheckpoints(ctx context.Context, store CheckpointStore, start CheckpointId, end CheckpointId) ([]*Checkpoint, error)
	FetchMilestones(ctx context.Context, store MilestoneStore, start MilestoneId, end MilestoneId) ([]*Milestone, error)
	FetchSpans(ctx context.Context, store SpanStore, start SpanId, end SpanId) ([]*Span, error)

	FetchCheckpointsFromBlock(ctx context.Context, store CheckpointStore, startBlock uint64) (Waypoints, error)
	FetchMilestonesFromBlock(ctx context.Context, store MilestoneStore, startBlock uint64) (Waypoints, error)
	FetchSpansFromBlock(ctx context.Context, store SpanStore, startBlock uint64) ([]*Span, error)

	OnCheckpointEvent(ctx context.Context, store CheckpointStore, callback func(*Checkpoint)) error
	OnMilestoneEvent(ctx context.Context, store MilestoneStore, callback func(*Milestone)) error
	OnSpanEvent(ctx context.Context, store SpanStore, callback func(*Span)) error
}

// ErrIncompleteMilestoneRange happens when FetchMilestones is called with an old start block because old milestones are evicted
var ErrIncompleteMilestoneRange = errors.New("milestone range doesn't contain the start block")
var ErrIncompleteCheckpointRange = errors.New("checkpoint range doesn't contain the start block")
var ErrIncompleteSpanRange = errors.New("span range doesn't contain the start block")

type heimdallImpl struct {
	client    HeimdallClient
	pollDelay time.Duration
	logger    log.Logger
}

func NewHeimdall(client HeimdallClient, logger log.Logger) Heimdall {
	h := heimdallImpl{
		client:    client,
		pollDelay: time.Second,
		logger:    logger,
	}
	return &h
}

func (h *heimdallImpl) LastCheckpointId(ctx context.Context, store CheckpointStore) (CheckpointId, bool, error) {
	// todo get this from store if its likely not changed (need timeout)

	count, err := h.client.FetchCheckpointCount(ctx)

	if err != nil {
		return 0, false, err
	}

	return CheckpointId(count), true, nil
}

func (h *heimdallImpl) FetchCheckpointsFromBlock(ctx context.Context, store CheckpointStore, startBlock uint64) (Waypoints, error) {
	count, _, err := h.LastCheckpointId(ctx, store)

	if err != nil {
		return nil, err
	}

	var checkpoints []Waypoint

	for i := count; i >= 1; i-- {
		c, err := h.FetchCheckpoints(ctx, store, i, i)
		if err != nil {
			if errors.Is(err, ErrNotInCheckpointList) {
				common.SliceReverse(checkpoints)
				return checkpoints, ErrIncompleteCheckpointRange
			}
			return nil, err
		}

		cmpResult := c[0].CmpRange(startBlock)
		// the start block is past the last checkpoint
		if cmpResult > 0 {
			return nil, nil
		}

		for _, c := range c {
			checkpoints = append(checkpoints, c)
		}

		// the checkpoint contains the start block
		if cmpResult == 0 {
			break
		}
	}

	common.SliceReverse(checkpoints)
	return checkpoints, nil
}

func (h *heimdallImpl) FetchCheckpoints(ctx context.Context, store CheckpointStore, start CheckpointId, end CheckpointId) ([]*Checkpoint, error) {
	var checkpoints []*Checkpoint

	lastCheckpointId, exists, err := store.LastCheckpointId(ctx)

	if err != nil {
		return nil, err
	}

	if exists && start <= lastCheckpointId {
		if end <= lastCheckpointId {
			lastCheckpointId = end
		}

		for id := start; id <= lastCheckpointId; id++ {
			checkpoint, err := store.GetCheckpoint(ctx, id)

			if err != nil {
				return nil, err
			}

			checkpoints = append(checkpoints, checkpoint)
		}

		start = lastCheckpointId + 1
	}

	for id := start; id <= end; id++ {
		checkpoint, err := h.client.FetchCheckpoint(ctx, int64(id))

		if err != nil {
			return nil, err
		}

		err = store.PutCheckpoint(ctx, id, checkpoint)

		if err != nil {
			return nil, err
		}

		checkpoints = append(checkpoints, checkpoint)
	}

	return checkpoints, nil
}

func (h *heimdallImpl) LastMilestoneId(ctx context.Context, store MilestoneStore) (MilestoneId, bool, error) {
	// todo get this from store if its likely not changed (need timeout)

	count, err := h.client.FetchMilestoneCount(ctx)

	if err != nil {
		return 0, false, err
	}

	return MilestoneId(count), true, nil
}

func (h *heimdallImpl) FetchMilestonesFromBlock(ctx context.Context, store MilestoneStore, startBlock uint64) (Waypoints, error) {
	last, _, err := h.LastMilestoneId(ctx, store)

	if err != nil {
		return nil, err
	}

	var milestones Waypoints

	for i := last; i >= 1; i-- {
		m, err := h.FetchMilestones(ctx, store, i, i)
		if err != nil {
			if errors.Is(err, ErrNotInMilestoneList) {
				common.SliceReverse(milestones)
				return milestones, ErrIncompleteMilestoneRange
			}
			return nil, err
		}

		cmpResult := m[0].CmpRange(startBlock)
		// the start block is past the last milestone
		if cmpResult > 0 {
			return nil, nil
		}

		for _, m := range m {
			milestones = append(milestones, m)
		}

		// the checkpoint contains the start block
		if cmpResult == 0 {
			break
		}
	}

	common.SliceReverse(milestones)
	return milestones, nil
}

func (h *heimdallImpl) FetchMilestones(ctx context.Context, store MilestoneStore, start MilestoneId, end MilestoneId) ([]*Milestone, error) {
	var milestones []*Milestone

	lastMilestoneId, exists, err := store.LastMilestoneId(ctx)

	if err != nil {
		return nil, err
	}

	if exists && start <= lastMilestoneId {
		if end <= lastMilestoneId {
			lastMilestoneId = end
		}

		for id := start; id <= lastMilestoneId; id++ {
			milestone, err := store.GetMilestone(ctx, id)

			if err != nil {
				return nil, err
			}

			milestones = append(milestones, milestone)
		}

		start = lastMilestoneId + 1
	}

	for id := start; id <= end; id++ {
		milestone, err := h.client.FetchMilestone(ctx, int64(id))

		if err != nil {
			return nil, err
		}

		err = store.PutMilestone(ctx, id, milestone)

		if err != nil {
			return nil, err
		}

		milestones = append(milestones, milestone)
	}

	return milestones, nil
}

func (h *heimdallImpl) LastSpanId(ctx context.Context, store SpanStore) (SpanId, bool, error) {
	span, err := h.FetchLatestSpan(ctx, store)

	if err != nil {
		return 0, false, err
	}

	return span.Id, true, nil
}

func (h *heimdallImpl) FetchLatestSpan(ctx context.Context, store SpanStore) (*Span, error) {
	return h.client.FetchLatestSpan(ctx)
}

func (h *heimdallImpl) FetchSpansFromBlock(ctx context.Context, store SpanStore, startBlock uint64) ([]*Span, error) {
	last, _, err := h.LastSpanId(ctx, store)

	if err != nil {
		return nil, err
	}

	var spans []*Span

	for i := last; i >= 1; i-- {
		m, err := h.FetchSpans(ctx, store, i, i)
		if err != nil {
			if errors.Is(err, ErrNotInSpanList) {
				common.SliceReverse(spans)
				return spans, ErrIncompleteSpanRange
			}
			return nil, err
		}

		cmpResult := m[0].CmpRange(startBlock)
		// the start block is past the last span
		if cmpResult > 0 {
			return nil, nil
		}

		spans = append(spans, m...)

		// the checkpoint contains the start block
		if cmpResult == 0 {
			break
		}
	}

	common.SliceReverse(spans)
	return spans, nil
}

func (h *heimdallImpl) FetchSpans(ctx context.Context, store SpanStore, start SpanId, end SpanId) ([]*Span, error) {
	var spans []*Span

	lastSpanId, exists, err := store.LastSpanId(ctx)

	if err != nil {
		return nil, err
	}

	if exists && start <= lastSpanId {
		if end <= lastSpanId {
			lastSpanId = end
		}

		for id := start; id <= lastSpanId; id++ {
			span, err := store.GetSpan(ctx, id)

			if err != nil {
				return nil, err
			}

			spans = append(spans, span)
		}

		start = lastSpanId + 1
	}

	for id := start; id <= end; id++ {
		span, err := h.client.FetchSpan(ctx, uint64(id))

		if err != nil {
			return nil, err
		}

		err = store.PutSpan(ctx, span)

		if err != nil {
			return nil, err
		}

		spans = append(spans, span)
	}

	return spans, nil
}

func (h *heimdallImpl) OnSpanEvent(ctx context.Context, store SpanStore, callback func(*Span)) error {
	currentCount, ok, err := store.LastSpanId(ctx)

	if err != nil {
		return err
	}

	if !ok {
		currentCount, _, err = h.LastSpanId(ctx, store)

		if err != nil {
			return err
		}
	}

	go func() {
		for {
			latestSpan, err := h.client.FetchLatestSpan(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					h.logger.Error("heimdallImpl.OnSpanEvent FetchSpanCount error", "err", err)
				}
				break
			}

			if latestSpan.Id <= currentCount {
				pollDelayTimer := time.NewTimer(h.pollDelay)
				select {
				case <-ctx.Done():
					return
				case <-pollDelayTimer.C:
				}
			} else {
				m, err := h.FetchSpans(ctx, store, currentCount+1, latestSpan.Id)
				currentCount = latestSpan.Id

				if err != nil {
					if !errors.Is(err, context.Canceled) {
						h.logger.Error("heimdallImpl.OnSpanEvent FetchSpan error", "err", err)
					}
					break
				}

				go callback(m[len(m)-1])
			}
		}
	}()

	return nil
}

func (h *heimdallImpl) OnCheckpointEvent(ctx context.Context, store CheckpointStore, callback func(*Checkpoint)) error {
	currentCount, ok, err := store.LastCheckpointId(ctx)

	if err != nil {
		return err
	}

	if !ok {
		currentCount, _, err = h.LastCheckpointId(ctx, store)

		if err != nil {
			return err
		}
	}

	go func() {
		for {
			count, err := h.client.FetchCheckpointCount(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					h.logger.Error("heimdallImpl.OnMilestoneEvent FetchMilestoneCount error", "err", err)
				}
				break
			}

			if count <= int64(currentCount) {
				pollDelayTimer := time.NewTimer(h.pollDelay)
				select {
				case <-ctx.Done():
					return
				case <-pollDelayTimer.C:
				}
			} else {
				m, err := h.FetchCheckpoints(ctx, store, currentCount+1, CheckpointId(count))
				currentCount = CheckpointId(count)

				if err != nil {
					if !errors.Is(err, context.Canceled) {
						h.logger.Error("heimdallImpl.OnMilestoneEvent FetchMilestone error", "err", err)
					}
					break
				}

				go callback(m[len(m)-1])
			}
		}
	}()

	return nil
}

func (h *heimdallImpl) OnMilestoneEvent(ctx context.Context, store MilestoneStore, callback func(*Milestone)) error {
	currentCount, ok, err := store.LastMilestoneId(ctx)

	if err != nil {
		return err
	}

	if !ok {
		currentCount, _, err = h.LastMilestoneId(ctx, store)

		if err != nil {
			return err
		}
	}

	go func() {
		for {
			count, err := h.client.FetchMilestoneCount(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					h.logger.Error("heimdallImpl.OnMilestoneEvent FetchMilestoneCount error", "err", err)
				}
				break
			}

			if count <= int64(currentCount) {
				pollDelayTimer := time.NewTimer(h.pollDelay)
				select {
				case <-ctx.Done():
					return
				case <-pollDelayTimer.C:
				}
			} else {
				m, err := h.FetchMilestones(ctx, store, currentCount+1, MilestoneId(count))
				currentCount = MilestoneId(count)

				if err != nil {
					if !errors.Is(err, context.Canceled) {
						h.logger.Error("heimdallImpl.OnMilestoneEvent FetchMilestone error", "err", err)
					}
					break
				}

				go callback(m[len(m)-1])
			}
		}
	}()

	return nil
}
