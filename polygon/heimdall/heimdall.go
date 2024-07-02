// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package heimdall

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/ledgerwatch/erigon-lib/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
)

// Heimdall is a wrapper of Heimdall HTTP API
//
//go:generate mockgen -typed=true -destination=./heimdall_mock.go -package=heimdall . Heimdall
type Heimdall interface {
	FetchLatestSpans(ctx context.Context, count uint) ([]*Span, error)

	FetchCheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)
	FetchMilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)

	RegisterMilestoneObserver(callback func(*Milestone)) polygoncommon.UnregisterFunc
	RegisterSpanObserver(callback func(*Span)) polygoncommon.UnregisterFunc
}

// ErrIncompleteMilestoneRange happens when FetchMilestones is called with an old start block because old milestones are evicted
var ErrIncompleteMilestoneRange = errors.New("milestone range doesn't contain the start block")
var ErrIncompleteCheckpointRange = errors.New("checkpoint range doesn't contain the start block")

const checkpointsBatchFetchThreshold = 100

type Option func(h *heimdall)

func WithStore(store Store) Option {
	return func(h *heimdall) {
		h.store = store
	}
}

func NewHeimdall(client HeimdallClient, logger log.Logger, options ...Option) Heimdall {
	h := &heimdall{
		logger:    logger,
		client:    client,
		pollDelay: time.Second,
		store:     NewNoopStore(), // TODO change default store to one which manages its own MDBX
	}

	for _, option := range options {
		option(h)
	}

	return h
}

type heimdall struct {
	client    HeimdallClient
	pollDelay time.Duration
	logger    log.Logger
	store     Store
}

func (h *heimdall) FetchCheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	h.logger.Debug(heimdallLogPrefix("fetching checkpoints from block"), "start", startBlock)
	startFetchTime := time.Now()

	lastStoredCheckpointId, _, err := h.store.LastCheckpointId(ctx)
	if err != nil {
		return nil, err
	}

	count, err := h.client.FetchCheckpointCount(ctx)
	if err != nil {
		return nil, err
	}

	latestCheckpointId := CheckpointId(count)
	checkpointsToFetch := count - int64(lastStoredCheckpointId)
	if checkpointsToFetch >= checkpointsBatchFetchThreshold {
		checkpoints, err := h.batchFetchCheckpoints(ctx, h.store, lastStoredCheckpointId, latestCheckpointId)
		if err != nil {
			return nil, err
		}
		if len(checkpoints) == 0 {
			return nil, errors.New("unexpected empty checkpoints")
		}
		if checkpoints[len(checkpoints)-1].CmpRange(startBlock) > 0 {
			// the start block is past the last checkpoint
			return nil, nil
		}

		startCheckpointIdx, found := sort.Find(len(checkpoints), func(i int) int {
			return checkpoints[i].CmpRange(startBlock)
		})
		if !found {
			return nil, ErrIncompleteCheckpointRange
		}

		checkpoints = checkpoints[startCheckpointIdx:]
		waypoints := make(Waypoints, len(checkpoints))
		for i, checkpoint := range checkpoints {
			waypoints[i] = checkpoint
		}

		return waypoints, nil
	}

	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	var checkpoints []Waypoint
	var endBlock uint64
	for i := latestCheckpointId; i >= 1; i-- {
		select {
		case <-progressLogTicker.C:
			h.logger.Info(
				heimdallLogPrefix("fetch checkpoints from block progress (backwards)"),
				"latestCheckpointId", latestCheckpointId,
				"currentCheckpointId", i,
				"startBlock", startBlock,
			)
		default:
			// carry on
		}

		c, err := h.FetchCheckpoints(ctx, i, i)
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
			endBlock = c.EndBlock().Uint64()
		}

		// the checkpoint contains the start block
		if cmpResult == 0 {
			break
		}
	}

	common.SliceReverse(checkpoints)

	h.logger.Debug(
		heimdallLogPrefix("finished fetching checkpoints from block"),
		"count", len(checkpoints),
		"start", startBlock,
		"end", endBlock,
		"time", time.Since(startFetchTime),
	)

	return checkpoints, nil
}

func (h *heimdall) FetchCheckpoints(ctx context.Context, start CheckpointId, end CheckpointId) ([]*Checkpoint, error) {
	var checkpoints []*Checkpoint

	lastCheckpointId, exists, err := h.store.LastCheckpointId(ctx)

	if err != nil {
		return nil, err
	}

	if exists && start <= lastCheckpointId {
		if end <= lastCheckpointId {
			lastCheckpointId = end
		}

		for id := start; id <= lastCheckpointId; id++ {
			checkpoint, err := h.store.GetCheckpoint(ctx, id)

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

		err = h.store.PutCheckpoint(ctx, id, checkpoint)

		if err != nil {
			return nil, err
		}

		checkpoints = append(checkpoints, checkpoint)
	}

	return checkpoints, nil
}

func (h *heimdall) LastMilestoneId(ctx context.Context) (MilestoneId, bool, error) {
	// todo get this from store if its likely not changed (need timeout)

	count, err := h.client.FetchMilestoneCount(ctx)

	if err != nil {
		return 0, false, err
	}

	return MilestoneId(count), true, nil
}

func (h *heimdall) FetchMilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	h.logger.Debug(heimdallLogPrefix("fetching milestones from block"), "start", startBlock)
	startFetchTime := time.Now()

	last, _, err := h.LastMilestoneId(ctx)
	if err != nil {
		return nil, err
	}

	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	var milestones Waypoints
	var endBlock uint64
	for i := last; i >= 1; i-- {
		select {
		case <-progressLogTicker.C:
			h.logger.Info(
				heimdallLogPrefix("fetching milestones from block progress (backwards)"),
				"milestone id", i,
				"last", last,
				"start", startBlock,
			)
		default:
			// carry on
		}

		m, err := h.FetchMilestones(ctx, i, i)
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
			endBlock = m.EndBlock().Uint64()
		}

		// the checkpoint contains the start block
		if cmpResult == 0 {
			break
		}
	}

	common.SliceReverse(milestones)

	h.logger.Debug(
		heimdallLogPrefix("finished fetching milestones from block"),
		"count", len(milestones),
		"start", startBlock,
		"end", endBlock,
		"time", time.Since(startFetchTime),
	)

	return milestones, nil
}

func (h *heimdall) FetchMilestones(ctx context.Context, start MilestoneId, end MilestoneId) ([]*Milestone, error) {
	var milestones []*Milestone

	lastMilestoneId, exists, err := h.store.LastMilestoneId(ctx)

	if err != nil {
		return nil, err
	}

	if exists && start <= lastMilestoneId {
		if end <= lastMilestoneId {
			lastMilestoneId = end
		}

		for id := start; id <= lastMilestoneId; id++ {
			milestone, err := h.store.GetMilestone(ctx, id)

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

		err = h.store.PutMilestone(ctx, id, milestone)

		if err != nil {
			return nil, err
		}

		milestones = append(milestones, milestone)
	}

	return milestones, nil
}

func (h *heimdall) LastSpanId(ctx context.Context) (SpanId, bool, error) {
	span, err := h.FetchLatestSpan(ctx)

	if err != nil {
		return 0, false, err
	}

	return span.Id, true, nil
}

func (h *heimdall) FetchLatestSpan(ctx context.Context) (*Span, error) {
	return h.client.FetchLatestSpan(ctx)
}

func (h *heimdall) FetchLatestSpans(ctx context.Context, count uint) ([]*Span, error) {
	if count == 0 {
		return nil, errors.New("can't fetch 0 latest spans")
	}

	span, err := h.FetchLatestSpan(ctx)
	if err != nil {
		return nil, err
	}

	latestSpans := make([]*Span, 0, count)
	latestSpans = append(latestSpans, span)
	count--

	for count > 0 {
		prevSpanRawId := span.RawId()
		if prevSpanRawId == 0 {
			break
		}

		span, err = h.client.FetchSpan(ctx, prevSpanRawId-1)
		if err != nil {
			return nil, err
		}

		latestSpans = append(latestSpans, span)
		count--
	}

	common.SliceReverse(latestSpans)
	return latestSpans, nil
}

func (h *heimdall) FetchSpans(ctx context.Context, start SpanId, end SpanId) ([]*Span, error) {
	var spans []*Span

	lastSpanId, exists, err := h.store.LastSpanId(ctx)

	if err != nil {
		return nil, err
	}

	if exists && start <= lastSpanId {
		if end <= lastSpanId {
			lastSpanId = end
		}

		for id := start; id <= lastSpanId; id++ {
			span, err := h.store.GetSpan(ctx, id)

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

		err = h.store.PutSpan(ctx, span)

		if err != nil {
			return nil, err
		}

		spans = append(spans, span)
	}

	return spans, nil
}

// RegisterSpanObserver
// TODO: this will be soon replaced by service.RegisterSpanObserver
func (h *heimdall) RegisterSpanObserver(cb func(*Span)) polygoncommon.UnregisterFunc {
	ctx, cancel := context.WithCancel(context.Background())
	err := h.registerSpanObserver(ctx, cb)
	if err != nil {
		panic(err)
	}
	return polygoncommon.UnregisterFunc(cancel)
}

func (h *heimdall) registerSpanObserver(ctx context.Context, cb func(*Span)) error {
	tip, ok, err := h.store.LastSpanId(ctx)
	if err != nil {
		return err
	}

	if !ok {
		tip, _, err = h.LastSpanId(ctx)
		if err != nil {
			return err
		}
	}

	go h.pollSpans(ctx, tip, cb)

	return nil
}

func (h *heimdall) pollSpans(ctx context.Context, tip SpanId, cb func(*Span)) {
	for ctx.Err() == nil {
		latestSpan, err := h.client.FetchLatestSpan(ctx)
		if err != nil {
			h.logger.Warn(
				heimdallLogPrefix("heimdall.pollSpans FetchLatestSpan failed"),
				"err", err,
			)

			if err := common.Sleep(ctx, h.pollDelay); err != nil {
				h.logPollerSleepCancelled("spans", err)
				return
			}
			// keep background goroutine alive in case of heimdall errors
			continue
		}

		if latestSpan.Id <= tip {
			if err := common.Sleep(ctx, h.pollDelay); err != nil {
				h.logPollerSleepCancelled("spans", err)
				return
			}
			continue
		}

		m, err := h.FetchSpans(ctx, tip+1, latestSpan.Id)
		if err != nil {
			h.logger.Warn(
				heimdallLogPrefix("heimdall.pollSpans FetchSpans failed"),
				"err", err,
			)

			if err := common.Sleep(ctx, h.pollDelay); err != nil {
				h.logPollerSleepCancelled("spans", err)
				return
			}
			// keep background goroutine alive in case of heimdall errors
			continue
		}

		tip = latestSpan.Id
		go cb(m[len(m)-1])
	}
}

// RegisterMilestoneObserver
// TODO: this will be soon replaced by service.RegisterMilestoneObserver
func (h *heimdall) RegisterMilestoneObserver(cb func(*Milestone)) polygoncommon.UnregisterFunc {
	ctx, cancel := context.WithCancel(context.Background())
	err := h.registerMilestoneObserver(ctx, cb)
	if err != nil {
		panic(err)
	}
	return polygoncommon.UnregisterFunc(cancel)
}

func (h *heimdall) registerMilestoneObserver(ctx context.Context, cb func(*Milestone)) error {
	tip, ok, err := h.store.LastMilestoneId(ctx)
	if err != nil {
		return err
	}

	if !ok {
		tip, _, err = h.LastMilestoneId(ctx)
		if err != nil {
			return err
		}
	}

	go h.pollMilestones(ctx, tip, cb)

	return nil
}

func (h *heimdall) pollMilestones(ctx context.Context, tip MilestoneId, cb func(*Milestone)) {
	for ctx.Err() == nil {
		count, err := h.client.FetchMilestoneCount(ctx)
		if err != nil {
			h.logger.Warn(
				heimdallLogPrefix("heimdall.pollMilestones FetchMilestoneCount failed"),
				"err", err,
			)

			if err := common.Sleep(ctx, h.pollDelay); err != nil {
				h.logPollerSleepCancelled("milestones", err)
				return
			}
			// keep background goroutine alive in case of heimdall errors
			continue
		}

		if count <= int64(tip) {
			if err := common.Sleep(ctx, h.pollDelay); err != nil {
				h.logPollerSleepCancelled("milestones", err)
				return
			}
			continue
		}

		// heimdall keeps only last 100 milestones
		var start MilestoneId
		end := MilestoneId(count)
		if end > 100 {
			start = end - 99
		} else {
			start = 1
		}
		start = max(tip+1, start)
		m, err := h.FetchMilestones(ctx, start, end)
		if err != nil {
			h.logger.Warn(
				heimdallLogPrefix("heimdall.pollMilestones FetchMilestones failed"),
				"err", err,
			)

			if err := common.Sleep(ctx, h.pollDelay); err != nil {
				h.logPollerSleepCancelled("milestones", err)
				return
			}
			// keep background goroutine alive in case of heimdall errors
			continue
		}

		tip = MilestoneId(count)
		go cb(m[len(m)-1])
	}
}

func (h *heimdall) batchFetchCheckpoints(
	ctx context.Context,
	store CheckpointStore,
	lastStored CheckpointId,
	latest CheckpointId,
) (Checkpoints, error) {
	// TODO: once heimdall API is fixed to return sorted items in pages we can only fetch
	//       the new pages after lastStoredCheckpointId using the checkpoints/list paging API
	//       (for now we have to fetch all of them)
	//       and also remove sorting we do after fetching

	h.logger.Debug(heimdallLogPrefix("batch fetching checkpoints"))

	fetchStartTime := time.Now()
	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	page := uint64(1)
	count := int64(latest)
	checkpoints := make(Checkpoints, 0, count)
	for count > 0 {
		checkpointsBatch, err := h.client.FetchCheckpoints(ctx, page, 10_000)
		if err != nil {
			return nil, err
		}

		select {
		case <-progressLogTicker.C:
			h.logger.Debug(
				heimdallLogPrefix("batch fetch checkpoints progress"),
				"page", page,
				"len", len(checkpoints),
			)
		default:
			// carry-on
		}

		checkpoints = append(checkpoints, checkpointsBatch...)
		count = count - int64(len(checkpointsBatch))
		page++
	}

	sort.Sort(checkpoints)

	for i, checkpoint := range checkpoints[lastStored:] {
		// checkpoint list API does not return "id" in the json response
		checkpoint.Id = CheckpointId(i + 1)
		err := store.PutCheckpoint(ctx, checkpoint.Id, checkpoint)
		if err != nil {
			return nil, err
		}
	}

	h.logger.Debug(
		heimdallLogPrefix("batch fetch checkpoints done"),
		"len", len(checkpoints),
		"duration", time.Since(fetchStartTime),
	)

	return checkpoints, nil
}

func (h *heimdall) logPollerSleepCancelled(poller string, err error) {
	h.logger.Info(heimdallLogPrefix("poller sleep cancelled"), "poller", poller, "err", err)
}
