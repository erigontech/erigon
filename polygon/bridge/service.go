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

package bridge

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	liberrors "github.com/erigontech/erigon-lib/common/errors"
	"github.com/erigontech/erigon-lib/log/v3"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

type eventFetcher interface {
	FetchStateSyncEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*EventRecordWithTime, error)
}

type ServiceConfig struct {
	Store        Store
	Logger       log.Logger
	BorConfig    *borcfg.BorConfig
	EventFetcher eventFetcher
}

func NewService(config ServiceConfig) *Service {
	return &Service{
		store:               config.Store,
		logger:              config.Logger,
		borConfig:           config.BorConfig,
		eventFetcher:        config.EventFetcher,
		reader:              NewReader(config.Store, config.Logger, config.BorConfig.StateReceiverContractAddress()),
		transientErrors:     poshttp.TransientErrors,
		fetchedEventsSignal: make(chan struct{}),
	}
}

type Service struct {
	store           Store
	logger          log.Logger
	borConfig       *borcfg.BorConfig
	eventFetcher    eventFetcher
	reader          *Reader
	transientErrors []error
	// internal state
	reachedTip             atomic.Bool
	fetchedEventsSignal    chan struct{}
	lastFetchedEventTime   atomic.Uint64
	lastProcessedBlockInfo atomic.Pointer[ProcessedBlockInfo]
	unwindMu               sync.Mutex
	ready                  ready
}

type ready struct {
	mu     sync.Mutex
	on     chan struct{}
	state  bool
	inited bool
}

func (r *ready) On() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	return r.on
}

func (r *ready) init() {
	if r.inited {
		return
	}
	r.on = make(chan struct{})
	r.inited = true
}

func (r *ready) set() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.state {
		return
	}
	r.state = true
	close(r.on)
}

func (s *Service) Ready(ctx context.Context) <-chan error {
	errc := make(chan error)

	go func() {
		select {
		case <-ctx.Done():
			errc <- ctx.Err()
		case <-s.ready.On():
			errc <- nil
		}

		close(errc)
	}()

	return errc
}

func (s *Service) Run(ctx context.Context) error {
	defer func() {
		if s.fetchedEventsSignal != nil {
			close(s.fetchedEventsSignal)
			s.fetchedEventsSignal = nil
		}
	}()

	err := s.store.Prepare(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	// get last known sync Id
	lastFetchedEventId, err := s.store.LastEventId(ctx)
	if err != nil {
		return err
	}

	lastProcessedEventId, err := s.store.LastProcessedEventId(ctx)
	if err != nil {
		return err
	}

	lastProcessedBlockInfo, ok, err := s.store.LastProcessedBlockInfo(ctx)
	if err != nil {
		return err
	}
	if ok {
		s.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	}

	// start syncing
	s.logger.Info(
		bridgeLogPrefix("running bridge service component"),
		"lastFetchedEventId", lastFetchedEventId,
		"lastProcessedEventId", lastProcessedEventId,
		"lastProcessedBlockNum", lastProcessedBlockInfo.BlockNum,
		"lastProcessedBlockTime", lastProcessedBlockInfo.BlockTime,
	)

	s.ready.set()

	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// start scraping events
		from := lastFetchedEventId + 1
		to := time.Now()
		events, err := s.eventFetcher.FetchStateSyncEvents(ctx, from, to, StateEventsFetchLimit)
		if err != nil {
			if liberrors.IsOneOf(err, s.transientErrors) {
				s.logger.Warn(
					bridgeLogPrefix("scraper transient err occurred"),
					"from", from,
					"to", to.Format(time.RFC3339),
					"err", err,
				)

				continue
			}

			return err
		}

		if len(events) == 0 {
			// we've reached the tip
			s.reachedTip.Store(true)
			s.signalFetchedEvents()
			if err := common.Sleep(ctx, time.Second); err != nil {
				return err
			}

			continue
		}

		orderedAndNoGaps := true
		knownEventID := lastFetchedEventId

		for i := 0; i < len(events); i++ {
			if events[i].ID == knownEventID+1 {
				knownEventID = events[i].ID
				continue
			}

			orderedAndNoGaps = false
		}

		if !orderedAndNoGaps {
			s.logger.Warn(
				bridgeLogPrefix("fetched new events are not ordered or contain gaps"),
				"count", len(events),
				"lastKnownEventId", lastFetchedEventId,
			)

			if err := common.Sleep(ctx, time.Second); err != nil {
				return err
			}

			continue
		}

		// we've received new events
		s.reachedTip.Store(false)
		if err := s.store.PutEvents(ctx, events); err != nil {
			return err
		}

		lastFetchedEvent := events[len(events)-1]
		lastFetchedEventId = lastFetchedEvent.ID

		lastFetchedEventTime := lastFetchedEvent.Time.Unix()
		if lastFetchedEventTime < 0 {
			// be defensive when casting from int64 to uint64
			return errors.New("lastFetchedEventTime cannot be negative")
		}

		s.lastFetchedEventTime.Store(uint64(lastFetchedEventTime))
		s.signalFetchedEvents()

		select {
		case <-logTicker.C:
			s.logger.Info(
				bridgeLogPrefix("fetched new events periodic progress"),
				"count", len(events),
				"lastFetchedEventId", lastFetchedEventId,
				"lastFetchedEventTime", lastFetchedEvent.Time.Format(time.RFC3339),
			)
		default: // continue
		}
	}
}

func (s *Service) Close() {
	s.store.Close()
}

func (s *Service) InitialBlockReplayNeeded(ctx context.Context) (uint64, bool, error) {
	lastFrozen := s.store.LastFrozenEventBlockNum()

	if blockInfo := s.lastProcessedBlockInfo.Load(); blockInfo != nil && blockInfo.BlockNum > lastFrozen {
		return blockInfo.BlockNum, false, nil
	}

	blockInfo, ok, err := s.store.LastProcessedBlockInfo(ctx)
	if err != nil {
		return 0, false, err
	}
	if ok && blockInfo.BlockNum > lastFrozen {
		// we have all info, no need to replay initial block
		return blockInfo.BlockNum, false, nil
	}

	// replay the last block we have in event snapshots
	return lastFrozen, true, nil
}

func (s *Service) ReplayInitialBlock(ctx context.Context, block *types.Block) error {
	lastProcessedBlockInfo := ProcessedBlockInfo{
		BlockNum:  block.NumberU64(),
		BlockTime: block.Time(),
	}

	s.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	return s.store.PutProcessedBlockInfo(ctx, []ProcessedBlockInfo{lastProcessedBlockInfo})
}

// ProcessNewBlocks iterates through all blocks and constructs a map from block number to sync events
func (s *Service) ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	s.unwindMu.Lock()
	defer s.unwindMu.Unlock()

	lastProcessedEventId, err := s.store.LastProcessedEventId(ctx)
	if err != nil {
		return err
	}

	var lastProcessedBlockInfo ProcessedBlockInfo
	if ptr := s.lastProcessedBlockInfo.Load(); ptr != nil {
		lastProcessedBlockInfo = *ptr
	} else {
		return errors.New("lastProcessedBlockInfo must be set before bridge processing")
	}

	from := blocks[0].NumberU64()
	s.logger.Debug(
		bridgeLogPrefix("processing new blocks"),
		"from", from,
		"to", blocks[len(blocks)-1].NumberU64(),
		"lastProcessedBlockNum", lastProcessedBlockInfo.BlockNum,
		"lastProcessedBlockTime", lastProcessedBlockInfo.BlockTime,
		"lastProcessedEventId", lastProcessedEventId,
	)

	blockNumToEventId := make(map[uint64]uint64)
	eventTxnToBlockNum := make(map[common.Hash]uint64)
	processedBlocks := make([]ProcessedBlockInfo, 0, 1+len(blocks)/int(s.borConfig.CalculateSprintLength(from)))
	for _, block := range blocks {
		// check if block is start of span and > 0
		blockNum := block.NumberU64()
		if blockNum == 0 || !s.borConfig.IsSprintStart(blockNum) {
			continue
		}
		if blockNum <= lastProcessedBlockInfo.BlockNum {
			continue
		}

		expectedNextBlockNum := lastProcessedBlockInfo.BlockNum + s.borConfig.CalculateSprintLength(lastProcessedBlockInfo.BlockNum)
		if blockNum != expectedNextBlockNum {
			return fmt.Errorf("nonsequential block in bridge processing: %d != %d", blockNum, expectedNextBlockNum)
		}

		blockTime := block.Time()
		toTime, err := s.blockEventsTimeWindowEnd(lastProcessedBlockInfo, blockNum, blockTime)
		if err != nil {
			return err
		}

		startId := lastProcessedEventId + 1
		var eventLimit *uint64

		if s.borConfig.OverrideStateSyncRecords != nil {
			if el, ok := s.borConfig.OverrideStateSyncRecords[strconv.FormatUint(blockNum, 10)]; ok {
				uel := uint64(el)
				eventLimit = &uel
			}

			for k, el := range s.borConfig.OverrideStateSyncRecords {
				if len(k) == 0 {
					continue
				}

				if k[0] != 'r' {
					continue
				}

				var from uint64
				var to uint64

				n, err := fmt.Sscanf(k, "r.%d-%d", &from, &to)
				if err != nil {
					return err
				}

				if n != 2 {
					return errors.New("failed to decode override state sync records")
				}

				if blockNum < from {
					continue
				}

				if blockNum > to {
					continue
				}

				if el == 0 {
					uel := uint64(el)
					eventLimit = &uel
				}
			}
		}

		var endId uint64

		if eventLimit == nil || *eventLimit > 0 {
			if err = s.waitForScraper(ctx, toTime); err != nil {
				return err
			}

			endId, err = s.store.LastEventIdWithinWindow(ctx, startId, time.Unix(int64(toTime), 0))
			if err != nil {
				return err
			}
		}

		if eventLimit != nil {
			if *eventLimit == 0 {
				endId = 0
			} else {
				if endId > startId && endId-startId >= *eventLimit {
					endId = startId + *eventLimit - 1
				}
			}
		}

		if endId > 0 {
			s.logger.Debug(
				bridgeLogPrefix("mapping events to block"),
				"blockNum", blockNum,
				"start", startId,
				"end", endId,
			)

			lastProcessedEventId = endId
			eventTxnHash := bortypes.ComputeBorTxHash(blockNum, block.Hash())
			eventTxnToBlockNum[eventTxnHash] = blockNum
			blockNumToEventId[blockNum] = endId
		}

		lastProcessedBlockInfo = ProcessedBlockInfo{
			BlockNum:  blockNum,
			BlockTime: blockTime,
		}

		// keep track for updating at the end as a last step (once all other updates have successfully been applied)
		processedBlocks = append(processedBlocks, lastProcessedBlockInfo)
	}

	if len(processedBlocks) == 0 {
		return nil
	}

	if err := s.store.PutBlockNumToEventId(ctx, blockNumToEventId); err != nil {
		return err
	}

	if err := s.store.PutEventTxnToBlockNum(ctx, eventTxnToBlockNum); err != nil {
		return err
	}

	if err := s.store.PutProcessedBlockInfo(ctx, processedBlocks); err != nil {
		return err
	}

	s.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	return nil
}

// Unwind delete unwindable bridge data.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func (s *Service) Unwind(ctx context.Context, blockNum uint64) error {
	s.logger.Info(bridgeLogPrefix("unwinding"), "blockNum", blockNum)

	s.unwindMu.Lock()
	defer s.unwindMu.Unlock()

	if err := s.store.Unwind(ctx, blockNum); err != nil {
		return err
	}

	lastProcessedBlockInfo, ok, err := s.store.LastProcessedBlockInfo(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("no last processed block info after unwind")
	}

	s.logger.Debug(
		bridgeLogPrefix("last processed block after unwind"),
		"blockNum", lastProcessedBlockInfo.BlockNum,
		"blockTime", lastProcessedBlockInfo.BlockTime,
	)

	s.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	return nil
}

func (s *Service) EventsWithinTime(ctx context.Context, timeFrom, timeTo time.Time) ([]*types.Message, error) {
	return s.reader.EventsWithinTime(ctx, timeFrom, timeTo)
}

// Events returns all sync events at blockNum
func (s *Service) Events(ctx context.Context, blockHash common.Hash, blockNum uint64) ([]*types.Message, error) {
	return s.reader.Events(ctx, blockHash, blockNum)
}

func (s *Service) EventTxnLookup(ctx context.Context, borTxHash common.Hash) (uint64, bool, error) {
	return s.reader.EventTxnLookup(ctx, borTxHash)
}

func (s *Service) blockEventsTimeWindowEnd(last ProcessedBlockInfo, blockNum uint64, blockTime uint64) (uint64, error) {
	if s.borConfig.IsIndore(blockNum) {
		stateSyncDelay := s.borConfig.CalculateStateSyncDelay(blockNum)
		return blockTime - stateSyncDelay, nil
	}

	return last.BlockTime, nil
}

func (s *Service) waitForScraper(ctx context.Context, toTime uint64) error {
	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()

	shouldLog := true
	reachedTip := s.reachedTip.Load()
	lastFetchedEventTime := s.lastFetchedEventTime.Load()
	for !reachedTip && toTime > lastFetchedEventTime {
		if shouldLog {
			s.logger.Debug(
				bridgeLogPrefix("waiting for event scrapping to catch up"),
				"reachedTip", reachedTip,
				"lastFetchedEventTime", lastFetchedEventTime,
				"toTime", toTime,
			)
		}

		if err := s.waitFetchedEventsSignal(ctx); err != nil {
			return err
		}

		reachedTip = s.reachedTip.Load()
		lastFetchedEventTime = s.lastFetchedEventTime.Load()

		select {
		case <-logTicker.C:
			shouldLog = true
		default:
			shouldLog = false
		}
	}

	return nil
}

func (s *Service) signalFetchedEvents() {
	select {
	case s.fetchedEventsSignal <- struct{}{}:
	default: // no-op, signal already queued
	}
}

func (s *Service) waitFetchedEventsSignal(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-s.fetchedEventsSignal:
		if !ok {
			return errors.New("fetchedEventsSignal channel closed")
		}
		return nil
	}
}
