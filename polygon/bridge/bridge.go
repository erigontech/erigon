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

	liberrors "github.com/erigontech/erigon-lib/common/errors"
	"github.com/erigontech/erigon-lib/log/v3"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type eventFetcher interface {
	FetchStateSyncEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error)
}

type Config struct {
	Store        Store
	Logger       log.Logger
	BorConfig    *borcfg.BorConfig
	EventFetcher eventFetcher
}

func NewBridge(config Config) *Bridge {
	return &Bridge{
		store:                        config.Store,
		logger:                       config.Logger,
		borConfig:                    config.BorConfig,
		eventFetcher:                 config.EventFetcher,
		stateReceiverContractAddress: libcommon.HexToAddress(config.BorConfig.StateReceiverContract),
		reader:                       NewReader(config.Store, config.Logger, config.BorConfig.StateReceiverContractAddress()),
		transientErrors: []error{
			heimdall.ErrBadGateway,
			heimdall.ErrServiceUnavailable,
			context.DeadlineExceeded,
		},
		fetchedEventsSignal:   make(chan struct{}),
		processedBlocksSignal: make(chan struct{}),
	}
}

type Bridge struct {
	store                        Store
	logger                       log.Logger
	borConfig                    *borcfg.BorConfig
	eventFetcher                 eventFetcher
	stateReceiverContractAddress libcommon.Address
	reader                       *Reader
	transientErrors              []error
	// internal state
	reachedTip             atomic.Bool
	fetchedEventsSignal    chan struct{}
	lastFetchedEventTime   atomic.Uint64
	processedBlocksSignal  chan struct{}
	lastProcessedBlockInfo atomic.Pointer[ProcessedBlockInfo]
	synchronizeMu          sync.Mutex
	unwindMu               sync.Mutex
	ready                  ready
}

type ready struct {
	mu     sync.Mutex
	on     chan struct{}
	state  bool
	inited bool
}

func (me *ready) On() <-chan struct{} {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.init()
	return me.on
}

func (me *ready) init() {
	if me.inited {
		return
	}
	me.on = make(chan struct{})
	me.inited = true
}

func (me *ready) set() {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.init()
	if me.state {
		return
	}
	me.state = true
	close(me.on)
}

func (b *Bridge) Ready(ctx context.Context) <-chan error {
	errc := make(chan error)

	go func() {
		select {
		case <-ctx.Done():
			errc <- ctx.Err()
		case <-b.ready.On():
			errc <- nil
		}

		close(errc)
	}()

	return errc
}

func (b *Bridge) Run(ctx context.Context) error {
	defer func() {
		if b.fetchedEventsSignal != nil {
			close(b.fetchedEventsSignal)
			b.fetchedEventsSignal = nil
		}

		if b.processedBlocksSignal != nil {
			close(b.processedBlocksSignal)
			b.processedBlocksSignal = nil
		}
	}()

	err := b.store.Prepare(ctx)
	if err != nil {
		return err
	}
	defer b.Close()

	// get last known sync Id
	lastFetchedEventId, err := b.store.LastEventId(ctx)
	if err != nil {
		return err
	}

	lastProcessedEventId, err := b.store.LastProcessedEventId(ctx)
	if err != nil {
		return err
	}

	lastProcessedBlockInfo, ok, err := b.store.LastProcessedBlockInfo(ctx)
	if err != nil {
		return err
	}
	if ok {
		b.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	}

	// start syncing
	b.logger.Debug(
		bridgeLogPrefix("running bridge component"),
		"lastFetchedEventId", lastFetchedEventId,
		"lastProcessedEventId", lastProcessedEventId,
		"lastProcessedBlockNum", lastProcessedBlockInfo.BlockNum,
		"lastProcessedBlockTime", lastProcessedBlockInfo.BlockTime,
	)

	b.ready.set()

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
		events, err := b.eventFetcher.FetchStateSyncEvents(ctx, from, to, heimdall.StateEventsFetchLimit)
		if err != nil {
			if liberrors.IsOneOf(err, b.transientErrors) {
				b.logger.Warn(
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
			b.reachedTip.Store(true)
			b.signalFetchedEvents()
			if err := libcommon.Sleep(ctx, time.Second); err != nil {
				return err
			}

			continue
		}

		// we've received new events
		b.reachedTip.Store(false)
		if err := b.store.PutEvents(ctx, events); err != nil {
			return err
		}

		lastFetchedEvent := events[len(events)-1]
		lastFetchedEventId = lastFetchedEvent.ID

		lastFetchedEventTime := lastFetchedEvent.Time.Unix()
		if lastFetchedEventTime < 0 {
			// be defensive when casting from int64 to uint64
			return errors.New("lastFetchedEventTime cannot be negative")
		}

		b.lastFetchedEventTime.Store(uint64(lastFetchedEventTime))
		b.signalFetchedEvents()

		select {
		case <-logTicker.C:
			b.logger.Debug(
				bridgeLogPrefix("fetched new events periodic progress"),
				"count", len(events),
				"lastFetchedEventId", lastFetchedEventId,
				"lastFetchedEventTime", lastFetchedEvent.Time.Format(time.RFC3339),
			)
		default: // continue
		}
	}
}

func (b *Bridge) Close() {
	b.store.Close()
}

func (b *Bridge) InitialBlockReplayNeeded(ctx context.Context) (uint64, bool, error) {
	lastFrozen := b.store.LastFrozenEventBlockNum()

	if blockInfo := b.lastProcessedBlockInfo.Load(); blockInfo != nil && blockInfo.BlockNum > lastFrozen {
		return 0, false, nil
	}

	blockInfo, ok, err := b.store.LastProcessedBlockInfo(ctx)
	if err != nil {
		return 0, false, err
	}
	if ok && blockInfo.BlockNum > lastFrozen {
		// we have all info, no need to replay
		return 0, false, nil
	}

	// replay the last block we have in event snapshots
	return b.store.LastFrozenEventBlockNum(), true, nil
}

func (b *Bridge) ReplayInitialBlock(ctx context.Context, block *types.Block) error {
	lastProcessedBlockInfo := ProcessedBlockInfo{
		BlockNum:  block.NumberU64(),
		BlockTime: block.Time(),
	}

	b.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	return b.store.PutProcessedBlockInfo(ctx, lastProcessedBlockInfo)
}

// ProcessNewBlocks iterates through all blocks and constructs a map from block number to sync events
func (b *Bridge) ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	b.unwindMu.Lock()
	defer b.unwindMu.Unlock()

	lastProcessedEventId, err := b.store.LastProcessedEventId(ctx)
	if err != nil {
		return err
	}

	var lastProcessedBlockInfo ProcessedBlockInfo
	if ptr := b.lastProcessedBlockInfo.Load(); ptr != nil {
		lastProcessedBlockInfo = *ptr
	} else {
		return errors.New("lastProcessedBlockInfo must be set before bridge processing")
	}

	b.logger.Debug(
		bridgeLogPrefix("processing new blocks"),
		"from", blocks[0].NumberU64(),
		"to", blocks[len(blocks)-1].NumberU64(),
		"lastProcessedBlockNum", lastProcessedBlockInfo.BlockNum,
		"lastProcessedBlockTime", lastProcessedBlockInfo.BlockTime,
		"lastProcessedEventId", lastProcessedEventId,
	)

	var processedBlock bool
	blockNumToEventId := make(map[uint64]uint64)
	eventTxnToBlockNum := make(map[libcommon.Hash]uint64)
	for _, block := range blocks {
		// check if block is start of span and > 0
		blockNum := block.NumberU64()
		if blockNum == 0 || !b.borConfig.IsSprintStart(blockNum) {
			continue
		}
		if blockNum <= lastProcessedBlockInfo.BlockNum {
			continue
		}

		expectedNextBlockNum := lastProcessedBlockInfo.BlockNum + b.borConfig.CalculateSprintLength(blockNum)
		if blockNum != expectedNextBlockNum {
			return fmt.Errorf("nonsequential block in bridge processing: %d != %d", blockNum, expectedNextBlockNum)
		}

		blockTime := block.Time()
		toTime, err := b.blockEventsTimeWindowEnd(lastProcessedBlockInfo, blockNum, blockTime)
		if err != nil {
			return err
		}

		if err = b.waitForScraper(ctx, toTime); err != nil {
			return err
		}

		startId := lastProcessedEventId + 1
		endId, err := b.store.LastEventIdWithinWindow(ctx, startId, time.Unix(int64(toTime), 0))
		if err != nil {
			return err
		}

		if b.borConfig.OverrideStateSyncRecords != nil {
			if eventLimit, ok := b.borConfig.OverrideStateSyncRecords[strconv.FormatUint(blockNum, 10)]; ok {
				if eventLimit == 0 {
					endId = 0
				} else {
					endId = startId + uint64(eventLimit) - 1
				}
			}
		}

		if endId > 0 {
			b.logger.Debug(
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

		processedBlock = true
		lastProcessedBlockInfo = ProcessedBlockInfo{
			BlockNum:  blockNum,
			BlockTime: blockTime,
		}
	}

	if !processedBlock {
		return nil
	}

	if err := b.store.PutBlockNumToEventId(ctx, blockNumToEventId); err != nil {
		return err
	}

	if err := b.store.PutEventTxnToBlockNum(ctx, eventTxnToBlockNum); err != nil {
		return err
	}

	if err := b.store.PutProcessedBlockInfo(ctx, lastProcessedBlockInfo); err != nil {
		return err
	}

	b.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	b.signalProcessedBlocks()
	return nil
}

// Synchronize blocks until events up to a given block are processed.
func (b *Bridge) Synchronize(ctx context.Context, blockNum uint64) error {
	// make Synchronize safe if unintentionally called by more than 1 goroutine at a time by using a lock
	// waitForProcessedBlock relies on signal channel which is safe if 1 goroutine waits on it at a time
	b.synchronizeMu.Lock()
	defer b.synchronizeMu.Unlock()

	b.logger.Debug(
		bridgeLogPrefix("synchronizing events..."),
		"blockNum", blockNum,
		"lastProcessedBlockNum", b.lastProcessedBlockInfo.Load().BlockNum,
	)

	return b.waitForProcessedBlock(ctx, blockNum)
}

// Unwind delete unwindable bridge data.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func (b *Bridge) Unwind(ctx context.Context, blockNum uint64) error {
	b.logger.Debug(bridgeLogPrefix("unwinding"), "blockNum", blockNum)

	b.unwindMu.Lock()
	defer b.unwindMu.Unlock()

	if err := b.store.Unwind(ctx, blockNum); err != nil {
		return err
	}

	lastProcessedBlockInfo, ok, err := b.store.LastProcessedBlockInfo(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("no last processed block info after unwind")
	}

	b.lastProcessedBlockInfo.Store(&lastProcessedBlockInfo)
	return nil
}

// Events returns all sync events at blockNum
func (b *Bridge) Events(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	return b.reader.Events(ctx, blockNum)
}

func (b *Bridge) EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	return b.reader.EventTxnLookup(ctx, borTxHash)
}

func (b *Bridge) blockEventsTimeWindowEnd(last ProcessedBlockInfo, blockNum uint64, blockTime uint64) (uint64, error) {
	if b.borConfig.IsIndore(blockNum) {
		stateSyncDelay := b.borConfig.CalculateStateSyncDelay(blockNum)
		return blockTime - stateSyncDelay, nil
	}

	return last.BlockTime, nil
}

func (b *Bridge) waitForScraper(ctx context.Context, toTime uint64) error {
	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()

	shouldLog := true
	reachedTip := b.reachedTip.Load()
	lastFetchedEventTime := b.lastFetchedEventTime.Load()
	for !reachedTip && toTime > lastFetchedEventTime {
		if shouldLog {
			b.logger.Debug(
				bridgeLogPrefix("waiting for event scrapping to catch up"),
				"reachedTip", reachedTip,
				"lastFetchedEventTime", lastFetchedEventTime,
				"toTime", toTime,
			)
		}

		if err := b.waitFetchedEventsSignal(ctx); err != nil {
			return err
		}

		reachedTip = b.reachedTip.Load()
		lastFetchedEventTime = b.lastFetchedEventTime.Load()

		select {
		case <-logTicker.C:
			shouldLog = true
		default:
			shouldLog = false
		}
	}

	return nil
}

func (b *Bridge) waitForProcessedBlock(ctx context.Context, blockNum uint64) error {
	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()

	sprintLen := b.borConfig.CalculateSprintLength(blockNum)
	blockNum -= blockNum % sprintLen // we only process events at sprint start
	shouldLog := true
	lastProcessedBlockNum := b.lastProcessedBlockInfo.Load().BlockNum
	for blockNum > lastProcessedBlockNum {
		if shouldLog {
			b.logger.Debug(
				bridgeLogPrefix("waiting for block processing to catch up"),
				"blockNum", blockNum,
				"lastProcessedBlockNum", lastProcessedBlockNum,
			)
		}

		if err := b.waitProcessedBlocksSignal(ctx); err != nil {
			return err
		}

		lastProcessedBlockNum = b.lastProcessedBlockInfo.Load().BlockNum

		select {
		case <-logTicker.C:
			shouldLog = true
		default:
			shouldLog = false
		}
	}

	return nil
}

func (b *Bridge) signalFetchedEvents() {
	select {
	case b.fetchedEventsSignal <- struct{}{}:
	default: // no-op, signal already queued
	}
}

func (b *Bridge) waitFetchedEventsSignal(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-b.fetchedEventsSignal:
		if !ok {
			return errors.New("fetchedEventsSignal channel closed")
		}
		return nil
	}
}

func (b *Bridge) signalProcessedBlocks() {
	select {
	case b.processedBlocksSignal <- struct{}{}:
	default: // no-op, signal already queued
	}
}

func (b *Bridge) waitProcessedBlocksSignal(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-b.processedBlocksSignal:
		if !ok {
			return errors.New("processedBlocksSignal channel closed")
		}
		return nil
	}
}
