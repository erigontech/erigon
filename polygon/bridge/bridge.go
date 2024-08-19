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
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/polygon/polygoncommon"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type eventFetcher interface {
	FetchStateSyncEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error)
}

func Assemble(dataDir string, logger log.Logger, borConfig *borcfg.BorConfig, eventFetcher eventFetcher) *Bridge {
	bridgeDB := polygoncommon.NewDatabase(dataDir, kv.PolygonBridgeDB, databaseTablesCfg, logger)
	bridgeStore := NewStore(bridgeDB)
	return NewBridge(bridgeStore, logger, borConfig, eventFetcher)
}

func NewBridge(store Store, logger log.Logger, borConfig *borcfg.BorConfig, eventFetcher eventFetcher) *Bridge {
	return &Bridge{
		store:                        store,
		logger:                       logger,
		borConfig:                    borConfig,
		eventFetcher:                 eventFetcher,
		stateReceiverContractAddress: libcommon.HexToAddress(borConfig.StateReceiverContract),
		fetchedEventsSignal:          make(chan struct{}),
		processedBlockSignal:         make(chan struct{}),
	}
}

type Bridge struct {
	store                        Store
	logger                       log.Logger
	borConfig                    *borcfg.BorConfig
	eventFetcher                 eventFetcher
	stateReceiverContractAddress libcommon.Address
	// internal state
	reachedTip             atomic.Bool
	fetchedEventsSignal    chan struct{}
	lastFetchedEventTime   atomic.Uint64
	processedBlockSignal   chan struct{}
	lastProcessedBlockNum  atomic.Uint64
	lastProcessedBlockTime atomic.Uint64
	lastProcessedEventID   atomic.Uint64
}

func (b *Bridge) Run(ctx context.Context) error {
	defer close(b.fetchedEventsSignal)
	defer close(b.processedBlockSignal)

	err := b.store.Prepare(ctx)
	if err != nil {
		return err
	}
	defer b.Close()

	// get last known sync ID
	lastFetchedEventID, err := b.store.LatestEventID(ctx)
	if err != nil {
		return err
	}

	lastProcessedEventID, err := b.store.LastProcessedEventID(ctx)
	if err != nil {
		return err
	}

	b.lastProcessedEventID.Store(lastProcessedEventID)

	// start syncing
	b.logger.Debug(
		bridgeLogPrefix("running bridge component"),
		"lastFetchedEventID", lastFetchedEventID,
		"lastProcessedEventID", lastProcessedEventID,
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// start scrapping events
		to := time.Now()
		events, err := b.eventFetcher.FetchStateSyncEvents(ctx, lastFetchedEventID+1, to, 50)
		if err != nil {
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
		lastFetchedEventID = lastFetchedEvent.ID

		lastFetchedEventTime := lastFetchedEvent.Time.Unix()
		if lastFetchedEventTime < 0 {
			// be defensive when casting from int64 to uint64
			panic(errors.New("lastFetchedEventTime cannot be negative"))
		}

		b.lastFetchedEventTime.Store(uint64(lastFetchedEventTime))

		b.logger.Debug(
			bridgeLogPrefix("fetched new events"),
			"count", len(events),
			"lastFetchedEventID", lastFetchedEventID,
			"lastFetchedEventTime", lastFetchedEvent.Time.Format(time.RFC3339),
		)

		b.signalFetchedEvents()
	}
}

func (b *Bridge) Close() {
	b.store.Close()
}

// ProcessNewBlocks iterates through all blocks and constructs a map from block number to sync events
func (b *Bridge) ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	blockNumToEventId := make(map[uint64]uint64)
	eventTxnToBlockNum := make(map[libcommon.Hash]uint64)
	for _, block := range blocks {
		// check if block is start of span
		blockNum := block.NumberU64()
		if !b.isSprintStart(blockNum) {
			continue
		}

		blockTime := block.Time()
		toTime, err := b.blockEventsTimeWindowEnd(blockNum, blockTime)
		if err != nil {
			return err
		}

		if err = b.waitForScrapper(ctx, toTime); err != nil {
			return err
		}

		startID := b.lastProcessedEventID.Load() + 1
		endID, err := b.store.LastEventIDWithinWindow(ctx, startID, time.Unix(int64(toTime), 0))
		if err != nil {
			return err
		}

		if endID > 0 {
			b.logger.Debug(
				bridgeLogPrefix("mapping events to block"),
				"blockNum", blockNum,
				"start", startID,
				"end", endID,
			)

			eventTxnHash := bortypes.ComputeBorTxHash(blockNum, block.Hash())
			eventTxnToBlockNum[eventTxnHash] = blockNum
			blockNumToEventId[blockNum] = startID
			b.lastProcessedEventID.Store(endID)
		}

		b.lastProcessedBlockNum.Store(blockNum)
		b.lastProcessedBlockTime.Store(blockTime)
		b.signalProcessedBlock()
	}

	err := b.store.PutBlockNumToEventID(ctx, blockNumToEventId)
	if err != nil {
		return err
	}

	err = b.store.PutEventTxnToBlockNum(ctx, eventTxnToBlockNum)
	if err != nil {
		return err
	}

	return nil
}

// Synchronize blocks until events up to a given block are processed.
func (b *Bridge) Synchronize(ctx context.Context, blockNum uint64) error {
	b.logger.Debug(
		bridgeLogPrefix("synchronizing events..."),
		"blockNum", blockNum,
		"lastProcessedBlockNum", b.lastProcessedBlockNum.Load(),
	)

	return b.waitForProcessedBlock(ctx, blockNum)
}

// Unwind deletes map entries till tip
func (b *Bridge) Unwind(ctx context.Context, blockNum uint64) error {
	return b.store.PruneEventIDs(ctx, blockNum)
}

// Events returns all sync events at blockNum
func (b *Bridge) Events(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	start, end, err := b.store.EventIDRange(ctx, blockNum)
	if err != nil {
		if errors.Is(err, ErrEventIDRangeNotFound) {
			return nil, nil
		}

		return nil, err
	}

	if end == 0 { // exception for tip processing
		end = b.lastProcessedEventID.Load()
	}

	eventsRaw := make([]*types.Message, 0, end-start+1)

	// get events from DB
	events, err := b.store.Events(ctx, start+1, end+1)
	if err != nil {
		return nil, err
	}

	b.logger.Debug(bridgeLogPrefix("events query db result"), "blockNum", blockNum, "eventCount", len(events))

	// convert to message
	for _, event := range events {
		msg := types.NewMessage(
			state.SystemAddress,
			&b.stateReceiverContractAddress,
			0, u256.Num0,
			core.SysCallGasLimit,
			u256.Num0,
			nil, nil,
			event, nil, false,
			true,
			nil,
		)

		eventsRaw = append(eventsRaw, &msg)
	}

	return eventsRaw, nil
}

func (b *Bridge) EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	return b.store.EventTxnToBlockNum(ctx, borTxHash)
}

func (b *Bridge) blockEventsTimeWindowEnd(blockNum uint64, blockTime uint64) (uint64, error) {
	if b.borConfig.IsIndore(blockNum) {
		stateSyncDelay := b.borConfig.CalculateStateSyncDelay(blockNum)
		return blockTime - stateSyncDelay, nil
	}

	lastProcessedBlockTime := b.lastProcessedBlockTime.Load()
	if lastProcessedBlockTime == 0 {
		return 0, errors.New("unknown last processed block time")
	}

	return lastProcessedBlockTime, nil
}

func (b *Bridge) waitForScrapper(ctx context.Context, toTime uint64) error {
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
	lastProcessedBlockNum := b.lastProcessedBlockNum.Load()
	for blockNum > lastProcessedBlockNum {
		if shouldLog {
			b.logger.Debug(
				bridgeLogPrefix("waiting for block processing to catch up"),
				"blockNum", blockNum,
				"lastProcessedBlockNum", lastProcessedBlockNum,
			)
		}

		if err := b.waitProcessedBlockSignal(ctx); err != nil {
			return err
		}

		lastProcessedBlockNum = b.lastProcessedBlockNum.Load()

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

func (b *Bridge) signalProcessedBlock() {
	select {
	case b.processedBlockSignal <- struct{}{}:
	default: // no-op, signal already queued
	}
}

func (b *Bridge) waitProcessedBlockSignal(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-b.processedBlockSignal:
		if !ok {
			return errors.New("processedBlockSignal channel closed")
		}
		return nil
	}
}

func (b *Bridge) isSprintStart(headerNum uint64) bool {
	if headerNum%b.borConfig.CalculateSprintLength(headerNum) != 0 || headerNum == 0 {
		return false
	}

	return true
}
