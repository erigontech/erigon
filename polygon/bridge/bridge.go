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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
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
	reader := NewReader(bridgeStore, logger, borConfig.StateReceiverContract)
	return NewBridge(bridgeStore, logger, borConfig, eventFetcher, reader)
}

func NewBridge(store Store, logger log.Logger, borConfig *borcfg.BorConfig, eventFetcher eventFetcher, reader *Reader) *Bridge {
	return &Bridge{
		store:                        store,
		logger:                       logger,
		borConfig:                    borConfig,
		eventFetcher:                 eventFetcher,
		stateReceiverContractAddress: libcommon.HexToAddress(borConfig.StateReceiverContract),
		reader:                       reader,
	}
}

type Bridge struct {
	store                        Store
	logger                       log.Logger
	borConfig                    *borcfg.BorConfig
	eventFetcher                 eventFetcher
	stateReceiverContractAddress libcommon.Address

	reader *Reader

	// internal state
	ready                    atomic.Bool
	lastProcessedBlockNumber atomic.Uint64
}

func (b *Bridge) Run(ctx context.Context) error {
	err := b.store.Prepare(ctx)
	if err != nil {
		return err
	}
	defer b.Close()

	// get last known sync ID
	lastEventID, err := b.store.LatestEventID(ctx)
	if err != nil {
		return err
	}

	// start syncing
	b.logger.Debug(bridgeLogPrefix("Bridge is running"), "lastEventID", lastEventID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// get all events from last sync ID to now
		to := time.Now()
		events, err := b.eventFetcher.FetchStateSyncEvents(ctx, lastEventID+1, to, 0)
		if err != nil {
			return err
		}

		if len(events) != 0 {
			b.ready.Store(false)
			if err := b.store.PutEvents(ctx, events); err != nil {
				return err
			}

			lastEventID = events[len(events)-1].ID
		} else {
			b.ready.Store(true)
			if err := libcommon.Sleep(ctx, 30*time.Second); err != nil {
				return err
			}
		}

		b.logger.Debug(bridgeLogPrefix(fmt.Sprintf("got %v new events, last event ID: %v, ready: %v", len(events), lastEventID, b.ready.Load())))
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

	if err := b.Synchronize(ctx, blocks[len(blocks)-1].NumberU64()); err != nil {
		return err
	}

	eventMap := make(map[uint64]uint64)
	txMap := make(map[libcommon.Hash]uint64)
	var prevSprintTime time.Time

	lastProcessedEventID, err := b.store.LastProcessedEventID(ctx)
	if err != nil {
		return err
	}

	for _, block := range blocks {
		// check if block is start of span and > 0
		blockNum := block.NumberU64()
		if blockNum == 0 || !b.borConfig.IsSprintStart(blockNum) {
			continue
		}

		var timeLimit time.Time
		if b.borConfig.IsIndore(blockNum) {
			stateSyncDelay := b.borConfig.CalculateStateSyncDelay(blockNum)
			timeLimit = time.Unix(int64(block.Time()-stateSyncDelay), 0)
		} else {
			timeLimit = prevSprintTime
		}

		prevSprintTime = time.Unix(int64(block.Time()), 0)

		lastID, err := b.store.LastEventIDWithinWindow(ctx, lastProcessedEventID, timeLimit)
		if err != nil {
			return err
		}

		if lastID > lastProcessedEventID {
			b.logger.Debug(bridgeLogPrefix(fmt.Sprintf("Creating map for block %d, end ID %d", blockNum, lastID)))

			k := bortypes.ComputeBorTxHash(blockNum, block.Hash())
			eventMap[blockNum] = lastID
			txMap[k] = blockNum
			lastProcessedEventID = lastID
		}

		b.lastProcessedBlockNumber.Store(blockNum)
	}

	err = b.store.PutEventIDs(ctx, eventMap)
	if err != nil {
		return err
	}

	err = b.store.PutEventTxnToBlockNum(ctx, txMap)
	if err != nil {
		return err
	}

	return nil
}

// Synchronize blocks till bridge has map at tip
func (b *Bridge) Synchronize(ctx context.Context, blockNum uint64) error {
	b.logger.Debug(bridgeLogPrefix("synchronizing events..."), "blockNum", blockNum)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if b.ready.Load() || b.lastProcessedBlockNumber.Load() >= blockNum {
			return nil
		}
	}
}

// Unwind deletes map entries till tip
func (b *Bridge) Unwind(ctx context.Context, blockNum uint64) error {
	return b.store.PruneEventIDs(ctx, blockNum)
}

// Events returns all sync events at blockNum
func (b *Bridge) Events(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	return b.reader.Events(ctx, blockNum)
}

func (b *Bridge) EventTxnLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error) {
	return b.reader.EventTxnLookup(ctx, borTxHash)
}
