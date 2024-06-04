package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
)

type fetchSyncEventsType func(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error)

type IDRange struct {
	start, end uint64
}

type Bridge struct {
	db                       *polygoncommon.Database
	ready                    bool
	eventMap                 map[uint64]IDRange // block num to eventID range
	lastProcessedBlockNumber uint64
	lastProcessedEventID     uint64

	log                log.Logger
	borConfig          *borcfg.BorConfig
	stateClientAddress libcommon.Address
	stateReceiverABI   abi.ABI
	fetchSyncEvents    fetchSyncEventsType
}

func NewBridge(dataDir string, logger log.Logger, borConfig *borcfg.BorConfig, fetchSyncEvents fetchSyncEventsType, stateReceiverABI abi.ABI) *Bridge {
	// create new db
	db := polygoncommon.NewDatabase(dataDir, logger)

	return &Bridge{
		db:                       db,
		log:                      logger,
		borConfig:                borConfig,
		fetchSyncEvents:          fetchSyncEvents,
		eventMap:                 map[uint64]IDRange{},
		lastProcessedBlockNumber: 0,
		lastProcessedEventID:     0,
		stateClientAddress:       libcommon.HexToAddress(borConfig.StateReceiverContract),
		stateReceiverABI:         stateReceiverABI,
	}
}

func (b *Bridge) Run(ctx context.Context) error {
	err := b.db.OpenOnce(ctx, kv.PolygonBridgeDB, databaseTablesCfg)
	if err != nil {
		return err
	}

	// start syncing
	b.log.Warn(bridgeLogPrefix("Bridge is running"))

	// get last known sync ID
	lastEventID, err := GetLatestEventID(ctx, b.db)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// get all events from last sync ID to now
		to := time.Now()
		events, err := b.fetchSyncEvents(ctx, lastEventID+1, to, 0)
		if err != nil {
			return err
		}

		if len(events) != 0 {
			b.ready = false
			if err := AddEvents(ctx, b.db, events, b.stateReceiverABI); err != nil {
				return err
			}

			lastEventID = events[len(events)-1].ID
		} else {
			b.ready = true
			if err := libcommon.Sleep(ctx, 30*time.Second); err != nil {
				return err
			}
		}

		b.log.Warn(bridgeLogPrefix(fmt.Sprintf("got %v new events, last event ID: %v, ready: %v", len(events), lastEventID, b.ready)))
	}
}

func (b *Bridge) Close() {
	b.db.Close()
}

// EngineService interface implementations

// ProcessNewBlocks iterates through all blocks and constructs a map from block number to sync events
func (b *Bridge) ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error {
	for _, block := range blocks {
		// check if block is start of span
		if b.isSprintStart(block.NumberU64()) {
			continue
		}

		blockTimestamp := time.Unix(int64(block.Time()), 0)
		lastDBID, err := GetSprintLastEventID(ctx, b.db, b.lastProcessedEventID, blockTimestamp, b.stateReceiverABI)
		if err != nil {
			return err
		}

		if lastDBID != 0 && lastDBID > b.lastProcessedEventID {
			b.log.Warn(bridgeLogPrefix(fmt.Sprintf("Creating map for block %d, start ID %d, end ID %d", block.NumberU64(), b.lastProcessedEventID, lastDBID)))
			b.eventMap[block.NumberU64()] = IDRange{b.lastProcessedEventID, lastDBID}

			b.lastProcessedEventID = lastDBID
		}

		b.lastProcessedBlockNumber = block.NumberU64()
	}

	return nil
}

// Synchronize blocks till bridge has map at tip
func (b *Bridge) Synchronize(ctx context.Context, tip *types.Header) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if b.ready || b.lastProcessedBlockNumber >= tip.Number.Uint64() {
			return nil
		}
	}
}

// Unwind deletes map entries till tip
func (b *Bridge) Unwind(ctx context.Context, tip *types.Header) error {
	for k := range b.eventMap {
		if k <= tip.Number.Uint64() {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			delete(b.eventMap, k)
		}
	}

	return nil
}

// GetEvents returns all sync events at blockNum
func (b *Bridge) GetEvents(ctx context.Context, blockNum uint64) []*types.Message {
	eventIDs, ok := b.eventMap[blockNum]
	if !ok {
		return []*types.Message{}
	}

	eventsRaw := make([]*types.Message, eventIDs.end-eventIDs.start+1)

	// get events from DB
	events, err := GetEvents(ctx, b.db, eventIDs)
	if err != nil {
		return nil
	}

	// convert to message
	for _, event := range events {
		msg := types.NewMessage(
			state.SystemAddress,
			&b.stateClientAddress,
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

	return eventsRaw
}

// Helper functions
func (b *Bridge) isSprintStart(headerNum uint64) bool {
	if headerNum%b.borConfig.CalculateSprintLength(headerNum) != 0 || headerNum == 0 {
		return false
	}

	return true
}
