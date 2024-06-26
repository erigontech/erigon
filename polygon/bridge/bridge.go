package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

type fetchSyncEventsType func(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error)

type Bridge struct {
	store                    Store
	ready                    bool
	lastProcessedBlockNumber uint64
	lastProcessedEventID     uint64

	log                log.Logger
	borConfig          *borcfg.BorConfig
	stateClientAddress libcommon.Address
	stateReceiverABI   abi.ABI
	fetchSyncEvents    fetchSyncEventsType
}

func NewBridge(store Store, logger log.Logger, borConfig *borcfg.BorConfig, fetchSyncEvents fetchSyncEventsType, stateReceiverABI abi.ABI) *Bridge {
	return &Bridge{
		store:                    store,
		log:                      logger,
		borConfig:                borConfig,
		fetchSyncEvents:          fetchSyncEvents,
		lastProcessedBlockNumber: 0,
		lastProcessedEventID:     0,
		stateClientAddress:       libcommon.HexToAddress(borConfig.StateReceiverContract),
		stateReceiverABI:         stateReceiverABI,
	}
}

func (b *Bridge) Run(ctx context.Context) error {
	err := b.store.Prepare(ctx)
	if err != nil {
		return err
	}

	// get last known sync ID
	lastEventID, err := b.store.GetLatestEventID(ctx)
	if err != nil {
		return err
	}

	// start syncing
	b.log.Debug(bridgeLogPrefix("Bridge is running"), "lastEventID", lastEventID)

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
			if err := b.store.AddEvents(ctx, events, b.stateReceiverABI); err != nil {
				return err
			}

			lastEventID = events[len(events)-1].ID
		} else {
			b.ready = true
			if err := libcommon.Sleep(ctx, 30*time.Second); err != nil {
				return err
			}
		}

		b.log.Debug(bridgeLogPrefix(fmt.Sprintf("got %v new events, last event ID: %v, ready: %v", len(events), lastEventID, b.ready)))
	}
}

func (b *Bridge) Close() {
	b.store.Close()
}

// EngineService interface implementations

// ProcessNewBlocks iterates through all blocks and constructs a map from block number to sync events
func (b *Bridge) ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error {
	eventMap := make(map[uint64]uint64)
	for _, block := range blocks {
		// check if block is start of span
		if b.isSprintStart(block.NumberU64()) {
			continue
		}

		blockTimestamp := time.Unix(int64(block.Time()), 0)
		lastDBID, err := b.store.GetSprintLastEventID(ctx, b.lastProcessedEventID, blockTimestamp, b.stateReceiverABI)
		if err != nil {
			return err
		}

		if lastDBID != 0 && lastDBID > b.lastProcessedEventID {
			b.log.Debug(bridgeLogPrefix(fmt.Sprintf("Creating map for block %d, start ID %d, end ID %d", block.NumberU64(), b.lastProcessedEventID, lastDBID)))
			eventMap[block.NumberU64()] = b.lastProcessedEventID

			b.lastProcessedEventID = lastDBID
		}

		b.lastProcessedBlockNumber = block.NumberU64()
	}

	err := b.store.StoreEventID(ctx, eventMap)
	if err != nil {
		return err
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
	return b.store.PruneEventIDs(ctx, tip.Number.Uint64())
}

// GetEvents returns all sync events at blockNum
func (b *Bridge) GetEvents(ctx context.Context, blockNum uint64) ([]*types.Message, error) {
	start, end, err := b.store.GetEventIDRange(ctx, blockNum)
	if err != nil {
		return nil, err
	}

	eventsRaw := make([]*types.Message, end-start+1)

	// get events from DB
	events, err := b.store.GetEvents(ctx, start, end)
	if err != nil {
		return nil, err
	}

	b.log.Debug(bridgeLogPrefix(fmt.Sprintf("got %v events for block %v", len(events), blockNum)))

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

	return eventsRaw, nil
}

// Helper functions
func (b *Bridge) isSprintStart(headerNum uint64) bool {
	if headerNum%b.borConfig.CalculateSprintLength(headerNum) != 0 || headerNum == 0 {
		return false
	}

	return true
}
