package bor

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/consensus/bor/clerk"
)

// Maximum allowed event record data size
const LegacyMaxStateSyncSize = 100000

// New max state sync size after hardfork
const MaxStateSyncSize = 30000

type EventRecordWithBlock struct {
	clerk.EventRecordWithTime
	BlockNumber uint64
}

func (h *Heimdall) StateSyncEvents(ctx context.Context, fromID uint64, to int64, limit int) (uint64, []*clerk.EventRecordWithTime, error) {
	h.Lock()
	defer h.Unlock()

	events := make([]*EventRecordWithBlock, 0, len(h.pendingSyncRecords))

	//var removalKeys []syncRecordKey

	for _ /*key*/, event := range h.pendingSyncRecords {
		if event.ID >= fromID {
			if event.Time.Unix() <= to {
				events = append(events, event)
			}
		}
		//else {
		//removalKeys = append(removalKeys, key)
		//}
	}

	if len(events) == 0 {
		h.logger.Info("Processed sync request", "from", fromID, "to", time.Unix(to, 0), "pending", len(h.pendingSyncRecords), "filtered", len(events))
		return 0, nil, nil
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].ID < events[j].ID
	})

	if len(events) > limit {
		events = events[0 : limit-1]
	}

	eventsWithTime := make([]*clerk.EventRecordWithTime, len(events))

	for i, event := range events {
		eventsWithTime[i] = &event.EventRecordWithTime
	}

	//for _, removalKey := range removalKeys {
	//	delete(h.pendingSyncRecords, removalKey)
	//}

	h.logger.Info("Processed sync request",
		"from", fromID, "to", time.Unix(to, 0),
		"pending", len(h.pendingSyncRecords), "filtered", len(events),
		"sent", fmt.Sprintf("%d-%d", events[0].ID, events[len(events)-1].ID))

	return events[len(events)-1].BlockNumber, eventsWithTime, nil
}

// handleStateSyncEvent - handle state sync event from rootchain
func (h *Heimdall) handleStateSynced(event *contracts.TestStateSenderStateSynced) error {
	h.Lock()
	defer h.Unlock()

	isOld, _ := h.isOldTx(event.Raw.TxHash, uint64(event.Raw.Index), BridgeEvents.ClerkEvent, event)

	if isOld {
		h.logger.Info("Ignoring send event as already processed",
			"event", "StateSynced",
			"id", event.Id,
			"contract", event.ContractAddress,
			"data", hex.EncodeToString(event.Data),
			"borChainId", h.chainConfig.ChainID,
			"txHash", event.Raw.TxHash,
			"logIndex", uint64(event.Raw.Index),
			"blockNumber", event.Raw.BlockNumber,
		)

		return nil
	}

	h.logger.Info(
		"⬜ New send event",
		"event", "StateSynced",
		"id", event.Id,
		"contract", event.ContractAddress,
		"data", hex.EncodeToString(event.Data),
		"borChainId", h.chainConfig.ChainID,
		"txHash", event.Raw.TxHash,
		"logIndex", uint64(event.Raw.Index),
		"blockNumber", event.Raw.BlockNumber,
	)

	if event.Raw.BlockNumber > h.getSpanOverrideHeight() && len(event.Data) > MaxStateSyncSize {
		h.logger.Info(`Data is too large to process, Resetting to ""`, "data", hex.EncodeToString(event.Data))
		event.Data = []byte{}
	} else if len(event.Data) > LegacyMaxStateSyncSize {
		h.logger.Info(`Data is too large to process, Resetting to ""`, "data", hex.EncodeToString(event.Data))
		event.Data = []byte{}
	}

	h.pendingSyncRecords[syncRecordKey{event.Raw.TxHash, uint64(event.Raw.Index)}] = &EventRecordWithBlock{
		EventRecordWithTime: clerk.EventRecordWithTime{
			EventRecord: clerk.EventRecord{
				ID:       event.Id.Uint64(),
				Contract: event.ContractAddress,
				Data:     event.Data,
				TxHash:   event.Raw.TxHash,
				LogIndex: uint64(event.Raw.Index),
				ChainID:  h.chainConfig.ChainID.String(),
			},
			Time: time.Now(),
		},
		BlockNumber: event.Raw.BlockNumber,
	}

	return nil
}

func (h *Heimdall) isOldTx(txHash libcommon.Hash, logIndex uint64, eventType BridgeEvent, event interface{}) (bool, error) {

	// define the endpoint based on the type of event
	var status bool

	switch eventType {
	case BridgeEvents.StakingEvent:
	case BridgeEvents.TopupEvent:
	case BridgeEvents.ClerkEvent:
		_, status = h.pendingSyncRecords[syncRecordKey{txHash, logIndex}]
	case BridgeEvents.SlashingEvent:
	}

	return status, nil
}
