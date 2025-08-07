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

package polygon

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/erigontech/erigon/cmd/devnet/contracts"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/polygon/bridge"
)

// Maximum allowed event record data size
const LegacyMaxStateSyncSize = 100000

// New max state sync size after hardfork
const MaxStateSyncSize = 30000

type EventRecordWithBlock struct {
	bridge.EventRecordWithTime
	BlockNumber uint64
}

func (h *Heimdall) startStateSyncSubscription() {
	var err error
	syncChan := make(chan *contracts.TestStateSenderStateSynced, 100)

	h.syncSubscription, err = h.syncSenderBinding.WatchStateSynced(&bind.WatchOpts{}, syncChan, nil, nil)

	if err != nil {
		h.unsubscribe()
		h.logger.Error("Failed to subscribe to sync events", "err", err)
		return
	}

	for stateSyncedEvent := range syncChan {
		if err := h.handleStateSynced(stateSyncedEvent); err != nil {
			h.logger.Error("L1 sync event processing failed", "event", stateSyncedEvent.Raw.Index, "err", err)
		}
	}
}

func (h *Heimdall) StateSyncEvents(ctx context.Context, fromID uint64, to int64) ([]*bridge.EventRecordWithTime, error) {
	h.Lock()
	defer h.Unlock()

	events := make([]*EventRecordWithBlock, 0, len(h.pendingSyncRecords))

	//var removalKeys []syncRecordKey

	var minEventTime *time.Time

	for _ /*key*/, event := range h.pendingSyncRecords {
		if event.ID >= fromID {
			if event.Time.Unix() < to {
				events = append(events, event)
			}

			eventTime := event.Time.Round(1 * time.Second)

			if minEventTime == nil || eventTime.Before(*minEventTime) {
				minEventTime = &eventTime
			}
		}
		//else {
		//removalKeys = append(removalKeys, key)
		//}
	}

	if len(events) == 0 {
		h.logger.Info("Processed sync request", "from", fromID, "to", time.Unix(to, 0), "min-time", minEventTime,
			"pending", len(h.pendingSyncRecords), "filtered", len(events))
		return nil, nil
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].ID < events[j].ID
	})

	eventsWithTime := make([]*bridge.EventRecordWithTime, len(events))
	for i, event := range events {
		eventsWithTime[i] = &event.EventRecordWithTime
	}

	//for _, removalKey := range removalKeys {
	//	delete(h.pendingSyncRecords, removalKey)
	//}

	h.logger.Info("Processed sync request",
		"from", fromID, "to", time.Unix(to, 0), "min-time", minEventTime,
		"pending", len(h.pendingSyncRecords), "filtered", len(events),
		"sent", fmt.Sprintf("%d-%d", events[0].ID, events[len(events)-1].ID))

	return eventsWithTime, nil
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
		EventRecordWithTime: bridge.EventRecordWithTime{
			EventRecord: bridge.EventRecord{
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
