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

package bordb

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func UnwindHeimdall(ctx context.Context, heimdallStore heimdall.Store, bridgeStore bridge.Store, tx kv.RwTx, unwindPoint uint64, limit int, unwindTypes []string) (int, error) {
	var deleted int

	fmt.Println("UH", unwindPoint, limit)

	if len(unwindTypes) == 0 || slices.Contains(unwindTypes, "events") {
		eventsDeleted, err := UnwindEvents(tx, unwindPoint, limit)

		if err != nil {
			return eventsDeleted, err
		}

		deleted = eventsDeleted
	}

	if len(unwindTypes) == 0 || slices.Contains(unwindTypes, "spans") {
		spansDeleted, err := heimdallStore.Spans().(interface {
			WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Span]
		}).WithTx(tx).DeleteToBlockNum(ctx, unwindPoint, limit)

		if spansDeleted > deleted {
			deleted = spansDeleted
		}

		if err != nil {
			return deleted, err
		}

		spansDeleted, err = heimdallStore.SpanBlockProducerSelections().(interface {
			WithTx(kv.Tx) heimdall.EntityStore[*heimdall.SpanBlockProducerSelection]
		}).WithTx(tx).DeleteToBlockNum(ctx, unwindPoint, limit)

		if spansDeleted > deleted {
			deleted = spansDeleted
		}
		if err != nil {
			return deleted, err
		}
	}

	if heimdall.CheckpointsEnabled() && (len(unwindTypes) == 0 || slices.Contains(unwindTypes, "checkpoints")) {
		checkPointsDeleted, err := heimdallStore.Checkpoints().(interface {
			WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Checkpoint]
		}).WithTx(tx).DeleteToBlockNum(ctx, unwindPoint, limit)

		if checkPointsDeleted > deleted {
			deleted = checkPointsDeleted
		}

		if err != nil {
			return deleted, err
		}
	}

	if heimdall.MilestonesEnabled() && (len(unwindTypes) == 0 || slices.Contains(unwindTypes, "milestones")) {
		milestonesDeleted, err := heimdallStore.Milestones().(interface {
			WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Milestone]
		}).WithTx(tx).DeleteToBlockNum(ctx, unwindPoint, limit)

		if milestonesDeleted > deleted {
			deleted = milestonesDeleted
		}

		if err != nil {
			return 0, err
		}
	}

	return deleted, nil
}

func UnwindEvents(tx kv.RwTx, unwindPoint uint64, limit int) (int, error) {
	fmt.Println("UE")

	cursor, err := tx.RwCursor(kv.BorEventNums)
	if err != nil {
		return 0, err
	}
	defer cursor.Close()

	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], unwindPoint+1)

	_, _, err = cursor.Seek(blockNumBuf[:])
	if err != nil {
		return 0, err
	}

	_, prevSprintLastIdBytes, err := cursor.Prev() // last event Id of previous sprint
	if err != nil {
		return 0, err
	}

	var prevSprintLastId uint64
	if prevSprintLastIdBytes == nil {
		// we are unwinding the first entry, remove all items from BorEvents
		prevSprintLastId = 0
	} else {
		prevSprintLastId = binary.BigEndian.Uint64(prevSprintLastIdBytes)
	}

	eventId := make([]byte, 8) // first event Id for this sprint
	binary.BigEndian.PutUint64(eventId, prevSprintLastId+1)

	eventCursor, err := tx.RwCursor(kv.BorEvents)
	if err != nil {
		return 0, err
	}
	defer eventCursor.Close()

	var deleted int

	for eventId, _, err = eventCursor.Seek(eventId); err == nil && eventId != nil; eventId, _, err = eventCursor.Next() {
		if err = eventCursor.DeleteCurrent(); err != nil {
			return deleted, err
		}
		deleted++
		if deleted == limit {
			break
		}
	}

	if err != nil {
		return deleted, err
	}

	k, _, err := cursor.Next() // move cursor back to this sprint
	if err != nil {
		return deleted, err
	}

	for ; err == nil && k != nil && deleted < limit; k, _, err = cursor.Next() {
		if err = cursor.DeleteCurrent(); err != nil {
			return deleted, err
		}
		deleted++
	}

	epbCursor, err := tx.RwCursor(kv.BorEventProcessedBlocks)
	if err != nil {
		return deleted, err
	}

	defer epbCursor.Close()
	for k, _, err = epbCursor.Seek(blockNumBuf[:]); err == nil && k != nil; k, _, err = epbCursor.Next() {
		if err = epbCursor.DeleteCurrent(); err != nil {
			return deleted, err
		}
	}

	return deleted, err
}
