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

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
)

type HeimdallUnwindCfg struct {
	KeepEvents                      bool
	KeepEventNums                   bool
	KeepEventProcessedBlocks        bool
	KeepSpans                       bool
	KeepSpanBlockProducerSelections bool
	KeepCheckpoints                 bool
	KeepMilestones                  bool
	Astrid                          bool
}

func (cfg *HeimdallUnwindCfg) ApplyUserUnwindTypeOverrides(userUnwindTypeOverrides []string) {
	if len(userUnwindTypeOverrides) > 0 {
		return
	}

	// If a user has specified an unwind type override it means we need to unwind all the tables that fall
	// inside that type but NOT unwind the tables for the types that have not been specified in the overrides.
	// Our default config value unwinds everything.
	// If we initialise that and keep track of all the "unseen" unwind type overrides then we can flip our config
	// to not unwind the tables for the "unseen" types.
	const events = "events"
	const spans = "spans"
	const checkpoints = "checkpoints"
	const milestones = "milestones"
	unwindTypes := map[string]struct{}{
		events:      {},
		spans:       {},
		checkpoints: {},
		milestones:  {},
	}

	for _, unwindType := range userUnwindTypeOverrides {
		if _, exists := unwindTypes[unwindType]; !exists {
			panic("unknown unwindType override " + unwindType)
		}

		delete(unwindTypes, unwindType)
	}

	// our config unwinds everything by default
	defaultCfg := HeimdallUnwindCfg{}
	defaultCfg.Astrid = cfg.Astrid
	// flip the config for the unseen type overrides
	for unwindType := range unwindTypes {
		switch unwindType {
		case events:
			defaultCfg.KeepEvents = true
			defaultCfg.KeepEventNums = true
			defaultCfg.KeepEventProcessedBlocks = true
		case spans:
			defaultCfg.KeepSpans = true
			defaultCfg.KeepSpanBlockProducerSelections = true
		case checkpoints:
			defaultCfg.KeepCheckpoints = true
		case milestones:
			defaultCfg.KeepMilestones = true
		default:
			panic(fmt.Sprintf("missing override logic for unwindType %s, please add it", unwindType))
		}
	}

	*cfg = defaultCfg
}

func UnwindHeimdall(ctx context.Context, heimdallStore heimdall.Store, bridgeStore bridge.Store, tx kv.RwTx, unwindPoint uint64, unwindCfg HeimdallUnwindCfg) error {
	if !unwindCfg.KeepEvents {
		if err := UnwindEvents(tx, unwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepEventNums {
		if err := UnwindEventNums(tx, unwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepEventProcessedBlocks {
		if err := UnwindEventProcessedBlocks(tx, unwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepSpans {
		if err := UnwindSpans(ctx, heimdallStore, tx, unwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepSpanBlockProducerSelections && unwindCfg.Astrid {
		if err := UnwindSpanBlockProducerSelections(ctx, heimdallStore, tx, unwindPoint); err != nil {
			return err
		}
	}

	if heimdall.CheckpointsEnabled() && !unwindCfg.KeepCheckpoints {
		if err := UnwindCheckpoints(ctx, heimdallStore, tx, unwindPoint); err != nil {
			return err
		}
	}

	if heimdall.MilestonesEnabled() && !unwindCfg.KeepMilestones {
		if err := UnwindMilestones(ctx, heimdallStore, tx, unwindPoint); err != nil {
			return err
		}
	}

	return nil
}

func UnwindEvents(tx kv.RwTx, unwindPoint uint64) error {
	eventNumsCursor, err := tx.Cursor(kv.BorEventNums)
	if err != nil {
		return err
	}
	defer eventNumsCursor.Close()

	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], unwindPoint+1)

	_, _, err = eventNumsCursor.Seek(blockNumBuf[:])
	if err != nil {
		return err
	}

	// keep last event ID of previous block with assigned events
	_, lastEventIdToKeep, err := eventNumsCursor.Prev()
	if err != nil {
		return err
	}

	var firstEventIdToRemove uint64
	if lastEventIdToKeep == nil {
		// there are no assigned events before the unwind block, remove all items from BorEvents
		firstEventIdToRemove = 0
	} else {
		firstEventIdToRemove = binary.BigEndian.Uint64(lastEventIdToKeep) + 1
	}

	from := make([]byte, 8)
	binary.BigEndian.PutUint64(from, firstEventIdToRemove)
	eventCursor, err := tx.RwCursor(kv.BorEvents)
	if err != nil {
		return err
	}
	defer eventCursor.Close()

	var k []byte
	for k, _, err = eventCursor.Seek(from); err == nil && k != nil; k, _, err = eventCursor.Next() {
		if err = eventCursor.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

func UnwindEventNums(tx kv.RwTx, unwindPoint uint64) error {
	c, err := tx.RwCursor(kv.BorEventNums)
	if err != nil {
		return err
	}

	defer c.Close()
	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], unwindPoint+1)
	var k []byte
	for k, _, err = c.Seek(blockNumBuf[:]); err == nil && k != nil; k, _, err = c.Next() {
		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

func UnwindEventProcessedBlocks(tx kv.RwTx, unwindPoint uint64) error {
	c, err := tx.RwCursor(kv.BorEventProcessedBlocks)
	if err != nil {
		return err
	}

	defer c.Close()
	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], unwindPoint+1)
	var k []byte
	for k, _, err = c.Seek(blockNumBuf[:]); err == nil && k != nil; k, _, err = c.Next() {
		if err = c.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

func UnwindSpans(ctx context.Context, heimdallStore heimdall.Store, tx kv.RwTx, unwindPoint uint64) error {
	_, err := heimdallStore.Spans().(interface {
		WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Span]
	}).WithTx(tx).DeleteFromBlockNum(ctx, unwindPoint)

	return err
}

func UnwindSpanBlockProducerSelections(ctx context.Context, heimdallStore heimdall.Store, tx kv.RwTx, unwindPoint uint64) error {
	_, err := heimdallStore.SpanBlockProducerSelections().(interface {
		WithTx(kv.Tx) heimdall.EntityStore[*heimdall.SpanBlockProducerSelection]
	}).WithTx(tx).DeleteFromBlockNum(ctx, unwindPoint)

	return err
}

func UnwindCheckpoints(ctx context.Context, heimdallStore heimdall.Store, tx kv.RwTx, unwindPoint uint64) error {
	_, err := heimdallStore.Checkpoints().(interface {
		WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Checkpoint]
	}).WithTx(tx).DeleteFromBlockNum(ctx, unwindPoint)

	return err
}

func UnwindMilestones(ctx context.Context, heimdallStore heimdall.Store, tx kv.RwTx, unwindPoint uint64) error {
	_, err := heimdallStore.Milestones().(interface {
		WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Milestone]
	}).WithTx(tx).DeleteFromBlockNum(ctx, unwindPoint)

	return err
}

func PruneHeimdall(ctx context.Context, heimdallStore heimdall.Store, bridgeStore bridge.Store, tx kv.RwTx, blocksTo uint64, blocksDeleteLimit int) (int, error) {
	var deleted int

	if tx != nil {
		bridgeStore = bridgeStore.(interface {
			WithTx(kv.Tx) bridge.Store
		}).WithTx(tx)
	}

	eventsDeleted, err := bridgeStore.PruneEvents(ctx, blocksTo, blocksDeleteLimit)

	if err != nil {
		return eventsDeleted, err
	}

	deleted = eventsDeleted

	spanStore := heimdallStore.Spans()

	if tx != nil {
		spanStore = spanStore.(interface {
			WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Span]
		}).WithTx(tx)
	}

	spansDeleted, err := spanStore.DeleteToBlockNum(ctx, blocksTo, blocksDeleteLimit)

	if spansDeleted > deleted {
		deleted = spansDeleted
	}

	if err != nil {
		return deleted, err
	}

	spanBPStore := heimdallStore.SpanBlockProducerSelections()

	if tx != nil {
		spanBPStore = spanBPStore.(interface {
			WithTx(kv.Tx) heimdall.EntityStore[*heimdall.SpanBlockProducerSelection]
		}).WithTx(tx)
	}

	spansDeleted, err = spanBPStore.DeleteToBlockNum(ctx, blocksTo, blocksDeleteLimit)

	if spansDeleted > deleted {
		deleted = spansDeleted
	}
	if err != nil {
		return deleted, err
	}

	if heimdall.CheckpointsEnabled() {
		checkpointStore := heimdallStore.Checkpoints()

		if tx != nil {
			checkpointStore = checkpointStore.(interface {
				WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Checkpoint]
			}).WithTx(tx)
		}

		checkPointsDeleted, err := checkpointStore.DeleteToBlockNum(ctx, blocksTo, blocksDeleteLimit)

		if checkPointsDeleted > deleted {
			deleted = checkPointsDeleted
		}

		if err != nil {
			return deleted, err
		}
	}

	if heimdall.MilestonesEnabled() {
		milestoneStore := heimdallStore.Milestones()

		if tx != nil {
			milestoneStore = milestoneStore.(interface {
				WithTx(kv.Tx) heimdall.EntityStore[*heimdall.Milestone]
			}).WithTx(tx)
		}

		milestonesDeleted, err := milestoneStore.DeleteToBlockNum(ctx, blocksTo, blocksDeleteLimit)

		if milestonesDeleted > deleted {
			deleted = milestonesDeleted
		}

		if err != nil {
			return 0, err
		}
	}

	return deleted, nil
}
