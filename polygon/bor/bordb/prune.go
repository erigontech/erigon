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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
)

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

	if heimdall.WaypointsEnabled() {
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

	if heimdall.WaypointsEnabled() {
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
