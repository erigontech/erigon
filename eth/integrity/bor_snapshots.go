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

package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/genesis"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/services"
)

func ValidateBorEvents(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, from, to uint64, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] ValidateBorEvents: done", "err", err)
	}()

	var cc *chain.Config

	if db == nil {
		genesis := genesis.BorMainnetGenesisBlock()
		cc = genesis.Config
	} else {
		err = db.View(ctx, func(tx kv.Tx) error {
			cc, err = chain.GetConfig(tx, nil)
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			err = fmt.Errorf("cant read chain config from db: %w", err)
			return err
		}
	}

	if cc.BorJSON == nil {
		return err
	}

	config := &borcfg.BorConfig{}

	if err := json.Unmarshal(cc.BorJSON, config); err != nil {
		err = fmt.Errorf("invalid chain config 'bor' JSON: %w", err)
		return err
	}

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	snapshots := blockReader.BorSnapshots().(*heimdall.RoSnapshots)

	var prevEventId uint64
	var maxBlockNum uint64

	if to > 0 {
		maxBlockNum = to
	} else {
		maxBlockNum = snapshots.SegmentsMax()
	}

	view := snapshots.View()
	defer view.Close()

	for _, eventSegment := range view.Events() {

		if from > 0 && eventSegment.From() < from {
			continue
		}

		if to > 0 && eventSegment.From() > to {
			break
		}

		prevEventId, err = heimdall.ValidateBorEvents(ctx, config, db, blockReader, eventSegment, prevEventId, maxBlockNum, failFast, logEvery)

		if err != nil && failFast {
			return err
		}
	}

	if db != nil {
		err = db.View(ctx, func(tx kv.Tx) error {
			if false {
				lastEventId, _, err := blockReader.LastEventId(ctx, tx)
				if err != nil {
					return err
				}

				polygonSyncProgress, err := stages.GetStageProgress(tx, stages.PolygonSync)
				if err != nil {
					return err
				}

				bodyProgress, err := stages.GetStageProgress(tx, stages.Bodies)
				if err != nil {
					return err
				}

				log.Info("[integrity] LAST Event", "event", lastEventId, "bor-progress", polygonSyncProgress, "body-progress", bodyProgress)
			}

			return nil
		})

		if err != nil {
			return err
		}
	}

	log.Info("[integrity] done checking bor events", "event", prevEventId)

	return nil
}

func ValidateBorSpans(ctx context.Context, logger log.Logger, dirs datadir.Dirs, snaps *heimdall.RoSnapshots, failFast bool) error {
	baseStore := heimdall.NewMdbxStore(logger, dirs.DataDir, true, 32)
	snapshotStore := heimdall.NewSpanSnapshotStore(baseStore.Spans(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	defer baseStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] ValidateBorSpans: done", "err", err)
	return err
}

func ValidateBorCheckpoints(ctx context.Context, logger log.Logger, dirs datadir.Dirs, snaps *heimdall.RoSnapshots, failFast bool) error {
	baseStore := heimdall.NewMdbxStore(logger, dirs.DataDir, true, 32)
	snapshotStore := heimdall.NewCheckpointSnapshotStore(baseStore.Checkpoints(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	defer baseStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] ValidateBorCheckpoints: done", "err", err)
	return err
}

func ValidateBorMilestones(ctx context.Context, logger log.Logger, dirs datadir.Dirs, snaps *heimdall.RoSnapshots, failFast bool) error {
	baseStore := heimdall.NewMdbxStore(logger, dirs.DataDir, true, 32)
	snapshotStore := heimdall.NewMilestoneSnapshotStore(baseStore.Milestones(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	defer baseStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] ValidateBorMilestones: done", "err", err)
	return err
}
