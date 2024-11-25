package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/erigontech/erigon/erigon-lib/chain"
	"github.com/erigontech/erigon/erigon-lib/kv"
	"github.com/erigontech/erigon/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/services"
)

func NoGapsInBorEvents(ctx context.Context, db kv.RoDB, blockReader services.FullBlockReader, from, to uint64, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] NoGapsInBorEvents: done", "err", err)
	}()

	var cc *chain.Config

	if db == nil {
		genesis := core.BorMainnetGenesisBlock()
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

	var prevEventId uint64 = 1
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

				borHeimdallProgress, err := stages.GetStageProgress(tx, stages.BorHeimdall)
				if err != nil {
					return err
				}

				polygonSyncProgress, err := stages.GetStageProgress(tx, stages.PolygonSync)
				if err != nil {
					return err
				}

				// bor heimdall and polygon sync are mutually exclusive, bor heimdall will be removed soon
				polygonSyncProgress = max(borHeimdallProgress, polygonSyncProgress)

				bodyProgress, err := stages.GetStageProgress(tx, stages.Bodies)
				if err != nil {
					return err
				}

				log.Info("[integrity] LAST Event", "event", lastEventId, "bor-progress", polygonSyncProgress, "body-progress", bodyProgress)

				if bodyProgress > borHeimdallProgress {
					for blockNum := maxBlockNum + 1; blockNum <= bodyProgress; blockNum++ {

					}
				}
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
