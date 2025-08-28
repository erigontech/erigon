package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	polychain "github.com/erigontech/erigon/polygon/chain"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func ValidateBorEvents(ctx context.Context, db kv.TemporalRoDB, blockReader blockReader, snapshots *heimdall.RoSnapshots, from, to uint64, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] BorEvents: done", "err", err)
	}()

	var cc *chain.Config

	if db == nil {
		genesis := polychain.BorMainnetGenesisBlock()
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

		prevEventId, err = ValidateEvents(ctx, config, db, blockReader, snapshots, eventSegment, prevEventId, maxBlockNum, failFast, logEvery)

		if err != nil && failFast {
			return err
		}
	}

	if db != nil {
		err = db.View(ctx, func(tx kv.Tx) error {
			if false {
				lastEventId, err := NewSnapshotStore(NewTxStore(tx), snapshots, nil).LastEventId(ctx)
				if err != nil {
					return err
				}

				bodyProgress, err := stages.GetStageProgress(tx, stages.Bodies)
				if err != nil {
					return err
				}

				log.Info("[integrity] LAST Event", "event", lastEventId, "body-progress", bodyProgress)
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
