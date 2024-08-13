package integrity

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

func checkBlockEvents(ctx context.Context, config *borcfg.BorConfig, blockReader services.FullBlockReader,
	block uint64, prevBlock uint64, eventId uint64, prevBlockStartId uint64, prevEventTime *time.Time, tx kv.Tx, failFast bool) (*time.Time, error) {
	header, err := blockReader.HeaderByNumber(ctx, tx, prevBlock)

	if err != nil {
		if failFast {
			return nil, fmt.Errorf("can't get header for block %d: %w", block, err)
		}

		log.Error("[integrity] NoGapsInBorEvents: can't get header for block", "block", block, "err", err)
	}

	events, err := blockReader.EventsByBlock(ctx, tx, header.Hash(), header.Number.Uint64())

	if err != nil {
		if failFast {
			return nil, fmt.Errorf("can't get events for block %d: %w", block, err)
		}

		log.Error("[integrity] NoGapsInBorEvents: can't get events for block", "block", block, "err", err)
	}

	if prevBlockStartId != 0 {
		if len(events) != int(eventId-prevBlockStartId) {
			if failFast {
				return nil, fmt.Errorf("block event mismatch at %d: expected: %d, got: %d", block, eventId-prevBlockStartId, len(events))
			}

			log.Error("[integrity] NoGapsInBorEvents: block event count mismatch", "block", block, "eventId", eventId, "expected", eventId-prevBlockStartId, "got", len(events))
		}
	}

	var lastBlockEventTime time.Time

	for i, event := range events {

		var eventId uint64

		if prevBlockStartId != 0 {
			eventId = bor.EventId(event)

			if eventId != prevBlockStartId+uint64(i) {
				if failFast {
					return nil, fmt.Errorf("invalid event id %d for event %d in block %d: expected: %d", eventId, i, block, prevBlockStartId+uint64(i))
				}

				log.Error("[integrity] NoGapsInBorEvents: invalid event id", "block", block, "event", i, "expected", prevBlockStartId+uint64(i), "got", eventId)
			}
		} else {
			eventId = prevBlockStartId + uint64(i)
		}

		eventTime := bor.EventTime(event)

		//if i != 0 {
		//	if eventTime.Before(lastBlockEventTime) {
		//		eventTime = lastBlockEventTime
		//	}
		//}

		if i == 0 {
			lastBlockEventTime = eventTime
		}

		if prevEventTime != nil {
			if eventTime.Before(*prevEventTime) {
				log.Warn("[integrity] NoGapsInBorEvents: event time before prev", "block", block, "event", eventId, "time", eventTime, "prev", *prevEventTime, "diff", -prevEventTime.Sub(eventTime))
			}
		}

		prevEventTime = &eventTime

		if !checkBlockWindow(ctx, eventTime, config, header, tx, blockReader) {
			from, to, _ := bor.CalculateEventWIndow(ctx, config, header, tx, blockReader)

			var diff time.Duration

			if eventTime.Before(from) {
				diff = -from.Sub(eventTime)
			} else if eventTime.After(to) {
				diff = to.Sub(eventTime)
			}

			if failFast {
				return nil, fmt.Errorf("invalid time %s for event %d in block %d: expected %s-%s", eventTime, eventId, block, from, to)
			}

			log.Error(fmt.Sprintf("[integrity] NoGapsInBorEvents: invalid event time at %d of %d", i, len(events)), "block", block, "event", eventId, "time", eventTime, "diff", diff, "expected", fmt.Sprintf("%s-%s", from, to), "block-start", prevBlockStartId, "first-time", lastBlockEventTime, "timestamps", fmt.Sprintf("%d-%d", from.Unix(), to.Unix()))
		}
	}

	return prevEventTime, nil
}

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

	snapshots := blockReader.BorSnapshots().(*freezeblocks.BorRoSnapshots)

	var prevEventId, prevBlock, prevBlockStartId uint64
	var maxBlockNum uint64
	var prevEventTime *time.Time

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

		g := eventSegment.Decompressor.MakeGetter()

		word := make([]byte, 0, 4096)

		for g.HasNext() {
			word, _ = g.Next(word[:0])

			block := binary.BigEndian.Uint64(word[length.Hash : length.Hash+length.BlockNum])
			eventId := binary.BigEndian.Uint64(word[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
			event := word[length.Hash+length.BlockNum+8:]

			recordId := bor.EventId(event)

			if recordId != eventId {
				if failFast {
					return fmt.Errorf("invalid event id %d in block %d: expected: %d", recordId, block, eventId)
				}

				log.Error("[integrity] NoGapsInBorEvents: invalid event id", "block", block, "event", recordId, "expected", eventId)
			}

			if prevEventId > 0 {
				switch {
				case eventId < prevEventId:
					if failFast {
						return fmt.Errorf("invaid bor event %d (prev=%d) at block=%d", eventId, prevEventId, block)
					}

					log.Error("[integrity] NoGapsInBorEvents: invalid bor event", "event", eventId, "prev", prevEventId, "block", block)

				case eventId != prevEventId+1:
					if failFast {
						return fmt.Errorf("missing bor event %d (prev=%d) at block=%d", eventId, prevEventId, block)
					}

					log.Error("[integrity] NoGapsInBorEvents: missing bor event", "event", eventId, "prev", prevEventId, "block", block)
				}
			}

			if prevEventId == 0 {
				log.Info("[integrity] checking bor events", "event", eventId, "block", block)
			}

			if prevBlock != 0 && prevBlock != block {
				var err error

				if db != nil {
					err = db.View(ctx, func(tx kv.Tx) error {
						prevEventTime, err = checkBlockEvents(ctx, config, blockReader, block, prevBlock, eventId, prevBlockStartId, prevEventTime, tx, failFast)
						return err
					})
				} else {
					prevEventTime, err = checkBlockEvents(ctx, config, blockReader, block, prevBlock, eventId, prevBlockStartId, prevEventTime, nil, failFast)
				}

				if err != nil {
					return err
				}

				prevBlockStartId = eventId
			}

			prevEventId = eventId
			prevBlock = block

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info("[integrity] NoGapsInBorEvents", "blockNum", fmt.Sprintf("%dK/%dK", binary.BigEndian.Uint64(word[length.Hash:length.Hash+length.BlockNum])/1000, maxBlockNum/1000))
			default:
			}
		}
	}

	if db != nil {
		err = db.View(ctx, func(tx kv.Tx) error {
			lastEventId, _, err := blockReader.LastEventId(ctx, tx)

			if err != nil {
				return err
			}

			borHeimdallProgress, err := stages.GetStageProgress(tx, stages.BorHeimdall)

			if err != nil {
				return err
			}

			bodyProgress, err := stages.GetStageProgress(tx, stages.Bodies)

			if err != nil {
				return err
			}

			fmt.Println("LAST Event", lastEventId, "BH", borHeimdallProgress, "B", bodyProgress)

			for blockNum := maxBlockNum + 1; blockNum <= bodyProgress; blockNum++ {

			}

			return nil
		})

		if err != nil {
			return err
		}
	}

	log.Info("[integrity] done checking bor events", "event", prevEventId, "block", prevBlock)

	return nil
}

func checkBlockWindow(ctx context.Context, eventTime time.Time, config *borcfg.BorConfig, header *types.Header, tx kv.Getter, headerReader services.HeaderReader) bool {
	from, to, err := bor.CalculateEventWIndow(ctx, config, header, tx, headerReader)

	if err != nil {
		return false
	}

	return !(eventTime.Before(from) || eventTime.After(to))
}
