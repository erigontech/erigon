package integrity

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

func NoGapsInBorEvents(ctx context.Context, db kv.RoDB, blockReader services.FullBlockReader, from, to uint64, failFast bool) (err error) {
	defer log.Info("[integrity] NoGapsInBorEvents: done", "err", err)

	var cc *chain.Config

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

			eventId := binary.BigEndian.Uint64(word[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
			block := binary.BigEndian.Uint64(word[length.Hash : length.Hash+length.BlockNum])

			if prevEventId > 0 && eventId != prevEventId+1 {
				if failFast {
					return fmt.Errorf("missing bor event %d at block=%d", eventId, block)
				}

				log.Error("[integrity] NoGapsInBorEvents: missing bor event", "event", eventId, "block", block)
			}

			if prevEventId == 0 {
				log.Info("[integrity] checking bor events", "event", eventId, "block", block)
			}

			if prevBlock != 0 && prevBlock != block {
				err := db.View(ctx, func(tx kv.Tx) error {
					header, err := blockReader.HeaderByNumber(ctx, tx, prevBlock)

					if err != nil {
						if failFast {
							return fmt.Errorf("can't get header for block %d: %w", block, err)
						}

						log.Error("[integrity] NoGapsInBorEvents: can't get header for block", "block", block, "err", err)
					}

					events, err := blockReader.EventsByBlock(ctx, tx, header.Hash(), header.Number.Uint64())

					if err != nil {
						if failFast {
							return fmt.Errorf("can't get events for block %d: %w", block, err)
						}

						log.Error("[integrity] NoGapsInBorEvents: can't get events for block", "block", block, "err", err)
					}

					if prevBlockStartId != 0 {
						if len(events) != int(eventId-prevBlockStartId) {
							if failFast {
								return fmt.Errorf("block event mismatch at %d: expected: %d, got: %d", block, eventId-prevBlockStartId, len(events))
							}

							log.Error("[integrity] NoGapsInBorEvents: block event mismatch", "block", block, "expected", eventId-prevBlockStartId, "got", len(events))
						}
					}

					for i, event := range events {
						if t := bor.EventTime(event); !checkBlockWindow(ctx, t, config, header, tx, blockReader) {
							from, to, _ := bor.CalculateEventWIndow(ctx, config, header, tx, blockReader)
							if failFast {
								return fmt.Errorf("invalid time %s for event %d in block %d: expected %s-%s", t, prevBlockStartId+uint64(i), block, from, to)
							}

							log.Error("[integrity] NoGapsInBorEvents: invalid event time", "block", block, "event", prevBlockStartId+uint64(i), "time", t, "expected", fmt.Sprintf("%s-%s", from, to))
						}
					}

					return nil
				})

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
