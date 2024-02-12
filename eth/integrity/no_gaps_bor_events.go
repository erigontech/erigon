package integrity

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

func NoGapsInBorEvents(ctx context.Context, db kv.RoDB, blockReader services.FullBlockReader, from, to uint64) error {
	defer log.Info("[integrity] NoGapsInBorEvents: done")
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
				return fmt.Errorf("missing bor event %d at block=%d", eventId, block)
			}

			if prevEventId == 0 {
				log.Info("[integrity] checking bor events", "event", eventId, "block", block)
			}

			if prevBlock != 0 && prevBlock != block {
				err := db.View(ctx, func(tx kv.Tx) error {
					header, err := blockReader.HeaderByNumber(ctx, tx, prevBlock)

					if err != nil {
						return fmt.Errorf("can't get header for block %d: %w", block, err)
					}

					events, err := blockReader.EventsByBlock(ctx, tx, header.Hash(), header.Number.Uint64())

					if err != nil {
						return fmt.Errorf("can't get events for block %d: %w", block, err)
					}

					if prevBlockStartId != 0 {
						if len(events) != int(eventId-prevBlockStartId) {
							return fmt.Errorf("block event mismatch at %d: expected: %d, got: %d", block, eventId-prevBlockStartId, len(events))
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
