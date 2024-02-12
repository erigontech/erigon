package integrity

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

func NoGapsInBorEvents(ctx context.Context, blockReader services.FullBlockReader, from, to uint64) error {
	defer log.Info("[integrity] NoGapsInBorEvents: done")
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	snapshots := blockReader.BorSnapshots().(*freezeblocks.BorRoSnapshots)

	var prevEventId, prevBlock uint64
	var maxBlockNum uint64

	if to > 0 {
		maxBlockNum = to
	} else {
		maxBlockNum = snapshots.SegmentsMax()
	}

	for _, eventSegment := range snapshots.View().Events() {

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
