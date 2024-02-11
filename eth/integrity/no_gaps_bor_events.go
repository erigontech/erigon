package integrity

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

func NoGapsInBorEvents(ctx context.Context, blockReader services.FullBlockReader) error {
	defer log.Info("[integrity] NoGapsInBorEvents: done")
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	snapshots := blockReader.BorSnapshots().(*freezeblocks.BorRoSnapshots)

	var first bool = true
	var prevEventId uint64

	for _, eventSegment := range snapshots.View().Events() {
		g := eventSegment.Decompressor.MakeGetter()

		var blockNumBuf [length.BlockNum]byte
		word := make([]byte, 0, 4096)

		for g.HasNext() {
			word, _ = g.Next(word[:0])
			if first || !bytes.Equal(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum]) {
				copy(blockNumBuf[:], word[length.Hash:length.Hash+length.BlockNum])
			}

			eventId := binary.BigEndian.Uint64(word[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])

			if eventId != prevEventId+1 {
				return fmt.Errorf("missing bor event %d at block=%d", eventId, binary.BigEndian.Uint64(word[length.Hash:length.Hash+length.BlockNum]))
			}

			prevEventId = eventId

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}

	return nil
}
