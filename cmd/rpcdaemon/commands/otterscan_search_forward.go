package commands

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// Given a ChunkLocator, moves forward over the chunks and inside each chunk, moves
// forward over the block numbers.
func NewForwardBlockProvider(chunkLocator ChunkLocator, minBlock uint64) BlockProvider {
	var iter roaring64.IntPeekable64
	var chunkProvider ChunkProvider
	isFirst := true
	finished := false

	return func() (uint64, bool, error) {
		if finished {
			return 0, false, nil
		}

		if isFirst {
			isFirst = false

			// Try to get first chunk
			var ok bool
			var err error
			chunkProvider, ok, err = chunkLocator(minBlock)
			if err != nil {
				finished = true
				return 0, false, err
			}
			if !ok {
				finished = true
				return 0, false, nil
			}
			if chunkProvider == nil {
				finished = true
				return 0, false, nil
			}

			// Has at least the first chunk; initialize the iterator
			chunk, ok, err := chunkProvider()
			if err != nil {
				finished = true
				return 0, false, err
			}
			if !ok {
				finished = true
				return 0, false, nil
			}

			bm := roaring64.NewBitmap()
			if _, err := bm.ReadFrom(bytes.NewReader(chunk)); err != nil {
				finished = true
				return 0, false, err
			}
			iter = bm.Iterator()

			// It can happen that on the first chunk we'll get a chunk that contains
			// the first block >= minBlock in the middle of the chunk/bitmap, so we
			// skip all previous blocks before it.
			iter.AdvanceIfNeeded(minBlock)

			// This means it is the last chunk and the min block is > the last one
			if !iter.HasNext() {
				finished = true
				return 0, false, nil
			}
		}

		nextBlock := iter.Next()
		hasNext := iter.HasNext()
		if !hasNext {
			iter = nil

			// Check if there is another chunk to get blocks from
			chunk, ok, err := chunkProvider()
			if err != nil {
				finished = true
				return 0, false, err
			}
			if !ok {
				finished = true
				return nextBlock, false, nil
			}

			hasNext = true

			bm := roaring64.NewBitmap()
			if _, err := bm.ReadFrom(bytes.NewReader(chunk)); err != nil {
				finished = true
				return 0, false, err
			}
			iter = bm.Iterator()
		}

		return nextBlock, hasNext, nil
	}
}

func NewCallCursorForwardBlockProvider(cursor kv.Cursor, addr common.Address, minBlock uint64) BlockProvider {
	chunkLocator := newCallChunkLocator(cursor, addr, true)
	return NewForwardBlockProvider(chunkLocator, minBlock)
}
