package commands

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

// Given a ChunkLocator, moves back over the chunks and inside each chunk, moves
// backwards over the block numbers.
func NewBackwardBlockProvider(chunkLocator ChunkLocator, maxBlock uint64) BlockProvider {
	// block == 0 means no max
	if maxBlock == 0 {
		maxBlock = MaxBlockNum
	}
	var iter roaring64.IntIterable64
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
			chunkProvider, ok, err = chunkLocator(maxBlock)
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

			// It can happen that on the first chunk we'll get a chunk that contains
			// the last block <= maxBlock in the middle of the chunk/bitmap, so we
			// remove all blocks after it (since there is no AdvanceIfNeeded() in
			// IntIterable64)
			if maxBlock != MaxBlockNum {
				bm.RemoveRange(maxBlock+1, MaxBlockNum)
			}
			iter = bm.ReverseIterator()

			// This means it is the last chunk and the min block is > the last one
			if !iter.HasNext() {
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

				iter = bm.ReverseIterator()
			}
		}

		nextBlock := iter.Next()
		hasNext := iter.HasNext()
		if !hasNext {
			iter = nil

			// Check if there is another chunk to get blocks from
			chunk, ok, err := chunkProvider()
			if err != nil {
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
			iter = bm.ReverseIterator()
		}

		return nextBlock, hasNext, nil
	}
}

func NewCallCursorBackwardBlockProvider(cursor kv.Cursor, addr common.Address, maxBlock uint64) BlockProvider {
	chunkLocator := newCallChunkLocator(cursor, addr, false)
	return NewBackwardBlockProvider(chunkLocator, maxBlock)
}
