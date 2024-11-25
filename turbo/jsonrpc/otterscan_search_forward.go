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

package jsonrpc

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/kv"
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
