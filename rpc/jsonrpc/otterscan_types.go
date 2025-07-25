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
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
)

// Bootstrap a function able to locate a series of byte chunks containing
// related block numbers, starting from a specific block number (greater or equal than).
type ChunkLocator func(block uint64) (chunkProvider ChunkProvider, ok bool, err error)

// Allows to iterate over a set of byte chunks.
//
// If err is not nil, it indicates an error and the other returned values should be
// ignored.
//
// If err is nil and ok is true, the returned chunk should contain the raw chunk data.
//
// If err is nil and ok is false, it indicates that there is no more data. Subsequent calls
// to the same function should return (nil, false, nil).
type ChunkProvider func() (chunk []byte, ok bool, err error)

type BlockProvider func() (nextBlock uint64, hasMore bool, err error)

// Standard key format for call from/to indexes [address + block]
func callIndexKey(addr common.Address, block uint64) []byte {
	key := make([]byte, length.Addr+8)
	copy(key[:length.Addr], addr.Bytes())
	binary.BigEndian.PutUint64(key[length.Addr:], block)
	return key
}

const MaxBlockNum = ^uint64(0)

// This ChunkLocator searches over a cursor with a key format of [common.Address, block uint64],
// where block is the first block number contained in the chunk value.
//
// It positions the cursor on the chunk that contains the first block >= minBlock.
func newCallChunkLocator(cursor kv.Cursor, addr common.Address, navigateForward bool) ChunkLocator {
	return func(minBlock uint64) (ChunkProvider, bool, error) {
		searchKey := callIndexKey(addr, minBlock)
		k, _, err := cursor.Seek(searchKey)
		if k == nil {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		return newCallChunkProvider(cursor, addr, navigateForward), true, nil
	}
}

// This ChunkProvider is built by NewForwardChunkLocator and advances the cursor forward until
// there is no more chunks for the desired addr.
func newCallChunkProvider(cursor kv.Cursor, addr common.Address, navigateForward bool) ChunkProvider {
	first := true
	var err error
	// TODO: is this flag really used?
	eof := false
	return func() ([]byte, bool, error) {
		if err != nil {
			return nil, false, err
		}
		if eof {
			return nil, false, nil
		}

		var k, v []byte
		if first {
			first = false
			k, v, err = cursor.Current()
		} else {
			if navigateForward {
				k, v, err = cursor.Next()
			} else {
				k, v, err = cursor.Prev()
			}
		}

		if err != nil {
			eof = true
			return nil, false, err
		}
		if !bytes.HasPrefix(k, addr.Bytes()) {
			eof = true
			return nil, false, nil
		}
		return v, true, nil
	}
}
