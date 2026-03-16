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

const MaxBlockNum = ^uint64(0)
