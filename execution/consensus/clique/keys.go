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

package clique

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common"
)

// SnapshotFullKey = SnapshotBucket + num (uint64 big endian) + hash
func SnapshotFullKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// SnapshotKey = SnapshotBucket + num (uint64 big endian)
func SnapshotKey(number uint64) []byte {
	return EncodeBlockNumber(number)
}

// SnapshotKey = SnapshotBucket + '0'
func LastSnapshotKey() []byte {
	return []byte{0}
}

const NumberLength = 8

// EncodeBlockNumber encodes a block number as big endian uint64
func EncodeBlockNumber(number uint64) []byte {
	enc := make([]byte, NumberLength)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}
